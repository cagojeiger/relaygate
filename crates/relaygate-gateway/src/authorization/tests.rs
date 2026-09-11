use std::{io, sync::Arc, time::Duration};

use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use relaygate_protocol::{BearerToken, ErrorCode};
use relaygate_token_issuer::{Action as IssuerAction, TokenIssuer};
use serde_json::{Value, json};
use tokio::time::Instant;

use super::*;
use crate::test_support::{
    TEST_AUDIENCE, TEST_ISSUER, TEST_JWK_X, TEST_JWK_Y, TEST_KID, TestAction, authorization_config,
    bearer_token, destination, signed_bearer_token, signed_bearer_token_with_header,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;
const MAX_TEST_PERMISSIONS: usize = 128;
const TEST_PRIVATE_KEY_PEM: &[u8] = b"-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgWTFfCGljY6aw3Hrt\nkHmPRiazukxPLb6ilpRAewjW8nihRANCAATDskChT+Altkm9X7MI69T3IUmrQU0L\n950IxEzvw/x5BMEINRMrXLBJhqzO9Bm+d6JbqA21YQmd1Kt4RzLJR1W+\n-----END PRIVATE KEY-----\n";

fn operation(action: Action, destination: relaygate_protocol::Destination) -> ControlOperation {
    match action {
        Action::Publish => ControlOperation::Publish {
            request_id: 1,
            destination,
        },
        Action::Dial => ControlOperation::Dial {
            connection_id: 1,
            destination,
            started_at: std::time::Instant::now(),
        },
    }
}

fn claims(issuer: &str, audience: Value, nbf: u64, exp: u64, permissions: Vec<Value>) -> Value {
    json!({
        "iss": issuer,
        "aud": audience,
        "nbf": nbf,
        "exp": exp,
        "permissions": permissions,
    })
}

fn exact_permission(action: &str, namespace: &str, destination: &str) -> Value {
    json!({
        "action": action,
        "namespace": namespace,
        "scope": { "kind": "exact", "name": destination },
    })
}

fn verify(token: &BearerToken, operation: &ControlOperation) -> Result<(), ErrorCode> {
    verify_with(authorization_config(), token, operation)
}

fn verify_with(
    config: AuthorizationConfig,
    token: &BearerToken,
    operation: &ControlOperation,
) -> Result<(), ErrorCode> {
    Verifier { config }.verify(token, operation).map(|_| ())
}

#[test]
fn authorization_failure_preserves_operation_correlation_and_observation() {
    let destination = destination("worker");
    let publish = ControlOperation::Publish {
        request_id: 41,
        destination: destination.clone(),
    };
    assert_eq!(
        publish.failure(ErrorCode::Unauthenticated),
        Frame::PublishFailed {
            request_id: 41,
            code: ErrorCode::Unauthenticated,
            message: "operation authorization failed".to_owned(),
        }
    );

    let dial = ControlOperation::Dial {
        connection_id: 42,
        destination,
        started_at: std::time::Instant::now(),
    };
    assert_eq!(
        dial.failure(ErrorCode::PermissionDenied),
        Frame::DialFailed {
            connection_id: 42,
            code: ErrorCode::PermissionDenied,
            observation: PeerObservation::NotObserved,
            message: "operation authorization failed".to_owned(),
        }
    );
}

#[test]
fn valid_exact_grants_authorize_only_the_claimed_action_and_destination() {
    let target = destination("worker.one");
    for (test_action, action) in [
        (TestAction::Publish, Action::Publish),
        (TestAction::Dial, Action::Dial),
    ] {
        let token = bearer_token(&target, test_action);
        assert!(verify(&token, &operation(action, target.clone())).is_ok());
        let other_action = match action {
            Action::Publish => Action::Dial,
            Action::Dial => Action::Publish,
        };
        assert_eq!(
            verify(&token, &operation(other_action, target.clone())),
            Err(ErrorCode::PermissionDenied)
        );
        assert_eq!(
            verify(&token, &operation(action, destination("worker.two"))),
            Err(ErrorCode::PermissionDenied)
        );
    }
}

#[test]
fn token_issuer_exact_grants_are_accepted_by_gateway_verifier() -> TestResult {
    let target = destination("worker.issued");
    let issuer =
        TokenIssuer::from_es256_pem(TEST_ISSUER, TEST_AUDIENCE, TEST_KID, TEST_PRIVATE_KEY_PEM)?;
    let token = BearerToken::new(
        issuer
            .issue_exact(IssuerAction::Dial, &target, Duration::from_secs(300))?
            .into_string(),
    )?;

    assert!(verify(&token, &operation(Action::Dial, target)).is_ok());
    Ok(())
}

#[test]
fn jwt_profile_type_accepts_standard_media_type_equivalents() {
    let target = destination("worker");
    let operation = operation(Action::Dial, target.clone());
    let now = jsonwebtoken::get_current_timestamp();
    let token_claims = claims(
        TEST_ISSUER,
        json!(TEST_AUDIENCE),
        now.saturating_sub(1),
        now.saturating_add(300),
        vec![exact_permission("dial", "test", "worker")],
    );

    for token_type in [
        TOKEN_TYPE,
        "RELAYGATE-OPERATION+JWT",
        "application/relaygate-operation+jwt",
        "APPLICATION/RELAYGATE-OPERATION+JWT",
    ] {
        let mut header = Header::new(Algorithm::ES256);
        header.typ = Some(token_type.to_owned());
        header.kid = Some(TEST_KID.to_owned());
        let token = signed_bearer_token_with_header(header, &token_claims);
        assert!(verify(&token, &operation).is_ok(), "typ={token_type}");
    }
}

#[test]
fn missing_or_wrong_profile_type_fails_closed() {
    let target = destination("worker");
    let operation = operation(Action::Dial, target.clone());
    let now = jsonwebtoken::get_current_timestamp();
    let token_claims = claims(
        TEST_ISSUER,
        json!(TEST_AUDIENCE),
        now.saturating_sub(1),
        now.saturating_add(300),
        vec![exact_permission("dial", "test", "worker")],
    );

    for token_type in [None, Some("JWT"), Some("another-profile+jwt")] {
        let mut header = Header::new(Algorithm::ES256);
        header.typ = token_type.map(str::to_owned);
        header.kid = Some(TEST_KID.to_owned());
        let token = signed_bearer_token_with_header(header, &token_claims);
        assert_eq!(verify(&token, &operation), Err(ErrorCode::Unauthenticated));
    }
}

#[test]
fn unsupported_critical_header_and_missing_kid_fail_closed() {
    let target = destination("worker");
    let operation = operation(Action::Dial, target.clone());
    let now = jsonwebtoken::get_current_timestamp();
    let token_claims = claims(
        TEST_ISSUER,
        json!(TEST_AUDIENCE),
        now.saturating_sub(1),
        now.saturating_add(300),
        vec![exact_permission("dial", "test", "worker")],
    );

    let mut critical = Header::new(Algorithm::ES256);
    critical.typ = Some(TOKEN_TYPE.to_owned());
    critical.kid = Some(TEST_KID.to_owned());
    critical.crit = Some(vec!["relaygate-extension".to_owned()]);
    critical.extras.insert("relaygate-extension", true);

    let mut empty_critical = Header::new(Algorithm::ES256);
    empty_critical.typ = Some(TOKEN_TYPE.to_owned());
    empty_critical.kid = Some(TEST_KID.to_owned());
    empty_critical.crit = Some(Vec::new());

    let mut missing_kid = Header::new(Algorithm::ES256);
    missing_kid.typ = Some(TOKEN_TYPE.to_owned());

    for header in [critical, empty_critical, missing_kid] {
        let token = signed_bearer_token_with_header(header, &token_claims);
        assert_eq!(verify(&token, &operation), Err(ErrorCode::Unauthenticated));
    }
}

#[test]
fn unsupported_algorithm_and_closed_claim_shapes_fail_closed() -> TestResult {
    let target = destination("worker");
    let operation = operation(Action::Dial, target);
    let now = jsonwebtoken::get_current_timestamp();
    let valid = claims(
        TEST_ISSUER,
        json!(TEST_AUDIENCE),
        now.saturating_sub(1),
        now.saturating_add(300),
        vec![exact_permission("dial", "test", "worker")],
    );

    let mut wrong_algorithm = Header::new(Algorithm::HS256);
    wrong_algorithm.typ = Some(TOKEN_TYPE.to_owned());
    wrong_algorithm.kid = Some(TEST_KID.to_owned());
    let token = BearerToken::new(encode(
        &wrong_algorithm,
        &valid,
        &EncodingKey::from_secret(b"test-only-hmac-key"),
    )?)?;
    assert_eq!(verify(&token, &operation), Err(ErrorCode::Unauthenticated));

    let mut invalid_claims = vec![json!("not-a-claims-object")];
    for required in ["iss", "aud", "nbf", "exp"] {
        let mut candidate = valid.clone();
        let object = candidate
            .as_object_mut()
            .ok_or("test claims must be a JSON object")?;
        assert!(object.remove(required).is_some());
        invalid_claims.push(candidate);
    }

    let mut unknown_top_level = valid.clone();
    unknown_top_level
        .as_object_mut()
        .ok_or("test claims must be a JSON object")?
        .insert("unexpected".to_owned(), json!(true));
    invalid_claims.push(unknown_top_level);

    let mut unknown_permission = valid.clone();
    unknown_permission
        .pointer_mut("/permissions/0")
        .and_then(Value::as_object_mut)
        .ok_or("test permission must be a JSON object")?
        .insert("unexpected".to_owned(), json!(true));
    invalid_claims.push(unknown_permission);

    let mut unknown_scope = valid.clone();
    unknown_scope
        .pointer_mut("/permissions/0/scope")
        .and_then(Value::as_object_mut)
        .ok_or("test scope must be a JSON object")?
        .insert("unexpected".to_owned(), json!(true));
    invalid_claims.push(unknown_scope);

    let mut invalid_action = valid.clone();
    *invalid_action
        .pointer_mut("/permissions/0/action")
        .ok_or("test action must exist")? = json!("delete");
    invalid_claims.push(invalid_action);

    let mut invalid_scope_kind = valid.clone();
    *invalid_scope_kind
        .pointer_mut("/permissions/0/scope/kind")
        .ok_or("test scope kind must exist")? = json!("prefix");
    invalid_claims.push(invalid_scope_kind);

    for candidate in invalid_claims {
        let token = signed_bearer_token(TEST_KID, &candidate);
        assert_eq!(verify(&token, &operation), Err(ErrorCode::Unauthenticated));
    }

    let mut missing_permissions = valid;
    assert!(
        missing_permissions
            .as_object_mut()
            .ok_or("test claims must be a JSON object")?
            .remove("permissions")
            .is_some()
    );
    let token = signed_bearer_token(TEST_KID, &missing_permissions);
    assert_eq!(verify(&token, &operation), Err(ErrorCode::PermissionDenied));
    Ok(())
}

#[test]
fn subtree_and_all_scopes_use_whole_label_boundaries() {
    let now = jsonwebtoken::get_current_timestamp();
    for (scope, target, expected) in [
        (
            json!({ "kind": "subtree", "name": "worker" }),
            destination("worker"),
            true,
        ),
        (
            json!({ "kind": "subtree", "name": "worker" }),
            destination("worker.seoul"),
            true,
        ),
        (
            json!({ "kind": "subtree", "name": "worker" }),
            destination("workerx"),
            false,
        ),
        (json!({ "kind": "all" }), destination("anything"), true),
    ] {
        let token = signed_bearer_token(
            TEST_KID,
            &claims(
                TEST_ISSUER,
                json!(TEST_AUDIENCE),
                now.saturating_sub(1),
                now.saturating_add(300),
                vec![json!({
                    "action": "dial",
                    "namespace": "test",
                    "scope": scope,
                })],
            ),
        );
        let result = verify(&token, &operation(Action::Dial, target));
        assert_eq!(result.is_ok(), expected);
        if !expected {
            assert_eq!(result, Err(ErrorCode::PermissionDenied));
        }
    }
}

#[test]
fn issuer_audience_kid_and_time_fail_closed() -> TestResult {
    let target = destination("worker");
    let operation = operation(Action::Publish, target.clone());
    let now = jsonwebtoken::get_current_timestamp();
    let permission = exact_permission("publish", "test", "worker");
    let invalid = [
        signed_bearer_token(
            TEST_KID,
            &claims(
                "https://wrong.example",
                json!(TEST_AUDIENCE),
                now.saturating_sub(1),
                now.saturating_add(300),
                vec![permission.clone()],
            ),
        ),
        signed_bearer_token(
            TEST_KID,
            &claims(
                TEST_ISSUER,
                json!("wrong-audience"),
                now.saturating_sub(1),
                now.saturating_add(300),
                vec![permission.clone()],
            ),
        ),
        signed_bearer_token(
            "unknown-kid",
            &claims(
                TEST_ISSUER,
                json!(TEST_AUDIENCE),
                now.saturating_sub(1),
                now.saturating_add(300),
                vec![permission.clone()],
            ),
        ),
        signed_bearer_token(
            TEST_KID,
            &claims(
                TEST_ISSUER,
                json!(TEST_AUDIENCE),
                now.saturating_add(60),
                now.saturating_add(300),
                vec![permission.clone()],
            ),
        ),
        signed_bearer_token(
            TEST_KID,
            &claims(
                TEST_ISSUER,
                json!(TEST_AUDIENCE),
                now.saturating_sub(120),
                now.saturating_sub(60),
                vec![permission],
            ),
        ),
    ];
    for token in invalid {
        assert_eq!(verify(&token, &operation), Err(ErrorCode::Unauthenticated));
    }

    let valid = bearer_token(&target, TestAction::Publish);
    let mut tampered = valid.expose_secret().as_bytes().to_vec();
    let signature_start = tampered
        .iter()
        .rposition(|byte| *byte == b'.')
        .ok_or("signed token had no signature")?
        + 1;
    let byte = tampered
        .get_mut(signature_start)
        .ok_or("signed token had an empty signature")?;
    *byte = if *byte == b'a' { b'b' } else { b'a' };
    let tampered = BearerToken::new(String::from_utf8(tampered)?)?;
    assert_eq!(
        verify(&tampered, &operation),
        Err(ErrorCode::Unauthenticated)
    );
    Ok(())
}

#[test]
fn current_and_next_keys_are_accepted_without_crossing_namespace() -> TestResult {
    const NEXT_KID: &str = "next-key";
    let keys = vec![
        Es256PublicKey::new(TEST_KID, TEST_JWK_X, TEST_JWK_Y)?,
        Es256PublicKey::new(NEXT_KID, TEST_JWK_X, TEST_JWK_Y)?,
    ];
    let config = AuthorizationConfig::new(
        TEST_AUDIENCE,
        vec![
            TrustedIssuer::new("test".parse()?, TEST_ISSUER, keys.clone())?,
            TrustedIssuer::new("other".parse()?, TEST_ISSUER, keys)?,
        ],
    )?;
    let now = jsonwebtoken::get_current_timestamp();
    let token_claims = claims(
        TEST_ISSUER,
        json!(TEST_AUDIENCE),
        now.saturating_sub(1),
        now.saturating_add(300),
        vec![exact_permission("publish", "test", "worker")],
    );
    let target = operation(Action::Publish, destination("worker"));
    for kid in [TEST_KID, NEXT_KID] {
        assert!(
            verify_with(
                config.clone(),
                &signed_bearer_token(kid, &token_claims),
                &target,
            )
            .is_ok()
        );
    }
    assert_eq!(
        verify_with(
            config.clone(),
            &signed_bearer_token("unregistered-key", &token_claims),
            &target,
        ),
        Err(ErrorCode::Unauthenticated)
    );
    assert_eq!(
        verify_with(
            config,
            &signed_bearer_token(TEST_KID, &token_claims),
            &operation(Action::Publish, "other/worker".parse()?),
        ),
        Err(ErrorCode::PermissionDenied)
    );
    Ok(())
}

#[test]
fn malformed_or_oversized_permission_sets_fail_closed() -> TestResult {
    let operation = operation(Action::Dial, destination("worker"));
    let now = jsonwebtoken::get_current_timestamp();
    let token = signed_bearer_token(
        TEST_KID,
        &claims(
            TEST_ISSUER,
            json!([TEST_AUDIENCE]),
            now.saturating_sub(1),
            now.saturating_add(300),
            Vec::new(),
        ),
    );
    assert_eq!(verify(&token, &operation), Err(ErrorCode::PermissionDenied));

    // A JWT this large cannot cross the bounded wire token type. Exercise the
    // decoded-claims guard directly so the independent semantic limit remains
    // covered if the wire bound changes.
    let oversized: Claims = serde_json::from_value(claims(
        TEST_ISSUER,
        json!([TEST_AUDIENCE]),
        now.saturating_sub(1),
        now.saturating_add(300),
        (0..=MAX_TEST_PERMISSIONS)
            .map(|_| exact_permission("dial", "test", "worker"))
            .collect(),
    ))?;
    assert!(!oversized.authorizes(Action::Dial, operation.destination()));
    Ok(())
}

#[test]
fn trust_configuration_is_bounded_and_unambiguous() -> TestResult {
    assert!(Es256PublicKey::new("", TEST_JWK_X, TEST_JWK_Y).is_err());
    assert!(Es256PublicKey::new(TEST_KID, "invalid", TEST_JWK_Y).is_err());
    let key = Es256PublicKey::new(TEST_KID, TEST_JWK_X, TEST_JWK_Y)?;
    let namespace: relaygate_destination::Namespace = "test".parse()?;
    assert!(TrustedIssuer::new(namespace.clone(), TEST_ISSUER, Vec::new()).is_err());
    assert!(
        TrustedIssuer::new(
            namespace.clone(),
            TEST_ISSUER,
            vec![key.clone(), key.clone()]
        )
        .is_err()
    );
    let issuer = TrustedIssuer::new(namespace, TEST_ISSUER, vec![key])?;
    assert!(AuthorizationConfig::new("", vec![issuer.clone()]).is_err());
    assert!(AuthorizationConfig::new(TEST_AUDIENCE, vec![issuer.clone(), issuer]).is_err());
    let same_namespace_other_issuer = TrustedIssuer::new(
        "test".parse()?,
        "https://other-issuer.test",
        vec![Es256PublicKey::new(TEST_KID, TEST_JWK_X, TEST_JWK_Y)?],
    )?;
    assert!(
        AuthorizationConfig::new(
            TEST_AUDIENCE,
            vec![
                authorization_config().issuers[0].clone(),
                same_namespace_other_issuer
            ],
        )
        .is_err()
    );
    assert!(
        authorization_config()
            .with_clock_skew(Duration::from_secs(301))
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn verification_capacity_and_deadline_are_bounded() -> TestResult {
    let authorization = Authorization::new(authorization_config(), 1);
    let held = Arc::clone(&authorization.slots).try_acquire_owned()?;
    let destination = destination("worker");
    let operation = operation(Action::Dial, destination.clone());
    assert!(matches!(
        authorization.start(bearer_token(&destination, TestAction::Dial), &operation),
        Err(ErrorCode::ResourceExhausted)
    ));
    drop(held);

    let job = authorization
        .start(bearer_token(&destination, TestAction::Dial), &operation)
        .map_err(|code| io::Error::other(format!("verification did not start: {code:?}")))?;
    assert!(matches!(
        job.finish(Instant::now()).await,
        Err(ErrorCode::DeadlineExceeded)
    ));
    Ok(())
}
