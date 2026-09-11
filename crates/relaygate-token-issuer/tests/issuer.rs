use std::time::{Duration, UNIX_EPOCH};

use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use relaygate_address::{DestinationName, NamespaceId, RouteAddress};
use relaygate_token_issuer::{Action, Permission, TokenIssuer, TokenIssuerError};
use serde_json::{Value, json};

const TEST_KID: &str = "issuer-key-v1";
const TEST_ISSUER: &str = "https://issuer.test";
const TEST_AUDIENCE: &str = "relaygate";
const TEST_PRIVATE_KEY_PEM: &[u8] = b"-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgWTFfCGljY6aw3Hrt\nkHmPRiazukxPLb6ilpRAewjW8nihRANCAATDskChT+Altkm9X7MI69T3IUmrQU0L\n950IxEzvw/x5BMEINRMrXLBJhqzO9Bm+d6JbqA21YQmd1Kt4RzLJR1W+\n-----END PRIVATE KEY-----\n";
const TEST_JWK_X: &str = "w7JAoU_gJbZJvV-zCOvU9yFJq0FNC_edCMRM78P8eQQ";
const TEST_JWK_Y: &str = "wQg1EytcsEmGrM70Gb53oluoDbVhCZ3Uq3hHMslHVb4";

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn issuer() -> TestResult<TokenIssuer> {
    Ok(TokenIssuer::from_es256_pem(
        TEST_ISSUER,
        TEST_AUDIENCE,
        TEST_KID,
        TEST_PRIVATE_KEY_PEM,
    )?)
}

fn decode_claims(token: &str) -> TestResult<Value> {
    let mut validation = Validation::new(Algorithm::ES256);
    validation.validate_exp = false;
    validation.validate_nbf = false;
    validation.set_required_spec_claims(&["iss", "aud", "nbf", "exp"]);
    validation.set_issuer(&[TEST_ISSUER]);
    validation.set_audience(&[TEST_AUDIENCE]);
    let data = decode::<Value>(
        token,
        &DecodingKey::from_ec_components(TEST_JWK_X, TEST_JWK_Y)?,
        &validation,
    )?;
    Ok(data.claims)
}

#[test]
fn issues_exact_publish_token_with_relaygate_profile() -> TestResult {
    let address: RouteAddress = "inference/stt.seoul".parse()?;
    let issued = issuer()?.issue_at(
        [Permission::exact(Action::Publish, &address)],
        UNIX_EPOCH + Duration::from_secs(1_000),
        Duration::from_secs(300),
    )?;

    let header = decode_header(issued.as_str())?;
    assert_eq!(header.alg, Algorithm::ES256);
    assert_eq!(header.kid.as_deref(), Some(TEST_KID));
    assert_eq!(
        header.typ.as_deref(),
        Some(relaygate_token_issuer::OPERATION_TOKEN_TYPE)
    );
    assert_eq!(issued.expires_at(), 1_300);
    assert!(!format!("{issued:?}").contains(issued.as_str()));

    let claims = decode_claims(issued.as_str())?;
    assert_eq!(
        claims,
        json!({
            "iss": TEST_ISSUER,
            "aud": TEST_AUDIENCE,
            "nbf": 1_000,
            "exp": 1_300,
            "permissions": [{
                "action": "publish",
                "namespace": "inference",
                "scope": { "kind": "exact", "destination": "stt.seoul" }
            }]
        })
    );
    Ok(())
}

#[test]
fn issues_all_supported_permission_scopes() -> TestResult {
    let namespace: NamespaceId = "inference".parse()?;
    let subtree: DestinationName = "stt".parse()?;
    let issued = issuer()?.issue_at(
        [
            Permission::subtree(Action::Dial, namespace.clone(), subtree),
            Permission::all(Action::Publish, namespace),
        ],
        UNIX_EPOCH + Duration::from_secs(2_000),
        Duration::from_secs(60),
    )?;
    let claims = decode_claims(issued.as_str())?;

    assert_eq!(
        claims["permissions"],
        json!([
            {
                "action": "dial",
                "namespace": "inference",
                "scope": { "kind": "subtree", "destination": "stt" }
            },
            {
                "action": "publish",
                "namespace": "inference",
                "scope": { "kind": "all" }
            }
        ])
    );
    Ok(())
}

#[test]
fn invalid_inputs_fail_before_or_during_issuance() -> TestResult {
    assert!(matches!(
        TokenIssuer::from_es256_pem("", TEST_AUDIENCE, TEST_KID, TEST_PRIVATE_KEY_PEM),
        Err(TokenIssuerError::InvalidText {
            field: "issuer",
            ..
        })
    ));
    assert!(matches!(
        TokenIssuer::from_es256_pem(TEST_ISSUER, TEST_AUDIENCE, TEST_KID, b"invalid"),
        Err(TokenIssuerError::InvalidPrivateKey)
    ));

    let issuer = issuer()?;
    let address: RouteAddress = "inference/stt.seoul".parse()?;
    assert!(matches!(
        issuer.issue_exact(Action::Dial, &address, Duration::ZERO),
        Err(TokenIssuerError::InvalidLifetime)
    ));
    assert!(matches!(
        issuer.issue_at(
            Vec::new(),
            UNIX_EPOCH + Duration::from_secs(1),
            Duration::from_secs(1)
        ),
        Err(TokenIssuerError::EmptyPermissions)
    ));
    assert!(matches!(
        issuer.issue_at(
            vec![Permission::exact(Action::Dial, &address); 129],
            UNIX_EPOCH + Duration::from_secs(1),
            Duration::from_secs(1)
        ),
        Err(TokenIssuerError::TooManyPermissions { .. })
    ));
    assert!(matches!(
        issuer.issue_at(
            vec![Permission::all(Action::Dial, "inference".parse()?); 128],
            UNIX_EPOCH + Duration::from_secs(1),
            Duration::from_secs(1)
        ),
        Err(TokenIssuerError::TokenTooLong { .. })
    ));
    Ok(())
}
