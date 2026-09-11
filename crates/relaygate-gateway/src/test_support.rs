use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use relaygate_protocol::{BearerToken, RouteAddress};
use serde::Serialize;

use crate::{AuthorizationConfig, Es256PublicKey, TrustedIssuer, authorization::TOKEN_TYPE};

pub(crate) const TEST_JWK_X: &str = "w7JAoU_gJbZJvV-zCOvU9yFJq0FNC_edCMRM78P8eQQ";
pub(crate) const TEST_JWK_Y: &str = "wQg1EytcsEmGrM70Gb53oluoDbVhCZ3Uq3hHMslHVb4";
const TEST_PRIVATE_KEY_DER: &[u8] = &[
    0x30, 0x81, 0x87, 0x02, 0x01, 0x00, 0x30, 0x13, 0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02,
    0x01, 0x06, 0x08, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07, 0x04, 0x6d, 0x30, 0x6b, 0x02,
    0x01, 0x01, 0x04, 0x20, 0x59, 0x31, 0x5f, 0x08, 0x69, 0x63, 0x63, 0xa6, 0xb0, 0xdc, 0x7a, 0xed,
    0x90, 0x79, 0x8f, 0x46, 0x26, 0xb3, 0xba, 0x4c, 0x4f, 0x2d, 0xbe, 0xa2, 0x96, 0x94, 0x40, 0x7b,
    0x08, 0xd6, 0xf2, 0x78, 0xa1, 0x44, 0x03, 0x42, 0x00, 0x04, 0xc3, 0xb2, 0x40, 0xa1, 0x4f, 0xe0,
    0x25, 0xb6, 0x49, 0xbd, 0x5f, 0xb3, 0x08, 0xeb, 0xd4, 0xf7, 0x21, 0x49, 0xab, 0x41, 0x4d, 0x0b,
    0xf7, 0x9d, 0x08, 0xc4, 0x4c, 0xef, 0xc3, 0xfc, 0x79, 0x04, 0xc1, 0x08, 0x35, 0x13, 0x2b, 0x5c,
    0xb0, 0x49, 0x86, 0xac, 0xce, 0xf4, 0x19, 0xbe, 0x77, 0xa2, 0x5b, 0xa8, 0x0d, 0xb5, 0x61, 0x09,
    0x9d, 0xd4, 0xab, 0x78, 0x47, 0x32, 0xc9, 0x47, 0x55, 0xbe,
];

pub(crate) const TEST_AUDIENCE: &str = "relaygate";
pub(crate) const TEST_ISSUER: &str = "https://issuer.test";
pub(crate) const TEST_KID: &str = "test-key";

#[derive(Debug, Clone, Copy)]
pub(crate) enum TestAction {
    Publish,
    Dial,
}

impl TestAction {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Publish => "publish",
            Self::Dial => "dial",
        }
    }
}

#[derive(Serialize)]
struct Claims<'a> {
    iss: &'a str,
    aud: &'a str,
    nbf: u64,
    exp: u64,
    permissions: [Permission<'a>; 1],
}

#[derive(Serialize)]
struct Permission<'a> {
    action: &'static str,
    namespace: &'a str,
    scope: Scope<'a>,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum Scope<'a> {
    Exact { destination: &'a str },
}

#[allow(clippy::expect_used)]
pub(crate) fn address(destination: &str) -> RouteAddress {
    format!("test/{destination}")
        .parse()
        .expect("valid test RouteAddress")
}

pub(crate) fn unique_address() -> RouteAddress {
    address(&uuid::Uuid::new_v4().to_string())
}

#[allow(clippy::expect_used)]
pub(crate) fn authorization_config() -> AuthorizationConfig {
    let key =
        Es256PublicKey::new(TEST_KID, TEST_JWK_X, TEST_JWK_Y).expect("static test JWK is valid");
    let issuer = TrustedIssuer::new(
        "test".parse().expect("static namespace is valid"),
        TEST_ISSUER,
        vec![key],
    )
    .expect("static trusted issuer is valid");
    AuthorizationConfig::new(TEST_AUDIENCE, vec![issuer])
        .expect("static authorization config is valid")
}

#[allow(clippy::expect_used)]
pub(crate) fn bearer_token(address: &RouteAddress, action: TestAction) -> BearerToken {
    let now = jsonwebtoken::get_current_timestamp();
    let claims = Claims {
        iss: TEST_ISSUER,
        aud: TEST_AUDIENCE,
        nbf: now.saturating_sub(1),
        exp: now.saturating_add(300),
        permissions: [Permission {
            action: action.as_str(),
            namespace: address.namespace().as_str(),
            scope: Scope::Exact {
                destination: address.destination().as_str(),
            },
        }],
    };
    signed_bearer_token(TEST_KID, &claims)
}

#[allow(clippy::expect_used)]
pub(crate) fn signed_bearer_token<T: Serialize>(kid: &str, claims: &T) -> BearerToken {
    let mut header = Header::new(Algorithm::ES256);
    header.typ = Some(TOKEN_TYPE.to_owned());
    header.kid = Some(kid.to_owned());
    signed_bearer_token_with_header(header, claims)
}

#[allow(clippy::expect_used)]
pub(crate) fn signed_bearer_token_with_header<T: Serialize>(
    header: Header,
    claims: &T,
) -> BearerToken {
    let key = EncodingKey::from_ec_der(TEST_PRIVATE_KEY_DER);
    let token = encode(&header, claims, &key).expect("test JWT signing succeeds");
    BearerToken::new(token).expect("signed test JWT is bounded")
}
