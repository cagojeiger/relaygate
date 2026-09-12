use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use relaygate_gateway::{AuthorizationConfig, Es256PublicKey, TrustedIssuer};
use relaygate_sdk::{AccessAction, AccessToken, AccessTokenSource, Destination};
use serde::Serialize;

const AUDIENCE: &str = "relaygate";
const ISSUER: &str = "https://issuer.test";
const KID: &str = "test-key";
const TOKEN_TYPE: &str = "relaygate-operation+jwt";
const JWK_X: &str = "w7JAoU_gJbZJvV-zCOvU9yFJq0FNC_edCMRM78P8eQQ";
const JWK_Y: &str = "wQg1EytcsEmGrM70Gb53oluoDbVhCZ3Uq3hHMslHVb4";
const PRIVATE_KEY_DER: &[u8] = &[
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

#[derive(Serialize)]
struct Claims<'a> {
    iss: &'static str,
    aud: &'static str,
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
    Exact { name: &'a str },
}

pub(crate) fn authorization_config()
-> Result<AuthorizationConfig, Box<dyn std::error::Error + Send + Sync>> {
    let key = Es256PublicKey::new(KID, JWK_X, JWK_Y)?;
    let issuer = TrustedIssuer::new("test".parse()?, ISSUER, vec![key])?;
    Ok(AuthorizationConfig::new(AUDIENCE, vec![issuer])?)
}

pub(crate) fn destination(
    name: &str,
) -> Result<Destination, relaygate_destination::DestinationError> {
    format!("test/{name}").parse()
}

#[allow(dead_code)]
pub(crate) fn unique_destination() -> Result<Destination, relaygate_destination::DestinationError> {
    destination(&uuid::Uuid::new_v4().to_string())
}

pub(crate) fn token_source(
    destination: &Destination,
    action: AccessAction,
) -> Result<AccessTokenSource, Box<dyn std::error::Error + Send + Sync>> {
    Ok(access_token(destination, action)?.into())
}

pub(crate) fn access_token(
    destination: &Destination,
    action: AccessAction,
) -> Result<AccessToken, Box<dyn std::error::Error + Send + Sync>> {
    let now = jsonwebtoken::get_current_timestamp();
    let action = match action {
        AccessAction::Publish => "publish",
        AccessAction::Dial => "dial",
        _ => return Err("unsupported test access action".into()),
    };
    let claims = Claims {
        iss: ISSUER,
        aud: AUDIENCE,
        nbf: now.saturating_sub(1),
        exp: now.saturating_add(300),
        permissions: [Permission {
            action,
            namespace: destination.namespace().as_str(),
            scope: Scope::Exact {
                name: destination.name().as_str(),
            },
        }],
    };
    let mut header = Header::new(Algorithm::ES256);
    header.typ = Some(TOKEN_TYPE.to_owned());
    header.kid = Some(KID.to_owned());
    let encoded = encode(&header, &claims, &EncodingKey::from_ec_der(PRIVATE_KEY_DER))?;
    Ok(AccessToken::new(encoded)?)
}
