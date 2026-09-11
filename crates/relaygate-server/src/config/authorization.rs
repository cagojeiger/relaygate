use std::{env, fs, io::Read, path::Path, time::Duration};

use anyhow::{Context, Result, bail};
use relaygate_gateway::{
    AuthorizationConfig, DEFAULT_AUTHORIZATION_CONCURRENCY, DEFAULT_AUTHORIZATION_TIMEOUT,
    Es256PublicKey, GatewayConfig, MAX_AUTHORIZATION_CONCURRENCY, MAX_AUTHORIZATION_TIMEOUT,
    TrustedIssuer,
};
use relaygate_route_table::Namespace;
use serde::Deserialize;

const CONFIG_ENV: &str = "RELAYGATE_AUTH_CONFIG_PATH";
const MAX_CONFIG_BYTES: usize = 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    version: u32,
    audience: String,
    #[serde(default = "default_clock_skew_seconds")]
    clock_skew_seconds: u64,
    issuers: Vec<IssuerDocument>,
    #[serde(default)]
    verification: VerificationDocument,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IssuerDocument {
    namespace: String,
    issuer: String,
    keys: Vec<JwkDocument>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JwkDocument {
    kid: String,
    kty: String,
    crv: String,
    alg: String,
    #[serde(rename = "use")]
    usage: String,
    x: String,
    y: String,
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct VerificationDocument {
    concurrency: usize,
    timeout_ms: u64,
}

impl Default for VerificationDocument {
    fn default() -> Self {
        Self {
            concurrency: DEFAULT_AUTHORIZATION_CONCURRENCY,
            timeout_ms: DEFAULT_AUTHORIZATION_TIMEOUT.as_millis() as u64,
        }
    }
}

fn default_clock_skew_seconds() -> u64 {
    30
}

pub(super) fn apply_from_env() -> Result<GatewayConfig> {
    let path = env::var(CONFIG_ENV).context("RELAYGATE_AUTH_CONFIG_PATH is required")?;
    load(Path::new(&path))
}

fn load(path: &Path) -> Result<GatewayConfig> {
    let metadata = fs::metadata(path).context("cannot inspect authorization config file")?;
    if !metadata.is_file() || metadata.len() > MAX_CONFIG_BYTES as u64 {
        bail!("authorization config must be a regular file of at most 1 MiB");
    }
    let file = fs::File::open(path).context("cannot open authorization config file")?;
    let mut bytes = Vec::new();
    file.take((MAX_CONFIG_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .context("cannot read authorization config file")?;
    parse(&bytes)
}

fn parse(bytes: &[u8]) -> Result<GatewayConfig> {
    if bytes.len() > MAX_CONFIG_BYTES {
        bail!("authorization config exceeds 1 MiB");
    }
    let document: Document = serde_json::from_slice(bytes)
        .map_err(|_| anyhow::anyhow!("authorization config is not valid version-1 JSON"))?;
    if document.version != 1 {
        bail!("authorization config version must be 1");
    }
    if !(1..=MAX_AUTHORIZATION_CONCURRENCY).contains(&document.verification.concurrency)
        || !(1..=MAX_AUTHORIZATION_TIMEOUT.as_millis() as u64)
            .contains(&document.verification.timeout_ms)
    {
        bail!("authorization verification limits are outside supported bounds");
    }
    let issuers = document
        .issuers
        .into_iter()
        .map(parse_issuer)
        .collect::<Result<Vec<_>>>()?;
    let authorization = AuthorizationConfig::new(document.audience, issuers)?
        .with_clock_skew(Duration::from_secs(document.clock_skew_seconds))?;
    Ok(GatewayConfig::new(authorization).with_authorization_limits(
        document.verification.concurrency,
        Duration::from_millis(document.verification.timeout_ms),
    ))
}

fn parse_issuer(document: IssuerDocument) -> Result<TrustedIssuer> {
    let namespace: Namespace = document
        .namespace
        .parse()
        .context("authorization issuer namespace is invalid")?;
    let keys = document
        .keys
        .into_iter()
        .map(|key| {
            if key.kty != "EC" || key.crv != "P-256" || key.alg != "ES256" || key.usage != "sig" {
                bail!("authorization keys must be EC P-256 ES256 signing JWKs");
            }
            Es256PublicKey::new(key.kid, key.x, key.y).map_err(Into::into)
        })
        .collect::<Result<Vec<_>>>()?;
    TrustedIssuer::new(namespace, document.issuer, keys).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::parse;

    const X: &str = "w7JAoU_gJbZJvV-zCOvU9yFJq0FNC_edCMRM78P8eQQ";
    const Y: &str = "wQg1EytcsEmGrM70Gb53oluoDbVhCZ3Uq3hHMslHVb4";

    fn document(extra: &str) -> Vec<u8> {
        format!(
            r#"{{"version":1,"audience":"relaygate","issuers":[{{"namespace":"alpha","issuer":"https://issuer.example","keys":[{{"kid":"current","kty":"EC","crv":"P-256","alg":"ES256","use":"sig","x":"{X}","y":"{Y}"}}]}}]{extra}}}"#,
        )
        .into_bytes()
    }

    #[test]
    fn parses_minimal_static_public_key_config() {
        assert!(parse(&document("")).is_ok());
    }

    #[test]
    fn rejects_unknown_fields_and_non_es256_keys() -> Result<(), Box<dyn std::error::Error>> {
        assert!(parse(&document(",\"private_key\":\"secret\"")).is_err());
        let mut invalid = document("");
        let text =
            String::from_utf8(std::mem::take(&mut invalid))?.replace("\"ES256\"", "\"RS256\"");
        assert!(parse(text.as_bytes()).is_err());
        Ok(())
    }

    #[test]
    fn rejects_unbounded_verification_settings() {
        assert!(
            parse(&document(
                ",\"verification\":{\"concurrency\":0,\"timeout_ms\":1}"
            ))
            .is_err()
        );
        assert!(parse(&document(",\"clock_skew_seconds\":301")).is_err());
    }
}
