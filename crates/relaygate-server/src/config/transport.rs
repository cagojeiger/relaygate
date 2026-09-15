use anyhow::{Result, bail};

use super::optional_env;

/// Test-only switches removed in 0.4; the explicit modes replace them.
const REMOVED_FLAGS: [&str; 2] = [
    "RELAYGATE_INSECURE_TEST_TRANSPORT",
    "RELAYGATE_RT_TRUSTED_LOCAL",
];

pub(super) fn reject_removed_flags() -> Result<()> {
    for name in REMOVED_FLAGS {
        if std::env::var_os(name).is_some() {
            bail!(
                "{name} is no longer supported; set RELAYGATE_SDK_TRANSPORT and RELAYGATE_INTERNAL_TRANSPORT explicitly"
            );
        }
    }
    Ok(())
}

pub(crate) fn sdk_tls_enabled() -> Result<bool> {
    reject_removed_flags()?;
    parse_sdk_tls(optional_env("RELAYGATE_SDK_TRANSPORT")?.as_deref())
}

fn parse_sdk_tls(mode: Option<&str>) -> Result<bool> {
    match mode {
        None | Some("tls") => Ok(true),
        Some("plaintext") => Ok(false),
        _ => bail!("RELAYGATE_SDK_TRANSPORT must be `tls` or `plaintext`"),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum InternalTransport {
    Mtls,
    Plaintext,
}

pub(super) fn internal_transport() -> Result<InternalTransport> {
    reject_removed_flags()?;
    parse_internal_transport(optional_env("RELAYGATE_INTERNAL_TRANSPORT")?.as_deref())
}

fn parse_internal_transport(mode: Option<&str>) -> Result<InternalTransport> {
    match mode {
        None | Some("mtls") => Ok(InternalTransport::Mtls),
        Some("plaintext") => Ok(InternalTransport::Plaintext),
        _ => bail!("RELAYGATE_INTERNAL_TRANSPORT must be `mtls` or `plaintext`"),
    }
}

#[cfg(test)]
mod tests {
    use super::{InternalTransport, parse_internal_transport, parse_sdk_tls};

    #[test]
    fn sdk_transport_is_secure_by_default() -> anyhow::Result<()> {
        assert!(parse_sdk_tls(None)?);
        assert!(parse_sdk_tls(Some("tls"))?);
        assert!(!parse_sdk_tls(Some("plaintext"))?);
        for mode in ["", "tcp", "auto", "TLS"] {
            assert!(parse_sdk_tls(Some(mode)).is_err());
        }
        Ok(())
    }

    #[test]
    fn internal_transport_defaults_secure_and_plaintext_requires_explicit_choice()
    -> anyhow::Result<()> {
        assert_eq!(parse_internal_transport(None)?, InternalTransport::Mtls);
        assert_eq!(
            parse_internal_transport(Some("mtls"))?,
            InternalTransport::Mtls
        );
        assert_eq!(
            parse_internal_transport(Some("plaintext"))?,
            InternalTransport::Plaintext
        );
        for value in ["", "tcp", "MTLS", "false"] {
            assert!(parse_internal_transport(Some(value)).is_err());
        }
        Ok(())
    }
}
