use std::env;

use anyhow::{Result, bail};

pub(crate) fn sdk_tls_enabled() -> Result<bool> {
    let mode = env::var("RELAYGATE_SDK_TRANSPORT")
        .map(Some)
        .or_else(|error| match error {
            env::VarError::NotPresent => Ok(None),
            other => Err(other),
        })?;
    parse_sdk_tls(mode.as_deref(), super::insecure_test_transport())
}

fn parse_sdk_tls(mode: Option<&str>, insecure_test: bool) -> Result<bool> {
    if mode.is_some() && insecure_test {
        bail!("RELAYGATE_SDK_TRANSPORT cannot be combined with legacy test transport flags");
    }
    match mode {
        None => Ok(!insecure_test),
        Some("tls") => Ok(true),
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
    let mode = env::var("RELAYGATE_INTERNAL_TRANSPORT")
        .map(Some)
        .or_else(|error| match error {
            env::VarError::NotPresent => Ok(None),
            other => Err(other),
        })?;
    parse_internal_transport(
        mode.as_deref(),
        super::insecure_test_transport(),
        env::var("RELAYGATE_RT_TRUSTED_LOCAL").ok().as_deref(),
    )
}

fn parse_internal_transport(
    mode: Option<&str>,
    insecure_test: bool,
    trusted_local: Option<&str>,
) -> Result<InternalTransport> {
    if let Some(mode) = mode {
        if insecure_test || trusted_local.is_some() {
            bail!(
                "RELAYGATE_INTERNAL_TRANSPORT cannot be combined with legacy test transport flags"
            );
        }
        return match mode {
            "mtls" => Ok(InternalTransport::Mtls),
            "plaintext" => Ok(InternalTransport::Plaintext),
            _ => bail!("RELAYGATE_INTERNAL_TRANSPORT must be `mtls` or `plaintext`"),
        };
    }
    if insecure_test {
        if trusted_local != Some("true") {
            bail!(
                "RELAYGATE_RT_TRUSTED_LOCAL must be `true` to enable the local/CI unauthenticated plain-TCP adapter"
            );
        }
        return Ok(InternalTransport::Plaintext);
    }
    if trusted_local.is_some() {
        bail!(
            "RELAYGATE_RT_TRUSTED_LOCAL is only valid with RELAYGATE_INSECURE_TEST_TRANSPORT=true"
        );
    }
    Ok(InternalTransport::Mtls)
}

#[cfg(test)]
mod tests {
    use super::{InternalTransport, parse_internal_transport};

    #[test]
    fn sdk_transport_is_secure_by_default() -> anyhow::Result<()> {
        assert!(super::parse_sdk_tls(None, false)?);
        assert!(super::parse_sdk_tls(Some("tls"), false)?);
        assert!(!super::parse_sdk_tls(Some("plaintext"), false)?);
        assert!(!super::parse_sdk_tls(None, true)?);
        for mode in ["", "tcp", "auto", "TLS"] {
            assert!(super::parse_sdk_tls(Some(mode), false).is_err());
        }
        assert!(super::parse_sdk_tls(Some("plaintext"), true).is_err());
        Ok(())
    }

    #[test]
    fn internal_transport_defaults_secure_and_plaintext_requires_explicit_choice()
    -> anyhow::Result<()> {
        assert_eq!(
            parse_internal_transport(None, false, None)?,
            InternalTransport::Mtls
        );
        assert_eq!(
            parse_internal_transport(Some("mtls"), false, None)?,
            InternalTransport::Mtls
        );
        assert_eq!(
            parse_internal_transport(Some("plaintext"), false, None)?,
            InternalTransport::Plaintext
        );
        for value in ["", "tcp", "MTLS", "false"] {
            assert!(parse_internal_transport(Some(value), false, None).is_err());
        }
        Ok(())
    }

    #[test]
    fn explicit_mode_rejects_ambiguous_test_flags() -> anyhow::Result<()> {
        for mode in ["mtls", "plaintext"] {
            assert!(parse_internal_transport(Some(mode), true, Some("true")).is_err());
            assert!(parse_internal_transport(Some(mode), false, Some("true")).is_err());
        }
        assert!(parse_internal_transport(None, true, None).is_err());
        assert!(parse_internal_transport(None, false, Some("true")).is_err());
        assert_eq!(
            parse_internal_transport(None, true, Some("true"))?,
            InternalTransport::Plaintext
        );
        Ok(())
    }

    #[test]
    fn trusted_local_adapter_requires_exact_opt_in() {
        for value in [None, Some("false"), Some("TRUE")] {
            assert!(parse_internal_transport(None, true, value).is_err());
        }
        assert!(parse_internal_transport(None, true, Some("true")).is_ok());
        assert!(parse_internal_transport(None, false, None).is_ok());
        assert!(parse_internal_transport(None, false, Some("true")).is_err());
    }
}
