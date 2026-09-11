use std::collections::HashSet;

use relaygate_address::{
    AddressError, DestinationName, MAX_DESTINATION_BYTES, MAX_LABEL_BYTES, NamespaceId,
    RouteAddress,
};

#[test]
fn canonical_names_round_trip_as_strings() -> Result<(), Box<dyn std::error::Error>> {
    let namespace = NamespaceId::new("inference")?;
    let destination = DestinationName::new("stt.seoul")?;
    let address = RouteAddress::new(namespace.clone(), destination.clone());

    assert_eq!(namespace.to_string(), "inference");
    assert_eq!(destination.to_string(), "stt.seoul");
    assert_eq!(address.to_string(), "inference/stt.seoul");
    assert_eq!("inference/stt.seoul".parse::<RouteAddress>()?, address);
    assert_eq!(serde_json::to_string(&address)?, r#""inference/stt.seoul""#);
    assert_eq!(
        serde_json::from_str::<RouteAddress>(r#""inference/stt.seoul""#)?,
        address
    );
    Ok(())
}

#[test]
fn rejects_noncanonical_names_and_selectors() {
    for invalid in [
        "",
        ".",
        ".stt",
        "stt.",
        "stt..seoul",
        "STT",
        "sTt",
        " stt",
        "stt ",
        "stt\n",
        "stt\0",
        "stt/seoul",
        "stt.*",
        "stt.>",
        "stt.#",
        "*",
        "stt._x",
        "stt.%61",
        "stt.서울",
        "-stt",
        "stt-",
        "stt.-seoul",
        "stt.seoul-",
    ] {
        assert!(
            DestinationName::new(invalid).is_err(),
            "accepted {invalid:?}"
        );
    }
    for invalid in ["", "stt.seoul", "*", "STT", "inference_1", "서울"] {
        assert!(NamespaceId::new(invalid).is_err(), "accepted {invalid:?}");
    }
    for invalid in [
        "stt.seoul",
        "/stt.seoul",
        "inference/",
        "inference/stt/seoul",
        "inference/stt.*",
        " inference/stt",
    ] {
        assert!(invalid.parse::<RouteAddress>().is_err());
    }
}

#[test]
fn enforces_dns_like_byte_limits() {
    let longest_label = "a".repeat(MAX_LABEL_BYTES);
    assert!(NamespaceId::new(&longest_label).is_ok());
    assert!(NamespaceId::new(&"a".repeat(MAX_LABEL_BYTES + 1)).is_err());
    assert!(DestinationName::new(&longest_label).is_ok());
    assert!(DestinationName::new(&"a".repeat(MAX_LABEL_BYTES + 1)).is_err());

    let maximum_destination = format!(
        "{}.{}.{}.{}",
        "a".repeat(63),
        "b".repeat(63),
        "c".repeat(63),
        "d".repeat(61)
    );
    assert_eq!(maximum_destination.len(), MAX_DESTINATION_BYTES);
    assert!(DestinationName::new(&maximum_destination).is_ok());
    assert_eq!(
        DestinationName::new(&(maximum_destination + "a")),
        Err(AddressError::DestinationLength)
    );
}

#[test]
fn descendant_relation_uses_whole_labels() -> Result<(), AddressError> {
    let prefix = DestinationName::new("stt.seoul")?;
    for value in ["stt.seoul.worker1", "stt.seoul.team.worker2"] {
        assert!(DestinationName::new(value)?.is_descendant_of(&prefix));
    }
    for value in [
        "stt.seoul",
        "stt",
        "stt.seoul-other",
        "stt.seoulish.a",
        "llm.seoul.a",
    ] {
        assert!(!DestinationName::new(value)?.is_descendant_of(&prefix));
    }
    Ok(())
}

#[test]
fn canonical_keys_separate_namespace_and_destination() -> Result<(), AddressError> {
    let first = RouteAddress::new(NamespaceId::new("ab")?, DestinationName::new("c")?);
    let second = RouteAddress::new(NamespaceId::new("a")?, DestinationName::new("bc")?);

    assert_ne!(first, second);
    assert_eq!(first.canonical_key(), b"\x00\x02ab\x00\x01c");
    assert_ne!(first.canonical_key(), second.canonical_key());

    let mut keys = HashSet::new();
    for namespace in ["a", "ab", "stt", "inference"] {
        for destination in ["b", "bc", "stt", "stt.seoul", "stt.seoul.worker1"] {
            let address = RouteAddress::new(
                NamespaceId::new(namespace)?,
                DestinationName::new(destination)?,
            );
            assert!(keys.insert(address.canonical_key()));
        }
    }
    assert_eq!(keys.len(), 20);
    Ok(())
}

#[test]
fn parser_errors_do_not_echo_untrusted_input() {
    let invalid = "log\nforged-event";
    let error = DestinationName::new(invalid).err();
    assert!(
        error
            .as_ref()
            .is_some_and(|error| !format!("{error} {error:?}").contains(invalid))
    );
}
