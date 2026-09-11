use relaygate_route_table::{Destination, ErrorCode, RouteTableError, ShardDirectory, ShardId};

const THREE_SHARD_DIRECTORY: &[u8] = br#"{"format_version":2,"authority_hash":"sha256-destination-modulo-v2","shards":[{"id":"rt-0","endpoint":"http://rt-0:8080"},{"id":"rt-1","endpoint":"http://rt-1:8080"},{"id":"rt-2","endpoint":"http://rt-2:8080"}]}"#;

#[test]
fn exact_artifact_bytes_define_generation_and_ordered_authority() -> Result<(), RouteTableError> {
    let directory = ShardDirectory::from_json_bytes(THREE_SHARD_DIRECTORY)?;

    assert_eq!(
        directory.generation().to_string(),
        "26538882fe1a6cb2d35c8f094d6ec1605bba0e0932c0e1f60068e224144633c1"
    );
    assert_eq!(directory.artifact_bytes(), THREE_SHARD_DIRECTORY);
    assert_eq!(directory.shards().len(), 3);
    for (destination, expected_shard) in [
        ("test/alpha", "rt-1"),
        ("test/beta", "rt-2"),
        ("test/00000006-0000-4000-8000-000000000006", "rt-0"),
    ] {
        assert_eq!(
            directory
                .authority(&destination.parse::<Destination>()?)
                .id()
                .as_str(),
            expected_shard
        );
    }
    Ok(())
}

#[test]
fn namespace_is_part_of_the_shard_hash() -> Result<(), RouteTableError> {
    let directory = ShardDirectory::from_json_bytes(THREE_SHARD_DIRECTORY)?;
    let test = "test/alpha".parse::<Destination>()?;
    let other = "other/alpha".parse::<Destination>()?;

    assert_ne!(test.canonical_key(), other.canonical_key());
    assert_eq!(directory.authority(&test).id().as_str(), "rt-1");
    assert_eq!(directory.authority(&other).id().as_str(), "rt-2");
    Ok(())
}

#[test]
fn byte_or_record_order_change_creates_a_different_generation() -> Result<(), RouteTableError> {
    let original = ShardDirectory::from_json_bytes(THREE_SHARD_DIRECTORY)?;

    let mut whitespace_changed = THREE_SHARD_DIRECTORY.to_vec();
    whitespace_changed.push(b'\n');
    let whitespace_changed = ShardDirectory::from_json_bytes(whitespace_changed)?;

    let reordered = ShardDirectory::from_json_bytes(
        br#"{"format_version":2,"authority_hash":"sha256-destination-modulo-v2","shards":[{"id":"rt-2","endpoint":"http://rt-2:8080"},{"id":"rt-1","endpoint":"http://rt-1:8080"},{"id":"rt-0","endpoint":"http://rt-0:8080"}]}"#,
    )?;

    assert_ne!(original.generation(), whitespace_changed.generation());
    assert_ne!(original.generation(), reordered.generation());
    let destination = "test/beta".parse::<Destination>()?;
    assert_ne!(
        original.authority(&destination).id(),
        reordered.authority(&destination).id()
    );
    Ok(())
}

#[test]
fn invalid_directory_artifacts_are_rejected() {
    let invalid_artifacts: [&[u8]; 9] = [
        br#"{"format_version":1,"authority_hash":"sha256-destination-modulo-v2","shards":[{"id":"rt-0","endpoint":"rt-0"}]}"#,
        br#"{"format_version":2,"authority_hash":"other","shards":[{"id":"rt-0","endpoint":"rt-0"}]}"#,
        br#"{"format_version":2,"authority_hash":"sha256-destination-modulo-v2","shards":[]}"#,
        br#"{"format_version":2,"authority_hash":"sha256-destination-modulo-v2","shards":[{"id":"","endpoint":"rt-0"}]}"#,
        br#"{"format_version":2,"authority_hash":"sha256-destination-modulo-v2","shards":[{"id":"rt-0","endpoint":""}]}"#,
        br#"{"format_version":2,"authority_hash":"sha256-destination-modulo-v2","shards":[{"id":"rt-0","endpoint":"a"},{"id":"rt-0","endpoint":"b"}]}"#,
        br#"{"format_version":2,"authority_hash":"sha256-destination-modulo-v2","shards":[{"id":"rt-0","endpoint":"a","typo":true}]}"#,
        br#"{"format_version":2,"authority_hash":"sha256-destination-modulo-v2","shards":[{"id":"rt-0","endpoint":"a"}],"bindings":[]}"#,
        br#"{"format_version":2,"authority_hash":"sha256-destination-modulo-v2","generation":"operator-value","shards":[{"id":"rt-0","endpoint":"a"}]}"#,
    ];

    for artifact in invalid_artifacts {
        assert!(matches!(
            ShardDirectory::from_json_bytes(artifact),
            Err(RouteTableError::InvalidArgument(_))
        ));
    }
}

#[test]
fn typed_identifiers_reject_invalid_values() {
    let empty_destination = "".parse::<Destination>().map_err(RouteTableError::from);
    let missing_namespace = "alpha"
        .parse::<Destination>()
        .map_err(RouteTableError::from);
    let wildcard_destination = "test/alpha.*"
        .parse::<Destination>()
        .map_err(RouteTableError::from);
    let shard_error = ShardId::new("");

    for error in [empty_destination, missing_namespace, wildcard_destination] {
        assert!(matches!(
            error,
            Err(ref error) if error.code() == ErrorCode::InvalidArgument
        ));
    }
    assert!(matches!(
        shard_error,
        Err(ref error) if error.code() == ErrorCode::InvalidArgument
    ));
}

#[test]
fn revision_zero_is_not_a_valid_domain_value() {
    assert!(matches!(
        relaygate_route_table::RegistrationRevision::new(0),
        Err(ref error) if error.code() == ErrorCode::InvalidArgument
    ));
}
