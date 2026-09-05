use relaygate_route_table::{DestinationId, ErrorCode, RouteTableError, ShardDirectory, ShardId};

const THREE_SHARD_DIRECTORY: &[u8] = br#"{"format_version":1,"authority_hash":"sha256-modulo-v1","shards":[{"id":"rt-0","endpoint":"http://rt-0:8080"},{"id":"rt-1","endpoint":"http://rt-1:8080"},{"id":"rt-2","endpoint":"http://rt-2:8080"}]}"#;

#[test]
fn exact_artifact_bytes_define_generation_and_ordered_authority() -> Result<(), RouteTableError> {
    let directory = ShardDirectory::from_json_bytes(THREE_SHARD_DIRECTORY)?;

    assert_eq!(
        directory.generation().to_string(),
        "651b4511fa54481a94bf1d93520b482c3bc04ddda0db1effeef39cb34afd87e0"
    );
    assert_eq!(directory.artifact_bytes(), THREE_SHARD_DIRECTORY);
    assert_eq!(directory.shards().len(), 3);
    assert_eq!(
        directory
            .authority(&DestinationId::new("00000002-0000-4000-8000-000000000002")?)
            .id()
            .as_str(),
        "rt-2"
    );
    assert_eq!(
        directory
            .authority(&DestinationId::new("00000005-0000-4000-8000-000000000005")?)
            .id()
            .as_str(),
        "rt-0"
    );
    assert_eq!(
        directory
            .authority(&DestinationId::new("00000006-0000-4000-8000-000000000006")?)
            .id()
            .as_str(),
        "rt-0"
    );
    Ok(())
}

#[test]
fn byte_or_record_order_change_creates_a_different_generation() -> Result<(), RouteTableError> {
    let original = ShardDirectory::from_json_bytes(THREE_SHARD_DIRECTORY)?;

    let mut whitespace_changed = THREE_SHARD_DIRECTORY.to_vec();
    whitespace_changed.push(b'\n');
    let whitespace_changed = ShardDirectory::from_json_bytes(whitespace_changed)?;

    let reordered = ShardDirectory::from_json_bytes(
        br#"{"format_version":1,"authority_hash":"sha256-modulo-v1","shards":[{"id":"rt-2","endpoint":"http://rt-2:8080"},{"id":"rt-1","endpoint":"http://rt-1:8080"},{"id":"rt-0","endpoint":"http://rt-0:8080"}]}"#,
    )?;

    assert_ne!(original.generation(), whitespace_changed.generation());
    assert_ne!(original.generation(), reordered.generation());
    assert_ne!(
        original
            .authority(&DestinationId::new("00000002-0000-4000-8000-000000000002")?)
            .id(),
        reordered
            .authority(&DestinationId::new("00000002-0000-4000-8000-000000000002")?)
            .id()
    );
    Ok(())
}

#[test]
fn invalid_directory_artifacts_are_rejected() {
    let invalid_artifacts: [&[u8]; 9] = [
        br#"{"format_version":2,"authority_hash":"sha256-modulo-v1","shards":[{"id":"rt-0","endpoint":"rt-0"}]}"#,
        br#"{"format_version":1,"authority_hash":"other","shards":[{"id":"rt-0","endpoint":"rt-0"}]}"#,
        br#"{"format_version":1,"authority_hash":"sha256-modulo-v1","shards":[]}"#,
        br#"{"format_version":1,"authority_hash":"sha256-modulo-v1","shards":[{"id":"","endpoint":"rt-0"}]}"#,
        br#"{"format_version":1,"authority_hash":"sha256-modulo-v1","shards":[{"id":"rt-0","endpoint":""}]}"#,
        br#"{"format_version":1,"authority_hash":"sha256-modulo-v1","shards":[{"id":"rt-0","endpoint":"a"},{"id":"rt-0","endpoint":"b"}]}"#,
        br#"{"format_version":1,"authority_hash":"sha256-modulo-v1","shards":[{"id":"rt-0","endpoint":"a","typo":true}]}"#,
        br#"{"format_version":1,"authority_hash":"sha256-modulo-v1","shards":[{"id":"rt-0","endpoint":"a"}],"mappings":[]}"#,
        br#"{"format_version":1,"authority_hash":"sha256-modulo-v1","generation":"operator-value","shards":[{"id":"rt-0","endpoint":"a"}]}"#,
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
    let client_error = DestinationId::new("");
    let non_uuid_error = DestinationId::new("alpha");
    let non_v4_error = DestinationId::new("00000000-0000-1000-8000-000000000000");
    let shard_error = ShardId::new("");

    assert!(matches!(
        client_error,
        Err(ref error) if error.code() == ErrorCode::InvalidArgument
    ));
    assert!(matches!(
        non_uuid_error,
        Err(ref error) if error.code() == ErrorCode::InvalidArgument
    ));
    assert!(matches!(
        non_v4_error,
        Err(ref error) if error.code() == ErrorCode::InvalidArgument
    ));
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
