use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest dir"));
    let proto_root = manifest_dir.join("proto");
    let relay_proto = proto_root.join("relaygate/relay/v1/relay.proto");
    let protoc = protoc_bin_vendored::protoc_bin_path()?;

    println!("cargo:rerun-if-changed={}", relay_proto.display());
    println!("cargo:rerun-if-changed={}", proto_root.display());

    let mut config = tonic_prost_build::Config::new();
    config.protoc_executable(protoc);
    tonic_prost_build::configure()
        .build_server(false)
        .build_transport(false)
        .compile_with_config(config, &[relay_proto], &[proto_root])?;
    Ok(())
}
