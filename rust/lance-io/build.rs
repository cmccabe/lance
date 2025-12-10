use std::io::Result;

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=protos");

    // Use vendored protobuf compiler if requested.
    std::env::set_var("PROTOC", protobuf_src::protoc());

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(
            &["../../protos/avalon.proto"],
            &["../../protos"],
        )?;

    Ok(())
}
