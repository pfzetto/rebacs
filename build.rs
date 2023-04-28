fn main() {
    tonic_build::configure()
        .build_server(true)
        .compile(&["proto/themis.proto"], &["proto"])
        .unwrap();
}
