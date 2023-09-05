fn main() {
    tonic_build::configure()
        .build_server(true)
        .compile(&["proto/rebacs.proto"], &["proto"])
        .unwrap();
}
