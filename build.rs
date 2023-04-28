fn main() {
    tonic_build::configure()
        .build_server(true)
        .compile(&["proto/graph.proto"], &["proto"])
        .unwrap();
}
