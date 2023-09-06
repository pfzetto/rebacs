#![feature(btree_cursors)]

use std::{env, sync::Arc, time::Duration};

use grpc_service::RebacService;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use log::info;
use rebacs_core::RelationGraph;
use serde::Deserialize;
use tokio::{
    fs::{self, File},
    io::BufReader,
    select,
    sync::mpsc::channel,
};
use tonic::transport::Server;

pub mod grpc_service;
pub mod rebacs_proto {

    tonic::include_proto!("eu.zettoit.rebacs");
}

use crate::rebacs_proto::rebac_service_server;

#[derive(Deserialize)]
struct IssuerDiscovery {
    public_key: String,
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    env_logger::init();

    info!("loading graph from graph.dat");
    let graph = if let Ok(file) = File::open("graph.dat").await {
        let mut reader = BufReader::new(file);
        RelationGraph::read_savefile(&mut reader).await
    } else {
        RelationGraph::default()
    };

    let graph = Arc::new(graph);

    let (save_tx, mut save_rx) = channel::<()>(32);
    let save_thread_graph = graph.clone();
    tokio::spawn(async move {
        loop {
            select! {
                _ = tokio::time::sleep(Duration::from_secs(30)) => {}
                _ = save_rx.recv() => {}
            };
            info!("saving graph");
            let _ = fs::copy("graph.dat", "graph.dat.bak").await;
            let mut file = File::create("graph.dat").await.unwrap();
            save_thread_graph.write_savefile(&mut file).await;
        }
    });

    let issuer = env::var("OIDC_ISSUER").expect("OIDC_ISSUER env var");
    info!("loading public key from {issuer}");
    let issuer_key = reqwest::get(&issuer)
        .await
        .unwrap()
        .json::<IssuerDiscovery>()
        .await
        .unwrap()
        .public_key;

    let pem = format!(
        "-----BEGIN PUBLIC KEY-----\n{}\n-----END PUBLIC KEY-----",
        issuer_key
    );

    let oidc_pubkey = DecodingKey::from_rsa_pem(pem.as_bytes()).unwrap();

    let mut oidc_validation = Validation::new(Algorithm::RS256);
    oidc_validation.set_issuer(&[&issuer]);
    oidc_validation.set_audience(&[env::var("OIDC_AUDIENCE").expect("OIDC_AUDIENCE env var")]);

    let rebac_service = RebacService {
        graph: graph.clone(),
        save_trigger: save_tx.clone(),
        oidc_pubkey,
        oidc_validation,
    };

    let listen = "[::]:50051";
    info!("starting grpc server on {listen}");
    Server::builder()
        .add_service(rebac_service_server::RebacServiceServer::new(
            rebac_service.clone(),
        ))
        .serve(listen.parse().unwrap())
        .await
        .unwrap()
}
