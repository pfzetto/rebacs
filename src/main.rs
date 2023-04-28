use std::{sync::Arc, time::Duration};

use graph::Graph;
use grpc_service::GraphService;
use tokio::{
    fs::{self, File},
    select,
    sync::{mpsc::channel, Mutex},
};
use tonic::transport::Server;

pub mod graph;
pub mod grpc_service;
pub mod themis_proto;

use crate::themis_proto::{
    object_service_server::ObjectServiceServer, query_service_server::QueryServiceServer,
    relation_service_server::RelationServiceServer,
};

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    pretty_env_logger::init();

    let graph = if let Ok(mut file) = File::open("graph.dat").await {
        Graph::from_file(&mut file).await
    } else {
        Graph::default()
    };

    let graph = Arc::new(Mutex::new(graph));

    let (save_tx, mut save_rx) = channel::<()>(32);
    let save_thread_graph = graph.clone();
    tokio::spawn(async move {
        loop {
            select! {
                _ = tokio::time::sleep(Duration::from_secs(30)) => {}
                _ = save_rx.recv() => {}
            };
            let graph = save_thread_graph.lock().await;

            let _ = fs::copy("graph.dat", "graph.dat.bak").await;
            let mut file = File::create("graph.dat").await.unwrap();
            graph.to_file(&mut file).await;
        }
    });

    let graph_service = GraphService {
        graph: graph.clone(),
        save_trigger: save_tx.clone(),
    };

    Server::builder()
        .add_service(ObjectServiceServer::new(graph_service.clone()))
        .add_service(RelationServiceServer::new(graph_service.clone()))
        .add_service(QueryServiceServer::new(graph_service))
        .serve("0.0.0.0:50051".parse().unwrap())
        .await
        .unwrap()
}
