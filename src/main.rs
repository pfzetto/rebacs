#![feature(btree_cursors)]

use std::{collections::HashMap, sync::Arc, time::Duration};

use grpc_service::GraphService;
use relation_set::RelationSet;
//use grpc_service::GraphService;
use tokio::{
    fs::{self, File},
    io::{AsyncBufReadExt, BufReader},
    select,
    sync::{mpsc::channel, Mutex},
};
use tonic::transport::Server;

pub mod grpc_service;
pub mod relation_set;
pub mod themis_proto;

use crate::themis_proto::{
    query_service_server::QueryServiceServer, relation_service_server::RelationServiceServer,
};

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    pretty_env_logger::init();

    let mut api_keys = HashMap::new();
    if let Ok(file) = File::open("api_keys.dat").await {
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let line = line.replace(' ', "");
            let mut line = line.split('=');
            let name = line.next().unwrap().to_string();
            let hash = line.next().unwrap().to_string();
            api_keys.insert(hash, name);
        }
    }

    let graph = if let Ok(mut file) = File::open("graph.dat").await {
        RelationSet::from_file(&mut file).await
    } else {
        RelationSet::new()
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
        api_keys: Arc::new(Mutex::new(api_keys)),
        graph: graph.clone(),
        save_trigger: save_tx.clone(),
    };

    Server::builder()
        .add_service(RelationServiceServer::new(graph_service.clone()))
        .add_service(QueryServiceServer::new(graph_service))
        .serve("[::]:50051".parse().unwrap())
        .await
        .unwrap()
}
