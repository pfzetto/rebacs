use std::{sync::Arc, time::Duration};

use kafka::{
    consumer::{Consumer, FetchOffset},
    producer::{Producer, Record, RequiredAcks},
};
use log::debug;
use serde::{Deserialize, Serialize};
use tokio::{
    runtime::Runtime,
    sync::{mpsc, RwLock},
    task::JoinHandle,
};

use crate::{
    graph::{Graph, ObjectRelation},
    object::{Object, ObjectOrSet, ObjectRef},
};

#[derive(Serialize, Deserialize, Debug)]
pub enum Event {
    AddObject(Object),
    RemoveObject(Object),
    AddRelation((ObjectOrSet, ObjectRelation)),
    RemoveRelation((ObjectOrSet, ObjectRelation)),
}

pub struct GraphProxy {
    graph: Arc<RwLock<Graph>>,
    producer_thread: JoinHandle<()>,
    producer_tx: mpsc::Sender<Event>,
    consumer_thread: JoinHandle<()>,
}
impl GraphProxy {
    pub async fn run() -> Self {
        let graph = Arc::new(RwLock::new(Graph::default()));
        let (producer_tx, mut producer_rx) = mpsc::channel(1024);

        let mut producer = Producer::from_hosts(vec!["localhost:9092".to_owned()])
            .with_ack_timeout(Duration::from_secs(1))
            .with_required_acks(RequiredAcks::One)
            .create()
            .unwrap();

        let producer_thread = tokio::spawn(async move {
            loop {
                if let Some(event) = producer_rx.recv().await {
                    let ser_event = serde_cbor::to_vec(&event).unwrap();
                    producer
                        .send(&Record::from_value("gpm", ser_event))
                        .unwrap();
                    debug!("emitted Event: {:?}", event);
                }
            }
        });

        let mut consumer = Consumer::from_hosts(vec!["localhost:9092".to_string()])
            .with_client_id("gpm_dev".to_string())
            .with_topic("gpm".to_string())
            .with_fallback_offset(FetchOffset::Earliest)
            .create()
            .unwrap();
        let consumer_graph = graph.clone();
        let consumer_thread = tokio::task::spawn_blocking(move || {
            let runtime = Runtime::new().unwrap();
            loop {
                for msg_sets in consumer.poll().unwrap().iter() {
                    for msg in msg_sets.messages() {
                        let event: Event = serde_cbor::from_slice(msg.value).unwrap();
                        debug!("received Event: {:?}", event);
                        let mut graph = runtime.block_on(consumer_graph.write());
                        match event {
                            Event::AddObject(obj) => {
                                graph.add_node(obj);
                            }
                            Event::RemoveObject(obj) => {
                                graph.remove_node(obj);
                            }
                            Event::AddRelation((src, dst)) => {
                                graph.add_relation(src, dst);
                            }
                            Event::RemoveRelation((src, dst)) => {
                                graph.remove_relation(src, dst);
                            }
                        };
                    }
                    consumer.consume_messageset(msg_sets).unwrap();
                }
                consumer.commit_consumed().unwrap();
            }
        });

        Self {
            graph,
            producer_thread,
            producer_tx,
            consumer_thread,
        }
    }

    pub fn stop(&mut self) {
        self.producer_thread.abort();
        self.consumer_thread.abort();
    }

    pub async fn add_node(&mut self, node: Object) {
        self.producer_tx.send(Event::AddObject(node)).await.unwrap();
    }
    pub async fn remove_node(&mut self, node: Object) {
        self.producer_tx
            .send(Event::RemoveObject(node))
            .await
            .unwrap();
    }
    pub async fn add_relation(&mut self, src: ObjectOrSet, dst: ObjectRelation) {
        self.producer_tx
            .send(Event::AddRelation((src, dst)))
            .await
            .unwrap();
    }
    pub async fn remove_relation(&mut self, src: ObjectOrSet, dst: ObjectRelation) {
        self.producer_tx
            .send(Event::RemoveRelation((src, dst)))
            .await
            .unwrap();
    }

    pub async fn get_node(&self, namespace: &str, id: &str) -> Option<ObjectRef> {
        let graph = self.graph.read().await;
        graph.get_node(namespace, id)
    }

    pub async fn is_related_to(
        &self,
        src: impl Into<ObjectOrSet>,
        dst: impl Into<ObjectRelation>,
    ) -> bool {
        let graph = self.graph.read().await;
        graph.is_related_to(src, dst)
    }
    pub async fn related_by(&self, src: impl Into<ObjectRelation>) -> Vec<ObjectOrSet> {
        let graph = self.graph.read().await;
        graph.related_by(src)
    }
}
