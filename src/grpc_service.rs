use std::collections::HashMap;
use std::sync::Arc;

use log::info;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

use crate::graph::{self, Graph, ObjectRelation};
use crate::themis_proto::{
    object_service_server::ObjectService, query_service_server::QueryService, relation::Src,
    relation_service_server::RelationService, Empty, ExistsResponse, GetRelatedToResponse,
    GetRelationsRequest, GetRelationsResponse, IsRelatedToResponse, Object, Relation, Set,
};

#[derive(Clone)]
pub struct GraphService {
    pub api_keys: Arc<Mutex<HashMap<String, String>>>,
    pub graph: Arc<Mutex<Graph>>,
    pub save_trigger: Sender<()>,
}

#[tonic::async_trait]
impl ObjectService for GraphService {
    async fn create(&self, request: Request<Object>) -> Result<Response<Empty>, Status> {
        let mut graph = self.graph.lock().await;

        authenticate(
            request.metadata(),
            &graph,
            &self.api_keys,
            &request.get_ref().namespace,
            "write",
        )
        .await?;

        if request.get_ref().namespace.is_empty() || request.get_ref().id.is_empty() {
            return Err(Status::invalid_argument("namespace and id must be set"));
        }

        graph.add_node(graph::Object::new(
            &request.get_ref().namespace,
            &request.get_ref().id,
        ));

        info!(
            "created object {}:{}",
            &request.get_ref().namespace,
            &request.get_ref().id
        );

        self.save_trigger.send(()).await.unwrap();

        Ok(Response::new(Empty {}))
    }
    async fn delete(&self, request: Request<Object>) -> Result<Response<Empty>, Status> {
        let mut graph = self.graph.lock().await;

        authenticate(
            request.metadata(),
            &graph,
            &self.api_keys,
            &request.get_ref().namespace,
            "write",
        )
        .await?;

        if request.get_ref().namespace.is_empty() || request.get_ref().id.is_empty() {
            return Err(Status::invalid_argument("namespace and id must be set"));
        }

        graph.remove_node(graph::Object::new(
            &request.get_ref().namespace,
            &request.get_ref().id,
        ));

        info!(
            "removed object {}:{}",
            &request.get_ref().namespace,
            &request.get_ref().id
        );

        self.save_trigger.send(()).await.unwrap();

        Ok(Response::new(Empty {}))
    }
    async fn exists(&self, request: Request<Object>) -> Result<Response<ExistsResponse>, Status> {
        let graph = self.graph.lock().await;

        authenticate(
            request.metadata(),
            &graph,
            &self.api_keys,
            &request.get_ref().namespace,
            "read",
        )
        .await?;

        if request.get_ref().namespace.is_empty() || request.get_ref().id.is_empty() {
            return Err(Status::invalid_argument("namespace and id must be set"));
        }

        let exists = graph
            .get_node(&request.get_ref().namespace, &request.get_ref().id)
            .is_some();

        Ok(Response::new(ExistsResponse { exists }))
    }
}

#[tonic::async_trait]
impl RelationService for GraphService {
    async fn create(&self, request: Request<Relation>) -> Result<Response<Empty>, Status> {
        let mut graph = self.graph.lock().await;

        let (src, dst, dst_namespace) = transform_relation(request.get_ref(), &graph)?;

        authenticate(
            request.metadata(),
            &graph,
            &self.api_keys,
            &dst_namespace,
            "write",
        )
        .await?;

        graph.add_relation(src, dst);

        info!("created relation");

        self.save_trigger.send(()).await.unwrap();

        Ok(Response::new(Empty {}))
    }
    async fn delete(&self, request: Request<Relation>) -> Result<Response<Empty>, Status> {
        let mut graph = self.graph.lock().await;

        let (src, dst, dst_namespace) = transform_relation(request.get_ref(), &graph)?;

        authenticate(
            request.metadata(),
            &graph,
            &self.api_keys,
            &dst_namespace,
            "write",
        )
        .await?;

        graph.remove_relation(src, dst);

        info!("removed relation relation");

        self.save_trigger.send(()).await.unwrap();

        Ok(Response::new(Empty {}))
    }
    async fn exists(&self, request: Request<Relation>) -> Result<Response<ExistsResponse>, Status> {
        let graph = self.graph.lock().await;

        let (src, dst, dst_namespace) = transform_relation(request.get_ref(), &graph)?;

        authenticate(
            request.metadata(),
            &graph,
            &self.api_keys,
            &dst_namespace,
            "read",
        )
        .await?;

        let exists = graph.has_relation(src, dst);

        Ok(Response::new(ExistsResponse { exists }))
    }
}

#[tonic::async_trait]
impl QueryService for GraphService {
    async fn is_related_to(
        &self,
        request: Request<Relation>,
    ) -> Result<Response<IsRelatedToResponse>, Status> {
        let graph = self.graph.lock().await;

        let related = if let Ok((src, dst, _)) = transform_relation(request.get_ref(), &graph) {
            graph.is_related_to(src, dst)
        } else {
            false
        };

        Ok(Response::new(IsRelatedToResponse { related }))
    }
    async fn get_related_to(
        &self,
        request: Request<Set>,
    ) -> Result<Response<GetRelatedToResponse>, Status> {
        let graph = self.graph.lock().await;

        authenticate(
            request.metadata(),
            &graph,
            &self.api_keys,
            &request.get_ref().namespace,
            "read",
        )
        .await?;

        let obj = graph
            .get_node(&request.get_ref().namespace, &request.get_ref().id)
            .ok_or(Status::not_found("object not found"))?;

        let rel = graph::Relation::new(&request.get_ref().relation);

        Ok(Response::new(GetRelatedToResponse {
            objects: graph
                .related_to(obj, rel)
                .into_iter()
                .map(|x| {
                    let obj = graph.object_from_ref(&x);
                    Object {
                        namespace: obj.namespace.to_string(),
                        id: obj.id,
                    }
                })
                .collect::<Vec<_>>(),
        }))
    }
    async fn get_relations(
        &self,
        request: Request<GetRelationsRequest>,
    ) -> Result<Response<GetRelationsResponse>, Status> {
        let graph = self.graph.lock().await;

        if request.get_ref().relation.is_empty() {
            return Err(Status::invalid_argument("relation must be set"));
        }

        let obj = request
            .get_ref()
            .object
            .as_ref()
            .ok_or(Status::invalid_argument("object must be set"))?;

        authenticate(
            request.metadata(),
            &graph,
            &self.api_keys,
            &obj.namespace,
            "read",
        )
        .await?;

        let obj = graph
            .get_node(&obj.namespace, &obj.id)
            .ok_or(Status::not_found("object not found"))?;

        Ok(Response::new(GetRelationsResponse {
            objects: graph
                .relations(ObjectRelation(
                    obj,
                    graph::Relation::new(&request.get_ref().relation),
                ))
                .into_iter()
                .map(|x| {
                    let obj = graph.object_from_ref(&x);
                    Object {
                        namespace: obj.namespace.to_string(),
                        id: obj.id,
                    }
                })
                .collect::<Vec<_>>(),
        }))
    }
}

fn transform_relation(
    rel: &Relation,
    graph: &Graph,
) -> Result<(graph::ObjectOrSet, graph::ObjectRelation, String), Status> {
    let src = match rel
        .src
        .as_ref()
        .ok_or(Status::invalid_argument("src must be set"))?
    {
        Src::SrcObj(object) => graph::ObjectOrSet::Object(
            graph
                .get_node(&object.namespace, &object.id)
                .ok_or(Status::not_found("src object could not be found"))?,
        ),
        Src::SrcSet(set) => graph::ObjectOrSet::Set((
            graph
                .get_node(&set.namespace, &set.id)
                .ok_or(Status::not_found("src object could not be found"))?,
            graph::Relation::new(&set.relation),
        )),
    };

    let dst = rel
        .dst
        .as_ref()
        .ok_or(Status::invalid_argument("dst must be set"))?;
    let dst_namespace = dst.namespace.to_string();
    let dst = graph
        .get_node(&dst.namespace, &dst.id)
        .ok_or(Status::not_found("dst object could not be found"))?;
    let dst = ObjectRelation(dst, graph::Relation::new(&rel.relation));

    Ok((src, dst, dst_namespace))
}

async fn authenticate(
    metadata: &MetadataMap,
    graph: &Graph,
    api_keys: &Arc<Mutex<HashMap<String, String>>>,
    namespace: &str,
    relation: &str,
) -> Result<(), Status> {
    let api_key = metadata
        .get("x-api-key")
        .map(|x| x.to_str().unwrap())
        .ok_or(Status::unauthenticated("x-api-key required"))?;

    let mut hasher = Sha256::new();
    hasher.update(api_key);
    let api_key = hex::encode(hasher.finalize());
    let api_keys = api_keys.lock().await;
    let api_key = api_keys
        .get(&api_key)
        .ok_or(Status::unauthenticated("api-key invalid"))?;

    let api_key = graph
        .get_node("themis_key", api_key)
        .ok_or(Status::unauthenticated("api-key invalid"))?;

    let ns_ref = graph
        .get_node("themis_ns", namespace)
        .ok_or(Status::permission_denied("no permission for namespace"))?;

    if !graph.is_related_to(api_key, (ns_ref, graph::Relation::new(relation))) {
        Err(Status::permission_denied("no permission for namespace"))
    } else {
        Ok(())
    }
}
