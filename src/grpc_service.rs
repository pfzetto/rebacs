use std::collections::HashMap;
use std::sync::Arc;

use log::info;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

use crate::rebacs_proto::{
    query_service_server::QueryService, relation_service_server::RelationService, Object,
    QueryGetRelatedItem, QueryGetRelatedReq, QueryGetRelatedRes, QueryGetRelationsItem,
    QueryGetRelationsReq, QueryGetRelationsRes, QueryIsRelatedToReq, QueryIsRelatedToRes,
    RelationCreateReq, RelationCreateRes, RelationDeleteReq, RelationDeleteRes, RelationExistsReq,
    RelationExistsRes,
};
use crate::relation_set::{ObjectOrSet, RelationSet};

#[derive(Clone)]
pub struct GraphService {
    pub api_keys: Arc<Mutex<HashMap<String, String>>>,
    pub graph: Arc<Mutex<RelationSet>>,
    pub save_trigger: Sender<()>,
}

const API_KEY_NS: &str = "rebacs_key";
const NAMESPACE_NS: &str = "rebacs_ns";

#[tonic::async_trait]
impl RelationService for GraphService {
    async fn create(
        &self,
        request: Request<RelationCreateReq>,
    ) -> Result<Response<RelationCreateRes>, Status> {
        let mut graph = self.graph.lock().await;

        let api_key = api_key_from_req(request.metadata(), &self.api_keys).await?;

        let req_src = request
            .get_ref()
            .src
            .as_ref()
            .ok_or(Status::invalid_argument("src must be set"))?;
        let req_dst = request
            .get_ref()
            .dst
            .as_ref()
            .ok_or(Status::invalid_argument("dst must be set"))?;
        let req_rel = &request.get_ref().relation;

        if req_rel.is_empty() {
            return Err(Status::invalid_argument("relation must be set"));
        }
        if req_dst.namespace.is_empty() {
            return Err(Status::invalid_argument("dst.namespace must be set"));
        }
        if req_dst.id.is_empty() {
            return Err(Status::invalid_argument("dst.id must be set"));
        }

        if !graph.has_recursive(
            (API_KEY_NS, &*api_key),
            "write",
            (NAMESPACE_NS, &*req_dst.namespace),
            u32::MAX,
        ) {
            return Err(Status::permission_denied(
                "missing dst.namespace write permissions",
            ))?;
        }

        if req_src.namespace.is_empty() {
            return Err(Status::invalid_argument("src.namespace must be set"));
        }
        if req_src.id.is_empty() {
            return Err(Status::invalid_argument("src.id must be set"));
        }
        let src: ObjectOrSet = if let Some(req_src_relation) = req_src.relation.as_deref() {
            if req_src_relation.is_empty() {
                return Err(Status::invalid_argument("src.relation must be set"));
            }

            (&*req_src.namespace, &*req_src.id, req_src_relation).into()
        } else {
            (&*req_src.namespace, &*req_src.id).into()
        };

        graph.insert(
            src.clone(),
            req_rel.clone(),
            (req_dst.namespace.clone(), req_dst.id.clone()),
        );

        info!("created relation");

        self.save_trigger.send(()).await.unwrap();

        Ok(Response::new(RelationCreateRes {}))
    }
    async fn delete(
        &self,
        request: Request<RelationDeleteReq>,
    ) -> Result<Response<RelationDeleteRes>, Status> {
        let mut graph = self.graph.lock().await;

        let api_key = api_key_from_req(request.metadata(), &self.api_keys).await?;

        let req_src = request
            .get_ref()
            .src
            .as_ref()
            .ok_or(Status::invalid_argument("src must be set"))?;
        let req_dst = request
            .get_ref()
            .dst
            .as_ref()
            .ok_or(Status::invalid_argument("dst must be set"))?;
        let req_rel = &request.get_ref().relation;

        if req_rel.is_empty() {
            return Err(Status::invalid_argument("relation must be set"));
        }
        if req_dst.namespace.is_empty() {
            return Err(Status::invalid_argument("dst.namespace must be set"));
        }
        if req_dst.id.is_empty() {
            return Err(Status::invalid_argument("dst.id must be set"));
        }

        if !graph.has_recursive(
            (API_KEY_NS, &*api_key),
            "write",
            (NAMESPACE_NS, &*req_dst.namespace),
            u32::MAX,
        ) {
            return Err(Status::permission_denied(
                "missing dst.namespace write permissions",
            ))?;
        }

        if req_src.namespace.is_empty() {
            return Err(Status::invalid_argument("src.namespace must be set"));
        }
        if req_src.id.is_empty() {
            return Err(Status::invalid_argument("src.id must be set"));
        }
        let src: ObjectOrSet = if let Some(req_src_relation) = req_src.relation.as_deref() {
            if req_src_relation.is_empty() {
                return Err(Status::invalid_argument("src.relation must be set"));
            }

            (&*req_src.namespace, &*req_src.id, req_src_relation).into()
        } else {
            (&*req_src.namespace, &*req_src.id).into()
        };

        graph.remove(src, req_rel.as_str(), (&*req_dst.namespace, &*req_dst.id));

        info!("deleted relation");

        self.save_trigger.send(()).await.unwrap();

        Ok(Response::new(RelationDeleteRes {}))
    }
    async fn exists(
        &self,
        request: Request<RelationExistsReq>,
    ) -> Result<Response<RelationExistsRes>, Status> {
        let graph = self.graph.lock().await;

        let api_key = api_key_from_req(request.metadata(), &self.api_keys).await?;

        let req_src = request
            .get_ref()
            .src
            .as_ref()
            .ok_or(Status::invalid_argument("src must be set"))?;
        let req_dst = request
            .get_ref()
            .dst
            .as_ref()
            .ok_or(Status::invalid_argument("dst must be set"))?;
        let req_rel = &request.get_ref().relation;

        if req_rel.is_empty() {
            return Err(Status::invalid_argument("relation must be set"));
        }
        if req_dst.namespace.is_empty() {
            return Err(Status::invalid_argument("dst.namespace must be set"));
        }
        if req_dst.id.is_empty() {
            return Err(Status::invalid_argument("dst.id must be set"));
        }

        if !graph.has_recursive(
            (API_KEY_NS, &*api_key),
            "read",
            (NAMESPACE_NS, &*req_dst.namespace),
            u32::MAX,
        ) {
            return Err(Status::permission_denied(
                "missing dst.namespace write permissions",
            ))?;
        }
        if req_src.namespace.is_empty() {
            return Err(Status::invalid_argument("src.namespace must be set"));
        }
        if req_src.id.is_empty() {
            return Err(Status::invalid_argument("src.id must be set"));
        }
        let src: ObjectOrSet = if let Some(req_src_relation) = req_src.relation.as_deref() {
            if req_src_relation.is_empty() {
                return Err(Status::invalid_argument("src.relation must be set"));
            }

            (&*req_src.namespace, &*req_src.id, req_src_relation).into()
        } else {
            (&*req_src.namespace, &*req_src.id).into()
        };

        let exists = graph.has(src, req_rel.as_str(), (&*req_dst.namespace, &*req_dst.id));

        Ok(Response::new(RelationExistsRes { exists }))
    }
}

#[tonic::async_trait]
impl QueryService for GraphService {
    async fn is_related_to(
        &self,
        request: Request<QueryIsRelatedToReq>,
    ) -> Result<Response<QueryIsRelatedToRes>, Status> {
        let graph = self.graph.lock().await;

        let api_key = api_key_from_req(request.metadata(), &self.api_keys).await?;

        let req_src = request
            .get_ref()
            .src
            .as_ref()
            .ok_or(Status::invalid_argument("src must be set"))?;
        let req_dst = request
            .get_ref()
            .dst
            .as_ref()
            .ok_or(Status::invalid_argument("dst must be set"))?;
        let req_rel = &request.get_ref().relation;

        if req_rel.is_empty() {
            return Err(Status::invalid_argument("relation must be set"));
        }
        if req_dst.namespace.is_empty() {
            return Err(Status::invalid_argument("dst.namespace must be set"));
        }
        if req_dst.id.is_empty() {
            return Err(Status::invalid_argument("dst.id must be set"));
        }

        if !graph.has_recursive(
            (API_KEY_NS, &*api_key),
            "read",
            (NAMESPACE_NS, &*req_dst.namespace),
            u32::MAX,
        ) {
            return Err(Status::permission_denied(
                "missing dst.namespace read permissions",
            ))?;
        }

        if req_src.namespace.is_empty() {
            return Err(Status::invalid_argument("src.namespace must be set"));
        }
        if req_src.id.is_empty() {
            return Err(Status::invalid_argument("src.id must be set"));
        }
        let src: ObjectOrSet = if let Some(req_src_relation) = req_src.relation.as_deref() {
            if req_src_relation.is_empty() {
                return Err(Status::invalid_argument("src.relation must be set"));
            }

            (&*req_src.namespace, &*req_src.id, req_src_relation).into()
        } else {
            (&*req_src.namespace, &*req_src.id).into()
        };

        let related = graph.has_recursive(
            src,
            req_rel.as_str(),
            (&*req_dst.namespace, &*req_dst.id),
            u32::MAX,
        );

        Ok(Response::new(QueryIsRelatedToRes { related }))
    }
    async fn get_related(
        &self,
        request: Request<QueryGetRelatedReq>,
    ) -> Result<Response<QueryGetRelatedRes>, Status> {
        let graph = self.graph.lock().await;

        let api_key = api_key_from_req(request.metadata(), &self.api_keys).await?;

        let req_dst = request
            .get_ref()
            .dst
            .as_ref()
            .ok_or(Status::invalid_argument("dst must be set"))?;
        let req_rel = &request.get_ref().relation;

        if req_dst.namespace.is_empty() {
            return Err(Status::invalid_argument("dst.namespace must be set"));
        }
        if req_dst.id.is_empty() {
            return Err(Status::invalid_argument("dst.id must be set"));
        }

        let req_namespace = &request.get_ref().namespace;
        let req_depth = &request.get_ref().depth;

        if !graph.has_recursive(
            (API_KEY_NS, &*api_key),
            "read",
            (NAMESPACE_NS, &*req_dst.namespace),
            u32::MAX,
        ) {
            return Err(Status::permission_denied(
                "missing dst.namespace read permissions",
            ))?;
        }

        let dst = (req_dst.namespace.as_ref(), req_dst.id.as_ref());

        let objects = graph
            .related_to(
                dst,
                req_rel.as_deref(),
                req_namespace.as_deref(),
                req_depth.unwrap_or(u32::MAX),
            )
            .into_iter()
            .map(|x| QueryGetRelatedItem {
                src: Some(Object {
                    namespace: x.1.namespace.to_string(),
                    id: x.1.id.to_string(),
                }),
                relation: x.0 .0.to_string(),
            })
            .collect::<_>();

        Ok(Response::new(QueryGetRelatedRes { objects }))
    }
    async fn get_relations(
        &self,
        request: Request<QueryGetRelationsReq>,
    ) -> Result<Response<QueryGetRelationsRes>, Status> {
        let graph = self.graph.lock().await;

        let api_key = api_key_from_req(request.metadata(), &self.api_keys).await?;

        let req_src = request
            .get_ref()
            .src
            .as_ref()
            .ok_or(Status::invalid_argument("src must be set"))?;
        let src = (&*req_src.namespace, &*req_src.id);

        let req_rel = &request.get_ref().relation;
        let req_namespace = &request.get_ref().namespace;
        let req_depth = &request.get_ref().depth;

        if !graph.has_recursive(
            (API_KEY_NS, &*api_key),
            "read",
            (NAMESPACE_NS, &*req_src.namespace),
            u32::MAX,
        ) {
            return Err(Status::permission_denied(
                "missing src.namespace read permissions",
            ))?;
        }

        let related = graph
            .relations(
                src,
                req_rel.as_deref(),
                req_namespace.as_deref(),
                req_depth.unwrap_or(u32::MAX),
            )
            .into_iter()
            .map(|x| QueryGetRelationsItem {
                dst: Some(Object {
                    namespace: x.1.namespace.to_string(),
                    id: x.1.id.to_string(),
                }),
                relation: x.0 .0.to_string(),
            })
            .collect::<_>();

        Ok(Response::new(QueryGetRelationsRes { related }))
    }
}

async fn api_key_from_req(
    metadata: &MetadataMap,
    api_keys: &Arc<Mutex<HashMap<String, String>>>,
) -> Result<String, Status> {
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
    Ok(api_key.to_string())
}
