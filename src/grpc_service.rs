use std::collections::HashMap;
use std::sync::Arc;

use log::info;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

use crate::relation_set::{ObjectOrSet, RelationSet};
use crate::themis_proto::{
    query_service_server::QueryService, relation::Src, relation_service_server::RelationService,
    Empty, ExistsResponse, GetRelatedToResponse, GetRelationsRequest, GetRelationsResponse,
    IsRelatedToResponse, Relation, Set,
};

#[derive(Clone)]
pub struct GraphService {
    pub api_keys: Arc<Mutex<HashMap<String, String>>>,
    pub graph: Arc<Mutex<RelationSet>>,
    pub save_trigger: Sender<()>,
}

#[tonic::async_trait]
impl RelationService for GraphService {
    async fn create(&self, request: Request<Relation>) -> Result<Response<Empty>, Status> {
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

        if !graph.has(
            ("themis_key", &*api_key),
            "write",
            ("themis_ns", &*req_dst.namespace),
        ) {
            return Err(Status::permission_denied(
                "missing dst.namespace write permissions",
            ))?;
        }

        let src: Result<ObjectOrSet, Status> = match req_src {
            Src::SrcObj(obj) => {
                if obj.namespace.is_empty() {
                    return Err(Status::invalid_argument("src.namespace must be set"));
                }
                if obj.id.is_empty() {
                    return Err(Status::invalid_argument("src.id must be set"));
                }

                Ok((&*obj.namespace, &*obj.id).into())
            }
            Src::SrcSet(set) => {
                if set.namespace.is_empty() {
                    return Err(Status::invalid_argument("src.namespace must be set"));
                }
                if set.id.is_empty() {
                    return Err(Status::invalid_argument("src.id must be set"));
                }
                if set.relation.is_empty() {
                    return Err(Status::invalid_argument("src.relation must be set"));
                }

                Ok((&*set.namespace, &*set.id, &*set.relation).into())
            }
        };
        let src = src?;

        graph.insert(
            src.clone(),
            req_rel.clone(),
            (req_dst.namespace.clone(), req_dst.id.clone()),
        );

        info!("created relation");

        self.save_trigger.send(()).await.unwrap();

        Ok(Response::new(Empty {}))
    }
    async fn delete(&self, request: Request<Relation>) -> Result<Response<Empty>, Status> {
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

        if !graph.has(
            ("themis_key", &*api_key),
            "write",
            ("themis_ns", &*req_dst.namespace),
        ) {
            return Err(Status::permission_denied(
                "missing dst.namespace write permissions",
            ))?;
        }
        let src: Result<ObjectOrSet, Status> = match req_src {
            Src::SrcObj(obj) => {
                if obj.namespace.is_empty() {
                    return Err(Status::invalid_argument("src.namespace must be set"));
                }
                if obj.id.is_empty() {
                    return Err(Status::invalid_argument("src.id must be set"));
                }

                Ok((&*obj.namespace, &*obj.id).into())
            }
            Src::SrcSet(set) => {
                if set.namespace.is_empty() {
                    return Err(Status::invalid_argument("src.namespace must be set"));
                }
                if set.id.is_empty() {
                    return Err(Status::invalid_argument("src.id must be set"));
                }
                if set.relation.is_empty() {
                    return Err(Status::invalid_argument("src.relation must be set"));
                }

                Ok((&*set.namespace, &*set.id, &*set.relation).into())
            }
        };
        let src = src?;

        graph.remove(src, req_rel.as_str(), (&*req_dst.namespace, &*req_dst.id));

        info!("deleted relation");

        self.save_trigger.send(()).await.unwrap();

        Ok(Response::new(Empty {}))
    }
    async fn exists(&self, request: Request<Relation>) -> Result<Response<ExistsResponse>, Status> {
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

        if !graph.has(
            ("themis_key", &*api_key),
            "read",
            ("themis_ns", &*req_dst.namespace),
        ) {
            return Err(Status::permission_denied(
                "missing dst.namespace write permissions",
            ))?;
        }
        let src: Result<ObjectOrSet, Status> = match req_src {
            Src::SrcObj(obj) => {
                if obj.namespace.is_empty() {
                    return Err(Status::invalid_argument("src.namespace must be set"));
                }
                if obj.id.is_empty() {
                    return Err(Status::invalid_argument("src.id must be set"));
                }

                Ok((&*obj.namespace, &*obj.id).into())
            }
            Src::SrcSet(set) => {
                if set.namespace.is_empty() {
                    return Err(Status::invalid_argument("src.namespace must be set"));
                }
                if set.id.is_empty() {
                    return Err(Status::invalid_argument("src.id must be set"));
                }
                if set.relation.is_empty() {
                    return Err(Status::invalid_argument("src.relation must be set"));
                }

                Ok((&*set.namespace, &*set.id, &*set.relation).into())
            }
        };
        let src = src?;

        let exists = graph.has(src, req_rel.as_str(), (&*req_dst.namespace, &*req_dst.id));

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

        if !graph.has(
            ("themis_key", &*api_key),
            "read",
            ("themis_ns", &*req_dst.namespace),
        ) {
            return Err(Status::permission_denied(
                "missing dst.namespace write permissions",
            ))?;
        }

        let src: Result<ObjectOrSet, Status> = match req_src {
            Src::SrcObj(obj) => {
                if obj.namespace.is_empty() {
                    return Err(Status::invalid_argument("src.namespace must be set"));
                }
                if obj.id.is_empty() {
                    return Err(Status::invalid_argument("src.id must be set"));
                }

                Ok((&*obj.namespace, &*obj.id).into())
            }
            Src::SrcSet(set) => {
                if set.namespace.is_empty() {
                    return Err(Status::invalid_argument("src.namespace must be set"));
                }
                if set.id.is_empty() {
                    return Err(Status::invalid_argument("src.id must be set"));
                }
                if set.relation.is_empty() {
                    return Err(Status::invalid_argument("src.relation must be set"));
                }

                Ok((&*set.namespace, &*set.id, &*set.relation).into())
            }
        };
        let src = src?;

        let related = graph.has_recursive(
            src,
            req_rel.as_str(),
            (&*req_dst.namespace, &*req_dst.id),
            u32::MAX,
        );

        Ok(Response::new(IsRelatedToResponse { related }))
    }
    async fn get_related_to(
        &self,
        request: Request<Set>,
    ) -> Result<Response<GetRelatedToResponse>, Status> {
        //let graph = self.graph.lock().await;

        //authenticate(
        //    request.metadata(),
        //    &graph,
        //    &self.api_keys,
        //    &request.get_ref().namespace,
        //    "read",
        //)
        //.await?;

        //let obj = graph
        //    .get_node(&request.get_ref().namespace, &request.get_ref().id)
        //    .ok_or(Status::not_found("object not found"))?;

        //let rel = graph::Relation::new(&request.get_ref().relation);

        //Ok(Response::new(GetRelatedToResponse {
        //    objects: graph
        //        .related_to(obj, rel)
        //        .into_iter()
        //        .map(|x| {
        //            let obj = graph.object_from_ref(&x);
        //            Object {
        //                namespace: obj.namespace.to_string(),
        //                id: obj.id,
        //            }
        //        })
        //        .collect::<Vec<_>>(),
        //}))
        todo!()
    }
    async fn get_relations(
        &self,
        request: Request<GetRelationsRequest>,
    ) -> Result<Response<GetRelationsResponse>, Status> {
        //let graph = self.graph.lock().await;

        //if request.get_ref().relation.is_empty() {
        //    return Err(Status::invalid_argument("relation must be set"));
        //}

        //let obj = request
        //    .get_ref()
        //    .object
        //    .as_ref()
        //    .ok_or(Status::invalid_argument("object must be set"))?;

        //authenticate(
        //    request.metadata(),
        //    &graph,
        //    &self.api_keys,
        //    &obj.namespace,
        //    "read",
        //)
        //.await?;

        //let obj = graph
        //    .get_node(&obj.namespace, &obj.id)
        //    .ok_or(Status::not_found("object not found"))?;

        //Ok(Response::new(GetRelationsResponse {
        //    objects: graph
        //        .relations(ObjectRelation(
        //            obj,
        //            graph::Relation::new(&request.get_ref().relation),
        //        ))
        //        .into_iter()
        //        .map(|x| {
        //            let obj = graph.object_from_ref(&x);
        //            Object {
        //                namespace: obj.namespace.to_string(),
        //                id: obj.id,
        //            }
        //        })
        //        .collect::<Vec<_>>(),
        //}))
        todo!()
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
