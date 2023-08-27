use std::sync::Arc;

use jsonwebtoken::{decode, DecodingKey, TokenData, Validation};
use log::info;
use serde::Deserialize;
use tokio::sync::mpsc::Sender;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

use crate::rebacs_proto::Object;
use crate::rebacs_proto::{
    rebac_service_server, ExistsReq, ExistsRes, GrantReq, GrantRes, IsPermittedReq, IsPermittedRes,
    RevokeReq, RevokeRes,
};
use crate::relation_set::{NodeId, RelationSet};

#[derive(Clone)]
pub struct RebacService {
    pub graph: Arc<RelationSet>,
    pub oidc_pubkey: DecodingKey,
    pub oidc_validation: Validation,
    pub save_trigger: Sender<()>,
}

const NAMESPACE_NS: &str = "namespace";
const USER_NS: &str = "user";
const GRANT_RELATION: &str = "grant";
const REVOKE_RELATION: &str = "revoke";

#[tonic::async_trait]
impl rebac_service_server::RebacService for RebacService {
    async fn grant(&self, request: Request<GrantReq>) -> Result<Response<GrantRes>, Status> {
        let token =
            extract_token(request.metadata(), &self.oidc_pubkey, &self.oidc_validation).await?;

        let (src, dst) = extract_src_dst(&request.get_ref().src, &request.get_ref().dst)?;

        if !is_permitted(&token, &dst, GRANT_RELATION, &self.graph).await {
            return Err(Status::permission_denied(
                "token not permitted to grant permissions on dst",
            ));
        }

        info!(
            "created relation {}:{}#{}@{}:{}#{} for {}",
            dst.namespace,
            dst.id,
            dst.relation.clone().unwrap_or_default(),
            src.namespace,
            src.id,
            src.relation.clone().unwrap_or_default(),
            token.claims.sub
        );

        self.graph.insert(src, dst).await;

        self.save_trigger.send(()).await.unwrap();

        Ok(Response::new(GrantRes {}))
    }
    async fn revoke(&self, request: Request<RevokeReq>) -> Result<Response<RevokeRes>, Status> {
        let token =
            extract_token(request.metadata(), &self.oidc_pubkey, &self.oidc_validation).await?;

        let (src, dst) = extract_src_dst(&request.get_ref().src, &request.get_ref().dst)?;

        if !is_permitted(&token, &dst, REVOKE_RELATION, &self.graph).await {
            return Err(Status::permission_denied(
                "token not permitted to revoke permissions on dst",
            ));
        }

        self.graph
            .remove(
                (
                    src.namespace.to_string(),
                    src.id.to_string(),
                    src.relation.clone(),
                ),
                (dst.namespace.clone(), dst.id.clone(), dst.relation.clone()),
            )
            .await;

        info!(
            "delted relation {}:{}#{}@{}:{}#{} for {}",
            dst.namespace,
            dst.id,
            dst.relation.clone().unwrap_or_default(),
            src.namespace,
            src.id,
            src.relation.clone().unwrap_or_default(),
            token.claims.sub
        );

        self.save_trigger.send(()).await.unwrap();

        Ok(Response::new(RevokeRes {}))
    }
    async fn exists(&self, request: Request<ExistsReq>) -> Result<Response<ExistsRes>, Status> {
        let token =
            extract_token(request.metadata(), &self.oidc_pubkey, &self.oidc_validation).await?;

        let (src, dst) = extract_src_dst(&request.get_ref().src, &request.get_ref().dst)?;

        let exists = self.graph.has(src, dst).await;

        Ok(Response::new(ExistsRes { exists }))
    }

    async fn is_permitted(
        &self,
        request: Request<IsPermittedReq>,
    ) -> Result<Response<IsPermittedRes>, Status> {
        let token =
            extract_token(request.metadata(), &self.oidc_pubkey, &self.oidc_validation).await?;

        let (src, dst) = extract_src_dst(&request.get_ref().src, &request.get_ref().dst)?;

        let permitted = self.graph.has_recursive(src, dst, None).await;

        Ok(Response::new(IsPermittedRes { permitted }))
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Claims {
    pub aud: Vec<String>,
    pub exp: usize,
    pub iat: usize,
    pub iss: String,
    pub sub: String,
    pub azp: String,

    pub name: Option<String>,
    pub preferred_username: Option<String>,
    pub given_name: Option<String>,
    pub family_name: Option<String>,
    pub email: Option<String>,
}

async fn extract_token(
    metadata: &MetadataMap,
    pubkey: &DecodingKey,
    validation: &Validation,
) -> Result<TokenData<Claims>, Status> {
    let token = metadata
        .get("authorization")
        .map(|x| x.to_str().unwrap())
        .ok_or(Status::unauthenticated("authorization header required"))?;

    let token = decode::<Claims>(token, pubkey, validation)
        .map_err(|_| Status::unauthenticated("authorization header invalid"))?;

    Ok(token)
}

async fn is_permitted(
    token: &TokenData<Claims>,
    dst: &NodeId,
    relation: &str,
    graph: &RelationSet,
) -> bool {
    let s1 = graph
        .has_recursive(
            (USER_NS, token.claims.sub.as_str()),
            (dst.namespace.as_str(), dst.id.as_str(), relation),
            None,
        )
        .await;

    let s2 = graph
        .has_recursive(
            (USER_NS, token.claims.sub.as_str()),
            (NAMESPACE_NS, dst.namespace.as_str(), relation),
            None,
        )
        .await;

    s1 || s2
}

fn extract_src_dst(src: &Option<Object>, dst: &Option<Object>) -> Result<(NodeId, NodeId), Status> {
    let src = src
        .as_ref()
        .ok_or(Status::invalid_argument("src must be set"))?;
    let src: NodeId = (src.namespace.clone(), src.id.clone(), src.relation.clone()).into();
    let dst = dst
        .as_ref()
        .ok_or(Status::invalid_argument("dst must be set"))?;
    let dst: NodeId = (dst.namespace.clone(), dst.id.clone(), dst.relation.clone()).into();

    if dst.namespace.is_empty() {
        return Err(Status::invalid_argument("dst.namespace must be set"));
    }
    if dst.id.is_empty() {
        return Err(Status::invalid_argument("dst.id must be set"));
    }

    if src.namespace.is_empty() {
        return Err(Status::invalid_argument("src.namespace must be set"));
    }
    if src.id.is_empty() {
        return Err(Status::invalid_argument("src.id must be set"));
    }

    Ok((src, dst))
}
