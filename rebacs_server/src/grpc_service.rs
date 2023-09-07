use std::sync::Arc;

use jsonwebtoken::{decode, DecodingKey, TokenData, Validation};
use log::info;
use rebacs_core::{RObject, RObjectOrSet, RSet, RelationGraph};
use serde::Deserialize;
use tokio::sync::mpsc::Sender;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

use crate::rebacs_proto::{
    exists_req, grant_req, is_permitted_req, rebac_service_server, revoke_req, ExistsReq,
    ExistsRes, ExpandReq, ExpandRes, ExpandResItem, GrantReq, GrantRes, IsPermittedReq,
    IsPermittedRes, Object, RevokeReq, RevokeRes, Set,
};

#[derive(Clone)]
pub struct RebacService {
    pub graph: Arc<RelationGraph>,
    pub oidc_pubkey: DecodingKey,
    pub oidc_validation: Validation,
    pub save_trigger: Sender<()>,
}

const USER_NS: &str = "user";

#[tonic::async_trait]
impl rebac_service_server::RebacService for RebacService {
    async fn grant(&self, request: Request<GrantReq>) -> Result<Response<GrantRes>, Status> {
        let token =
            extract_token(request.metadata(), &self.oidc_pubkey, &self.oidc_validation).await?;
        let user: RObject = (USER_NS, token.claims.sub.as_str()).into();

        let src = extract_src(request.get_ref().src.clone(), &token.claims.sub)?;
        let dst = extract_dst(request.get_ref().dst.as_ref())?;

        if !self.graph.can_write(&user, &dst, None).await {
            return Err(Status::permission_denied(
                "token not permitted to grant permissions on dst",
            ));
        }
        info!(
            "created relation {}:{}#{}@{}:{}#{} for {}",
            dst.namespace(),
            dst.id(),
            dst.relation(),
            src.namespace(),
            src.id(),
            src.relation().map(|x| x.to_string()).unwrap_or_default(),
            token.claims.sub
        );

        self.graph.insert(src, &dst).await;

        self.save_trigger.send(()).await.unwrap();

        Ok(Response::new(GrantRes {}))
    }
    async fn revoke(&self, request: Request<RevokeReq>) -> Result<Response<RevokeRes>, Status> {
        let token =
            extract_token(request.metadata(), &self.oidc_pubkey, &self.oidc_validation).await?;
        let user: RObject = (USER_NS, token.claims.sub.as_str()).into();

        let src = extract_src(request.get_ref().src.clone(), &token.claims.sub)?;
        let dst = extract_dst(request.get_ref().dst.as_ref())?;

        if !self.graph.can_write(&user, &dst, None).await {
            return Err(Status::permission_denied(
                "token not permitted to revoke permissions on dst",
            ));
        }

        self.graph.remove(&src, &dst).await;

        info!(
            "delted relation {}:{}#{}@{}:{}#{} for {}",
            dst.namespace(),
            dst.id(),
            dst.relation(),
            src.namespace(),
            src.id(),
            src.relation().map(|x| x.to_string()).unwrap_or_default(),
            token.claims.sub
        );

        self.save_trigger.send(()).await.unwrap();

        Ok(Response::new(RevokeRes {}))
    }
    async fn exists(&self, request: Request<ExistsReq>) -> Result<Response<ExistsRes>, Status> {
        let token =
            extract_token(request.metadata(), &self.oidc_pubkey, &self.oidc_validation).await?;

        let src = extract_src(request.get_ref().src.clone(), &token.claims.sub)?;
        let dst = extract_dst(request.get_ref().dst.as_ref())?;

        let exists = self.graph.has(src, &dst).await;

        Ok(Response::new(ExistsRes { exists }))
    }

    async fn is_permitted(
        &self,
        request: Request<IsPermittedReq>,
    ) -> Result<Response<IsPermittedRes>, Status> {
        let token =
            extract_token(request.metadata(), &self.oidc_pubkey, &self.oidc_validation).await?;

        let src = extract_src(request.get_ref().src.clone(), &token.claims.sub)?;
        let dst = extract_dst(request.get_ref().dst.as_ref())?;

        let permitted = self.graph.check(src, &dst, None).await;

        Ok(Response::new(IsPermittedRes { permitted }))
    }

    async fn expand(&self, request: Request<ExpandReq>) -> Result<Response<ExpandRes>, Status> {
        let token =
            extract_token(request.metadata(), &self.oidc_pubkey, &self.oidc_validation).await?;
        let dst = extract_dst(request.get_ref().dst.as_ref())?;

        let user: RObject = (USER_NS, token.claims.sub.as_str()).into();
        if !self.graph.can_write(&user, &dst, None).await {
            return Err(Status::permission_denied(
                "token not permitted to expand permissions on dst",
            ));
        }

        let expanded = self
            .graph
            .expand(&dst)
            .await
            .into_iter()
            .map(|(v, path)| ExpandResItem {
                src: Some(Object {
                    namespace: v.namespace().to_string(),
                    id: v.id().to_string(),
                }),
                path: path
                    .into_iter()
                    .map(|w| Set {
                        namespace: w.namespace().to_string(),
                        id: w.id().to_string(),
                        relation: w.relation().to_string(),
                    })
                    .collect(),
            })
            .collect();

        Ok(Response::new(ExpandRes { expanded }))
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

fn extract_src<'a>(
    src: Option<impl Into<RObjectOrSet<'a>>>,
    subject: &str,
) -> Result<RObjectOrSet<'a>, Status> {
    if let Some(src) = src {
        let src: RObjectOrSet<'_> = src.into();
        if src.namespace().is_empty() {
            Err(Status::invalid_argument("src.namespace must be set"))
        } else if src.id().is_empty() {
            Err(Status::invalid_argument("src.id must be set"))
        } else {
            Ok(src)
        }
    } else {
        Ok((USER_NS, subject, None).into())
    }
}

fn extract_dst(dst: Option<&Set>) -> Result<RSet, Status> {
    let dst = dst
        .as_ref()
        .ok_or(Status::invalid_argument("dst must be set"))?;
    let dst: RSet = (dst.namespace.clone(), dst.id.clone(), dst.relation.clone()).into();

    if dst.namespace().is_empty() {
        return Err(Status::invalid_argument("dst.namespace must be set"));
    }
    if dst.id().is_empty() {
        return Err(Status::invalid_argument("dst.id must be set"));
    }

    Ok(dst)
}

macro_rules! from_src {
    ($src:path) => {
        impl From<$src> for RObjectOrSet<'_> {
            fn from(value: $src) -> Self {
                use $src;
                match value {
                    Src::SrcObj(obj) => (obj.namespace, obj.id, None).into(),
                    Src::SrcSet(set) => (set.namespace, set.id, Some(set.relation)).into(),
                }
            }
        }
    };
}
from_src!(grant_req::Src);
from_src!(revoke_req::Src);
from_src!(exists_req::Src);
from_src!(is_permitted_req::Src);
