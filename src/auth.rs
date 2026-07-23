use crate::error::AppError;
use axum::{extract::Request, middleware::Next, response::Response};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use subtle::ConstantTimeEq;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Identity {
    pub tenant_id: Uuid,
    pub user_id: Uuid,
    pub consumer_id: Uuid,
    pub actor: String,
    pub role: TokenRole,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TokenRole {
    Reader,
    Writer,
    Verifier,
    Owner,
}

impl TokenRole {
    pub fn can_write(self) -> bool {
        !matches!(self, Self::Reader)
    }
    pub fn is_owner(self) -> bool {
        self == Self::Owner
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenGrant {
    pub token_sha256: String,
    pub tenant_id: Uuid,
    pub user_id: Uuid,
    pub consumer_id: Uuid,
    pub actor: String,
    pub role: TokenRole,
}

#[derive(Clone)]
pub struct AuthConfig {
    grants: Vec<ParsedGrant>,
}

#[derive(Clone)]
struct ParsedGrant {
    token_sha256: [u8; 32],
    identity: Identity,
}

impl AuthConfig {
    pub fn new(grants: Vec<TokenGrant>) -> Result<Self, String> {
        if grants.is_empty() {
            return Err("at least one token grant is required".into());
        }
        let mut parsed = Vec::with_capacity(grants.len());
        let mut seen = HashSet::with_capacity(grants.len());
        for grant in grants {
            let bytes = hex::decode(&grant.token_sha256).map_err(|_| "token hash must be hex")?;
            let token_sha256: [u8; 32] = bytes
                .try_into()
                .map_err(|_| "token hash must be 32 bytes")?;
            if !seen.insert(token_sha256) {
                return Err("duplicate token hash".into());
            }
            parsed.push(ParsedGrant {
                token_sha256,
                identity: Identity {
                    tenant_id: grant.tenant_id,
                    user_id: grant.user_id,
                    consumer_id: grant.consumer_id,
                    actor: grant.actor,
                    role: grant.role,
                },
            });
        }
        Ok(Self { grants: parsed })
    }

    fn authenticate(&self, token: &str) -> Option<Identity> {
        let candidate: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let mut matched = None;
        for grant in &self.grants {
            if bool::from(candidate.ct_eq(&grant.token_sha256)) {
                matched = Some(grant.identity.clone());
            }
        }
        matched
    }
}

pub async fn require_auth(
    axum::extract::State(config): axum::extract::State<AuthConfig>,
    mut request: Request,
    next: Next,
) -> Result<Response, AppError> {
    let token = request
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|value| !value.is_empty())
        .ok_or(AppError::Unauthorized)?;
    let identity = config.authenticate(token).ok_or(AppError::Unauthorized)?;
    request.extensions_mut().insert(identity);
    Ok(next.run(request).await)
}
