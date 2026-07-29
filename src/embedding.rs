use crate::error::AppError;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::time::Duration;

pub const EMBEDDING_DIMS: usize = 1024;

#[derive(Debug, Clone)]
pub struct Embedding {
    pub values: Vec<f32>,
    pub model: String,
}

#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    async fn embed(&self, text: &str) -> Result<Embedding, AppError>;
    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Embedding>, AppError> {
        let mut embeddings = Vec::with_capacity(texts.len());
        for text in texts {
            embeddings.push(self.embed(text).await?);
        }
        Ok(embeddings)
    }
}

#[derive(Clone)]
pub struct HttpEmbedder {
    client: reqwest::Client,
    endpoint: reqwest::Url,
    expected_model: String,
}

#[derive(Serialize)]
struct EmbedRequest<'a> {
    texts: Vec<&'a str>,
}

#[derive(Deserialize)]
struct EmbedResponse {
    embeddings: Vec<Vec<f32>>,
    model: String,
    dims: usize,
    count: usize,
}

impl HttpEmbedder {
    pub fn new(base_url: &str, expected_model: String) -> Result<Self, AppError> {
        let mut endpoint = reqwest::Url::parse(base_url)
            .map_err(|error| AppError::Invalid(format!("invalid embedder URL: {error}")))?;
        if !matches!(endpoint.scheme(), "http" | "https") {
            return Err(AppError::Invalid(
                "embedder URL must use http or https".into(),
            ));
        }
        if !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(AppError::Invalid(
                "embedder URL cannot contain credentials, query, or fragment".into(),
            ));
        }
        let host = endpoint
            .host_str()
            .ok_or_else(|| AppError::Invalid("embedder URL requires a host".into()))?;
        let private_host = host == "localhost"
            || host.parse::<IpAddr>().is_ok_and(|ip| match ip {
                IpAddr::V4(ip) => ip.is_private() || ip.is_loopback(),
                IpAddr::V6(ip) => ip.is_loopback() || (ip.segments()[0] & 0xfe00) == 0xfc00,
            });
        if !private_host {
            return Err(AppError::Invalid(
                "embedder must be a literal private/loopback address or localhost".into(),
            ));
        }
        endpoint.set_path("/embed");
        endpoint.set_query(None);
        endpoint.set_fragment(None);
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(1))
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| AppError::Internal(error.to_string()))?;
        Ok(Self {
            client,
            endpoint,
            expected_model,
        })
    }
}

#[async_trait]
impl EmbeddingProvider for HttpEmbedder {
    async fn embed(&self, text: &str) -> Result<Embedding, AppError> {
        self.embed_batch(&[text.to_string()])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| AppError::Internal("embedder returned no vector".into()))
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Embedding>, AppError> {
        if texts.is_empty() || texts.len() > 64 {
            return Err(AppError::Invalid(
                "embedding batch must contain 1..64 texts".into(),
            ));
        }
        let response = self
            .client
            .post(self.endpoint.clone())
            .json(&EmbedRequest {
                texts: texts.iter().map(String::as_str).collect(),
            })
            .send()
            .await
            .map_err(|_| AppError::Internal("embedder unavailable".into()))?
            .error_for_status()
            .map_err(|_| AppError::Internal("embedder rejected request".into()))?
            .json::<EmbedResponse>()
            .await
            .map_err(|error| AppError::Internal(format!("invalid embedder response: {error}")))?;
        if response.count != texts.len()
            || response.dims != EMBEDDING_DIMS
            || response.embeddings.len() != texts.len()
            || response.model != self.expected_model
        {
            return Err(AppError::Internal("embedder contract mismatch".into()));
        }
        let mut embeddings = Vec::with_capacity(texts.len());
        for values in response.embeddings {
            if values.len() != EMBEDDING_DIMS
                || values.iter().any(|v| !v.is_finite())
                || values.iter().all(|v| *v == 0.0)
            {
                return Err(AppError::Internal(
                    "embedder returned an invalid vector".into(),
                ));
            }
            embeddings.push(Embedding {
                values,
                model: response.model.clone(),
            });
        }
        Ok(embeddings)
    }
}
