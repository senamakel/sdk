//! Typed request/response DTOs for the direct OpenRouter media surface
//! (`/agent-integrations/openrouter/{images,videos}`).
//!
//! [`super::openrouter`] already exposes this surface with `impl Serialize`
//! request bodies and [`crate::api::types::DynamicResponse`] / loosely-typed
//! replies (mirroring how the OpenAI-shaped chat/completions/messages routes
//! on that surface are handled, since OpenRouter's own API is the contract
//! there). Media has a much smaller, stable request/response shape — OpenRouter's
//! `ImageGenerationRequest` / `VideoGenerationRequest` — so this module adds a
//! fully typed alternative on top of the same routes. Every field is optional
//! except `model` (and `prompt` for images), and `extra` catches any upstream
//! field this module does not yet know about, so a typed caller never loses
//! access to a new OpenRouter parameter.
//!
//! Field names and shapes are taken from OpenRouter's published OpenAPI
//! schemas (`ImageGenerationRequest`, `VideoGenerationRequest`, `FrameImage`,
//! `InputReference`, `ContentPartImage`), confirmed 2026-09-24 against
//! <https://openrouter.ai/docs/api/api-reference/images/generate-an-image.md>
//! and
//! <https://openrouter.ai/docs/api/api-reference/video-generation/submit-a-video-generation-request.md>.

use super::AgentIntegrationsApi;
use crate::{enc, Error, QueryParam};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// `image_url` reference used by both `input_references` and `frame_images`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct OpenRouterImageUrl {
    pub url: String,
}

/// An image reference (`{ type: "image_url", image_url: { url } }`), used for
/// `input_references` on both images and videos. OpenRouter's `InputReference`
/// also allows `audio_url`/`video_url` variants on the video route (honored by
/// providers that support them); those are not modeled as a separate typed
/// variant here — pass them via a raw [`serde_json::Value`] in
/// [`OpenRouterVideoRequest::extra`] under `input_references` if needed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContentPartImage {
    #[serde(rename = "type")]
    pub kind: String,
    pub image_url: OpenRouterImageUrl,
}

impl ContentPartImage {
    /// Build an `{ type: "image_url", image_url: { url } }` reference.
    pub fn image_url(url: impl Into<String>) -> Self {
        Self {
            kind: "image_url".to_owned(),
            image_url: OpenRouterImageUrl { url: url.into() },
        }
    }
}

/// A first/last-frame image for video generation
/// (`ContentPartImage` plus `frame_type`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FrameImage {
    #[serde(rename = "type")]
    pub kind: String,
    pub image_url: OpenRouterImageUrl,
    /// `"first_frame"` or `"last_frame"`.
    pub frame_type: String,
}

impl FrameImage {
    pub fn first_frame(url: impl Into<String>) -> Self {
        Self {
            kind: "image_url".to_owned(),
            image_url: OpenRouterImageUrl { url: url.into() },
            frame_type: "first_frame".to_owned(),
        }
    }

    pub fn last_frame(url: impl Into<String>) -> Self {
        Self {
            kind: "image_url".to_owned(),
            image_url: OpenRouterImageUrl { url: url.into() },
            frame_type: "last_frame".to_owned(),
        }
    }
}

/// `POST /agent-integrations/openrouter/images` request body
/// (OpenRouter's `ImageGenerationRequest`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct OpenRouterImageRequest {
    /// Bare or `openrouter/`-namespaced slug; the backend accepts either.
    pub model: String,
    pub prompt: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub n: Option<u32>,
    /// Convenience pixel-size shorthand, e.g. `"2048x2048"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<String>,
    /// Normalized resolution tier: `"512"`, `"1K"`, `"2K"`, `"4K"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<String>,
    /// e.g. `"16:9"`, `"1:1"`, `"auto"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aspect_ratio: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quality: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_format: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_compression: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub background: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<i64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_references: Vec<ContentPartImage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Any upstream field this struct does not yet model (e.g. `provider`,
    /// `trace`), merged into the request body verbatim.
    #[serde(flatten, default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

impl OpenRouterImageRequest {
    pub fn new(model: impl Into<String>, prompt: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            prompt: prompt.into(),
            ..Default::default()
        }
    }
}

/// One generated image in [`OpenRouterImageResponse::data`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct OpenRouterImageData {
    pub b64_json: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
}

/// Usage/cost block on an image generation response. `cost` is the real USD
/// OpenRouter charged for this call and is what billing (on the backend) uses.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct OpenRouterImageUsage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost: Option<f64>,
    #[serde(default)]
    pub prompt_tokens: u64,
    #[serde(default)]
    pub completion_tokens: u64,
    #[serde(default)]
    pub total_tokens: u64,
}

/// `POST /agent-integrations/openrouter/images` response
/// (OpenRouter's `ImageGenerationResponse`, forwarded verbatim by the backend).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct OpenRouterImageResponse {
    #[serde(default)]
    pub created: i64,
    #[serde(default)]
    pub data: Vec<OpenRouterImageData>,
    #[serde(default)]
    pub usage: OpenRouterImageUsage,
}

/// `POST /agent-integrations/openrouter/videos` request body
/// (OpenRouter's `VideoGenerationRequest`). Only `model` is required upstream;
/// `prompt` is optional for image/frame-driven generations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct OpenRouterVideoRequest {
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
    /// Duration in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<u32>,
    /// e.g. `"720p"`, `"1080p"`, `"1K"`, `"4K"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<String>,
    /// e.g. `"16:9"`, `"9:16"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aspect_ratio: Option<String>,
    /// Exact pixel dimensions, e.g. `"1280x720"`. Interchangeable with
    /// `resolution` + `aspect_ratio`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generate_audio: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<i64>,
    /// First/last-frame guidance images.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub frame_images: Vec<FrameImage>,
    /// Reference assets (image/audio/video) guiding generation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_references: Vec<ContentPartImage>,
    /// Continue/edit a completed job, per its `id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub callback_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Any upstream field this struct does not yet model (e.g. `provider`,
    /// `trace`, `creativity`, `upscale_factor`), merged verbatim.
    #[serde(flatten, default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

impl OpenRouterVideoRequest {
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            ..Default::default()
        }
    }
}

/// Video bytes plus the upstream `content-type`, since
/// [`crate::HttpClient::send_bytes_query`] does not surface response headers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenRouterVideoContent {
    pub bytes: Vec<u8>,
    /// Upstream `content-type`, when the response carried one.
    pub content_type: Option<String>,
}

impl AgentIntegrationsApi<'_> {
    /// Generate an image against OpenRouter with a typed request/response.
    /// Synchronous — billed at the exact `usage.cost` this response reports,
    /// plus the configured margin. See [`AgentIntegrationsApi::openrouter_create_image`]
    /// for the untyped/passthrough variant.
    pub async fn openrouter_images(
        &self,
        request: &OpenRouterImageRequest,
    ) -> Result<OpenRouterImageResponse, Error> {
        const PATH: &str = "/agent-integrations/openrouter/images";
        let body = serde_json::to_value(request)?;
        if matches!(body.get("stream"), Some(Value::Bool(true))) {
            return Err(Error::StreamingNotSupported(PATH.to_owned()));
        }
        self.send(Method::POST, PATH, &[], Some(&body), true).await
    }

    /// List image-generation models, typed. Equivalent to
    /// [`AgentIntegrationsApi::list_openrouter_image_models`].
    pub async fn openrouter_image_models(
        &self,
        query: &[QueryParam],
    ) -> Result<super::openrouter::OpenRouterMediaModelsResponse, Error> {
        self.send(
            Method::GET,
            "/agent-integrations/openrouter/images/models",
            query,
            None,
            true,
        )
        .await
    }

    /// Submit a video generation job with a typed request. Asynchronous and
    /// billed on submit — see [`AgentIntegrationsApi::get_openrouter_video`]
    /// to poll and [`Self::openrouter_video_content`] / [`Self::openrouter_video_content_with_type`]
    /// to download once `status` is `"completed"`.
    pub async fn openrouter_videos(
        &self,
        request: &OpenRouterVideoRequest,
    ) -> Result<super::openrouter::OpenRouterVideoJob, Error> {
        const PATH: &str = "/agent-integrations/openrouter/videos";
        let body = serde_json::to_value(request)?;
        if matches!(body.get("stream"), Some(Value::Bool(true))) {
            return Err(Error::StreamingNotSupported(PATH.to_owned()));
        }
        self.send(Method::POST, PATH, &[], Some(&body), true).await
    }

    /// List video-generation models, typed. Equivalent to
    /// [`AgentIntegrationsApi::list_openrouter_video_models`].
    pub async fn openrouter_video_models(
        &self,
        query: &[QueryParam],
    ) -> Result<super::openrouter::OpenRouterMediaModelsResponse, Error> {
        self.send(
            Method::GET,
            "/agent-integrations/openrouter/videos/models",
            query,
            None,
            true,
        )
        .await
    }

    /// Download the rendered video along with its upstream `content-type`.
    /// Prefer [`AgentIntegrationsApi::openrouter_video_content`]
    /// when the content type is not needed — this variant makes one extra
    /// header read but the same request.
    pub async fn openrouter_video_content_with_type(
        &self,
        job_id: &str,
        index: Option<u32>,
    ) -> Result<OpenRouterVideoContent, Error> {
        let path = format!(
            "/agent-integrations/openrouter/videos/{}/content",
            enc(job_id)
        );
        let query = [("index", index.map(|i| i.to_string()))];
        let (bytes, content_type) = self
            .bytes_query_with_type(Method::GET, &path, &query)
            .await?;
        Ok(OpenRouterVideoContent {
            bytes,
            content_type,
        })
    }
}
