//! Sarvam AI: Indic speech-to-text (Saaras), text-to-speech (Bulbul), chat
//! (`sarvam-105b`) and a metered streaming relay for live transcription and
//! live speech.
//!
//! Speech is billed at Sarvam's list price (₹30/hour of audio, ₹30 per 10,000
//! characters) plus the integration margin; chat is billed at cost. Field
//! names follow Sarvam's own API (`snake_case`).
//!
//! # Streaming relay protocol
//!
//! [`AgentIntegrationsApi::sarvam_create_live_session`] returns a single-use
//! ticket and a `wsUrl`. Open a plain WebSocket to `wsUrl` within 60 seconds
//! and speak Sarvam's streaming protocol:
//!
//! - `transcribe`: send `{"event":"audio_input","audio":"<base64 PCM>"}` in the
//!   `encoding` and `sample_rate` set in the [`SarvamLiveTranscription`]
//!   request (default 16 kHz `linear16`), plus `flush`, `speech_start`,
//!   `speech_end`, `ping` and `end` events; receive `session.begin`, `vad.*`, `transcript.partial`,
//!   `transcript.final` and `session.end`.
//! - `speech`: send `{"type":"text","data":{"text":"..."}}`, `{"type":"flush"}`
//!   and `{"type":"ping"}`; receive `audio` chunks and a `final` event.
//!
//! - `agent`: a Sarvam Voice Agents conversation; send `client.media.*` frames
//!   and answer `server.system.ping` with `client.system.pong`; receive
//!   Sarvam's `server.*` messages.
//!
//! The session configuration is fixed when the ticket is minted, so `config`
//! frames the client sends are dropped. The relay closes with one of the
//! [`SARVAM_LIVE_CLOSE_UNAUTHORIZED`] family of codes, or `1000` after
//! Sarvam's `session.end`.

use super::AgentIntegrationsApi;
use crate::api::types::DynamicResponse;
use crate::{enc, Error};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Relay close code: the ticket is missing, expired, or already used.
pub const SARVAM_LIVE_CLOSE_UNAUTHORIZED: u16 = 4401;
/// Relay close code: the balance no longer covers the per-session reserve.
pub const SARVAM_LIVE_CLOSE_INSUFFICIENT_CREDITS: u16 = 4402;
/// Relay close code: idle timeout or maximum session duration reached.
pub const SARVAM_LIVE_CLOSE_TIMEOUT: u16 = 4408;
/// Relay close code: the upstream Sarvam connection failed.
pub const SARVAM_LIVE_CLOSE_UPSTREAM_ERROR: u16 = 1011;

/// Form fields of `POST /agent-integrations/sarvam/speech-to-text`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct SarvamSpeechToTextOptions {
    /// `saaras:v4` (default) or `saaras:v3`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// `transcribe` (default), `translate`, `verbatim`, `translit` or `codemix`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// BCP-47 code such as `hi-IN`, or `unknown` to auto-detect.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub with_timestamps: Option<bool>,
}

/// Sarvam's transcript plus the measured duration and the amount charged.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct SarvamSpeechToTextResponse {
    #[serde(default)]
    pub transcript: String,
    #[serde(default)]
    pub language_code: Option<String>,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub timestamps: Option<Value>,
    #[serde(default, rename = "durationSeconds")]
    pub duration_seconds: f64,
    #[serde(default, rename = "costUsd")]
    pub cost_usd: f64,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Body of `POST /agent-integrations/sarvam/text-to-speech`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct SarvamTextToSpeechRequest {
    /// At most 2500 characters on `bulbul:v3`, 1500 on `bulbul:v2`.
    pub text: String,
    /// One of the 11 supported codes, e.g. `hi-IN`.
    pub language_code: String,
    /// `bulbul:v3` (default) or `bulbul:v2`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Lowercase voice name, e.g. `shubh`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speaker: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pace: Option<f64>,
    /// `bulbul:v3` only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    /// `bulbul:v2` only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pitch: Option<f64>,
    /// `bulbul:v2` only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loudness: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speech_sample_rate: Option<u32>,
    /// `wav` (default), `mp3`, `linear16`, `mulaw`, `alaw`, `opus`, `flac` or `aac`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_audio_codec: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enable_preprocessing: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dict_id: Option<String>,
}

/// Base64 audio plus the characters billed and the amount charged.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct SarvamTextToSpeechResponse {
    #[serde(default)]
    pub audios: Vec<String>,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub characters: u64,
    #[serde(default, rename = "costUsd")]
    pub cost_usd: f64,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Live streaming speech-to-text.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct SarvamLiveTranscription {
    #[serde(
        default,
        rename = "maxMinutes",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_minutes: Option<u32>,
    /// `saaras:v4` (default) or `saaras:v3-realtime`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Defaults to `auto`. Realtime spells Odia `or-IN`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language_code: Option<String>,
    /// `transcribe`, `translate`, `verbatim`, `translit` or `codemix`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stt_mode: Option<String>,
    /// `linear16` (default), `linear32`, `mulaw` or `alaw`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encoding: Option<String>,
    /// `16000` (default) or `8000`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_rate: Option<u32>,
    /// `vad` (default) or `manual`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpointing: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub return_timestamps: Option<bool>,
}

/// Live streaming text-to-speech.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct SarvamLiveSpeech {
    pub language_code: String,
    #[serde(
        default,
        rename = "maxMinutes",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_minutes: Option<u32>,
    /// `bulbul:v3` (default) or `bulbul:v2`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speaker: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pace: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speech_sample_rate: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_audio_codec: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_buffer_size: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_chunk_length: Option<u32>,
}

/// A Sarvam Voice Agents (Samvaad) conversation with an agent authored in
/// Sarvam's dashboard. Send `client.media.audio_chunk` frames
/// (`audio_base64`: 16-bit mono PCM at `input_sample_rate`), optional
/// `client.media.text`, and answer each `server.system.ping` with a
/// `client.system.pong` carrying its `event_id`. Billed per second connected.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct SarvamLiveAgent {
    /// One of the agents the backend allow-lists.
    pub app_id: String,
    #[serde(
        default,
        rename = "maxMinutes",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_minutes: Option<u32>,
    /// Committed agent version; latest when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,
    /// `16000` (default) or `8000`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_sample_rate: Option<u32>,
    /// `16000` (default) or `22050`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_sample_rate: Option<u32>,
    /// At most 50 string, number or boolean values.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_variables: Option<Map<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_language_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_bot_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_state_name: Option<String>,
}

/// Body of `POST /agent-integrations/sarvam/live/sessions`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "mode", rename_all = "lowercase")]
pub enum SarvamLiveSessionRequest {
    Transcribe(SarvamLiveTranscription),
    Speech(SarvamLiveSpeech),
    Agent(SarvamLiveAgent),
}

/// A single-use relay ticket. Connect a WebSocket to `ws_url` before
/// `ticket_expires_at`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct SarvamLiveTicket {
    pub session_id: String,
    pub ticket: String,
    pub ws_url: String,
    pub ticket_expires_at: String,
    pub model: String,
    pub mode: String,
    pub max_minutes: u32,
    /// Balance reserved for each open session.
    #[serde(default)]
    pub reserve_usd: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct SarvamLiveUsageTotals {
    #[serde(default)]
    pub audio_seconds: f64,
    #[serde(default)]
    pub characters: u64,
    /// Connected Voice Agents time.
    #[serde(default)]
    pub agent_seconds: f64,
}

/// Status and metered usage of a streaming session.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct SarvamLiveSession {
    pub session_id: String,
    pub mode: String,
    pub model: String,
    /// `PENDING`, `ACTIVE`, `CLOSED`, `EXPIRED` or `FAILED`.
    pub status: String,
    #[serde(default)]
    pub max_minutes: u32,
    #[serde(default)]
    pub ticket_expires_at: Option<String>,
    #[serde(default)]
    pub started_at: Option<String>,
    #[serde(default)]
    pub closed_at: Option<String>,
    #[serde(default)]
    pub close_reason: Option<String>,
    #[serde(default)]
    pub charged_usd: f64,
    #[serde(default)]
    pub usage_totals: SarvamLiveUsageTotals,
}

impl AgentIntegrationsApi<'_> {
    /// Transcribe (or translate to English) up to 30 seconds of audio.
    pub async fn sarvam_speech_to_text(
        &self,
        file_name: &str,
        bytes: Vec<u8>,
        options: &SarvamSpeechToTextOptions,
    ) -> Result<SarvamSpeechToTextResponse, Error> {
        let mut form = reqwest::multipart::Form::new().part(
            "file",
            reqwest::multipart::Part::bytes(bytes).file_name(file_name.to_owned()),
        );
        if let Some(value) = &options.model {
            form = form.text("model", value.clone());
        }
        if let Some(value) = &options.mode {
            form = form.text("mode", value.clone());
        }
        if let Some(value) = &options.language_code {
            form = form.text("language_code", value.clone());
        }
        if let Some(value) = options.with_timestamps {
            form = form.text("with_timestamps", value.to_string());
        }
        let value = self
            .http
            .post_multipart("/agent-integrations/sarvam/speech-to-text", form)
            .await?;
        Ok(serde_json::from_value(value)?)
    }

    /// Synthesize speech; the audio comes back base64-encoded in `audios`.
    pub async fn sarvam_text_to_speech(
        &self,
        request: &SarvamTextToSpeechRequest,
    ) -> Result<SarvamTextToSpeechResponse, Error> {
        self.post("/agent-integrations/sarvam/text-to-speech", request)
            .await
    }

    /// OpenAI-compatible chat completion on `sarvam-105b` or
    /// `sarvam-105b-conversations`.
    ///
    /// `stream: true` is rejected with [`Error::StreamingNotSupported`], as on
    /// [`Self::openrouter_chat_completion`].
    pub async fn sarvam_chat_completion(
        &self,
        request: &impl Serialize,
    ) -> Result<DynamicResponse, Error> {
        self.passthrough("/agent-integrations/sarvam/chat/completions", request)
            .await
    }

    /// Open a metered streaming session and get its single-use relay ticket.
    pub async fn sarvam_create_live_session(
        &self,
        request: &SarvamLiveSessionRequest,
    ) -> Result<SarvamLiveTicket, Error> {
        self.post("/agent-integrations/sarvam/live/sessions", request)
            .await
    }

    /// Status and metered usage of a streaming session owned by the caller.
    pub async fn sarvam_live_session(&self, session_id: &str) -> Result<SarvamLiveSession, Error> {
        self.send(
            Method::GET,
            &format!(
                "/agent-integrations/sarvam/live/sessions/{}",
                enc(session_id)
            ),
            &[],
            None,
            true,
        )
        .await
    }
}
