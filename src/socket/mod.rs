//! Authenticated Socket.IO transport for live backend events.
//!
//! The backend uses Socket.IO v4 framing on `/socket.io/`; it is not a raw
//! WebSocket endpoint. [`SocketConnection`] owns the protocol connection,
//! automatically reconnects after network failures, exposes all event names
//! through [`SocketConnection::next_event`], and supports JSON, binary, and
//! acknowledgement-based emits.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use url::Url;

use crate::Error;
mod supervisor;
use supervisor::{Command, OutgoingPayload, TokenProvider};

pub mod events;

/// A Socket.IO event payload.
///
/// Socket.IO text packets may carry more than one JSON argument, so JSON is a
/// vector rather than a single value. Most TinyHumans events use the first
/// element. Binary packets remain raw bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SocketPayload {
    Json(Vec<Value>),
    Binary(Vec<u8>),
}

impl SocketPayload {
    /// Decode the first JSON argument as a concrete event type.
    pub fn decode<T: DeserializeOwned>(&self, event_name: &str) -> Result<T, Error> {
        let Self::Json(values) = self else {
            return Err(Error::UnexpectedSocketPayload(event_name.to_owned()));
        };
        let value = values
            .first()
            .cloned()
            .ok_or_else(|| Error::UnexpectedSocketPayload(event_name.to_owned()))?;
        Ok(serde_json::from_value(value)?)
    }
}

/// One incoming Socket.IO event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SocketEvent {
    pub name: String,
    pub payload: SocketPayload,
}

impl SocketEvent {
    /// Construct an event. Useful when forwarding or testing event decoders.
    pub fn new(name: impl Into<String>, payload: SocketPayload) -> Self {
        Self {
            name: name.into(),
            payload,
        }
    }

    /// Decode the event's first JSON argument.
    pub fn decode<T: DeserializeOwned>(&self) -> Result<T, Error> {
        self.payload.decode(&self.name)
    }
}

/// A connected, authenticated Socket.IO client plus its incoming event queue.
pub struct SocketConnection {
    commands: mpsc::Sender<Command>,
    events: mpsc::Receiver<SocketEvent>,
    loop_active: Arc<AtomicBool>,
    stopped_on_failure: Arc<AtomicBool>,
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

impl fmt::Debug for SocketConnection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SocketConnection")
            .finish_non_exhaustive()
    }
}

impl SocketConnection {
    pub(crate) async fn connect(
        base_url: String,
        token_provider: TokenProvider,
    ) -> Result<Self, Error> {
        validate_endpoint(&base_url)?;
        let token = next_token(token_provider.as_ref()).ok_or(Error::MissingSocketToken)?;
        let (commands, command_rx) = mpsc::channel(128);
        let (event_tx, events) = mpsc::channel(256);
        let loop_active = Arc::new(AtomicBool::new(false));
        let stopped_on_failure = Arc::new(AtomicBool::new(false));
        tokio::spawn(supervisor::run(
            base_url,
            token,
            Arc::clone(&token_provider),
            command_rx,
            event_tx,
            Arc::clone(&loop_active),
            Arc::clone(&stopped_on_failure),
        ));

        Ok(Self {
            commands,
            events,
            loop_active,
            stopped_on_failure,
        })
    }

    /// Wait for the next incoming event.
    ///
    /// Returns `None` only after every transport callback has gone away. Network
    /// interruptions normally produce `close`/`error` events and reconnect.
    pub async fn next_event(&mut self) -> Option<SocketEvent> {
        self.events.recv().await
    }

    /// Emit any serializable JSON payload to any public backend socket event.
    pub async fn emit<T: Serialize>(&self, event: &str, payload: &T) -> Result<(), Error> {
        self.client
            .emit(event, serde_json::to_value(payload)?)
            .await?;
        Ok(())
    }

    /// Emit a binary Socket.IO packet.
    pub async fn emit_binary(&self, event: &str, bytes: Vec<u8>) -> Result<(), Error> {
        self.client.emit(event, bytes).await?;
        Ok(())
    }

    /// Emit JSON and decode the server acknowledgement.
    pub async fn emit_with_ack<T, R>(
        &self,
        event: &str,
        payload: &T,
        timeout: Duration,
    ) -> Result<R, Error>
    where
        T: Serialize,
        R: DeserializeOwned,
    {
        let (sender, receiver) = oneshot::channel();
        let sender = Arc::new(Mutex::new(Some(sender)));
        let callback_sender = Arc::clone(&sender);

        self.client
            .emit_with_ack(
                event,
                serde_json::to_value(payload)?,
                timeout,
                move |payload, _| {
                    let sender = Arc::clone(&callback_sender);
                    async move {
                        if let Some(sender) = sender.lock().expect("ack sender poisoned").take() {
                            let _ = sender.send(SocketPayload::from_socket_io(payload));
                        }
                    }
                    .boxed()
                },
            )
            .await?;

        let payload = tokio::time::timeout(timeout, receiver)
            .await
            .map_err(|_| Error::SocketAckTimeout)?
            .map_err(|_| Error::SocketAckClosed)?;
        payload.decode(event)
    }

    /// Close the connection intentionally.
    pub async fn disconnect(&self) -> Result<(), Error> {
        self.loop_active.store(false, Ordering::Release);
        self.client.disconnect().await?;
        Ok(())
    }

    /// Whether the SDK's bounded reconnect loop is still active.
    pub fn loop_active(&self) -> bool {
        self.loop_active.load(Ordering::Acquire)
    }

    /// Whether reconnecting stopped after a terminal token failure.
    pub fn stopped_on_failure(&self) -> bool {
        self.stopped_on_failure.load(Ordering::Acquire)
    }
}

fn is_invalid_token_payload(payload: &Payload) -> bool {
    let message = match payload {
        Payload::Text(values) => values
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join(" "),
        Payload::Binary(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        #[allow(deprecated)]
        Payload::String(text) => text.clone(),
    };
    let message = message.to_ascii_lowercase();
    message.contains("invalid token") || message.contains("token expired")
}

fn next_token(provider: &(dyn Fn() -> Option<String> + Send + Sync)) -> Option<String> {
    provider().filter(|token| !token.trim().is_empty())
}

fn validate_endpoint(endpoint: &str) -> Result<Url, Error> {
    let url = Url::parse(endpoint).map_err(|_| Error::UnsafeSocketEndpoint)?;
    let secure = matches!(url.scheme(), "https" | "wss");
    let loopback_transport = matches!(url.scheme(), "http" | "ws");
    let loopback = url.host_str().is_some_and(|host| {
        host.eq_ignore_ascii_case("localhost")
            || host
                .trim_start_matches('[')
                .trim_end_matches(']')
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if !(secure || (loopback_transport && loopback))
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(Error::UnsafeSocketEndpoint);
    }
    Ok(url)
}

fn websocket_url(base_url: &str) -> String {
    let mut url = Url::parse(base_url).expect("validated socket endpoint URL");
    let scheme = match url.scheme() {
        "https" => "wss".to_owned(),
        "http" => "ws".to_owned(),
        "wss" | "ws" => url.scheme().to_owned(),
        _ => unreachable!("endpoint validation permits only HTTP(S)/WS(S)"),
    };
    url.set_scheme(&scheme).expect("websocket schemes are compatible");
    let base_path = url.path().trim_end_matches('/');
    let path = if base_path.is_empty() {
        "/socket.io/".to_owned()
    } else {
        format!("{base_path}/socket.io/")
    };
    url.set_path(&path);
    url.set_query(Some("EIO=4&transport=websocket"));
    url.to_string()
}

fn redirect_target(current: &str, location: &str) -> Result<String, Error> {
    let current_url = Url::parse(current).map_err(|_| Error::UnsafeSocketEndpoint)?;
    let mut target = current_url
        .join(location)
        .map_err(|_| Error::UnsafeSocketEndpoint)?;
    let target_scheme = match target.scheme() {
        "http" => "ws".to_owned(),
        "https" => "wss".to_owned(),
        "ws" | "wss" => target.scheme().to_owned(),
        _ => return Err(Error::UnsafeSocketEndpoint),
    };
    target
        .set_scheme(&target_scheme)
        .map_err(|_| Error::UnsafeSocketEndpoint)?;
    validate_endpoint(target.as_str())?;
    if target.origin() != current_url.origin() {
        return Err(Error::CrossOriginSocketRedirect);
    }
    Ok(target.to_string())
}
