//! Authenticated Socket.IO transport for live backend events.
//!
//! The backend uses Socket.IO v4 framing on `/socket.io/`; it is not a raw
//! WebSocket endpoint. [`SocketConnection`] owns the protocol connection,
//! automatically reconnects after network failures, exposes all event names
//! through [`SocketConnection::next_event`], and supports JSON, binary, and
//! acknowledgement-based emits.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::FutureExt;
use rust_socketio::asynchronous::{Client, ClientBuilder, ReconnectSettings};
use rust_socketio::Payload;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use url::Url;

use crate::Error;

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
    fn from_socket_io(payload: Payload) -> Self {
        match payload {
            Payload::Text(values) => Self::Json(values),
            Payload::Binary(bytes) => Self::Binary(bytes.to_vec()),
            #[allow(deprecated)]
            Payload::String(text) => Self::Json(vec![
                serde_json::from_str(&text).unwrap_or(Value::String(text))
            ]),
        }
    }

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
    client: Client,
    events: mpsc::UnboundedReceiver<SocketEvent>,
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
        token_provider: Arc<dyn Fn() -> Option<String> + Send + Sync>,
    ) -> Result<Self, Error> {
        validate_endpoint(&base_url)?;
        let token = next_token(token_provider.as_ref()).ok_or(Error::MissingSocketToken)?;
        let (sender, events) = mpsc::unbounded_channel();

        let any_sender = sender.clone();
        let open_sender = sender.clone();
        let close_sender = sender.clone();
        let connect_sender = sender.clone();
        let error_sender = sender;
        let loop_active = Arc::new(AtomicBool::new(false));
        let stopped_on_failure = Arc::new(AtomicBool::new(false));
        let reconnect_provider = Arc::clone(&token_provider);
        let reconnect_stopped = Arc::clone(&stopped_on_failure);
        let error_stopped = Arc::clone(&stopped_on_failure);
        let error_active = Arc::clone(&loop_active);
        let (ready_tx, ready_rx) = oneshot::channel();
        let ready_tx = Arc::new(Mutex::new(Some(ready_tx)));
        let ready_callback_tx = Arc::clone(&ready_tx);
        let client = tokio::time::timeout(Duration::from_secs(10), ClientBuilder::new(base_url)
            .auth(serde_json::json!({ "token": token }))
            .opening_header("x-sdk-client", "tinyhumans-rust")
            .on_any(move |event, payload, _| {
                let sender = any_sender.clone();
                async move {
                    let _ = sender.send(SocketEvent::new(
                        event.to_string(),
                        SocketPayload::from_socket_io(payload),
                    ));
                }
                .boxed()
            })
            .on("open", move |payload, _| {
                let sender = open_sender.clone();
                async move {
                    let _ = sender.send(SocketEvent::new(
                        "open",
                        SocketPayload::from_socket_io(payload),
                    ));
                }
                .boxed()
            })
            .on("close", move |payload, _| {
                let sender = close_sender.clone();
                async move {
                    let _ = sender.send(SocketEvent::new(
                        "close",
                        SocketPayload::from_socket_io(payload),
                    ));
                }
                .boxed()
            })
            .on("error", move |payload, client| {
                let sender = error_sender.clone();
                let stopped = Arc::clone(&error_stopped);
                let active = Arc::clone(&error_active);
                async move {
                    if is_invalid_token_payload(&payload) {
                        stopped.store(true, Ordering::Release);
                        active.store(false, Ordering::Release);
                        let _ = client.disconnect().await;
                    }
                    let _ = sender.send(SocketEvent::new(
                        "error",
                        SocketPayload::from_socket_io(payload),
                    ));
                }
                .boxed()
            })
            .on_reconnect(move || {
                let provider = Arc::clone(&reconnect_provider);
                let stopped = Arc::clone(&reconnect_stopped);
                async move {
                    let mut settings = ReconnectSettings::new();
                    match next_token(provider.as_ref()) {
                        Some(token) => settings.auth(serde_json::json!({ "token": token })),
                        None => {
                            stopped.store(true, Ordering::Release);
                            // An empty credential forces the backend to reject
                            // the attempt; the error callback then closes the
                            // retry loop instead of reusing stale auth.
                            settings.auth(serde_json::json!({ "token": "" }));
                        }
                    }
                    settings
                }
                .boxed()
            })
            .reconnect(true)
            .reconnect_delay(1_000, 30_000)
            .max_reconnect_attempts(10)
            .on("connect", move |payload, _| {
                if let Some(ready) = ready_callback_tx.lock().expect("ready sender poisoned").take() {
                    let _ = ready.send(());
                }
                let sender = connect_sender.clone();
                async move {
                    let _ = sender.send(SocketEvent::new(
                        "connect",
                        SocketPayload::from_socket_io(payload),
                    ));
                }
                .boxed()
            })
            .connect()).await.map_err(|_| Error::SocketConnectTimeout)??;

        // The builder returns as soon as it writes the CONNECT packet. Wait for
        // the server's CONNECT ACK before exposing this connection to callers.
        tokio::time::timeout(Duration::from_secs(10), ready_rx)
            .await
            .map_err(|_| Error::SocketConnectTimeout)?
            .map_err(|_| Error::SocketAckClosed)?;
        loop_active.store(true, Ordering::Release);

        Ok(Self { client, events, loop_active, stopped_on_failure })
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
        Payload::Text(values) => values.iter().map(Value::to_string).collect::<Vec<_>>().join(" "),
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

