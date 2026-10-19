//! Authenticated Socket.IO transport for live backend events.
//!
//! The backend uses Socket.IO v4 framing on `/socket.io/`; it is not a raw
//! WebSocket endpoint. [`SocketConnection`] owns the protocol connection,
//! automatically reconnects after network failures, exposes all event names
//! through [`SocketConnection::next_event`], and supports JSON, binary, and
//! acknowledgement-based emits.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::FutureExt;
use rust_socketio::asynchronous::{
    Client, ClientBuilder, ConnectionMetadata, ManagedClient, NamespaceConnectOutcome,
};
use rust_socketio::Payload;
use rust_socketio::TransportType;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use url::{Host, Url};

use crate::Error;

pub mod events;

/// Options for an explicitly bounded Socket.IO connection attempt.
///
/// The legacy [`TinyHumansClient::connect_socket`](crate::TinyHumansClient::connect_socket)
/// entry point keeps its existing reconnect behavior. This options-based entry
/// point defaults to one attempt and owns the transport task until [`SocketConnection::close`]
/// or drop.
#[derive(Clone, Debug)]
pub struct SocketConnectOptions {
    endpoint: Option<String>,
    connect_timeout: Duration,
    close_timeout: Duration,
    single_attempt: bool,
}

impl SocketConnectOptions {
    /// Create bounded single-attempt options with the default ten-second
    /// connect and two-second close deadlines.
    pub fn new() -> Self {
        Self::default()
    }

    /// Use an already-resolved Socket.IO endpoint verbatim.
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Bound DNS, transport establishment, upgrade, and namespace handshake.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Bound asynchronous close and task joining.
    pub fn close_timeout(mut self, timeout: Duration) -> Self {
        self.close_timeout = timeout;
        self
    }

    /// Select SDK-internal reconnect behavior. Defaults to one attempt.
    pub fn single_attempt(mut self, single_attempt: bool) -> Self {
        self.single_attempt = single_attempt;
        self
    }
}

impl Default for SocketConnectOptions {
    fn default() -> Self {
        Self {
            endpoint: None,
            connect_timeout: Duration::from_secs(10),
            close_timeout: Duration::from_secs(2),
            single_attempt: true,
        }
    }
}

/// The outcome of the Socket.IO namespace handshake.
#[derive(Debug, Clone, PartialEq)]
pub enum SocketNamespaceOutcome {
    /// The namespace accepted the connection and assigned the optional SID.
    Ready { sid: Option<String> },
    /// The namespace rejected the connection with its structured payload.
    Rejected { payload: Value },
    /// The connection closed before the namespace accepted or rejected it.
    Closed,
}

impl From<NamespaceConnectOutcome> for SocketNamespaceOutcome {
    fn from(outcome: NamespaceConnectOutcome) -> Self {
        match outcome {
            NamespaceConnectOutcome::Ready { sid } => Self::Ready { sid },
            NamespaceConnectOutcome::Rejected { payload } => Self::Rejected { payload },
            NamespaceConnectOutcome::Closed => Self::Closed,
        }
    }
}

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
    managed: Option<tokio::sync::Mutex<ManagedClient>>,
    events: mpsc::UnboundedReceiver<SocketEvent>,
    namespace_outcome: Option<SocketNamespaceOutcome>,
    connection_metadata: Option<ConnectionMetadata>,
    close_timeout: Duration,
}

impl fmt::Debug for SocketConnection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SocketConnection")
            .finish_non_exhaustive()
    }
}

impl SocketConnection {
    pub(crate) async fn connect(base_url: String, token: String) -> Result<Self, Error> {
        let (sender, events) = mpsc::unbounded_channel();
        let client = socket_builder(base_url, token, sender).connect().await?;

        Ok(Self {
            client,
            managed: None,
            events,
            namespace_outcome: None,
            connection_metadata: None,
            close_timeout: Duration::from_secs(2),
        })
    }

    pub(crate) async fn connect_with_options(
        base_url: String,
        token: String,
        options: SocketConnectOptions,
    ) -> Result<Self, Error> {
        let endpoint = match options.endpoint.clone() {
            Some(endpoint) => endpoint,
            None => default_socket_endpoint(&base_url)?,
        };
        validate_credential_endpoint(&endpoint)?;
        let (sender, events) = mpsc::unbounded_channel();
        let builder = socket_builder(endpoint, token, sender)
            .transport_type(TransportType::Websocket)
            .reconnect(!options.single_attempt)
            .reconnect_on_disconnect(!options.single_attempt);

        timeout(options.connect_timeout, async move {
            let mut managed = builder.connect_managed().await?;
            let namespace_outcome = managed.wait_for_namespace().await.into();
            let connection_metadata = managed.connection_metadata().await;
            let client = (*managed).clone();
            Ok(Self {
                client,
                managed: Some(tokio::sync::Mutex::new(managed)),
                events,
                namespace_outcome: Some(namespace_outcome),
                connection_metadata: Some(connection_metadata),
                close_timeout: options.close_timeout,
            })
        })
        .await
        .map_err(|_| Error::SocketConnectTimeout)?
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
        if let Some(managed) = self.managed.as_ref() {
            let mut managed = managed.lock().await;
            let result = timeout(self.close_timeout, managed.shutdown())
                .await
                .map_err(|_| Error::SocketCloseTimeout)?;
            if result.is_err() && !managed.is_shutdown() {
                result?;
            }
            Ok(())
        } else {
            self.client.disconnect().await?;
            Ok(())
        }
    }

    /// The namespace server's acceptance or rejection of this connection.
    pub fn namespace_outcome(&self) -> Option<&SocketNamespaceOutcome> {
        self.namespace_outcome.as_ref()
    }

    /// The original endpoint, final endpoint, and redirects used at connect.
    pub fn connection_metadata(&self) -> Option<&ConnectionMetadata> {
        self.connection_metadata.as_ref()
    }

    /// Close the managed connection and join its owned tasks within the
    /// configured bound. On timeout the handle remains owned and this method
    /// can be retried.
    pub async fn close(&self) -> Result<(), Error> {
        if self.managed.is_some() {
            self.disconnect().await
        } else {
            timeout(self.close_timeout, self.client.disconnect())
                .await
                .map_err(|_| Error::SocketCloseTimeout)??;
            Ok(())
        }
    }
}

fn validate_credential_endpoint(endpoint: &str) -> Result<(), Error> {
    let url = Url::parse(endpoint)?;
    if !matches!(url.scheme(), "ws" | "wss") {
        return Err(Error::UnsupportedSocketScheme(url.scheme().to_owned()));
    }
    if url.scheme() == "ws" && is_remote_host(&url) {
        return Err(Error::InsecureSocketEndpoint);
    }
    Ok(())
}

fn default_socket_endpoint(base_url: &str) -> Result<String, Error> {
    let mut url = Url::parse(base_url)?;
    match url.scheme() {
        "http" => url
            .set_scheme("ws")
            .expect("HTTP and WS URL schemes are compatible"),
        "https" => url
            .set_scheme("wss")
            .expect("HTTPS and WSS URL schemes are compatible"),
        "ws" | "wss" => {}
        scheme => return Err(Error::UnsupportedSocketScheme(scheme.to_owned())),
    }
    url.set_path("/socket.io/");
    url.set_query(Some("EIO=4&transport=websocket"));
    Ok(url.into())
}

fn is_remote_host(url: &Url) -> bool {
    match url.host() {
        Some(Host::Domain(host)) => !host.eq_ignore_ascii_case("localhost"),
        Some(Host::Ipv4(address)) => !address.is_loopback(),
        Some(Host::Ipv6(address)) => !address.is_loopback(),
        None => true,
    }
}

fn socket_builder(
    endpoint: String,
    token: String,
    sender: mpsc::UnboundedSender<SocketEvent>,
) -> ClientBuilder {
    let any_sender = sender.clone();
    let open_sender = sender.clone();
    let close_sender = sender.clone();
    let error_sender = sender;
    ClientBuilder::new(endpoint)
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
        .on("error", move |payload, _| {
            let sender = error_sender.clone();
            async move {
                let _ = sender.send(SocketEvent::new(
                    "error",
                    SocketPayload::from_socket_io(payload),
                ));
            }
            .boxed()
        })
}
