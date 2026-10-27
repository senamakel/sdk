//! SDK-owned Engine.IO/Socket.IO supervisor.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::http::header::LOCATION;
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use super::{
    is_invalid_token_text, redirect_target, validate_endpoint, websocket_url, SocketEvent,
    SocketPayload,
};
use crate::Error;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_REDIRECT_HOPS: usize = 5;
const MAX_RECONNECT_ATTEMPTS: u32 = 10;
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const EMIT_QUEUE_CAPACITY: usize = 128;

type SocketStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub(super) type TokenProvider = Arc<dyn Fn() -> Option<String> + Send + Sync>;

pub(super) enum Command {
    Emit {
        event: String,
        payload: OutgoingPayload,
        ack: Option<oneshot::Sender<Result<SocketPayload, Error>>>,
        sent: Option<oneshot::Sender<Result<(), Error>>>,
    },
    Disconnect(oneshot::Sender<()>),
}

pub(super) enum OutgoingPayload {
    Json(Value),
    Binary(Vec<u8>),
}

pub(super) async fn run(
    base_url: String,
    initial_token: String,
    token_provider: TokenProvider,
    mut commands: mpsc::Receiver<Command>,
    events: mpsc::Sender<SocketEvent>,
    loop_active: Arc<AtomicBool>,
    stopped_on_failure: Arc<AtomicBool>,
) {
    loop_active.store(true, Ordering::Release);
    stopped_on_failure.store(false, Ordering::Release);

    let mut pending = VecDeque::new();
    let mut token = Some(initial_token);
    let mut attempts = 0u32;
    let mut backoff = Duration::from_secs(1);

    'supervisor: loop {
        if collect_commands(&mut commands, &mut pending) {
            break;
        }
        let Some(token) = token
            .take()
            .or_else(|| token_provider().filter(|value| !value.trim().is_empty()))
        else {
            stopped_on_failure.store(true, Ordering::Release);
            send_error(&events, "socket token provider returned no token").await;
            break;
        };

        let mut endpoint = websocket_url(&base_url);
        let mut socket = match connect_with_redirects(&mut endpoint).await {
            Ok(socket) => socket,
            Err(ConnectFailure::InvalidToken(message)) => {
                stopped_on_failure.store(true, Ordering::Release);
                send_error(&events, &message).await;
                break;
            }
            Err(ConnectFailure::Retryable(message)) => {
                attempts = attempts.saturating_add(1);
                send_error(&events, &message).await;
                if attempts == 5 {
                    send_escalation(&events, attempts).await;
                }
                if attempts >= MAX_RECONNECT_ATTEMPTS {
                    stopped_on_failure.store(true, Ordering::Release);
                    break;
                }
                if sleep_or_disconnect(backoff, &mut commands, &mut pending).await {
                    break 'supervisor;
                }
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }
        };

        match socket_connect_ack(&mut socket, &token).await {
            Ok(()) => {
                let _ = events
                    .send(SocketEvent::new("connect", SocketPayload::Json(vec![json!({})])))
                    .await;
            }
            Err(ConnectFailure::InvalidToken(message)) => {
                stopped_on_failure.store(true, Ordering::Release);
                send_error(&events, &message).await;
                break;
            }
            Err(ConnectFailure::Retryable(message)) => {
                attempts = attempts.saturating_add(1);
                send_error(&events, &message).await;
                if attempts == 5 {
                    send_escalation(&events, attempts).await;
                }
                if attempts >= MAX_RECONNECT_ATTEMPTS {
                    stopped_on_failure.store(true, Ordering::Release);
                    break;
                }
                if sleep_or_disconnect(backoff, &mut commands, &mut pending).await {
                    break 'supervisor;
                }
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }
        }

        let lost = drive_ready_socket(&mut socket, &mut commands, &mut pending, &events).await;
        drain_pending(&mut pending);
        if !lost {
            break;
        }
        let _ = events
            .send(SocketEvent::new("close", SocketPayload::Json(vec![])))
            .await;
        attempts = 0;
        backoff = Duration::from_secs(1);
        if sleep_or_disconnect(backoff, &mut commands, &mut pending).await {
            break;
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }

    drain_pending(&mut pending);
    while let Ok(command) = commands.try_recv() {
        fail_command(command);
    }
    loop_active.store(false, Ordering::Release);
}

async fn connect_with_redirects(url: &mut String) -> Result<SocketStream, ConnectFailure> {
    for hop in 0..=MAX_REDIRECT_HOPS {
        validate_endpoint(url).map_err(|error| ConnectFailure::Retryable(error.to_string()))?;
        let mut request = url
            .as_str()
            .into_client_request()
            .map_err(|error| ConnectFailure::Retryable(error.to_string()))?;
        request.headers_mut().insert(
            "x-sdk-client",
            tokio_tungstenite::tungstenite::http::HeaderValue::from_static("tinyhumans-rust"),
        );
        match timeout(CONNECT_TIMEOUT, connect_async(request)).await {
            Err(_) => return Err(ConnectFailure::Retryable("websocket upgrade timed out".into())),
            Ok(Ok((socket, _))) => return Ok(socket),
            Ok(Err(tokio_tungstenite::tungstenite::Error::Http(response)))
                if is_redirect(response.status()) =>
            {
                if hop == MAX_REDIRECT_HOPS {
                    return Err(ConnectFailure::Retryable("too many socket redirects".into()));
                }
                let location = response
                    .headers()
                    .get(LOCATION)
                    .and_then(|value| value.to_str().ok())
                    .ok_or_else(|| ConnectFailure::Retryable("redirect missing Location".into()))?;
                *url = redirect_target(url, location)
                    .map_err(|error| ConnectFailure::Retryable(error.to_string()))?;
            }
            Ok(Err(error)) => return Err(ConnectFailure::Retryable(error.to_string())),
        }
    }
    Err(ConnectFailure::Retryable("too many socket redirects".into()))
}

fn is_redirect(status: StatusCode) -> bool {
    matches!(status.as_u16(), 301 | 302 | 307 | 308)
}

async fn socket_connect_ack(socket: &mut SocketStream, token: &str) -> Result<(), ConnectFailure> {
    let open = timeout(CONNECT_TIMEOUT, socket.next())
        .await
        .map_err(|_| ConnectFailure::Retryable("Engine.IO open timed out".into()))?
        .ok_or_else(|| ConnectFailure::Retryable("closed before Engine.IO open".into()))?
        .map_err(|error| ConnectFailure::Retryable(error.to_string()))?;
    let Message::Text(open) = open else {
        return Err(ConnectFailure::Retryable("invalid Engine.IO open packet".into()));
    };
    if !open.starts_with('0') {
        return Err(ConnectFailure::Retryable("missing Engine.IO open packet".into()));
    }
    let auth = serde_json::to_string(&json!({ "token": token }))
        .map_err(|error| ConnectFailure::Retryable(error.to_string()))?;
    socket
        .send(Message::Text(format!("40{auth}")))
        .await
        .map_err(|error| ConnectFailure::Retryable(error.to_string()))?;

    loop {
        let message = timeout(CONNECT_TIMEOUT, socket.next())
            .await
            .map_err(|_| ConnectFailure::Retryable("Socket.IO CONNECT ACK timed out".into()))?
            .ok_or_else(|| ConnectFailure::Retryable("closed before Socket.IO CONNECT ACK".into()))?
            .map_err(|error| ConnectFailure::Retryable(error.to_string()))?;
        match message {
            Message::Text(frame) if frame.starts_with("40") => return Ok(()),
            Message::Text(frame) if frame.starts_with("44") => {
                if is_invalid_token_text(&frame) {
                    return Err(ConnectFailure::InvalidToken(frame));
                }
                return Err(ConnectFailure::Retryable(frame));
            }
            Message::Text(frame) if frame == "2" => {
                socket
                    .send(Message::Text("3".into()))
                    .await
                    .map_err(|error| ConnectFailure::Retryable(error.to_string()))?;
            }
            Message::Ping(data) => socket
                .send(Message::Pong(data))
                .await
                .map_err(|error| ConnectFailure::Retryable(error.to_string()))?,
            Message::Close(_) => {
                return Err(ConnectFailure::Retryable(
                    "closed before Socket.IO CONNECT ACK".into(),
                ));
            }
            _ => {}
        }
    }
}

enum ConnectFailure {
    Retryable(String),
    InvalidToken(String),
}

fn collect_commands(commands: &mut mpsc::Receiver<Command>, pending: &mut VecDeque<Command>) -> bool {
    loop {
        match commands.try_recv() {
            Ok(Command::Disconnect(reply)) => {
                let _ = reply.send(());
                return true;
            }
            Ok(command) if pending.len() < EMIT_QUEUE_CAPACITY => pending.push_back(command),
            Ok(command) => fail_command(command),
            Err(_) => return false,
        }
    }
}

async fn sleep_or_disconnect(
    duration: Duration,
    commands: &mut mpsc::Receiver<Command>,
    pending: &mut VecDeque<Command>,
) -> bool {
    tokio::select! {
        _ = sleep(duration) => false,
        command = commands.recv() => match command {
            Some(Command::Disconnect(reply)) => { let _ = reply.send(()); true }
            Some(command) if pending.len() < EMIT_QUEUE_CAPACITY => { pending.push_back(command); false }
            Some(command) => { fail_command(command); false }
            None => true,
        }
    }
}

async fn drive_ready_socket(
    socket: &mut SocketStream,
    commands: &mut mpsc::Receiver<Command>,
    pending: &mut VecDeque<Command>,
    events: &mpsc::Sender<SocketEvent>,
) -> bool {
    let mut ack_id = 0u64;
    let mut acknowledgements = VecDeque::<(u64, oneshot::Sender<Result<SocketPayload, Error>>)>::new();
    let mut binary_event: Option<(String, usize)> = None;
    loop {
        if let Some(command) = pending.pop_front() {
            if command_is_cancelled(&command) {
                fail_command(command);
                continue;
            }
            if !send_command(socket, command, &mut ack_id, &mut acknowledgements).await {
                fail_acks(acknowledgements);
                return true;
            }
            continue;
        }
        tokio::select! {
            command = commands.recv() => match command {
                Some(Command::Disconnect(reply)) => {
                    let _ = reply.send(());
                    fail_acks(acknowledgements);
                    return false;
                }
                Some(command) => pending.push_back(command),
                None => { fail_acks(acknowledgements); return false; }
            },
            message = socket.next() => match message {
                Some(Ok(Message::Text(frame))) => {
                    if frame == "2" {
                        if socket.send(Message::Text("3".into())).await.is_err() { fail_acks(acknowledgements); return true; }
                    } else if frame.starts_with("43") {
                        handle_ack(&frame, &mut acknowledgements);
                    } else if frame.starts_with("42") {
                        if let Some((name, payload)) = parse_event(&frame[2..]) {
                            let _ = events.send(SocketEvent::new(name, SocketPayload::Json(payload))).await;
                        }
                    } else if frame.starts_with("451-") {
                        binary_event = parse_binary_event(&frame[3..]);
                    } else if frame.starts_with("41") {
                        fail_acks(acknowledgements);
                        return true;
                    } else if frame.starts_with("44") {
                        let _ = events.send(SocketEvent::new("error", SocketPayload::Json(vec![Value::String(frame)]))).await;
                        fail_acks(acknowledgements);
                        return true;
                    }
                }
                Some(Ok(Message::Binary(bytes))) => {
                    if let Some((name, _)) = binary_event.take() {
                        let _ = events.send(SocketEvent::new(name, SocketPayload::Binary(bytes))).await;
                    }
                }
                Some(Ok(Message::Ping(bytes))) => { if socket.send(Message::Pong(bytes)).await.is_err() { fail_acks(acknowledgements); return true; } }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => { fail_acks(acknowledgements); return true; }
                _ => {}
            }
        }
    }
}

async fn send_command(
    socket: &mut SocketStream,
    command: Command,
    ack_id: &mut u64,
    acknowledgements: &mut VecDeque<(u64, oneshot::Sender<Result<SocketPayload, Error>>)>,
) -> bool {
    match command {
        Command::Disconnect(reply) => {
            let _ = reply.send(());
            false
        }
        Command::Emit { event, payload, ack, sent } => {
            let id = ack.as_ref().map(|_| { *ack_id += 1; *ack_id });
            let packet = match (&payload, id) {
                (OutgoingPayload::Json(value), Some(id)) => format!("42{id}{}", json!([event, value])),
                (OutgoingPayload::Json(value), None) => format!("42{}", json!([event, value])),
                (OutgoingPayload::Binary(_), Some(_)) => {
                    if let Some(sent) = sent { let _ = sent.send(Err(Error::SocketAckClosed)); }
                    if let Some(ack) = ack { let _ = ack.send(Err(Error::SocketAckClosed)); }
                    return true;
                }
                (OutgoingPayload::Binary(bytes), None) => {
                    let header = format!("451-[{},{{\"_placeholder\":true,\"num\":0}}]", json!(event));
                    if socket.send(Message::Text(header)).await.is_err() || socket.send(Message::Binary(bytes.clone())).await.is_err() {
                        if let Some(sent) = sent { let _ = sent.send(Err(Error::SocketAckClosed)); }
                        return false;
                    }
                    if let Some(sent) = sent { let _ = sent.send(Ok(())); }
                    return true;
                }
            };
            if let Err(_) = socket.send(Message::Text(packet)).await {
                if let Some(sent) = sent { let _ = sent.send(Err(Error::SocketAckClosed)); }
                if let Some(ack) = ack { let _ = ack.send(Err(Error::SocketAckClosed)); }
                return false;
            }
            if let Some(sent) = sent { let _ = sent.send(Ok(())); }
            if let (Some(id), Some(ack)) = (id, ack) { acknowledgements.push_back((id, ack)); }
            true
        }
    }
}

fn parse_event(json_text: &str) -> Option<(String, Vec<Value>)> {
    let mut values = serde_json::from_str::<Vec<Value>>(json_text).ok()?;
    let name = values.first()?.as_str()?.to_owned();
    values.remove(0);
    Some((name, values))
}

fn parse_binary_event(packet: &str) -> Option<(String, usize)> {
    let (_, json) = packet.split_once('-')?;
    let values = serde_json::from_str::<Vec<Value>>(json).ok()?;
    Some((values.first()?.as_str()?.to_owned(), 1))
}

fn handle_ack(
    frame: &str,
    acknowledgements: &mut VecDeque<(u64, oneshot::Sender<Result<SocketPayload, Error>>)>,
) {
    let Some(tail) = frame.strip_prefix("43") else { return; };
    let digit_count = tail.bytes().take_while(u8::is_ascii_digit).count();
    let Ok(id) = tail[..digit_count].parse::<u64>() else { return; };
    let Ok(values) = serde_json::from_str::<Vec<Value>>(&tail[digit_count..]) else { return; };
    if let Some(index) = acknowledgements.iter().position(|(pending, _)| *pending == id) {
        if let Some((_, sender)) = acknowledgements.remove(index) {
            let _ = sender.send(Ok(SocketPayload::Json(values)));
        }
    }
}

fn fail_acks(acks: VecDeque<(u64, oneshot::Sender<Result<SocketPayload, Error>>)>) {
    for (_, sender) in acks { let _ = sender.send(Err(Error::SocketAckClosed)); }
}

fn drain_pending(pending: &mut VecDeque<Command>) {
    while let Some(command) = pending.pop_front() {
        match command {
            Command::Emit { ack, sent, .. } => {
                if let Some(sent) = sent { let _ = sent.send(Err(Error::SocketAckClosed)); }
                if let Some(ack) = ack { let _ = ack.send(Err(Error::SocketAckClosed)); }
            }
            Command::Disconnect(reply) => { let _ = reply.send(()); }
        }
    }
}

fn fail_command(command: Command) {
    match command {
        Command::Emit { ack, sent, .. } => {
            if let Some(sent) = sent { let _ = sent.send(Err(Error::SocketAckClosed)); }
            if let Some(ack) = ack { let _ = ack.send(Err(Error::SocketAckClosed)); }
        }
        Command::Disconnect(reply) => { let _ = reply.send(()); }
    }
}

fn command_is_cancelled(command: &Command) -> bool {
    matches!(command, Command::Emit { ack: Some(sender), .. } if sender.is_closed())
}

async fn send_error(events: &mpsc::Sender<SocketEvent>, message: &str) {
    let _ = events
        .send(SocketEvent::new(
            "error",
            SocketPayload::Json(vec![Value::String(message.to_owned())]),
        ))
        .await;
}

async fn send_escalation(events: &mpsc::Sender<SocketEvent>, attempts: u32) {
    let _ = events
        .send(SocketEvent::new(
            "reconnect:escalated",
            SocketPayload::Json(vec![json!({ "attempts": attempts })]),
        ))
        .await;
}
