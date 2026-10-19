#![cfg(feature = "socket")]

use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use std::time::Duration;
use tinyhumans_sdk::socket::{SocketEvent, SocketPayload};
use tinyhumans_sdk::{Error, TinyHumansClient};
use tokio::net::TcpListener;
use tokio::time::{timeout, Instant};
use tokio_tungstenite::{accept_hdr_async, tungstenite::Message};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Joined {
    channel_id: String,
}

#[test]
fn socket_event_decodes_typed_json_payloads() {
    let event = SocketEvent::new(
        "bot:joined",
        SocketPayload::Json(vec![json!({ "channelId": "channel-1" })]),
    );

    let joined: Joined = event.decode().unwrap();
    assert_eq!(joined.channel_id, "channel-1");
}

#[test]
fn binary_socket_payload_is_not_misdecoded_as_json() {
    let event = SocketEvent::new("agent:audio:chunk", SocketPayload::Binary(vec![1, 2, 3]));
    let error = event.decode::<Joined>().unwrap_err();
    assert!(matches!(error, Error::UnexpectedSocketPayload(_)));
}

#[tokio::test]
async fn socket_connection_requires_a_bearer_token() {
    let error = TinyHumansClient::new("https://api.tinyhumans.ai")
        .connect_socket()
        .await
        .unwrap_err();
    assert!(matches!(error, Error::MissingSocketToken));
}

async fn socket_fixture(
    namespace_reply: &'static str,
) -> (String, tokio::task::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint = format!("ws://{address}/socket.io/?EIO=4&transport=websocket");
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (mut socket, request) = accept_hdr_async(
            stream,
            |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                Ok(response)
            },
        )
        .await
        .unwrap();
        socket
            .send(Message::Text(
                r#"0{"sid":"engine-1","upgrades":[],"pingInterval":25000,"pingTimeout":20000}"#
                    .into(),
            ))
            .await
            .unwrap();
        let connect = socket.next().await.unwrap().unwrap().into_text().unwrap();
        socket
            .send(Message::Text(namespace_reply.into()))
            .await
            .unwrap();
        let _ = timeout(Duration::from_secs(2), socket.next()).await;
        request.uri().to_string() + "|" + &connect
    });
    (endpoint, task)
}

#[tokio::test]
async fn managed_socket_connects_to_the_explicit_endpoint_and_exposes_namespace_sid() {
    let (endpoint, server) = socket_fixture(r#"40{"sid":"namespace-1"}"#).await;
    let mut socket = TinyHumansClient::new("http://ignored.invalid")
        .with_token(Some("secret".into()))
        .connect_socket_with_options(
            tinyhumans_sdk::socket::SocketConnectOptions::new()
                .endpoint(endpoint)
                .connect_timeout(Duration::from_secs(2)),
        )
        .await
        .unwrap();

    assert_eq!(
        socket.namespace_outcome(),
        Some(&tinyhumans_sdk::socket::SocketNamespaceOutcome::Ready {
            sid: Some("namespace-1".into())
        })
    );
    let metadata = socket.connection_metadata().unwrap();
    assert_eq!(metadata.original_endpoint.path(), "/socket.io/");
    assert_eq!(
        metadata.original_endpoint.query(),
        Some("EIO=4&transport=websocket")
    );
    let recorded = server.await.unwrap();
    assert!(recorded.starts_with("/socket.io/?EIO=4&transport=websocket|40"));
    socket.close().await.unwrap();
}

#[tokio::test]
async fn managed_socket_preserves_structured_namespace_rejection() {
    let (endpoint, server) =
        socket_fixture(r#"44{"message":"denied","data":{"reason":"stale-token"}}"#).await;
    let socket = TinyHumansClient::new("http://ignored.invalid")
        .with_token(Some("secret".into()))
        .connect_socket_with_options(
            tinyhumans_sdk::socket::SocketConnectOptions::new()
                .endpoint(endpoint)
                .connect_timeout(Duration::from_secs(2)),
        )
        .await
        .unwrap();

    assert_eq!(
        socket.namespace_outcome(),
        Some(&tinyhumans_sdk::socket::SocketNamespaceOutcome::Rejected {
            payload: serde_json::json!({
                "message": "denied",
                "data": {"reason": "stale-token"}
            })
        })
    );
    server.abort();
}

#[tokio::test]
async fn managed_socket_namespace_silence_is_bounded_by_one_attempt_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint = format!("ws://{address}/socket.io/?EIO=4&transport=websocket");
    let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        socket
            .send(Message::Text(
                r#"0{"sid":"engine-2","upgrades":[],"pingInterval":25000,"pingTimeout":20000}"#
                    .into(),
            ))
            .await
            .unwrap();
        let _ = socket.next().await;
        let _ = timeout(Duration::from_secs(2), socket.next()).await;
        let _ = closed_tx.send(());
    });
    let started = Instant::now();
    let error = TinyHumansClient::new("http://ignored.invalid")
        .with_token(Some("secret".into()))
        .connect_socket_with_options(
            tinyhumans_sdk::socket::SocketConnectOptions::new()
                .endpoint(endpoint)
                .connect_timeout(Duration::from_millis(100)),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, Error::SocketConnectTimeout));
    assert!(started.elapsed() < Duration::from_secs(1));
    timeout(Duration::from_secs(1), closed_rx)
        .await
        .unwrap()
        .unwrap();
    server.abort();
}

#[test]
fn unknown_socket_events_remain_available_to_generic_consumers() {
    let event = SocketEvent::new("bot:joined", SocketPayload::Json(vec![json!({"ok": true})]));
    assert_eq!(event.name, "bot:joined");
    assert!(matches!(event.payload, SocketPayload::Json(_)));
}
