#![cfg(feature = "socket")]

use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tinyhumans_sdk::socket::{SocketEvent, SocketPayload};
use tinyhumans_sdk::{Error, TinyHumansClient};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_tungstenite::{accept_async, tungstenite::Message};

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

#[tokio::test]
async fn emit_with_ack_queues_until_socket_connect_ack() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (auth_tx, auth_rx) = oneshot::channel();
    let (ready_tx, ready_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        socket
            .send(Message::Text(
                r#"0{"sid":"eio-1","pingInterval":20000,"pingTimeout":20000}"#.into(),
            ))
            .await
            .unwrap();
        let Some(Ok(Message::Text(auth))) = socket.next().await else {
            panic!("missing SIO CONNECT")
        };
        assert!(auth.contains("access-token"));
        auth_tx.send(()).unwrap();
        ready_rx.await.unwrap();
        socket
            .send(Message::Text(r#"40{"sid":"sio-1"}"#.into()))
            .await
            .unwrap();
        let Some(Ok(Message::Text(emit))) = socket.next().await else {
            panic!("queued emit missing")
        };
        assert!(emit.contains("device:hello"));
        let rest = emit.strip_prefix("42").unwrap();
        let id_end = rest.find('[').unwrap();
        let id = &rest[..id_end];
        socket
            .send(Message::Text(format!("43{id}[{{\"accepted\":true}}]")))
            .await
            .unwrap();
    });

    let client =
        TinyHumansClient::new(format!("http://{address}")).with_token(Some("access-token".into()));
    let connection = client.connect_socket().await.unwrap();
    auth_rx.await.unwrap();
    let emit = tokio::spawn(async move {
        connection
            .emit_with_ack::<_, serde_json::Value>(
                "device:hello",
                &json!({"ok": true}),
                std::time::Duration::from_secs(3),
            )
            .await
    });
    tokio::task::yield_now().await;
    ready_tx.send(()).unwrap();
    let ack = emit.await.unwrap().unwrap();
    assert_eq!(ack["accepted"], true);
    server.await.unwrap();
}

#[tokio::test]
async fn invalid_socket_token_stops_supervisor() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        socket
            .send(Message::Text(r#"0{"sid":"eio-1"}"#.into()))
            .await
            .unwrap();
        let Some(Ok(Message::Text(_auth))) = socket.next().await else {
            panic!("missing SIO CONNECT")
        };
        socket
            .send(Message::Text(r#"44{"message":"Invalid token"}"#.into()))
            .await
            .unwrap();
    });

    let client =
        TinyHumansClient::new(format!("http://{address}")).with_token(Some("expired-token".into()));
    let mut connection = client.connect_socket().await.unwrap();
    let event = tokio::time::timeout(std::time::Duration::from_secs(3), connection.next_event())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.name, "error");
    for _ in 0..10 {
        if connection.stopped_on_failure() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(connection.stopped_on_failure());
    assert!(!connection.loop_active());
    server.await.unwrap();
}

#[test]
fn unknown_socket_events_remain_available_to_generic_consumers() {
    let event = SocketEvent::new("bot:joined", SocketPayload::Json(vec![json!({"ok": true})]));
    assert_eq!(event.name, "bot:joined");
    assert!(matches!(event.payload, SocketPayload::Json(_)));
}
