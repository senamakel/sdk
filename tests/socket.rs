#![cfg(feature = "socket")]

use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use std::time::Duration;
use tinyhumans_sdk::socket::{SocketEvent, SocketPayload};
use tinyhumans_sdk::{Error, TinyHumansClient};
use tokio::net::TcpListener;
use tokio::time::{timeout, Instant};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{http::header::LOCATION, http::StatusCode, Message},
};

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
        let (request_uri_tx, request_uri_rx) = tokio::sync::oneshot::channel();
        let mut request_uri_tx = Some(request_uri_tx);
        let mut socket = accept_hdr_async(
            stream,
            move |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                  response| {
                if let Some(sender) = request_uri_tx.take() {
                    let _ = sender.send(request.uri().to_string());
                }
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
        let connect = loop {
            let message = timeout(Duration::from_secs(2), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if let Ok(text) = message.into_text() {
                if text.starts_with("40") {
                    break text;
                }
            }
        };
        socket
            .send(Message::Text(namespace_reply.into()))
            .await
            .unwrap();
        while let Ok(Some(Ok(message))) = timeout(Duration::from_secs(2), socket.next()).await {
            if message.is_close() {
                break;
            }
        }
        request_uri_rx.await.unwrap() + "|" + &connect
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
    socket.close().await.unwrap();
    let recorded = server.await.unwrap();
    assert!(
        recorded.starts_with("/socket.io/?EIO=4&transport=websocket|40"),
        "unexpected request and namespace packet: {recorded:?}"
    );
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
        loop {
            match timeout(Duration::from_secs(2), socket.next()).await {
                Ok(Some(Ok(message))) if message.is_close() => break,
                Ok(Some(Ok(_))) => continue,
                _ => break,
            }
        }
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

#[tokio::test]
async fn managed_socket_rejects_remote_plaintext_before_sending_auth() {
    let error = TinyHumansClient::new("https://api.example.test")
        .with_token(Some("secret".into()))
        .connect_socket_with_options(
            tinyhumans_sdk::socket::SocketConnectOptions::new()
                .endpoint("ws://socket.example.test/socket.io/?EIO=4&transport=websocket"),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, Error::InsecureSocketEndpoint));
}

#[tokio::test]
async fn managed_socket_records_relative_upgrade_redirect_metadata() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint = format!("ws://{address}/socket.io/?EIO=4&transport=websocket");
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();
        for hop in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let (request_tx, request_rx) = tokio::sync::oneshot::channel();
            let mut request_tx = Some(request_tx);
            let response = accept_hdr_async(
                stream,
                move |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                      mut response| {
                    if let Some(sender) = request_tx.take() {
                        let _ = sender.send(request.uri().to_string());
                    }
                    if hop == 0 {
                        *response.status_mut() = StatusCode::FOUND;
                        response.headers_mut().insert(
                            LOCATION,
                            "?EIO=4&transport=websocket&step=final".parse().unwrap(),
                        );
                    }
                    Ok(response)
                },
            )
            .await;
            requests.push(request_rx.await.unwrap());
            if let Ok(mut socket) = response {
                socket
                    .send(Message::Text(
                        r#"0{"sid":"engine-redirect","upgrades":[],"pingInterval":25000,"pingTimeout":20000}"#.into(),
                    ))
                    .await
                    .unwrap();
                loop {
                    let message = socket.next().await.unwrap().unwrap();
                    if message.to_text().is_ok_and(|text| text.starts_with("40")) {
                        break;
                    }
                }
                socket
                    .send(Message::Text(r#"40{"sid":"namespace-redirect"}"#.into()))
                    .await
                    .unwrap();
                while let Ok(Some(Ok(message))) =
                    timeout(Duration::from_secs(2), socket.next()).await
                {
                    if message.is_close() {
                        break;
                    }
                }
                break;
            }
        }
        requests
    });

    let mut socket = TinyHumansClient::new("http://ignored.invalid")
        .with_token(Some("secret".into()))
        .connect_socket_with_options(
            tinyhumans_sdk::socket::SocketConnectOptions::new()
                .endpoint(endpoint)
                .connect_timeout(Duration::from_secs(2)),
        )
        .await
        .unwrap();
    let metadata = socket.connection_metadata().unwrap();
    assert_eq!(metadata.redirects.len(), 1);
    assert_eq!(metadata.redirects[0].status, 302);
    assert_eq!(
        metadata.redirects[0].to.query(),
        Some("EIO=4&transport=websocket&step=final")
    );
    assert!(metadata
        .final_endpoint
        .query()
        .unwrap()
        .contains("step=final"));
    socket.close().await.unwrap();
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].starts_with("/socket.io/?EIO=4&transport=websocket"));
    assert!(requests[1].contains("step=final"));
}

#[tokio::test]
async fn managed_socket_enforces_three_redirects_on_websocket_upgrade() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint = format!("ws://{address}/socket.io/?EIO=4&transport=websocket");
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();
        for hop in 0..4 {
            let (stream, _) = listener.accept().await.unwrap();
            let (request_tx, request_rx) = tokio::sync::oneshot::channel();
            let mut request_tx = Some(request_tx);
            let _ = accept_hdr_async(
                stream,
                move |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                      mut response| {
                    if let Some(sender) = request_tx.take() {
                        let _ = sender.send(request.uri().to_string());
                    }
                    *response.status_mut() = StatusCode::FOUND;
                    response.headers_mut().insert(
                        LOCATION,
                        format!("?EIO=4&transport=websocket&hop={}", hop + 1)
                            .parse()
                            .unwrap(),
                    );
                    Ok(response)
                },
            )
            .await;
            requests.push(request_rx.await.unwrap());
        }
        requests
    });
    let error = TinyHumansClient::new("http://ignored.invalid")
        .with_token(Some("secret".into()))
        .connect_socket_with_options(
            tinyhumans_sdk::socket::SocketConnectOptions::new()
                .endpoint(endpoint)
                .connect_timeout(Duration::from_secs(2)),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, Error::Socket(_)));
    let requests = timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        requests.len(),
        4,
        "initial request plus three followed redirects"
    );
}

#[tokio::test]
async fn managed_socket_keeps_one_attempt_after_peer_close() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint = format!("ws://{address}/socket.io/?EIO=4&transport=websocket");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        socket
            .send(Message::Text(
                r#"0{"sid":"engine-3","upgrades":[],"pingInterval":25000,"pingTimeout":20000}"#
                    .into(),
            ))
            .await
            .unwrap();
        loop {
            let message = socket.next().await.unwrap().unwrap();
            if message.to_text().is_ok_and(|text| text.starts_with("40")) {
                break;
            }
        }
        socket
            .send(Message::Text(r#"40{"sid":"namespace-3"}"#.into()))
            .await
            .unwrap();
        let _ = socket.close(None).await;
        timeout(Duration::from_millis(1300), listener.accept())
            .await
            .is_ok()
    });

    let mut socket = TinyHumansClient::new("http://ignored.invalid")
        .with_token(Some("secret".into()))
        .connect_socket_with_options(
            tinyhumans_sdk::socket::SocketConnectOptions::new()
                .endpoint(endpoint)
                .connect_timeout(Duration::from_secs(2))
                .single_attempt(true),
        )
        .await
        .unwrap();
    let stale_reconnect = timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();

    assert!(
        !stale_reconnect,
        "single-attempt socket reconnected after close"
    );
    socket.close().await.unwrap();
}

#[tokio::test]
async fn managed_socket_delivers_events_and_ack_payloads() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint = format!("ws://{address}/socket.io/?EIO=4&transport=websocket");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        socket
            .send(Message::Text(
                r#"0{"sid":"engine-4","upgrades":[],"pingInterval":25000,"pingTimeout":20000}"#
                    .into(),
            ))
            .await
            .unwrap();
        loop {
            let message = socket.next().await.unwrap().unwrap();
            if message.to_text().is_ok_and(|text| text.starts_with("40")) {
                break;
            }
        }
        socket
            .send(Message::Text(r#"40{"sid":"namespace-4"}"#.into()))
            .await
            .unwrap();
        socket
            .send(Message::Text(r#"42["server:event",{"ok":true}]"#.into()))
            .await
            .unwrap();
        socket
            .send(Message::Text(
                r#"451-["server:binary",{"_placeholder":true,"num":0}]"#.into(),
            ))
            .await
            .unwrap();
        socket.send(Message::Binary(vec![7, 8, 9])).await.unwrap();
        socket
            .send(Message::Text(
                r#"451-["server:binary",{"_placeholder":true,"num":0}]"#.into(),
            ))
            .await
            .unwrap();
        socket.send(Message::Binary(vec![7, 8, 9])).await.unwrap();
        loop {
            let message = socket.next().await.unwrap().unwrap();
            let Ok(text) = message.to_text() else {
                continue;
            };
            if let Some(packet) = text.strip_prefix("42") {
                let ack_id: String = packet
                    .chars()
                    .take_while(|character| character.is_ascii_digit())
                    .collect();
                if !ack_id.is_empty() {
                    socket
                        .send(Message::Text(format!("43{ack_id}\"ack-ok\"")))
                        .await
                        .unwrap();
                    break;
                }
            }
        }
    });
    let mut socket = TinyHumansClient::new("http://ignored.invalid")
        .with_token(Some("secret".into()))
        .connect_socket_with_options(
            tinyhumans_sdk::socket::SocketConnectOptions::new()
                .endpoint(endpoint)
                .connect_timeout(Duration::from_secs(2)),
        )
        .await
        .unwrap();

    let event = timeout(Duration::from_secs(1), async {
        loop {
            let event = socket.next_event().await.unwrap();
            if event.name == "server:event" {
                break event;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(event.name, "server:event");
    assert_eq!(
        event.payload,
        SocketPayload::Json(vec![serde_json::json!({"ok": true})])
    );
    let binary_event = timeout(Duration::from_secs(1), async {
        loop {
            let event = socket.next_event().await.unwrap();
            if event.name == "server:binary" {
                break event;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(binary_event.payload, SocketPayload::Binary(vec![7, 8, 9]));
    let binary_event = timeout(Duration::from_secs(1), async {
        loop {
            let event = socket.next_event().await.unwrap();
            if event.name == "server:binary" {
                break event;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(binary_event.payload, SocketPayload::Binary(vec![7, 8, 9]));
    let ack: String = socket
        .emit_with_ack(
            "client:event",
            &serde_json::json!({"ping": true}),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(ack, "ack-ok");
    socket.close().await.unwrap();
    server.await.unwrap();
}

#[test]
fn unknown_socket_events_remain_available_to_generic_consumers() {
    let event = SocketEvent::new("bot:joined", SocketPayload::Json(vec![json!({"ok": true})]));
    assert_eq!(event.name, "bot:joined");
    assert!(matches!(event.payload, SocketPayload::Json(_)));
}
