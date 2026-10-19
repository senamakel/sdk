use super::*;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_tungstenite::{tungstenite::Message, accept_async};

#[tokio::test]
async fn concurrent_close_deadline_includes_waiting_for_shutdown_lock() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint = format!("ws://{address}/socket.io/?EIO=4&transport=websocket");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        socket
            .send(Message::Text(
                r#"0{"sid":"engine-lock","upgrades":[],"pingInterval":25000,"pingTimeout":20000}"#.into(),
            ))
            .await
            .unwrap();
        loop {
            let message = timeout(Duration::from_secs(2), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if message.to_text().is_ok_and(|text| text.starts_with("40")) {
                break;
            }
        }
        socket
            .send(Message::Text(r#"40{"sid":"namespace-lock"}"#.into()))
            .await
            .unwrap();
        let _ = timeout(Duration::from_secs(2), socket.next()).await;
    });

    let socket = Arc::new(
        SocketConnection::connect_with_options(
            "http://ignored.invalid".into(),
            "secret".into(),
            SocketConnectOptions::new()
                .endpoint(endpoint)
                .connect_timeout(Duration::from_secs(2))
                .close_timeout(Duration::from_millis(100)),
        )
        .await
        .unwrap(),
    );
    // Hold the lifecycle mutex to model a concurrent close already waiting on
    // task shutdown; a second close must spend its deadline acquiring it too.
    let guard = socket.managed.as_ref().unwrap().lock().await;
    let closing_socket = Arc::clone(&socket);
    let mut close = tokio::spawn(async move { closing_socket.close().await });
    let bounded = timeout(Duration::from_millis(300), &mut close).await;
    drop(guard);
    if bounded.is_err() {
        let _ = close.await;
    }
    assert!(matches!(
        bounded,
        Ok(Ok(Err(Error::SocketCloseTimeout)))
    ));
    server.await.unwrap();
}
