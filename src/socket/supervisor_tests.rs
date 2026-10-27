use super::*;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

#[tokio::test]
async fn every_websocket_redirect_hop_has_its_own_connect_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0; 1024];
        let _ = stream.read(&mut request).await.unwrap();
        sleep(Duration::from_millis(150)).await;
    });
    let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let mut endpoint = format!("ws://{address}/socket.io/?EIO=4&transport=websocket");
    let started = Instant::now();
    let error =
        connect_with_redirects_within(&mut endpoint, &mut shutdown_rx, Duration::from_millis(25))
            .await
            .unwrap_err();
    assert!(matches!(error, ConnectFailure::Retryable(message) if message.contains("timed out")));
    assert!(started.elapsed() >= Duration::from_millis(25));
    server.await.unwrap();
}

#[tokio::test]
async fn cross_origin_redirect_is_rejected_before_the_target_handshake() {
    let redirect = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let redirect_address = redirect.local_addr().unwrap();
    let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target_address = target.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = redirect.accept().await.unwrap();
        let mut request = [0; 1024];
        let _ = stream.read(&mut request).await.unwrap();
        let response = format!(
            "HTTP/1.1 302 Found\r\nLocation: ws://{target_address}/socket.io/?EIO=4&transport=websocket\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        tokio::io::AsyncWriteExt::write_all(&mut stream, response.as_bytes())
            .await
            .unwrap();
    });
    let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let mut endpoint = format!("ws://{redirect_address}/socket.io/?EIO=4&transport=websocket");
    let result =
        connect_with_redirects_within(&mut endpoint, &mut shutdown_rx, Duration::from_secs(1))
            .await;
    assert!(
        matches!(result, Err(ConnectFailure::Retryable(message)) if message.contains("redirect changed origin"))
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), target.accept())
            .await
            .is_err()
    );
    server.await.unwrap();
}

#[test]
fn socket_event_parser_preserves_all_json_arguments() {
    assert_eq!(
        parse_event(r#"["device:update",{"value":1},"extra"]"#),
        Some((
            "device:update".to_owned(),
            vec![json!({"value": 1}), json!("extra")]
        ))
    );
}
