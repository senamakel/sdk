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
