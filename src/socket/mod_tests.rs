use super::*;

#[test]
fn endpoint_policy_allows_loopback_http_and_remote_tls() {
    assert!(validate_endpoint("http://127.0.0.1:7788").is_ok());
    assert!(validate_endpoint("http://[::1]:7788").is_ok());
    assert!(validate_endpoint("https://api.example.test").is_ok());
    assert!(validate_endpoint("wss://api.example.test").is_ok());
}

#[test]
fn endpoint_policy_rejects_remote_plaintext_and_unknown_schemes() {
    assert!(matches!(
        validate_endpoint("http://api.example.test"),
        Err(Error::UnsafeSocketEndpoint)
    ));
    assert!(matches!(
        validate_endpoint("ftp://localhost"),
        Err(Error::UnsafeSocketEndpoint)
    ));
    assert!(matches!(
        validate_endpoint("not a url"),
        Err(Error::UnsafeSocketEndpoint)
    ));
}

#[test]
fn invalid_token_detection_matches_terminal_connect_rejections_only() {
    assert!(is_invalid_token_payload(&Payload::Text(vec![
        serde_json::json!({"message": "Invalid token"})
    ])));
    assert!(is_invalid_token_payload(&Payload::Text(vec![
        serde_json::json!("session token expired")
    ])));
    assert!(!is_invalid_token_payload(&Payload::Text(vec![
        serde_json::json!({"message": "gateway unavailable"})
    ])));
}

#[test]
fn token_provider_is_queried_for_each_connection_attempt() {
    let attempts = std::sync::atomic::AtomicUsize::new(0);
    let provider = || {
        let attempt = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Some(format!("token-{attempt}"))
    };

    assert_eq!(next_token(&provider).as_deref(), Some("token-0"));
    assert_eq!(next_token(&provider).as_deref(), Some("token-1"));
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[test]
fn redirect_policy_follows_same_origin_and_rejects_cross_origin_credentials() {
    assert_eq!(
        redirect_target("wss://api.example.test/socket.io/?EIO=4", "/edge/socket.io/?EIO=4")
            .unwrap(),
        "wss://api.example.test/edge/socket.io/?EIO=4"
    );
    assert!(matches!(
        redirect_target(
            "wss://api.example.test/socket.io/?EIO=4",
            "wss://other.example.test/socket.io/?EIO=4"
        ),
        Err(Error::CrossOriginSocketRedirect)
    ));
}

#[test]
fn backend_url_is_converted_to_engine_io_websocket_endpoint() {
    assert_eq!(
        websocket_url("https://api.example.test/base/"),
        "wss://api.example.test/base/socket.io/?EIO=4&transport=websocket"
    );
    assert_eq!(
        websocket_url("http://127.0.0.1:7788"),
        "ws://127.0.0.1:7788/socket.io/?EIO=4&transport=websocket"
    );
}
