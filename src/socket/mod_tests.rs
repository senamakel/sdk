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
