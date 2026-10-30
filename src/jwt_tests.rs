use super::*;

fn jwt_with_payload(payload_json: &str) -> String {
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload_json);
    // Header and signature are irrelevant — nothing here verifies them.
    format!("eyJhbGciOiJIUzI1NiJ9.{payload}.sig")
}

#[test]
fn bearer_value_trims_surrounding_whitespace_only() {
    assert_eq!(bearer_authorization_value("my_token"), "Bearer my_token");
    assert_eq!(
        bearer_authorization_value("  spaced_token  "),
        "Bearer spaced_token"
    );
    assert_eq!(bearer_authorization_value(""), "Bearer ");
    assert_eq!(bearer_authorization_value("   "), "Bearer ");
    // Interior whitespace is preserved — see the doc comment.
    assert_eq!(
        bearer_authorization_value("token with spaces"),
        "Bearer token with spaces"
    );
}

#[test]
fn decodes_payload_claims() {
    let token = jwt_with_payload(r#"{"sub":"u1","exp":1700000000}"#);
    let claims = decode_jwt_payload(&token).unwrap();
    assert_eq!(claims["sub"], "u1");
}

#[test]
fn reads_integer_exp() {
    let token = jwt_with_payload(r#"{"sub":"u1","exp":1700000000}"#);
    assert_eq!(decode_jwt_exp_unix(&token), Some(1_700_000_000));
}

#[test]
fn reads_float_exp() {
    let token = jwt_with_payload(r#"{"exp":1700000000.0}"#);
    assert_eq!(decode_jwt_exp_unix(&token), Some(1_700_000_000));
}

#[test]
fn none_when_exp_absent() {
    let token = jwt_with_payload(r#"{"sub":"u1"}"#);
    assert_eq!(decode_jwt_exp_unix(&token), None);
}

#[test]
fn none_for_non_jwt_or_malformed_input() {
    assert_eq!(decode_jwt_exp_unix("not-a-jwt"), None);
    assert_eq!(decode_jwt_exp_unix(""), None);
    // Payload segment "b" is not valid base64 JSON.
    assert_eq!(decode_jwt_exp_unix("a.b"), None);
    // Host sentinel for a local offline session — must be None, not a panic.
    assert_eq!(decode_jwt_exp_unix("local-session-xyz"), None);
}
