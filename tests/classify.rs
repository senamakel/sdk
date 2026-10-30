use serde_json::{json, Value};
use tinyhumans_sdk::classify::{channel_message_path, is_unmatched_route_body};
use tinyhumans_sdk::Error;

#[test]
fn channel_message_path_parses_canonical_form() {
    assert_eq!(
        channel_message_path("/channels/telegram/messages/1103"),
        Some(("telegram", "1103"))
    );
}

#[test]
fn channel_message_path_accepts_base_path_prefix_and_query() {
    assert_eq!(
        channel_message_path("/api/v1/channels/discord/messages/abc?x=1"),
        Some(("discord", "abc"))
    );
}

#[test]
fn channel_message_path_rejects_other_routes() {
    assert_eq!(channel_message_path("/channels/telegram/threads/1"), None);
    assert_eq!(channel_message_path("/channels/telegram/messages"), None);
    assert_eq!(channel_message_path("/teams/me/usage"), None);
}

#[test]
fn html_and_empty_bodies_are_route_absence() {
    assert!(is_unmatched_route_body(&Value::String(
        "<!DOCTYPE html><pre>Cannot PATCH /channels/telegram/messages/1</pre>".into()
    )));
    assert!(is_unmatched_route_body(&Value::String("   ".into())));
    assert!(is_unmatched_route_body(&Value::Null));
}

#[test]
fn json_envelope_bodies_are_handler_answers() {
    assert!(!is_unmatched_route_body(
        &json!({"success": false, "error": "not found"})
    ));
    assert!(!is_unmatched_route_body(&json!([])));
}

#[test]
fn error_is_unmatched_route_404_needs_404_and_non_json_body() {
    let html = Value::String("Cannot PATCH /x".into());
    assert!(Error::Status {
        status: 404,
        body: html.clone()
    }
    .is_unmatched_route_404());
    assert!(!Error::Status {
        status: 500,
        body: html
    }
    .is_unmatched_route_404());
    assert!(!Error::Status {
        status: 404,
        body: json!({"success": false})
    }
    .is_unmatched_route_404());
}
