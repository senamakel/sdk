//! Bounded multipart responses preserve transport safety at the SDK boundary.

use tinyhumans_sdk::TinyHumansClient;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const LIMIT: usize = 65_536;

#[tokio::test]
async fn oversized_multipart_response_is_rejected() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/openai/v1/audio/transcriptions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"text": "x".repeat(LIMIT)})),
        )
        .mount(&server)
        .await;
    let client = TinyHumansClient::new(server.uri());
    let result = client
        .raw()
        .post_multipart_with_response_limit(
            "/openai/v1/audio/transcriptions",
            reqwest::multipart::Form::new(),
            LIMIT,
        )
        .await;
    assert!(
        matches!(result, Err(tinyhumans_sdk::Error::ResponseTooLarge { max_bytes }) if max_bytes == LIMIT),
        "response exceeded the transcription byte budget"
    );
}

#[tokio::test]
async fn rejected_multipart_response_does_not_echo_credentials() {
    let server = MockServer::start().await;
    let secret = "sensitive echoed credential";
    Mock::given(method("POST"))
        .and(path("/openai/v1/audio/transcriptions"))
        .respond_with(
            ResponseTemplate::new(401)
                .set_body_json(serde_json::json!({"error": secret, "nested": [secret]})),
        )
        .mount(&server)
        .await;
    let client = TinyHumansClient::new(server.uri()).with_token(Some(secret.into()));
    let result = client
        .raw()
        .post_multipart_with_response_limit(
            "/openai/v1/audio/transcriptions",
            reqwest::multipart::Form::new(),
            LIMIT,
        )
        .await
        .unwrap_err();
    assert!(
        !result.to_string().contains(secret),
        "error leaked the supplied credential"
    );
}

#[tokio::test]
async fn response_at_the_budget_is_accepted_without_redacting_success() {
    let server = MockServer::start().await;
    let secret = "successful transcription";
    let value = serde_json::json!({"text": secret});
    let budget = value.to_string().len();
    Mock::given(method("POST"))
        .and(path("/openai/v1/audio/transcriptions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(value.clone()))
        .mount(&server)
        .await;
    let client = TinyHumansClient::new(server.uri()).with_token(Some(secret.into()));
    let result = client
        .raw()
        .post_multipart_with_response_limit(
            "/openai/v1/audio/transcriptions",
            reqwest::multipart::Form::new(),
            budget,
        )
        .await
        .unwrap();
    assert_eq!(result, value);
}

#[tokio::test]
async fn unsuccessful_envelope_redacts_api_key_echoes_on_success_status() {
    let server = MockServer::start().await;
    let secret = "echoed api credential";
    Mock::given(method("POST"))
        .and(path("/openai/v1/audio/transcriptions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({"success": false, "error": secret, "details": [secret]}),
        ))
        .mount(&server)
        .await;
    let client = TinyHumansClient::new(server.uri()).with_api_key(Some(secret.into()));
    let result = client
        .raw()
        .post_multipart_with_response_limit(
            "/openai/v1/audio/transcriptions",
            reqwest::multipart::Form::new(),
            LIMIT,
        )
        .await
        .unwrap_err();
    assert!(!result.to_string().contains(secret));
    assert!(result.to_string().contains("[REDACTED]"));
}

#[tokio::test]
async fn a_credential_matching_a_protocol_key_keeps_the_failure_classification() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/openai/v1/audio/transcriptions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({"success": false, "error": "success", "details": {"success": "success"}}),
        ))
        .mount(&server)
        .await;
    let client = TinyHumansClient::new(server.uri()).with_token(Some("success".into()));
    let result = client
        .raw()
        .post_multipart_with_response_limit(
            "/openai/v1/audio/transcriptions",
            reqwest::multipart::Form::new(),
            LIMIT,
        )
        .await;
    assert!(
        matches!(&result, Err(tinyhumans_sdk::Error::Envelope { .. })),
        "{result:?}"
    );
    let error = result.unwrap_err().to_string();
    assert!(!error.contains("success"));
}

#[tokio::test]
async fn marker_colliding_credentials_are_removed_from_status_and_envelope_errors() {
    let server = MockServer::start().await;
    for status in [401, 200] {
        server.reset().await;
        Mock::given(method("POST"))
            .and(path("/openai/v1/audio/transcriptions"))
            .respond_with(ResponseTemplate::new(status).set_body_json(
                serde_json::json!({"success": false, "error": "REDACTED", "details": {"[REDACTED]": "REDACTED"}}),
            ))
            .mount(&server)
            .await;
        let client = TinyHumansClient::new(server.uri())
            .with_token(Some("REDACTED".into()))
            .with_api_key(Some("[REDACTED]".into()));
        let error = client
            .raw()
            .post_multipart_with_response_limit(
                "/openai/v1/audio/transcriptions",
                reqwest::multipart::Form::new(),
                LIMIT,
            )
            .await
            .unwrap_err();
        if status == 401 {
            assert!(matches!(&error, tinyhumans_sdk::Error::Status { .. }));
        } else {
            assert!(matches!(&error, tinyhumans_sdk::Error::Envelope { .. }));
        }
        assert!(!error.to_string().contains("REDACTED"));
        assert!(!format!("{error:?}").contains("REDACTED"));
    }
}
