use serde_json::json;
use tinyhumans_sdk::TinyHumansClient;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn ok(data: serde_json::Value) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({"success": true, "data": data}))
}

#[tokio::test]
async fn list_mascots_unwraps_envelope() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/mascots"))
        .respond_with(ok(json!([{"id": "fox"}])))
        .mount(&server)
        .await;

    let client = TinyHumansClient::new(server.uri());
    let result = client.mascots().list_mascots().await.unwrap();
    assert_eq!(result, json!([{"id": "fox"}]));
}

#[tokio::test]
async fn get_demo_gets() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/mascots/demo"))
        .respond_with(ok(json!({"page": "demo"})))
        .mount(&server)
        .await;

    let client = TinyHumansClient::new(server.uri());
    let result = client.mascots().get_demo().await.unwrap();
    assert_eq!(result, json!({"page": "demo"}));
}

#[tokio::test]
async fn get_mascot_uses_path_param() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/mascots/fox"))
        .respond_with(ok(json!({"id": "fox"})))
        .mount(&server)
        .await;

    let client = TinyHumansClient::new(server.uri());
    let result = client.mascots().get_mascot("fox").await.unwrap();
    assert_eq!(result, json!({"id": "fox"}));
}

#[tokio::test]
async fn get_mascot_riv_uses_path_param() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/mascots/fox/riv"))
        .respond_with(ok(json!({"url": "cdn/fox.riv"})))
        .mount(&server)
        .await;

    let client = TinyHumansClient::new(server.uri());
    let result = client.mascots().get_mascot_riv("fox").await.unwrap();
    assert_eq!(result, json!({"url": "cdn/fox.riv"}));
}
