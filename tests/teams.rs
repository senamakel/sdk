use serde_json::json;
use tinyhumans_sdk::TinyHumansClient;
use wiremock::{
    matchers::{method, path, query_param},
    Mock, MockServer, ResponseTemplate,
};

#[tokio::test]
async fn exposes_user_team_routes() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/teams"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"success":true,"data":[]})))
        .mount(&server)
        .await;
    assert_eq!(
        TinyHumansClient::new(server.uri())
            .teams()
            .list_teams()
            .await
            .unwrap(),
        json!([])
    );
}

#[tokio::test]
async fn get_my_usage_for_range_sends_range_query() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/teams/me/usage"))
        .and(query_param("range", "30d"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "success": true,
            "data": {"range": "30d"}
        })))
        .mount(&server)
        .await;

    assert_eq!(
        TinyHumansClient::new(server.uri())
            .teams()
            .get_my_usage_for_range("30d")
            .await
            .unwrap(),
        json!({"range": "30d"})
    );
}

// `Url::parse` resolves `.`/`..` segments, literal or percent-encoded, so
// `remove_member(team, "..")` would send `DELETE /teams/{team}/`. The SDK
// must refuse the request instead of re-targeting it.
#[tokio::test]
async fn dot_segment_ids_are_refused_not_retargeted() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"success":true,"data":{}})))
        .mount(&server)
        .await;
    let client = TinyHumansClient::new(server.uri());
    let typed = client.teams().remove_member("team1", "..").await;
    assert!(
        matches!(typed, Err(tinyhumans_sdk::Error::RouteNotExposed(..))),
        "{typed:?}"
    );
    for raw in [
        "/teams/team1/members/.",
        "/teams/team1/members/%2E%2e",
        "/teams/team1/members/.%2e?x=1",
        "/teams/team1/members/.\t.",
        "/teams/team1\\..",
    ] {
        let result = client
            .raw()
            .send(reqwest::Method::DELETE, raw, &[], None, true)
            .await;
        assert!(
            matches!(result, Err(tinyhumans_sdk::Error::RouteNotExposed(..))),
            "{raw:?}: {result:?}"
        );
    }
    assert!(server.received_requests().await.unwrap().is_empty());
}
