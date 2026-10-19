# Streaming

The SDK supports the live transport exposed by the backend: authenticated
Socket.IO for channels, agent media, WebRTC signaling, and
webhook tunnels.

## Socket.IO

`connect_socket` authenticates with the same bearer token and reconnects after
transport failures. `next_event` receives the complete backend event catalog.
Use `emit`, `emit_binary`, and `emit_with_ack` to send; the constants in
`socket::events` prevent event-name typos, and `SocketEvent::decode` turns a
JSON payload into whatever type the event carries.

Call `connect_socket_with_options` when the caller owns retry and credential
refresh policy. It defaults to one attempt and bounds connection establishment
through the namespace handshake. An explicit endpoint is used as supplied;
include the Socket.IO path and Engine.IO query, for example
`wss://api.example.test/socket.io/?EIO=4&transport=websocket`.

```rust
use serde::Deserialize;
use tinyhumans_sdk::socket::events;
use tinyhumans_sdk::TinyHumansClient;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Joined {
    channel_id: String,
}

# async fn run() -> Result<(), tinyhumans_sdk::Error> {
let client = TinyHumansClient::new("https://api.tinyhumans.ai")
    .with_token(std::env::var("TINYHUMANS_TOKEN").ok());
let mut socket = client.connect_socket().await?;

while let Some(event) = socket.next_event().await {
    if event.name == events::inbound::BOT_JOINED {
        let joined: Joined = event.decode()?;
        println!("joined {}", joined.channel_id);
    }
}
# Ok(())
# }
```

```rust
use std::time::Duration;
use tinyhumans_sdk::socket::{SocketConnectOptions, SocketNamespaceOutcome};
use tinyhumans_sdk::TinyHumansClient;

# async fn run() -> Result<(), tinyhumans_sdk::Error> {
let client = TinyHumansClient::new("https://api.tinyhumans.ai")
    .with_token(std::env::var("TINYHUMANS_TOKEN").ok());
let mut socket = client
    .connect_socket_with_options(
        SocketConnectOptions::new()
            .endpoint("wss://api.tinyhumans.ai/socket.io/?EIO=4&transport=websocket")
            .connect_timeout(Duration::from_secs(10))
            .close_timeout(Duration::from_secs(2)),
    )
    .await?;

match socket.namespace_outcome() {
    Some(SocketNamespaceOutcome::Ready { sid }) => println!("connected: {sid:?}"),
    Some(SocketNamespaceOutcome::Rejected { payload }) => println!("rejected: {payload}"),
    Some(SocketNamespaceOutcome::Closed) => println!("closed during namespace handshake"),
    None => unreachable!("options-based connections report a namespace outcome"),
}
println!("redirects: {:?}", socket.connection_metadata().unwrap().redirects);
socket.close().await?;
# Ok(())
# }
```

The options-based connection owns its polling/reconnect task. `close` is
bounded and joins that task; if a close wait is canceled or times out, the
connection retains ownership so the caller can retry. Dropping the connection
requests cancellation. JSON and binary events and acknowledgement emits work
the same way as with the reconnecting `connect_socket` API.

Socket.IO binary packets are surfaced as `SocketPayload::Binary`; JSON events
are `SocketPayload::Json`. This allows agent audio/video chunks and tunnel
frames to be forwarded without string conversion.
