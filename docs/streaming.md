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

Socket.IO binary packets are surfaced as `SocketPayload::Binary`; JSON events
are `SocketPayload::Json`. This allows agent audio/video chunks and tunnel
frames to be forwarded without string conversion.
