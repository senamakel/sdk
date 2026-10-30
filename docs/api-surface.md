# API Surface

The SDK surface is grounded in the deployed Swagger/OpenAPI contract at
<https://api.tinyhumans.ai/swagger.json>. The spec reports TinyHumans API
`1.0.0` with 253 paths and 279 operations. The Rust SDK exposes one typed
method per public operation — **205 public async methods across the 22 namespaces
below**.
The remaining 32 administrative and 12 webhook-receiver operations are
intentionally excluded, including legacy routes whose summaries explicitly say
they are admin-only.

"Administrative" means *platform* administration. It does not cover operations
gated by a role within a resource the caller belongs to: `PUT /teams/{teamId}`,
`DELETE /teams/{teamId}/members/{userId}`, and
`PUT /teams/{teamId}/members/{userId}/role` say "(admin only)" in the contract,
but that is the **team-admin role** — held by every user over their own personal
team — not platform administrator rights. They are exposed on the `teams`
namespace. Teams were folded into users on the backend; the membership and
invite operations are deprecated in the contract and answer `410 Gone`.

"Webhook" exclusion means webhook *receivers* — the endpoints providers call
into (Stripe, Telegram, Discord, GitHub, Composio, Coinbase, Sentry, Twilio,
and the tunnel ingress paths). They carry no `bearerAuth`, are authenticated by
provider signature, and an SDK caller invoking one would be forging provider
traffic. The user-owned webhook *tunnel* CRUD under `/webhooks/core` is
ordinary bearer-authenticated user-facing API and is exposed as the `webhooks`
namespace.

| Namespace | Base path | Auth | Examples |
| --- | --- | --- | --- |
| `health` | `/` | none | `check()` liveness |
| `auth` | `/auth` | bearer | email login, OAuth, `me()`, integration tokens |
| `inference` | `/openai` | bearer | `GET /v1/models`, chat completions, responses, transcription |
| `agentIntegrations` | `/agent-integrations` | bearer | Composio, Parallel, media generation, maps, Tenor, crypto, OpenRouter |
| `apiKeys` | `/api-keys` | bearer | create, list, and revoke user API keys |
| `budgets` | `/budgets` | bearer | team budgets and seat allocations |
| `openCompany` | `/opencompany` | bearer | company instances, lifecycle, and custom domains |
| `payments` | `/payments` | bearer | Stripe, Coinbase, credits, transactions, plans |
| `feedback` | `/feedback` | bearer | create, ingest, list, detail, vote, and comments |
| `teams` | `/teams` | bearer | personal-team detail, usage, and billing; membership/invite routes are retired (`410`) |
| `channels` | `/channels` | bearer | messages, reactions, typing, threads |
| `mascots` | `/mascots` | mixed | catalog, manifests, demo page, Rive assets |
| `memory` | `/memory` | bearer | write experiences, recall, answer, events, facts/beliefs/understanding, derivation status, blobs, forget, scopes (billed per call), free period (whether memory is free right now) |
| `announcements` | `/announcements` | bearer | latest active announcement |
| `coupons` | `/coupons` | bearer | redemption and coupon history |
| `invite` | `/invite` | mixed | invite status, redemption, and owned codes |
| `referral` | `/referral` | bearer | referral stats and claim |
| `rewards` | `/rewards` | bearer | reward snapshot and Discord unlink |
| `redirect` | `/r` | none | resolve short redirect codes |
| `webhooks` | `/webhooks` | bearer | webhook tunnel CRUD and bandwidth budget |

The checked-in namespace manifest and Rust `PUBLIC_ROUTES` registry are
generated deterministically:

```bash
node scripts/sync-openapi.mjs
node scripts/sync-openapi.mjs --check
```

Most JSON responses use the hosted-backend envelope:

```json
{
  "success": true,
  "data": {}
}
```

SDK request helpers unwrap this envelope by default. The raw helper can return the
full response body when callers need status metadata or non-standard payloads.

Multipart callers can use `post_multipart_with_response_limit(path, form, max_bytes)`
to bound the response body while it is read. Responses exceeding the budget return
`Error::ResponseTooLarge` before JSON parsing. Failed responses redact echoes of
the client's configured bearer token and API key, including nested values and
object keys; successful payloads remain unchanged. The existing `post_multipart`
helper retains its previous behavior.

## Gemini

`agent_integrations::gemini` covers the Gemini API, billed at Google's paid-tier
rates plus a 10% premium:

- `gemini_generate_content(model, &GeminiGenerateContentRequest)` posts a native
  Gemini `generateContent` body to
  `/agent-integrations/gemini/models/{model}/generate-content`. Tools are limited
  to `GeminiTool::google_search()`, `GeminiTool::google_maps()` (with
  `GeminiToolConfig.retrieval_config.lat_lng`) and `GeminiTool::functions(..)`.
  The response is Google's `GenerateContentResponse`, including
  `groundingMetadata`, plus `cost_usd`.
- `gemini_create_live_session(&GeminiLiveSessionRequest)` opens a metered Live
  session: `Conversation` (native audio, Google Search and function calling) or
  `Transcribe` (`gemini-3.5-transcribe-live`). It returns a single-use ticket and
  a `ws_url`. Connect a plain WebSocket to `ws_url` within 60 seconds and speak
  the Gemini Live protocol (`realtimeInput`, `clientContent`, `toolResponse`).
  The session setup is fixed at mint time and a client `setup` frame is ignored.
  The backend relays the socket and meters every turn server-side. Close codes
  are exported as `GEMINI_LIVE_CLOSE_*`: 4401 bad ticket, 4402 insufficient
  credits (each open session reserves a minimum balance), 4408 idle or max
  duration, 1011 upstream failure.
- `gemini_live_session(id)` returns a session's status, turn count, charged
  amount and usage totals.

Live billing notes:

- Each turn is billed from the `usageMetadata` Google sends on `turnComplete`.
  Its prompt count is the turn's full context, so long sessions cost more per
  turn.
- `Transcribe` sessions get no usage reports from Google. The backend bills the
  PCM audio the client streams (32 tokens per second) plus the final transcript
  text.
- A `Conversation` with `GeminiTool::google_search()` bills one search query per
  turn, because Live does not report its searches.
- For function calling, answer each `toolCall.functionCalls[]` entry with a
  `toolResponse` frame carrying the same `id` and `name`. The relay forwards it
  unchanged.
- Gemini 2.5 text models are not offered, because Google no longer serves them
  to new keys. `gemini-2.5-flash-native-audio-preview-12-2025` remains available
  for Live.

The crate has no raw WebSocket client dependency, so the relay connection is
left to the caller's WebSocket library of choice.

## Sarvam

`agent_integrations::sarvam` covers Sarvam AI's Indic speech and chat models.
Request fields follow Sarvam's own API (`snake_case`). Speech is billed at
Sarvam's list price (₹30 per hour of audio, ₹30 per 10,000 characters) plus the
integration margin, and chat is billed at cost.

- `sarvam_speech_to_text(file_name, bytes, &SarvamSpeechToTextOptions)` uploads
  up to 30 seconds of audio (`saaras:v4` by default; `mode: "translate"` returns
  English). The response is Sarvam's transcript plus `duration_seconds` (measured
  from the file, billed in whole seconds) and `cost_usd`.
- `sarvam_text_to_speech(&SarvamTextToSpeechRequest)` returns base64 `audios`
  (`bulbul:v3` by default, 11 languages) plus the `characters` billed and
  `cost_usd`.
- `sarvam_chat_completion(&body)` posts an OpenAI-shaped chat completion for
  `sarvam-105b` or `sarvam-105b-conversations`. `sarvam-105b` reasons by default
  and bills hidden reasoning tokens as output; send `"reasoning_effort": null` to
  turn that off. `stream: true` is rejected, as on the OpenRouter methods.
- `sarvam_create_live_session(&SarvamLiveSessionRequest)` opens a metered
  streaming session: `Transcribe` (streaming speech-to-text) or `Speech`
  (streaming text-to-speech). Connect a plain WebSocket to `ws_url` within 60
  seconds and speak Sarvam's streaming protocol: `audio_input` events with
  base64 PCM in the `encoding` and `sample_rate` set on the `Transcribe` request
  (default 16 kHz `linear16`), `text`/`flush` messages
  for `Speech`. The configuration is fixed at mint time and client `config`
  frames are dropped. Close codes are exported as `SARVAM_LIVE_CLOSE_*` and
  match Gemini Live's; the relay also closes with `1000` after Sarvam's
  `session.end`.
- `SarvamLiveSessionRequest::Agent(SarvamLiveAgent { app_id, .. })` opens a
  Sarvam Voice Agents conversation with an agent authored in Sarvam's dashboard
  and allow-listed by the backend. Send `client.media.audio_chunk` frames
  (`audio_base64`: 16-bit mono PCM at `input_sample_rate`) and answer every
  `server.system.ping` with a `client.system.pong` carrying its `event_id`. It is
  billed per second from `server.action.interaction_connected` to close, and the
  backend refuses it (503) until an operator configures the per-minute price.
- `sarvam_live_session(id)` returns a session's status, charged amount and
  usage totals (`audio_seconds`, `characters`, `agent_seconds`).

Streaming billing: transcription bills every second of audio sent once Sarvam
accepts the session, so a session Sarvam rejects at connect costs nothing.
Speech bills characters as Sarvam returns audio for them, and each `text`
message may carry at most 2,500 characters (longer ones are dropped).

## OpenRouter media generation

`agent_integrations::openrouter` exposes the direct OpenRouter proxy under
`/agent-integrations/openrouter/*`, including image (`POST /images`) and video
(`POST /videos`, `GET /videos/{jobId}`, `GET /videos/{jobId}/content`)
generation with untyped `impl Serialize` request bodies (OpenRouter's own API
is the contract for this surface).

`agent_integrations::openrouter_media` adds a fully typed alternative for the
media routes only — `OpenRouterImageRequest`/`OpenRouterImageResponse`,
`OpenRouterVideoRequest`, `ContentPartImage`, `FrameImage` — with an `extra`
flattened map on each request struct so an upstream field this module does not
yet model is still forwarded. Both modules call the same routes; pick whichever
fits the caller (`openrouter_images`/`openrouter_videos`/
`openrouter_image_models`/`openrouter_video_models` for typed,
`openrouter_create_image`/`openrouter_create_video`/
`list_openrouter_image_models`/`list_openrouter_video_models` for
passthrough). `get_openrouter_video`, `openrouter_video_content`, and the typed
module's `openrouter_video_content_with_type` (which also surfaces the
upstream `content-type`, since the plain byte helper drops response headers)
are shared by both.

`OpenRouterMediaModel` (returned by both the typed and untyped model listings)
carries OpenRouter's capability descriptors verbatim when the backend's
catalog published them: `supported_parameters`/`architecture` for image
models, and `supported_resolutions`/`supported_aspect_ratios`/
`supported_durations`/`supported_sizes`/`supported_frame_images`/
`generate_audio`/`seed`/`allowed_passthrough_parameters` for video models —
so a caller can validate a request against a model's real capabilities before
submitting it.

The older GMI-backed `agent_integrations::media_generation` module
(`/agent-integrations/media-generation/*`) is deprecated in favor of the
OpenRouter surface above; its methods are `#[deprecated]` but remain
functional.
