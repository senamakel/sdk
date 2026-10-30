//! Classification of backend failure shapes that only this backend's wire
//! behaviour can explain.
//!
//! Hosts turn these into their own typed recovery states; the SDK only says
//! what a response *means* on the deployed backend.

use serde_json::Value;

use crate::Error;

/// Extract `(provider, message_id)` from a channel-message path of the shape
/// `…/channels/<provider>/messages/<id>`.
///
/// Accepts the canonical four-segment form and any base-path prefix
/// (`/api/v1/channels/telegram/messages/1103`) through a sliding window, so a
/// backend URL mounted under a prefix still classifies. Returns `None` for
/// paths that do not contain that four-segment subsequence.
pub fn channel_message_path(path: &str) -> Option<(&str, &str)> {
    let path = path.split(['?', '#']).next().unwrap_or(path);
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    segments
        .windows(4)
        .find(|window| window[0] == "channels" && window[2] == "messages")
        .map(|window| (window[1], window[3]))
}

/// Whether a 404 body came from *no route matching* rather than from a
/// handler reporting a missing resource.
///
/// The backend registers no catch-all 404, so an unmatched route falls through
/// to Express's built-in `finalhandler`, which answers with an HTML page
/// (`Cannot PATCH /channels/…`). Every handler-level 404 answers with a JSON
/// envelope instead. So a body that parses as a JSON object or array means a
/// handler answered; anything else (HTML, plain text, empty) is route absence.
///
/// `body` is the value [`Error::Status`] carries: parsed JSON when the body
/// parsed, otherwise the raw text as a JSON string.
pub fn is_unmatched_route_body(body: &Value) -> bool {
    match body {
        Value::Null => true,
        Value::String(text) => {
            let trimmed = text.trim();
            trimmed.is_empty() || serde_json::from_str::<Value>(trimmed).is_err()
        }
        _ => false,
    }
}

impl Error {
    /// `true` for a 404 whose body shows no backend route matched the request
    /// (see [`is_unmatched_route_body`]).
    pub fn is_unmatched_route_404(&self) -> bool {
        matches!(self, Error::Status { status: 404, body } if is_unmatched_route_body(body))
    }
}
