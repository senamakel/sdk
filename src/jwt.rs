//! JWT helpers for TinyHumans session tokens.
//!
//! The backend issues a bare JWT as the session token. These helpers read it —
//! they never verify it. The backend stays the authority on validity: a token
//! revoked before its `exp` still returns 401, and callers must handle that.
//! Reading `exp` locally is only an optimisation that avoids sending a request
//! with a token already known to be dead.
//!
//! Where the token is *stored* is the host's concern (OS keyring, config file,
//! environment), so this module deliberately covers only parsing and header
//! formatting.

use base64::Engine;
use serde_json::Value;

/// Format a token as an `Authorization: Bearer …` header value.
///
/// Surrounding whitespace is trimmed — tokens pasted by hand or read from a
/// file routinely carry a trailing newline, and the backend rejects the header
/// if it survives. Interior whitespace is left alone: it cannot appear in a
/// well-formed JWT, so trimming it would mask a malformed token rather than
/// fix one.
pub fn bearer_authorization_value(token: &str) -> String {
    format!("Bearer {}", token.trim())
}

/// Decode a JWT's payload without verifying the signature.
///
/// Returns `None` for anything that is not a JWT with a base64url payload
/// holding JSON — including the non-JWT sentinels hosts sometimes store for
/// offline or local sessions, which must not panic here.
pub fn decode_jwt_payload(token: &str) -> Option<Value> {
    // JWT = header.payload.signature (base64url, no padding). Only the payload
    // segment is needed. Padded input is accepted as a fallback because not
    // every issuer omits padding.
    let payload_b64 = token.trim().split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload_b64)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(payload_b64))
        .ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Read a JWT's `exp` claim as a Unix timestamp in seconds.
///
/// Returns seconds rather than a date type so the crate stays free of a
/// datetime dependency; hosts convert with whatever they already use.
/// `exp` is a NumericDate, so both integer and float encodings are accepted.
pub fn decode_jwt_exp_unix(token: &str) -> Option<i64> {
    decode_jwt_payload(token)?
        .get("exp")
        .and_then(|value| value.as_i64().or_else(|| value.as_f64().map(|f| f as i64)))
}

#[cfg(test)]
#[path = "jwt_tests.rs"]
mod tests;
