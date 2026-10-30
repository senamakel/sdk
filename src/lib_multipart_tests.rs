use super::*;

#[test]
fn credential_echoes_are_removed_from_nested_error_payloads() {
    let bearer = "echoed bearer";
    let api_key = "echoed api credential";
    let mut value = serde_json::json!({
        "error": format!("rejected {bearer}"),
        bearer: [api_key, {"detail": bearer}, 3, false, null],
    });
    redact_credential_echoes(&mut value, &[Some(bearer), Some(api_key)]);
    let output = value.to_string();
    assert!(!output.contains(bearer));
    assert!(!output.contains(api_key));
    assert!(output.contains("[REDACTED]"));
    assert_eq!(value["[REDACTED]"][2], 3);
}

#[test]
fn absent_or_blank_credentials_leave_diagnostics_unchanged() {
    let mut value = serde_json::json!({"error": ["message", 4, true, null]});
    let original = value.clone();
    redact_credential_echoes(&mut value, &[None, Some("  ")]);
    assert_eq!(value, original);
}

#[test]
fn overlapping_credentials_are_redacted_without_exposing_a_suffix() {
    let mut value = serde_json::json!({"abc": ["abcdef-secret", "abc"]});
    redact_credential_echoes(&mut value, &[Some("abc"), Some("abcdef-secret")]);
    assert_eq!(
        value,
        serde_json::json!({"[REDACTED]": ["[REDACTED]", "[REDACTED]"]})
    );
}

#[test]
fn redaction_markers_cannot_echo_configured_credentials() {
    for secret in ["REDACTED", "[REDACTED]", "["] {
        let mut value = serde_json::json!({secret: [secret, "ordinary diagnostic"]});
        redact_credential_echoes(&mut value, &[Some(secret)]);
        let object = value.as_object().unwrap();
        assert!(object.keys().all(|key| !key.contains(secret)));
        let values = object.values().next().unwrap().as_array().unwrap();
        assert!(!values[0].as_str().unwrap().contains(secret));
        assert_eq!(values[1], "ordinary diagnostic");
    }
}

#[test]
fn removing_a_marker_colliding_credential_cannot_join_another_credential() {
    let mut value = serde_json::json!("aREDACTEDbc");
    redact_credential_echoes(&mut value, &[Some("REDACTED"), Some("abc")]);
    assert!(!value.as_str().unwrap().contains("REDACTED"));
    assert!(!value.as_str().unwrap().contains("abc"));
}
