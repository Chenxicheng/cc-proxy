use super::*;

fn metadata(usage: &UsageConversion) -> Value {
    let mut record = json!({});
    record_usage_metadata(&mut record, usage);
    record
}

#[test]
fn maps_complete_cache_usage_without_losing_totals() {
    for (cached, written, expected_input) in [(0, 0, 100), (80, 0, 20), (0, 30, 70), (60, 30, 10)] {
        let usage = convert_usage(Some(&json!({
            "prompt_tokens": 100,
            "completion_tokens": 10,
            "prompt_tokens_details": {
                "cached_tokens": cached,
                "cache_write_tokens": written
            }
        })));
        assert_eq!(usage.client["input_tokens"], expected_input);
        assert_eq!(usage.client["output_tokens"], 10);
        assert_eq!(usage.client["cache_read_input_tokens"], cached);
        assert_eq!(usage.client["cache_creation_input_tokens"], written);
        assert_eq!(metadata(&usage), json!({"usageStatus":"complete"}));
    }
}

#[test]
fn preserves_known_cache_counts_when_other_details_are_missing() {
    for (details, expected, missing) in [
        (
            json!({"cached_tokens": 80, "cache_write_tokens": null}),
            json!({"input_tokens":20,"output_tokens":10,"cache_read_input_tokens":80}),
            vec!["prompt_tokens_details.cache_write_tokens"],
        ),
        (
            json!({"cache_write_tokens": 30}),
            json!({"input_tokens":70,"output_tokens":10,"cache_creation_input_tokens":30}),
            vec!["prompt_tokens_details.cached_tokens"],
        ),
    ] {
        let usage = convert_usage(Some(&json!({
            "prompt_tokens": 100,
            "completion_tokens": 10,
            "prompt_tokens_details": details
        })));
        assert_eq!(usage.client, expected);
        assert_eq!(metadata(&usage)["usageStatus"], "incomplete");
        assert_eq!(metadata(&usage)["usageMissingFields"], json!(missing));
    }

    for details in [None, Some(Value::Null)] {
        let mut raw = json!({"prompt_tokens":100,"completion_tokens":10});
        if let Some(details) = details {
            raw["prompt_tokens_details"] = details;
        }
        let usage = convert_usage(Some(&raw));
        assert_eq!(usage.client, json!({"input_tokens":100,"output_tokens":10}));
        assert_eq!(metadata(&usage)["usageStatus"], "incomplete");
        assert_eq!(
            metadata(&usage)["usageMissingFields"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
    }
}

#[test]
fn degrades_invalid_usage_without_discarding_legal_totals() {
    for details in [
        json!({"cached_tokens":"80","cache_write_tokens":0}),
        json!({"cached_tokens":80,"cache_write_tokens":30}),
        json!([]),
    ] {
        let usage = convert_usage(Some(&json!({
            "prompt_tokens": 100,
            "completion_tokens": 10,
            "prompt_tokens_details": details
        })));
        assert_eq!(usage.client, json!({"input_tokens":100,"output_tokens":10}));
        assert_eq!(metadata(&usage), json!({"usageStatus":"invalid"}));
    }

    let usage = convert_usage(Some(&json!({"prompt_tokens":"bad","completion_tokens":10})));
    assert_eq!(usage.client, json!({"input_tokens":0,"output_tokens":10}));
    assert_eq!(metadata(&usage), json!({"usageStatus":"invalid"}));
}

#[test]
fn distinguishes_unreported_usage_from_real_zeroes() {
    for raw in [None, Some(&Value::Null)] {
        let usage = convert_usage(raw);
        assert_eq!(usage.client, json!({"input_tokens":0,"output_tokens":0}));
        assert_eq!(metadata(&usage), json!({"usageStatus":"unreported"}));
    }

    let usage = convert_usage(Some(&json!({
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "prompt_tokens_details": {"cached_tokens":0,"cache_write_tokens":0}
    })));
    assert_eq!(metadata(&usage), json!({"usageStatus":"complete"}));
}
