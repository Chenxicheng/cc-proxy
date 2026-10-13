use serde_json::{Map, Value, json};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UsageStatus {
    Complete,
    Incomplete,
    Unreported,
    Invalid,
}

impl UsageStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Incomplete => "incomplete",
            Self::Unreported => "unreported",
            Self::Invalid => "invalid",
        }
    }
}

pub(crate) struct UsageConversion {
    pub(super) client: Value,
    status: UsageStatus,
    missing_fields: Vec<&'static str>,
}

pub(crate) fn convert_usage(usage: Option<&Value>) -> UsageConversion {
    let Some(usage) = usage.filter(|usage| !usage.is_null()) else {
        return conversion(0, 0, UsageStatus::Unreported);
    };
    let Some(usage) = usage.as_object() else {
        return conversion(0, 0, UsageStatus::Invalid);
    };
    let prompt_tokens = token(usage, "prompt_tokens");
    let completion_tokens = token(usage, "completion_tokens");
    let (Some(prompt_tokens), Some(completion_tokens)) = (prompt_tokens, completion_tokens) else {
        return conversion(
            prompt_tokens.unwrap_or(0),
            completion_tokens.unwrap_or(0),
            UsageStatus::Invalid,
        );
    };

    let mut result = conversion(prompt_tokens, completion_tokens, UsageStatus::Complete);
    let Some(details) = usage
        .get("prompt_tokens_details")
        .filter(|value| !value.is_null())
    else {
        result.status = UsageStatus::Incomplete;
        result.missing_fields = vec![
            "prompt_tokens_details.cached_tokens",
            "prompt_tokens_details.cache_write_tokens",
        ];
        return result;
    };
    let Some(details) = details.as_object() else {
        result.status = UsageStatus::Invalid;
        return result;
    };

    let cached_tokens = optional_token(details, "cached_tokens");
    let cache_write_tokens = optional_token(details, "cache_write_tokens");
    if cached_tokens.is_err() || cache_write_tokens.is_err() {
        result.status = UsageStatus::Invalid;
        return result;
    }
    let cached_tokens = cached_tokens.expect("checked result");
    let cache_write_tokens = cache_write_tokens.expect("checked result");
    let Some(classified_tokens) = cached_tokens
        .unwrap_or(0)
        .checked_add(cache_write_tokens.unwrap_or(0))
        .filter(|classified| *classified <= prompt_tokens)
    else {
        result.status = UsageStatus::Invalid;
        return result;
    };

    result.client["input_tokens"] = json!(prompt_tokens - classified_tokens);
    if let Some(tokens) = cached_tokens {
        result.client["cache_read_input_tokens"] = json!(tokens);
    } else {
        result.status = UsageStatus::Incomplete;
        result
            .missing_fields
            .push("prompt_tokens_details.cached_tokens");
    }
    if let Some(tokens) = cache_write_tokens {
        result.client["cache_creation_input_tokens"] = json!(tokens);
    } else {
        result.status = UsageStatus::Incomplete;
        result
            .missing_fields
            .push("prompt_tokens_details.cache_write_tokens");
    }
    result
}

pub(crate) fn record_usage_metadata(record: &mut Value, usage: &UsageConversion) {
    record["usageStatus"] = json!(usage.status.as_str());
    if !usage.missing_fields.is_empty() {
        record["usageMissingFields"] = json!(usage.missing_fields);
    }
}

fn conversion(input_tokens: u64, output_tokens: u64, status: UsageStatus) -> UsageConversion {
    UsageConversion {
        client: json!({"input_tokens": input_tokens, "output_tokens": output_tokens}),
        status,
        missing_fields: Vec::new(),
    }
}

fn token(object: &Map<String, Value>, field: &str) -> Option<u64> {
    object.get(field).and_then(Value::as_u64)
}

fn optional_token(object: &Map<String, Value>, field: &str) -> Result<Option<u64>, ()> {
    match object.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value.as_u64().map(Some).ok_or(()),
    }
}

#[cfg(test)]
#[path = "../../tests/unit/protocol/usage.rs"]
mod tests;
