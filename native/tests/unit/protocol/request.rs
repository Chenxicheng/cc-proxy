use super::*;
use axum::http::StatusCode;

fn convert_request(body: &Value, base_url: &str) -> Result<Value, AppError> {
    convert_request_with_mode(body, base_url, AssistantPrefillMode::Unsupported)
}

#[test]
fn matches_shared_request_fixtures() {
    let fixtures: Value = serde_json::from_str(include_str!(
        "../../../../bench/fixtures/request-conversion.json"
    ))
    .unwrap();
    for fixture in fixtures.as_array().unwrap() {
        let actual = convert_request_with_mode(
            &fixture["input"],
            "https://api.openai.com/v1",
            AssistantPrefillMode::Native,
        )
        .unwrap();
        assert_eq!(actual, fixture["expected"], "fixture {}", fixture["name"]);
    }
}

#[test]
fn rejects_empty_required_strings() {
    for body in [
        json!({"model": "", "max_tokens": 1, "messages": [{"role": "user", "content": "ok"}]}),
        json!({"model": "model", "max_tokens": 1, "messages": [{"role": "", "content": "ok"}]}),
        json!({"model": "model", "max_tokens": 1, "messages": [{"role": "user", "content": [{"type": "", "text": "ok"}]}]}),
    ] {
        assert!(validate_request(&body).is_err());
    }
}

#[test]
fn enforces_assistant_prefill_capability_and_maps_tool_controls() {
    let body = json!({
        "model": "gpt-4",
        "max_tokens": 10,
        "messages": [
            {"role": "user", "content": "Return JSON"},
            {"role": "assistant", "content": "{"}
        ],
        "tools": [{
            "type": "custom",
            "name": "lookup",
            "description": "Lookup a value",
            "input_schema": {"type": "object"},
            "cache_control": {"type": "ephemeral"}
        }],
        "tool_choice": {
            "type": "tool",
            "name": "lookup",
            "disable_parallel_tool_use": true
        }
    });
    validate_request(&body).unwrap();
    let error = convert_request(&body, "https://api.openai.com/v1").unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert!(
        error
            .message
            .contains("upstreamCapabilities.assistantPrefill")
    );

    let converted = convert_request_with_mode(
        &body,
        "https://api.openai.com/v1",
        AssistantPrefillMode::ContinueFinalMessage,
    )
    .unwrap();
    assert_eq!(
        converted["messages"],
        json!([
            {"role": "user", "content": "Return JSON"},
            {"role": "assistant", "content": "{"}
        ])
    );
    assert_eq!(converted["continue_final_message"], true);
    assert!(converted["tools"][0]["function"].get("strict").is_none());
    assert_eq!(converted["tool_choice"]["function"]["name"], "lookup");
    assert_eq!(converted["parallel_tool_calls"], false);
    let mut parallel = body.clone();
    parallel["tool_choice"]["disable_parallel_tool_use"] = json!(false);
    let parallel = convert_request_with_mode(
        &parallel,
        "https://api.openai.com/v1",
        AssistantPrefillMode::Native,
    )
    .unwrap();
    assert_eq!(parallel["parallel_tool_calls"], true);
    let mut strict = body.clone();
    strict["tools"][0]["strict"] = json!(true);
    let error = convert_request_with_mode(
        &strict,
        "https://api.openai.com/v1",
        AssistantPrefillMode::Native,
    )
    .unwrap_err();
    assert!(error.message.contains("tools[0].strict"));

    let unicode = json!({
        "model": "gpt-4", "max_tokens": 10,
        "messages": [{"role": "assistant", "content": "🙂🙂"}]
    });
    let converted = convert_request_with_mode(
        &unicode,
        "https://api.openai.com/v1",
        AssistantPrefillMode::Native,
    )
    .unwrap();
    assert_eq!(converted["messages"][0]["content"], "🙂🙂");
    assert!(converted.get("continue_final_message").is_none());

    let ordinary = json!({
        "model": "gpt-4", "max_tokens": 10,
        "messages": [{"role": "user", "content": "hello"}]
    });
    let converted = convert_request_with_mode(
        &ordinary,
        "https://api.openai.com/v1",
        AssistantPrefillMode::ContinueFinalMessage,
    )
    .unwrap();
    assert!(converted.get("continue_final_message").is_none());
}

#[test]
fn preserves_reasoning_only_for_glm_and_qwen_tool_history() {
    for model in ["glm-5.2", "qwen3-coder"] {
        let body = json!({
            "model": model, "max_tokens": 10,
            "messages": [
                {"role": "assistant", "content": [
                    {"type": "thinking", "thinking": "private", "signature": "sig"},
                    {"type": "tool_use", "id": "toolu_1", "name": "lookup", "input": {}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "toolu_1", "content": "done"}
                ]}
            ]
        });
        let converted = convert_request(&body, "https://example.com/v1").unwrap();
        assert_eq!(converted["messages"][0]["reasoning_content"], "private");
    }

    let generic = json!({
        "model": "gpt-4", "max_tokens": 10,
        "messages": [
            {"role": "assistant", "content": [
                {"type": "thinking", "thinking": "private", "signature": "sig"},
                {"type": "tool_use", "id": "toolu_1", "name": "lookup", "input": {}}
            ]},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "toolu_1", "content": "done"}
            ]}
        ]
    });
    let converted = convert_request(&generic, "https://example.com/v1").unwrap();
    assert!(converted["messages"][0].get("reasoning_content").is_none());
}

#[test]
fn rejects_structured_output_and_preserves_native_assistant_prefill() {
    let body = json!({
        "model": "gpt-5", "max_tokens": 10,
        "messages": [
            {"role": "user", "content": "Return JSON"},
            {"role": "assistant", "content": [{"type": "text", "text": "{"}]}
        ],
        "output_config": {
            "format": {"type": "json_schema", "schema": {"type": "object"}}
        }
    });
    let error = convert_request_with_mode(
        &body,
        "https://api.openai.com/v1",
        AssistantPrefillMode::Native,
    )
    .unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert!(error.message.contains("output_config.format"));

    let only_prefill = json!({
        "model": "gpt-4", "max_tokens": 10,
        "messages": [{"role": "assistant", "content": "```"}]
    });
    let converted = convert_request_with_mode(
        &only_prefill,
        "https://api.openai.com/v1",
        AssistantPrefillMode::Native,
    )
    .unwrap();
    assert_eq!(converted["messages"][0]["content"], "```");
}

#[test]
fn ignores_empty_system_text_like_typescript() {
    let body = json!({
        "model": "gpt-4", "max_tokens": 10,
        "system": [{"type": "text", "text": ""}],
        "messages": [{"role": "user", "content": "hello"}]
    });
    let converted = convert_request(&body, "https://api.openai.com/v1").unwrap();
    assert_eq!(
        converted["messages"],
        json!([{"role": "user", "content": "hello"}])
    );
}

#[test]
fn repairs_duplicate_tool_ids_and_matches_results_in_order() {
    let body = json!({
        "model": "gpt-4",
        "max_tokens": 100,
        "messages": [
            {"role": "assistant", "content": [
                {"type": "tool_use", "id": "duplicate_tool_id", "name": "first", "input": {}},
                {"type": "tool_use", "id": "duplicate_tool_id", "name": "second", "input": {}}
            ]},
            {"role": "user", "content": null},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "duplicate_tool_id", "content": "one"},
                {"type": "tool_result", "tool_use_id": "duplicate_tool_id", "content": "two"}
            ]}
        ]
    });
    let converted = convert_request(&body, "https://api.openai.com/v1").unwrap();
    let calls = converted["messages"][0]["tool_calls"].as_array().unwrap();
    assert_ne!(calls[0]["id"], calls[1]["id"]);
    assert_eq!(converted["messages"][1]["tool_call_id"], calls[0]["id"]);
    assert_eq!(converted["messages"][2]["tool_call_id"], calls[1]["id"]);
}

#[test]
fn repairs_duplicate_tool_ids_using_javascript_utf16_length() {
    let original = "abcdefg😀wxyz";
    let mut ids = IdContext::default();
    assert_eq!(unique_tool_id(original, &mut ids), original);
    let repaired = unique_tool_id(original, &mut ids);
    assert!(repaired.starts_with("abcdefg"));
    assert_eq!(
        repaired.encode_utf16().count(),
        original.encode_utf16().count()
    );
}

#[test]
fn rejects_invalid_image_sources() {
    for source in [
        json!({"type": "base64", "media_type": "image/png", "data": "%%%"}),
        json!({"type": "base64", "media_type": "image/png", "data": "T0s=AAA"}),
        json!({"type": "base64", "media_type": "image/svg+xml", "data": "PHN2Zz4="}),
        json!({"type": "url", "url": "ftp://example.com/a.png"}),
        json!({"type": "url", "url": "not a url"}),
        json!({"type": "url"}),
        json!({"type": "file_id", "file_id": "file_1"}),
    ] {
        let body = json!({
            "model": "gpt-4o",
            "max_tokens": 100,
            "messages": [{"role": "user", "content": [{"type": "image", "source": source}]}]
        });
        assert!(convert_request(&body, "https://api.openai.com/v1").is_err());
    }

    let tool_image = json!({
        "model": "gpt-4o",
        "max_tokens": 100,
        "messages": [{"role": "user", "content": [{
            "type": "tool_result",
            "tool_use_id": "toolu_1",
            "content": [{"type": "image", "source": {"type": "url", "url": "ftp://example.com/a.png"}}]
        }]}]
    });
    assert!(convert_request(&tool_image, "https://api.openai.com/v1").is_err());
}

#[test]
fn rejects_unsupported_roles_and_generation_fields() {
    let body = json!({
        "model": "gpt-4",
        "max_tokens": 10,
        "stream": false,
        "system": "base",
        "top_k": 40,
        "messages": [
            {"role": "user", "content": "hello", "unknown": true},
            {"role": "system", "content": "mid-turn instruction"},
            {"role": "hook", "content": "hook text"},
            {"role": "user", "content": [{"type": "search_result", "content": []}]}
        ],
        "stop_sequences": [],
        "tool_choice": {"type": "none"},
        "output_config": {"format": {"type": "json_schema", "schema": {"type": "object"}}}
    });
    let error = convert_request(&body, "https://api.openai.com/v1").unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert!(
        error
            .message
            .contains("messages[2].role: role must be user, assistant, or system")
    );
    for field in ["top_k", "output_config.format"] {
        let mut body = json!({
            "model": "gpt-4", "max_tokens": 10,
            "messages": [{"role": "user", "content": "hello"}]
        });
        if field == "top_k" {
            body["top_k"] = json!(40);
        } else {
            body["output_config"] =
                json!({"format": {"type": "json_schema", "schema": {"type": "object"}}});
        }
        assert!(
            convert_request(&body, "https://api.openai.com/v1").is_err(),
            "{field}"
        );
    }
}

#[test]
fn rejects_malformed_collection_fields_and_undeclared_tool_choice() {
    let base = json!({
        "model": "gpt-4", "max_tokens": 10,
        "messages": [{"role":"user","content":"hello"}]
    });
    for (field, value) in [
        ("tools", json!({"name":"lookup"})),
        ("stop_sequences", json!("stop")),
        ("stop_sequences", json!(["stop", 1])),
    ] {
        let mut body = base.clone();
        body[field] = value;
        let error = convert_request(&body, "https://api.openai.com/v1").unwrap_err();
        assert!(error.message.contains(field), "{}", error.message);
    }

    for tools in [
        None,
        Some(json!([{
            "name":"other","input_schema":{"type":"object"}
        }])),
    ] {
        let mut body = base.clone();
        if let Some(tools) = tools {
            body["tools"] = tools;
        }
        body["tool_choice"] = json!({"type":"tool","name":"lookup"});
        let error = convert_request(&body, "https://api.openai.com/v1").unwrap_err();
        assert!(error.message.contains("not declared"), "{}", error.message);
    }
}

#[test]
fn rejects_tool_results_without_a_preceding_tool_use() {
    let body = json!({
        "model":"gpt-4","max_tokens":10,
        "messages":[{"role":"user","content":[{
            "type":"tool_result","tool_use_id":"orphan","content":"result"
        }]}]
    });
    let error = convert_request(&body, "https://api.openai.com/v1").unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert!(error.message.contains("no preceding tool_use"));
}

#[test]
fn rejects_unmappable_mid_conversation_system_fields_and_blocks() {
    for system in [
        json!({"role": "system", "content": "instruction", "clear_at": "next_user_message"}),
        json!({"role": "system", "content": "instruction", "output_config": {"effort": "high"}}),
        json!({"role": "system", "content": [{"type": "tool_use", "id": "toolu_1", "name": "lookup", "input": {}}]}),
    ] {
        let body = json!({
            "model": "gpt-4", "max_tokens": 10,
            "messages": [{"role": "user", "content": "hello"}, system]
        });
        let error = convert_request(&body, "https://api.openai.com/v1").unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(error.message.contains("messages[1]"));
    }
}

#[test]
fn joins_mid_conversation_system_text_blocks() {
    let body = json!({
        "model": "gpt-4", "max_tokens": 10,
        "messages": [
            {"role": "user", "content": "hello"},
            {"role": "system", "content": [
                {"type": "text", "text": "First"},
                {"type": "text", "text": "Second"}
            ]},
            {"role": "user", "content": "continue"}
        ]
    });
    let converted = convert_request(&body, "https://api.openai.com/v1").unwrap();
    assert_eq!(
        converted["messages"][1],
        json!({"role": "system", "content": "First\nSecond"})
    );
}

#[test]
fn maps_explicit_xhigh_effort_for_openai_reasoning_models() {
    let body = json!({
        "model": "gpt-5", "max_tokens": 10,
        "messages": [{"role": "user", "content": "hello"}],
        "output_config": {"effort": "xhigh"}
    });
    let converted = convert_request(&body, "https://api.openai.com/v1").unwrap();
    assert_eq!(converted["reasoning_effort"], "xhigh");
}

#[test]
fn maps_tool_result_text_and_ignores_thinking_history() {
    let body = json!({
        "model": "gpt-4",
        "max_tokens": 10,
        "messages": [
            {"role": "assistant", "content": [
                {"type": "thinking", "thinking": "private", "signature": "sig"},
                {"type": "tool_use", "id": "toolu_matched", "name": "lookup", "input": {}}
            ]},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "toolu_matched", "content": [
                    {"type": "text", "text": "one"},
                    {"type": "text", "text": "two"}
                ], "is_error": true},
                {"type": "text", "text": "continue"}
            ]}
        ]
    });
    let converted = convert_request(&body, "https://api.openai.com/v1").unwrap();
    assert_eq!(
        converted["messages"][0],
        json!({
            "role": "assistant",
            "content": null,
            "tool_calls": [{
                "id": "toolu_matched",
                "type": "function",
                "function": {"name": "lookup", "arguments": "{}"}
            }]
        })
    );
    assert_eq!(
        converted["messages"][1],
        json!({"role": "tool", "tool_call_id": "toolu_matched", "content": "Error: one\ntwo"})
    );
    assert_eq!(
        converted["messages"][2],
        json!({"role": "user", "content": "continue"})
    );
}

#[test]
fn recognizes_plain_and_namespaced_qwen3_models() {
    for model in ["qwen3.6-35b-a3b", "qwen/qwen3.6-35b-a3b"] {
        let body = json!({
            "model": model,
            "max_tokens": 10,
            "stream": true,
            "thinking": {"type": "enabled"},
            "messages": [{"role": "user", "content": "hello"}]
        });
        let converted = convert_request(&body, "http://127.0.0.1:1234/v1").unwrap();
        assert_eq!(converted["enable_thinking"], true, "{model}");
    }

    let unrelated = json!({
        "model": "vendor/not-qwen3",
        "max_tokens": 10,
        "stream": true,
        "thinking": {"type": "enabled"},
        "messages": [{"role": "user", "content": "hello"}]
    });
    let converted = convert_request(&unrelated, "http://127.0.0.1:1234/v1").unwrap();
    assert!(converted.get("enable_thinking").is_none());
}

#[test]
fn qwen_thinking_uses_upstream_stream_without_changing_other_requests() {
    let mut body = json!({
        "model": "qwen/qwen3.6-35b-a3b",
        "max_tokens": 10,
        "stream": false,
        "thinking": {"type": "enabled"},
        "messages": [{"role": "user", "content": "hello"}]
    });
    let enabled = convert_request(&body, "http://127.0.0.1:1234/v1").unwrap();
    assert_eq!(enabled["stream"], true);
    assert_eq!(enabled["stream_options"], json!({"include_usage": true}));
    assert_eq!(enabled["enable_thinking"], true);

    body["thinking"]["type"] = json!("disabled");
    let disabled = convert_request(&body, "http://127.0.0.1:1234/v1").unwrap();
    assert_eq!(disabled["stream"], false);
    assert!(disabled.get("stream_options").is_none());
    assert_eq!(disabled["enable_thinking"], false);

    body.as_object_mut().unwrap().remove("thinking");
    let absent = convert_request(&body, "http://127.0.0.1:1234/v1").unwrap();
    assert_eq!(absent["stream"], false);
    assert!(absent.get("enable_thinking").is_none());

    body["model"] = json!("gpt-4o");
    body["thinking"] = json!({"type": "enabled"});
    let other = convert_request(&body, "http://127.0.0.1:1234/v1").unwrap();
    assert_eq!(other["stream"], false);
}
