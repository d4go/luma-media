//! AI 接入层：把一个 OpenAI 兼容的 chat/completions 端点封装成自然语言 → 自动化规则的编译器。
//!
//! 支持任何 OpenAI 兼容协议的服务（OpenAI、DeepSeek、阿里云百炼 compatible-mode、本地 Ollama 的
//! `/v1` 端点等），用户只需要在设置里填写 `base_url` 和可选的 `api_key`。模型只负责"翻译"，不参与
//! 运行时逐条决策：产出会被校验为标准的 AutomationInput，再由前端确认后走既有的 `/automations` 链路。

use std::time::{Duration, Instant};

use axum::{
    Json, Router,
    extract::State,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    AppState,
    error::{AppError, AppResult},
    product::{AutomationInput, validate_automation},
};

const AI_SETTING_KEY: &str = "ai_config";

/// 编译用的系统提示词：要求模型只输出一个严格符合 AutomationInput 结构的 JSON 对象。
const COMPILE_SYSTEM_PROMPT: &str = r#"你是 Luma（家庭 NAS 媒体获取与入库系统）的自动化规则编译器。
你的唯一任务：把用户的自然语言需求翻译成一条自动化规则，并只输出一个 json 对象，不要输出任何解释、前后缀或 Markdown 代码块。

自动化规则由三部分组成：
1. WHEN（triggerType 触发器）—— 决定何时检查资源：
   - "NEW_RESOURCE"：只要发现新资源就检查（默认）
   - "FOLLOWED_ACTOR_UPDATE"：已关注的演员出现新资源时检查
   - "SCHEDULE"：按 cron 定时检查（此时必须在 triggerConfig 里给出 "cron" 字段）
   - cron 使用 5 段格式：分 时 日 月 星期（星期 0 和 7 都表示周日），例如 "0 20 * * 1" 表示每周一 20:00。定时按服务器本地时区解释。
2. IF（conditions 过滤条件，均为可选）：
   - providerKey（string）：来源标识，留空表示不限来源
   - keyword（string）：标题关键词子串匹配，留空表示不限（番号前缀如 "ABC"）
   - minScore（number 0-100）：最低推荐评分，默认 70
   - requireSubtitle（boolean）：是否必须包含字幕
3. THEN（执行方式）：
   - actionType 固定为 "ACQUIRE"
   - mode：CONFIRM（先确认，默认）/ AUTO（自动获取）/ NOTIFY（仅通知）

必须严格输出如下结构的 JSON（键名区分大小写，camelCase）：
{
  "name": "简短规则名",
  "enabled": true,
  "triggerType": "NEW_RESOURCE",
  "triggerConfig": {},
  "conditions": { "providerKey": "", "keyword": "", "minScore": 70, "requireSubtitle": false },
  "actionType": "ACQUIRE",
  "actionConfig": {},
  "mode": "CONFIRM"
}

映射规则：
- 用户没有明确要求定时时，triggerType 用 "NEW_RESOURCE"。
- 用户说"自动下载/自动获取"时 mode 用 "AUTO"；说"提醒/通知我"用 "NOTIFY"；否则默认 "CONFIRM"。
- 用户提到"每个星期一晚上8点"这类定时需求时，triggerType 用 "SCHEDULE"，并在 triggerConfig 里给出 cron（例如 {"cron": "0 20 * * 1"}）。
- keyword 提取作品番号前缀或明确的标题关键词；没有关键词时留空。
- 用户提到"要字幕/中文字幕"时 requireSubtitle 用 true。
- name 使用简短中文规则名。"#;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AiSettings {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_provider_kind")]
    pub provider_kind: String,
    #[serde(default)]
    pub base_url: String,
    #[serde(default)]
    pub api_key: String,
    #[serde(default = "default_model")]
    pub model: String,
    #[serde(default = "default_temperature")]
    pub temperature: f64,
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    #[serde(default = "default_max_tokens")]
    pub max_tokens: u32,
}

fn default_true() -> bool {
    true
}
fn default_provider_kind() -> String {
    "openai".into()
}
fn default_model() -> String {
    "gpt-4o-mini".into()
}
fn default_temperature() -> f64 {
    0.2
}
fn default_timeout() -> u64 {
    60
}
fn default_max_tokens() -> u32 {
    2000
}

impl Default for AiSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            provider_kind: default_provider_kind(),
            base_url: String::new(),
            api_key: String::new(),
            model: default_model(),
            temperature: default_temperature(),
            timeout_secs: default_timeout(),
            max_tokens: default_max_tokens(),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AiSettingsView {
    enabled: bool,
    provider_kind: String,
    base_url: String,
    has_api_key: bool,
    model: String,
    temperature: f64,
    timeout_secs: u64,
    max_tokens: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CompileInput {
    prompt: String,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/ai/settings", get(get_ai_settings).put(update_ai_settings))
        .route("/ai/test", post(test_ai))
        .route("/ai/compile", post(compile_ai))
}

async fn load_ai_settings(state: &AppState) -> AppResult<AiSettings> {
    let raw: Option<String> = sqlx::query_scalar("SELECT value FROM app_setting WHERE key = ?")
        .bind(AI_SETTING_KEY)
        .fetch_optional(&state.pool)
        .await?;
    Ok(raw
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or_default())
}

async fn save_ai_settings(state: &AppState, settings: &AiSettings) -> AppResult<()> {
    let value = serde_json::to_string(settings).map_err(|error| AppError::Internal(error.into()))?;
    sqlx::query(
        "INSERT INTO app_setting(key,value,updated_at) VALUES(?,?,datetime('now')) \
         ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')",
    )
    .bind(AI_SETTING_KEY)
    .bind(value)
    .execute(&state.pool)
    .await?;
    Ok(())
}

async fn get_ai_settings(State(state): State<AppState>) -> AppResult<Json<AiSettingsView>> {
    let settings = load_ai_settings(&state).await?;
    Ok(Json(AiSettingsView {
        enabled: settings.enabled,
        provider_kind: settings.provider_kind,
        base_url: settings.base_url,
        has_api_key: !settings.api_key.is_empty(),
        model: settings.model,
        temperature: settings.temperature,
        timeout_secs: settings.timeout_secs,
        max_tokens: settings.max_tokens,
    }))
}

async fn update_ai_settings(
    State(state): State<AppState>,
    Json(input): Json<AiSettings>,
) -> AppResult<Json<AiSettingsView>> {
    let mut settings = input;
    if !settings.provider_kind.is_empty() && !matches!(settings.provider_kind.as_str(), "openai" | "ollama" | "custom") {
        return Err(AppError::BadRequest(
            "providerKind 必须是 openai、ollama 或 custom".into(),
        ));
    }
    if settings.base_url.trim().is_empty() {
        return Err(AppError::BadRequest("请填写 AI 服务的 base_url".into()));
    }
    if settings.model.trim().is_empty() {
        return Err(AppError::BadRequest("请填写模型名称".into()));
    }
    // 留空的 api_key 表示保持已保存的密钥不变。
    if settings.api_key.is_empty() {
        settings.api_key = load_ai_settings(&state).await?.api_key;
    }
    save_ai_settings(&state, &settings).await?;
    get_ai_settings(State(state)).await
}

async fn test_ai(
    State(state): State<AppState>,
    Json(input): Json<AiSettings>,
) -> AppResult<Json<Value>> {
    let mut settings = input;
    let saved = load_ai_settings(&state).await?;
    if settings.base_url.trim().is_empty() {
        settings = saved;
    } else if settings.api_key.is_empty() {
        settings.api_key = saved.api_key;
    }
    if settings.base_url.trim().is_empty() {
        return Err(AppError::BadRequest("请先配置 AI 接入地址".into()));
    }
    let (reply, latency) = chat_completion(
        &settings,
        "你是一个连接测试助手，请始终用 json 格式回复。",
        "请回复一个 json 对象：{\"ok\": true}，不要输出其他内容。",
    )
    .await?;
    Ok(Json(json!({
        "ok": true,
        "message": format!("连接成功，模型 {} 响应：{}", settings.model, reply.trim()),
        "latencyMs": latency.as_millis() as u64,
        "model": settings.model,
    })))
}

async fn compile_ai(
    State(state): State<AppState>,
    Json(input): Json<CompileInput>,
) -> AppResult<Json<Value>> {
    let prompt = input.prompt.trim().to_string();
    if prompt.is_empty() {
        return Err(AppError::BadRequest("请输入需求描述".into()));
    }
    let settings = load_ai_settings(&state).await?;
    if !settings.enabled || settings.base_url.trim().is_empty() {
        return Err(AppError::BadRequest(
            "AI 接入尚未启用或未配置，请先在设置中完成配置".into(),
        ));
    }
    let (content, _) = chat_completion(&settings, COMPILE_SYSTEM_PROMPT, &prompt).await?;
    let parsed = extract_json(&content).map_err(AppError::BadRequest)?;
    let rule: AutomationInput = serde_json::from_value(parsed)
        .map_err(|error| AppError::BadRequest(format!("模型输出无法映射为自动化规则：{error}")))?;
    validate_automation(&rule)?;
    let rule_value = serde_json::to_value(&rule).map_err(|error| AppError::Internal(error.into()))?;
    Ok(Json(json!({
        "rule": rule_value,
        "explanation": explain_rule(&rule),
        "rawContent": content,
    })))
}

/// 调用 OpenAI 兼容的 chat/completions 端点，返回模型文本与耗时。
async fn chat_completion(
    settings: &AiSettings,
    system: &str,
    user: &str,
) -> Result<(String, Duration), AppError> {
    let mut body = json!({
        "model": settings.model,
        "messages": [
            { "role": "system", "content": system },
            { "role": "user", "content": user }
        ],
        "temperature": settings.temperature,
        "stream": false,
    });
    if settings.max_tokens > 0 {
        body["max_tokens"] = json!(settings.max_tokens);
    }
    if settings.provider_kind == "ollama" {
        body["format"] = json!("json");
    } else {
        body["response_format"] = json!({ "type": "json_object" });
    }

    let url = format!("{}/chat/completions", settings.base_url.trim_end_matches('/'));
    let mut request = reqwest::Client::new().post(&url).json(&body);
    if !settings.api_key.is_empty() {
        request = request.header("Authorization", format!("Bearer {}", settings.api_key));
    }
    let started = Instant::now();
    let response = request
        .timeout(Duration::from_secs(settings.timeout_secs.max(1)))
        .send()
        .await
        .map_err(|error| AppError::BadRequest(format!("无法连接 AI 服务：{error}")))?;
    let latency = started.elapsed();
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|error| AppError::Internal(error.into()))?;
    if !status.is_success() {
        return Err(AppError::BadRequest(format!(
            "AI 服务返回 HTTP {}：{}",
            status.as_u16(),
            truncate(&text, 500)
        )));
    }
    let value: Value = serde_json::from_str(&text)
        .map_err(|error| AppError::BadRequest(format!("AI 服务响应不是有效 JSON：{error}")))?;
    let content = value
        .pointer("/choices/0/message/content")
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::BadRequest("AI 服务响应缺少 choices[0].message.content".into()))?;
    Ok((content.to_string(), latency))
}

/// 从模型文本里稳健地抽出第一个 JSON 对象（容忍 Markdown 代码块和前后缀说明）。
fn extract_json(text: &str) -> Result<Value, String> {
    let mut stripped = text.trim();
    if stripped.starts_with("```") {
        stripped = stripped.trim_start_matches("```json").trim_start_matches("```");
    }
    if let Some(end) = stripped.rfind("```") {
        stripped = &stripped[..end];
    }
    stripped = stripped.trim();
    let start = stripped.find('{').ok_or("模型响应中没有 JSON 对象")?;
    let end = stripped.rfind('}').ok_or("模型响应中没有 JSON 对象")?;
    if end < start {
        return Err("模型响应的 JSON 结构不完整".into());
    }
    serde_json::from_str(&stripped[start..=end]).map_err(|error| format!("JSON 解析失败：{error}"))
}

/// 把规则确定性翻译回一句可读的话，供前端确认，不需要第二次模型调用。
fn explain_rule(rule: &AutomationInput) -> String {
    let when = match rule.trigger_type.as_str() {
        "SCHEDULE" => rule
            .trigger_config
            .get("cron")
            .and_then(Value::as_str)
            .map(|cron| format!("定时（cron: {cron}）"))
            .unwrap_or_else(|| "定时检查".into()),
        "FOLLOWED_ACTOR_UPDATE" => "关注演员出新资源时".into(),
        _ => "发现新资源时".into(),
    };
    let mut conditions = Vec::new();
    if let Some(object) = rule.conditions.as_object() {
        if let Some(keyword) = object.get("keyword").and_then(Value::as_str) {
            if !keyword.is_empty() {
                conditions.push(format!("关键词“{keyword}”"));
            }
        }
        if let Some(provider) = object.get("providerKey").and_then(Value::as_str) {
            if !provider.is_empty() {
                conditions.push(format!("来源 {provider}"));
            }
        }
        if let Some(min_score) = object.get("minScore").and_then(Value::as_f64) {
            conditions.push(format!("评分 ≥ {min_score}"));
        }
        if object
            .get("requireSubtitle")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            conditions.push("含字幕".to_string());
        }
    }
    let then = match rule.mode.as_str() {
        "AUTO" => "自动获取",
        "NOTIFY" => "仅通知",
        _ => "确认后获取",
    };
    let if_part = if conditions.is_empty() {
        "无额外条件".to_string()
    } else {
        conditions.join("、")
    };
    format!("WHEN {when} · IF {if_part} · THEN {then}")
}

fn truncate(text: &str, max: usize) -> String {
    let mut chars = text.chars();
    let mut out = String::new();
    while out.len() < max {
        match chars.next() {
            Some(ch) => out.push(ch),
            None => break,
        }
    }
    if chars.next().is_some() {
        out.push('…');
    }
    out
}
