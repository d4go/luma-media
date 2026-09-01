pub fn rank_resource(
    title: &str,
    size: Option<&str>,
    published_at: &str,
    provider: &str,
) -> (f64, Vec<String>) {
    let lower = title.to_lowercase();
    let mut score = 50.0;
    let mut reasons = vec![format!("来源 {provider} 可用")];
    if lower.contains("中文字幕") || lower.contains("chinese") || lower.contains("-c") {
        score += 24.0;
        reasons.push("包含中文字幕标记".into());
    }
    if lower.contains("4k") || lower.contains("2160") {
        score += 16.0;
        reasons.push("4K 清晰度".into());
    } else if lower.contains("1080") {
        score += 10.0;
        reasons.push("1080p 清晰度".into());
    }
    if size.is_some() {
        score += 3.0;
        reasons.push("提供文件大小".into());
    }
    if !published_at.is_empty() {
        score += 2.0;
        reasons.push("提供发布时间".into());
    }
    (score, reasons)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explains_high_value_resource_traits() {
        let (score, reasons) =
            rank_resource("ABC-123 4K 中文字幕", Some("5 GB"), "2026-08-01", "mock");
        assert!(score > 90.0);
        assert!(reasons.iter().any(|reason| reason.contains("中文字幕")));
        assert!(reasons.iter().any(|reason| reason.contains("4K")));
    }
}
