use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, anyhow};
use serde_json::{Value, json};

use crate::{models::MediaItem, provider::MetaTubeClient};

pub struct WriteReport {
    pub written: Vec<PathBuf>,
    pub skipped: Vec<PathBuf>,
}

pub struct WriteOptions<'a> {
    pub output_format: &'a str,
    pub overwrite_policy: &'a str,
    pub overwrite_nfo: bool,
    pub overwrite_image: bool,
    pub provider: &'a str,
    pub remote_id: &'a str,
}

pub async fn write_sidecars(
    client: &MetaTubeClient,
    media: &MediaItem,
    remote: &Value,
    options: WriteOptions<'_>,
) -> anyhow::Result<WriteReport> {
    let media_path = Path::new(&media.path);
    if !media_path.is_file() {
        return Err(anyhow!("media file does not exist: {}", media.path));
    }
    let parent = media_path
        .parent()
        .ok_or_else(|| anyhow!("media file has no parent directory"))?;
    let stem = media_path
        .file_stem()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("media filename is not valid UTF-8"))?;
    let overwrite_text = options.overwrite_nfo || options.overwrite_policy == "always";
    let overwrite_images = options.overwrite_image || options.overwrite_policy == "always";
    let mut report = WriteReport {
        written: Vec::new(),
        skipped: Vec::new(),
    };

    if matches!(options.output_format, "nfo" | "both") {
        let path = media_path.with_extension("nfo");
        write_or_skip(
            &path,
            build_nfo(remote, options.provider, options.remote_id).as_bytes(),
            overwrite_text,
            &mut report,
        )
        .await?;
    }

    if matches!(options.output_format, "json" | "both") {
        let path = media_path.with_extension("metadata.json");
        let document = json!({
            "metadata": remote,
            "luma": {
                "provider": options.provider,
                "remoteId": options.remote_id,
                "mediaPath": media.path,
            }
        });
        let bytes = serde_json::to_vec_pretty(&document)?;
        write_or_skip(&path, &bytes, overwrite_text, &mut report).await?;
    }

    if let Some(url) = first_string(remote, &["big_cover_url", "cover_url"]) {
        write_image(
            client,
            url,
            parent,
            stem,
            "poster",
            overwrite_images,
            &mut report,
        )
        .await?;
    }
    if let Some(url) = first_string(remote, &["big_thumb_url", "thumb_url"]) {
        write_image(
            client,
            url,
            parent,
            stem,
            "fanart",
            overwrite_images,
            &mut report,
        )
        .await?;
    }

    Ok(report)
}

async fn write_image(
    client: &MetaTubeClient,
    url: &str,
    parent: &Path,
    stem: &str,
    kind: &str,
    overwrite: bool,
    report: &mut WriteReport,
) -> anyhow::Result<()> {
    let existing = ["jpg", "png", "webp"]
        .into_iter()
        .map(|extension| parent.join(format!("{stem}-{kind}.{extension}")))
        .find(|path| path.exists());
    if !overwrite && let Some(path) = existing {
        report.skipped.push(path);
        return Ok(());
    }

    let image = client
        .download_image(url)
        .await
        .with_context(|| format!("failed to download {kind} image"))?;
    let path = parent.join(format!("{stem}-{kind}.{}", image.extension));
    atomic_write(&path, &image.bytes)
        .await
        .with_context(|| format!("failed to write {}", path.display()))?;
    report.written.push(path);
    Ok(())
}

async fn write_or_skip(
    path: &Path,
    bytes: &[u8],
    overwrite: bool,
    report: &mut WriteReport,
) -> anyhow::Result<()> {
    if path.exists() && !overwrite {
        report.skipped.push(path.to_path_buf());
        return Ok(());
    }
    atomic_write(path, bytes)
        .await
        .with_context(|| format!("failed to write {}", path.display()))?;
    report.written.push(path.to_path_buf());
    Ok(())
}

async fn atomic_write(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("output file has no parent directory"))?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("output filename is not valid UTF-8"))?;
    let temporary = parent.join(format!(
        ".{filename}.luma-{}-{nonce}.tmp",
        std::process::id()
    ));
    tokio::fs::write(&temporary, bytes).await?;
    if let Err(error) = tokio::fs::rename(&temporary, path).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(error.into());
    }
    Ok(())
}

fn build_nfo(remote: &Value, provider: &str, remote_id: &str) -> String {
    let title = string(remote, "title").unwrap_or_default();
    let original_title = first_string(remote, &["original_title", "number"]).unwrap_or_default();
    let plot = first_string(remote, &["summary", "plot", "description"]).unwrap_or_default();
    let release_date = first_string(remote, &["release_date", "premiered"]).unwrap_or_default();
    let year = string(remote, "year")
        .map(str::to_owned)
        .or_else(|| release_date.get(0..4).map(str::to_owned))
        .unwrap_or_default();
    let mut xml =
        String::from("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<movie>\n");
    tag(&mut xml, "title", title);
    tag(&mut xml, "originaltitle", original_title);
    tag(
        &mut xml,
        "sorttitle",
        first_string(remote, &["number", "title"]).unwrap_or(title),
    );
    tag(&mut xml, "plot", plot);
    tag(&mut xml, "outline", plot);
    tag(
        &mut xml,
        "director",
        string(remote, "director").unwrap_or_default(),
    );
    tag(&mut xml, "premiered", release_date);
    tag(&mut xml, "year", &year);
    tag_value(&mut xml, "runtime", remote.get("runtime"));
    tag_value(
        &mut xml,
        "rating",
        remote.get("score").or_else(|| remote.get("rating")),
    );
    tag(
        &mut xml,
        "studio",
        first_string(remote, &["maker", "studio"]).unwrap_or_default(),
    );
    tag(
        &mut xml,
        "set",
        string(remote, "series").unwrap_or_default(),
    );
    xml.push_str(&format!(
        "  <uniqueid type=\"metatube\" default=\"true\">{}:{}</uniqueid>\n",
        escape_xml(provider),
        escape_xml(remote_id)
    ));

    for genre in string_array(remote.get("genres")) {
        tag(&mut xml, "genre", genre);
    }
    for actor in remote
        .get("actors")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let (name, thumb) = if let Some(name) = actor.as_str() {
            (name, None)
        } else {
            (
                actor
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
                actor
                    .get("image_url")
                    .or_else(|| actor.get("thumb_url"))
                    .and_then(Value::as_str),
            )
        };
        if !name.is_empty() {
            xml.push_str("  <actor>\n");
            tag_indented(&mut xml, "name", name, 4);
            if let Some(thumb) = thumb {
                tag_indented(&mut xml, "thumb", thumb, 4);
            }
            xml.push_str("  </actor>\n");
        }
    }
    if let Some(url) = first_string(remote, &["big_cover_url", "cover_url"]) {
        xml.push_str(&format!(
            "  <thumb aspect=\"poster\">{}</thumb>\n",
            escape_xml(url)
        ));
    }
    if let Some(url) = first_string(remote, &["big_thumb_url", "thumb_url"]) {
        xml.push_str("  <fanart>\n");
        tag_indented(&mut xml, "thumb", url, 4);
        xml.push_str("  </fanart>\n");
    }
    xml.push_str("</movie>\n");
    xml
}

fn tag(output: &mut String, name: &str, value: &str) {
    tag_indented(output, name, value, 2);
}

fn tag_indented(output: &mut String, name: &str, value: &str, spaces: usize) {
    if !value.is_empty() {
        output.push_str(&format!(
            "{}<{name}>{}</{name}>\n",
            " ".repeat(spaces),
            escape_xml(value)
        ));
    }
}

fn tag_value(output: &mut String, name: &str, value: Option<&Value>) {
    if let Some(value) = value {
        if let Some(text) = value.as_str() {
            tag(output, name, text);
        } else if value.is_number() {
            tag(output, name, &value.to_string());
        }
    }
}

fn string<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
}

fn first_string<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter().find_map(|key| string(value, key))
}

fn string_array(value: Option<&Value>) -> Vec<&str> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter(|text| !text.is_empty())
        .collect()
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nfo_escapes_metadata_and_contains_jellyfin_fields() {
        let remote = json!({
            "title": "A & B <Movie>",
            "number": "ABC-123",
            "release_date": "2026-08-06",
            "genres": ["Drama"],
            "actors": [{"name": "Alice", "image_url": "http://example/actor.jpg"}]
        });
        let xml = build_nfo(&remote, "Mock", "movie-1");
        assert!(xml.contains("<title>A &amp; B &lt;Movie&gt;</title>"));
        assert!(xml.contains("<year>2026</year>"));
        assert!(xml.contains("<genre>Drama</genre>"));
        assert!(xml.contains("Mock:movie-1"));
    }
}
