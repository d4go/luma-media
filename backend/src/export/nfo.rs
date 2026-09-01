use std::path::{Path, PathBuf};

use quick_xml::{
    Writer,
    events::{BytesDecl, BytesEnd, BytesStart, BytesText, Event},
};
use sqlx::{Row, SqlitePool};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportActor {
    pub name: String,
    pub avatar_url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalNfoDocument {
    pub code: String,
    pub title: String,
    pub original_title: Option<String>,
    pub summary: String,
    pub release_date: Option<String>,
    pub duration_minutes: Option<i64>,
    pub poster_url: Option<String>,
    pub backdrop_url: Option<String>,
    pub studio: Option<String>,
    pub actors: Vec<ExportActor>,
    pub genres: Vec<String>,
}

pub async fn load_document(
    pool: &SqlitePool,
    media_id: i64,
) -> anyhow::Result<CanonicalNfoDocument> {
    let row = sqlx::query("SELECT normalized_code,title,original_title,summary,release_date,duration_minutes,poster_url,backdrop_url FROM media WHERE id=?")
        .bind(media_id)
        .fetch_one(pool)
        .await?;
    let actors = sqlx::query("SELECT actor.name,actor.avatar_url FROM media_actor JOIN actor ON actor.id=media_actor.actor_id WHERE media_actor.media_id=? ORDER BY media_actor.billing_order,actor.name")
        .bind(media_id)
        .fetch_all(pool)
        .await?
        .iter()
        .map(|row| ExportActor {
            name: row.get("name"),
            avatar_url: row.get("avatar_url"),
        })
        .collect();
    let genres = sqlx::query_scalar("SELECT tag FROM media_tag WHERE media_id=? ORDER BY tag")
        .bind(media_id)
        .fetch_all(pool)
        .await?;
    let studio = sqlx::query_scalar("SELECT COALESCE(json_extract(raw_json,'$.studio'),json_extract(raw_json,'$.maker')) FROM metadata_source_record WHERE media_id=? AND COALESCE(json_extract(raw_json,'$.studio'),json_extract(raw_json,'$.maker')) IS NOT NULL ORDER BY priority DESC,provider_key LIMIT 1")
        .bind(media_id)
        .fetch_optional(pool)
        .await?
        .flatten();
    Ok(CanonicalNfoDocument {
        code: row.get("normalized_code"),
        title: row.get("title"),
        original_title: row.get("original_title"),
        summary: row.get("summary"),
        release_date: row.get("release_date"),
        duration_minutes: row.get("duration_minutes"),
        poster_url: row.get("poster_url"),
        backdrop_url: row.get("backdrop_url"),
        studio,
        actors,
        genres,
    })
}

pub fn render(document: &CanonicalNfoDocument) -> anyhow::Result<Vec<u8>> {
    let mut writer = Writer::new_with_indent(Vec::new(), b' ', 2);
    writer.write_event(Event::Decl(BytesDecl::new(
        "1.0",
        Some("UTF-8"),
        Some("yes"),
    )))?;
    writer.write_event(Event::Start(BytesStart::new("movie")))?;
    text_element(&mut writer, "title", &document.title)?;
    text_element(
        &mut writer,
        "originaltitle",
        document.original_title.as_deref().unwrap_or(&document.code),
    )?;
    text_element(
        &mut writer,
        "sorttitle",
        &document.code.to_ascii_uppercase(),
    )?;
    text_element(&mut writer, "plot", &document.summary)?;
    if let Some(studio) = document.studio.as_deref() {
        text_element(&mut writer, "studio", studio)?;
    }
    if let Some(release_date) = document.release_date.as_deref() {
        text_element(&mut writer, "premiered", release_date)?;
        if let Some(year) = release_date.get(0..4) {
            text_element(&mut writer, "year", year)?;
        }
    }
    if let Some(duration) = document.duration_minutes {
        text_element(&mut writer, "runtime", &duration.to_string())?;
    }
    for genre in &document.genres {
        text_element(&mut writer, "genre", genre)?;
    }
    for actor in &document.actors {
        writer.write_event(Event::Start(BytesStart::new("actor")))?;
        text_element(&mut writer, "name", &actor.name)?;
        if let Some(avatar_url) = actor.avatar_url.as_deref() {
            text_element(&mut writer, "thumb", avatar_url)?;
        }
        writer.write_event(Event::End(BytesEnd::new("actor")))?;
    }
    if let Some(poster_url) = document.poster_url.as_deref() {
        let mut poster = BytesStart::new("thumb");
        poster.push_attribute(("aspect", "poster"));
        writer.write_event(Event::Start(poster))?;
        writer.write_event(Event::Text(BytesText::new(poster_url)))?;
        writer.write_event(Event::End(BytesEnd::new("thumb")))?;
    }
    if let Some(backdrop_url) = document.backdrop_url.as_deref() {
        writer.write_event(Event::Start(BytesStart::new("fanart")))?;
        text_element(&mut writer, "thumb", backdrop_url)?;
        writer.write_event(Event::End(BytesEnd::new("fanart")))?;
    }
    let mut unique_id = BytesStart::new("uniqueid");
    unique_id.push_attribute(("type", "luma"));
    unique_id.push_attribute(("default", "true"));
    writer.write_event(Event::Start(unique_id))?;
    writer.write_event(Event::Text(BytesText::new(
        &document.code.to_ascii_uppercase(),
    )))?;
    writer.write_event(Event::End(BytesEnd::new("uniqueid")))?;
    writer.write_event(Event::End(BytesEnd::new("movie")))?;
    Ok(writer.into_inner())
}

pub async fn write_for_video(
    pool: &SqlitePool,
    media_id: i64,
    video_path: &Path,
    overwrite: bool,
) -> anyhow::Result<PathBuf> {
    let nfo_path = video_path.with_extension("nfo");
    if !overwrite && tokio::fs::try_exists(&nfo_path).await? {
        return Ok(nfo_path);
    }
    let document = load_document(pool, media_id).await?;
    let bytes = render(&document)?;
    super::atomic_write(&nfo_path, &bytes).await?;
    Ok(nfo_path)
}

fn text_element(writer: &mut Writer<Vec<u8>>, name: &str, value: &str) -> anyhow::Result<()> {
    if value.trim().is_empty() {
        return Ok(());
    }
    writer.write_event(Event::Start(BytesStart::new(name)))?;
    writer.write_event(Event::Text(BytesText::new(value)))?;
    writer.write_event(Event::End(BytesEnd::new(name)))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use quick_xml::{Reader, events::Event};

    use super::*;

    #[test]
    fn canonical_nfo_is_well_formed_and_escapes_text() {
        let bytes = render(&CanonicalNfoDocument {
            code: "ABC-123".into(),
            title: "A & B <Movie>".into(),
            original_title: Some("原題".into()),
            summary: "One < two & three".into(),
            release_date: Some("2026-08-10".into()),
            duration_minutes: Some(120),
            poster_url: Some("https://example.test/p?a=1&b=2".into()),
            backdrop_url: None,
            studio: Some("Studio & Co".into()),
            actors: vec![
                ExportActor {
                    name: "Alice".into(),
                    avatar_url: None,
                },
                ExportActor {
                    name: "Bob".into(),
                    avatar_url: Some("https://example.test/bob.jpg".into()),
                },
            ],
            genres: vec!["Drama".into(), "4K".into()],
        })
        .unwrap();
        let xml = String::from_utf8(bytes).unwrap();
        assert!(xml.contains("A &amp; B &lt;Movie&gt;"));
        assert!(xml.contains("type=\"luma\" default=\"true\""));
        assert_eq!(xml.matches("<actor>").count(), 2);
        let mut reader = Reader::from_str(&xml);
        loop {
            match reader.read_event().unwrap() {
                Event::Eof => break,
                _ => {}
            }
        }
    }
}
