use crate::fetch::{FetchMode, FetchResponse, PageKind};

pub trait ProviderAdapter: Send + Sync {
    fn key(&self) -> &'static str;
    fn label(&self) -> &'static str;
    fn default_fetch_mode(&self) -> FetchMode;
    fn classify(&self, response: &FetchResponse) -> PageKind;
}

pub fn contains_selector(html: &str, selectors: &[&str]) -> bool {
    let document = scraper::Html::parse_document(html);
    selectors.iter().any(|selector| {
        scraper::Selector::parse(selector)
            .ok()
            .is_some_and(|selector| document.select(&selector).next().is_some())
    })
}
