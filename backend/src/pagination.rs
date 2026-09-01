use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(default)]
#[serde(rename_all = "camelCase")]
pub struct PageParams {
    pub page: u32,
    pub page_size: u32,
}

impl PageParams {
    pub fn page(self) -> u32 {
        self.page.max(1)
    }

    pub fn page_size(self) -> u32 {
        self.page_size.clamp(1, 200)
    }

    pub fn offset(self) -> i64 {
        ((self.page() - 1) * self.page_size()) as i64
    }

    pub fn limit(self) -> i64 {
        self.page_size() as i64
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Paged<T> {
    pub items: Vec<T>,
    pub total: i64,
    pub page: u32,
    pub page_size: u32,
    pub total_pages: u32,
}

impl<T> Paged<T> {
    pub fn total_pages(total: i64, page_size: u32) -> u32 {
        if total <= 0 {
            0
        } else {
            ((total - 1) / i64::from(page_size) + 1) as u32
        }
    }

    pub fn new(items: Vec<T>, total: i64, params: PageParams) -> Self {
        let page = params.page();
        let page_size = params.page_size();
        Paged {
            items,
            total,
            page,
            page_size,
            total_pages: Self::total_pages(total, page_size),
        }
    }
}
