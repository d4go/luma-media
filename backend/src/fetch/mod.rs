mod classifier;
mod http;
mod manager;
mod model;

pub use classifier::{PageKind, classify_transport};
pub use manager::FetchManager;
pub use model::{FetchError, FetchMethod, FetchMode, FetchRequest, FetchResponse, FetchTransport};
