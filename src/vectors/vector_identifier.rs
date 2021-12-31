use core::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct VectorIdentifier {
    pub doc_id: Uuid,
    pub field: String,
    pub paragraph: i32,
    pub sentence: i32,
}

impl fmt::Display for VectorIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}/{}/{}",
            self.doc_id.to_string(),
            self.field,
            self.paragraph,
            self.sentence
        )
    }
}
