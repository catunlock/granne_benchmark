use core::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct VectorIdentifier {
    pub doc_id: Uuid,
    pub field: String,
    pub paragraph_id: Uuid,
    pub start: i32,
    pub end: i32
}

// f"{self.rid}/{field_key}/{subfield}/{paragraph.start}-{paragraph.end}"
// EX: UUID/field1/UUID/20-200
impl fmt::Display for VectorIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}/{}/{}-{}",
            self.doc_id.to_string(),
            self.field,
            self.paragraph_id.to_string(),
            self.start,
            self.end
        )
    }
}
