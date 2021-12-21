use std::path::PathBuf;

use super::{INDEX_PATH, ELEMENTS_PATH, DIRTY_PATH, COMMIT_LOCK_PATH, WRITER_LOCK_PATH, DELETED_PATH};

pub struct Location(pub PathBuf);

impl Location {
    pub fn elements_path(&self) -> PathBuf {
        self.0.join(ELEMENTS_PATH)
    }

    pub fn index_path(&self) -> PathBuf {
        self.0.join(INDEX_PATH)
    }

    pub fn dirty_path(&self) -> PathBuf {
        self.0.join(DIRTY_PATH)
    }

    pub fn commit_lock_path(&self) -> PathBuf {
        self.0.join(COMMIT_LOCK_PATH)
    }

    pub fn writer_lock_path(&self) -> PathBuf {
        self.0.join(WRITER_LOCK_PATH)
    }

    pub fn deleted_path(&self) -> PathBuf {
        self.0.join(DELETED_PATH)
    }

    pub fn path(&self) -> PathBuf {
        self.0.clone()
    }
}