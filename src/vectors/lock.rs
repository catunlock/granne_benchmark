use std::path::PathBuf;

pub struct Lock {
    location: PathBuf
}

impl Lock {
    pub fn open<T: Into<PathBuf>>(location: T) -> Result<Self, String> {
        let location = location.into();
        let dir = location.parent().unwrap();

        if !dir.exists() {
            return Err("Parent directory doesn't exist.".to_string());
        }

        Ok(Lock{location})
    }

    pub fn lock(&self) -> Result<Self, String> {
        match File::create(self.location.dirty_path()) {
            Ok(_) => debug!("Set dirty file"),
            Err(e) => error!("Error setting dirty file: {}", e.to_string()),
        }
    }

    pub fn unlock(&self) {

    }
}