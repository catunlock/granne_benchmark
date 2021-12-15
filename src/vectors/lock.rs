use std::{path::PathBuf, fs::File, time::Duration};

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

    pub fn try_lock(&self) -> Result<(), String> {
        if self.is_locked() {
            return Err("Lock already adquired".to_string());
        }

        match File::create(&self.location) {
            Ok(_) => {
                debug!("Set dirty file {:?}", self.location);
                Ok(())
            },
            Err(e) => {
                let message = format!("Error setting dirty file: {}", e.to_string());
                error!("{}", message);
                Err(message)
            },
        }
    }

    pub fn is_locked(&self) -> bool {
        self.location.exists()
    }

    pub fn unlock(&self) {
        match std::fs::remove_file(&self.location) {
            Ok(_) => debug!("Removed {:?}", self.location),
            Err(_) => trace!("Remove ignored, {:?} doesn't exist", self.location),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use rand::{distributions::Alphanumeric, Rng};
    use tempfile::NamedTempFile;

    use super::Lock;

    fn get_random_string(length: usize) -> String {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(length)
            .map(char::from)
            .collect()
    }

    fn get_temp_path() -> PathBuf {
        let temp_path = std::env::temp_dir();
        temp_path.join(get_random_string(7))
    }

    #[test]
    fn basic_lock() {
        
        let temp_file = get_temp_path();
        let lock = Lock::open(&temp_file).unwrap();

        assert!(lock.try_lock().is_ok());
        assert!(temp_file.exists());
        lock.unlock();
        assert!(!temp_file.exists());
    }

    #[test]
    fn try_second_lock() {
        let temp_file = get_temp_path();
        let lock1 = Lock::open(&temp_file).unwrap();
        let lock2 = Lock::open(&temp_file).unwrap();

        assert!(lock1.try_lock().is_ok());
        assert!(lock1.is_locked());
        assert!(lock2.try_lock().is_err());
        
        lock1.unlock();
        assert!(!lock1.is_locked());
        assert!(!lock2.is_locked());
        assert!(lock2.try_lock().is_ok());
        assert!(lock2.is_locked());
    }

    #[test]
    fn destroy_dont_delete_lock_file() {
        let temp_file = get_temp_path();
        {
            let lock = Lock::open(&temp_file).unwrap();
            assert!(lock.try_lock().is_ok());
        }
        
        assert!(temp_file.exists());

        let lock2 = Lock::open(&temp_file).unwrap();
        assert!(lock2.try_lock().is_err());

        lock2.unlock();
        assert!(lock2.try_lock().is_ok());
    }

    #[test]
    fn parent_dir_doesnt_exists() {
        let temp_file = "/tmp/this_dir_doesnt_exists/lock";
        let lock = Lock::open(&temp_file);
        assert!(lock.is_err());
    }
}