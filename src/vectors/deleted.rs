use std::{path::PathBuf, fs::{File, OpenOptions}, io};


pub struct Deleted {
    location: PathBuf
}

impl Deleted {
    pub fn open<T: Into<PathBuf>>(location: T) -> Result<Self, io::Error> {
        let location = location.into();

        let file = OpenOptions::new().append(true).open(location)?;
        let mmap = unsafe { Mmap::map(&file)? };
        // ... use the read-only memory map ...
        let mut mut_mmap = mmap.make_mut()?;
        mut_mmap.deref_mut().write_all(b"hello, world!")?;

        Ok(Deleted {
            location
        })
    }
}