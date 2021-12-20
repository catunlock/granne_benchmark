use std::{path::PathBuf, fs::{File, OpenOptions}, io::{self, Write, Read, SeekFrom, Seek}, 
ops::DerefMut, fmt::Debug};

use memmap::{Mmap, MmapMut};


pub struct DeletedFile {
    file: File,
}

impl DeletedFile {
    pub fn open<T: Into<PathBuf>>(location: T) -> Result<Self, io::Error> {
        let location = location.into();

        debug!("Opening DeletedFile at: {:?}", &location);
        let file = OpenOptions::new()
            .append(true)
            .read(true)
            .write(true)
            .create(true)
            .open(location.clone())?;

        Ok(DeletedFile {
            file
        })
    }

    pub fn search(&mut self, idx: u32) -> Result<bool, io::Error> {
        let mut buf = [0u8; 4];
        self.file.seek(SeekFrom::Start(0))?;
        
        trace!("Searching for: {}", idx);
        while self.file.read_exact(&mut buf).is_ok() {
            let idx_file: u32 = bincode::deserialize(&buf).unwrap();
            trace!("\tfound: {}", idx_file);
            if idx_file == idx {
                trace!("Founded!");
                return Ok(true);
            }
        }
        return Ok(false);
    }

    pub fn append(&mut self, idx: u32) -> Result<(), io::Error> {
        let bin = bincode::serialize(&idx).unwrap();
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&bin)?;
        self.file.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use log::LevelFilter;

    use super::DeletedFile;

    fn init() {
        let _ = env_logger::builder()
            .filter_level(LevelFilter::Trace)
            .is_test(true)
            .try_init();
    }
    
    #[test]
    fn vectors_and_slice_equality() {
        let v = vec![1,456, 65570];
        assert!(v == [1, 456, 65570]);
    }

    #[test]
    fn add_and_search() {
        init();
        let tempfile = tempfile::NamedTempFile::new().unwrap();
        let mut deleted = DeletedFile::open(tempfile.path()).unwrap();

        deleted.append(1).unwrap();
        deleted.append(2).unwrap();
        deleted.append(3).unwrap();
        deleted.append(256).unwrap();

        assert!(deleted.search(1).unwrap());
        assert!(deleted.search(256).unwrap());
    }
}