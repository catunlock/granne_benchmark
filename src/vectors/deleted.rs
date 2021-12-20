use std::{path::PathBuf, fs::{File, OpenOptions}, io::{self, Write, Read}, ops::DerefMut, fmt::Debug};

use memmap::{Mmap, MmapMut};


pub struct DeletedFile {
    location: PathBuf,
    mmap: MmapMut,
    vector: Vec<u32>
}

impl DeletedFile {
    pub fn open<T: Into<PathBuf>>(location: T) -> Result<Self, io::Error> {
        let location = location.into();

        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .write(true)
            .open(location.clone())?;

        debug!("Opening Mmap file at: {:?}", &location);
        let mmap = unsafe { Mmap::map(&file) };
        let mmap = match mmap {
            Ok(mmap) => mmap.make_mut()?,
            Err(e) => {
                error!("Error opening mmap: {}", e.to_string());
                todo!("Find a way to create a empty Mmap")
            }
        };

        let vector = match bincode::deserialize(&mmap) {
            Ok(v) => v,
            Err(e) => {
                error!("Decoding vector stored in file: {}", e.to_string());
                Vec::new()
            },
        };

        Ok(DeletedFile {
            location,
            mmap: mmap,
            vector
        })
    }

    pub fn search(&self, idx: u32) -> bool {
        let bin_idx = bincode::serialize(&idx).unwrap();

        let iter = self.mmap.chunks(4);
        for chunk in iter {
            if chunk == bin_idx {
                return true;
            }
        }
        return false;
    }

    pub fn append(&mut self, idx: u32) -> Result<(), io::Error> {
        
        let mut bin = bincode::serialize(&idx).unwrap().as_slice();

        let mmap = self.mmap.deref_mut();
        mmap = [*mmap, *bin].concat();
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), io::Error> {
        let mut_mmap = self.mmap.make_mut()?;
        mut_mmap.flush()
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
        let mut deleted = DeletedFile::open("deleted.hex").unwrap();

        deleted.append(1).unwrap();
        assert!(deleted.search(1));

        deleted.append(3).unwrap();
        
        deleted.flush().unwrap();
        
    }
}