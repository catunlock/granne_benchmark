use std::{path::PathBuf, fs::{File, OpenOptions}, io::{self, Write, Read, SeekFrom, Seek}, 
ops::DerefMut, fmt::Debug, collections::HashSet, hash::Hash};

use memmap::{Mmap, MmapMut};

pub struct DeletedFileReader {
    file: File,
}

pub struct DeletedFileWriter {
    file: File,
}

impl DeletedFileReader {
    pub fn open<T: Into<PathBuf>>(location: T) -> Result<Self, io::Error> {
        let location = location.into();

        debug!("Opening DeletedFile Reader at: {:?}", &location);
        let file = OpenOptions::new()
            .read(true)
            .open(location.clone())?;

        Ok(DeletedFileReader {
            file
        })
    }

    pub fn contains(&mut self, idx: u32) -> Result<bool, io::Error> {
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

    pub fn len(&self) -> usize {
        (self.file.metadata().unwrap().len() / (u32::BITS /8) as u64).try_into().unwrap()
    }
}

impl Iterator for DeletedFileReader {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buf = [0u8; 4];
        match self.file.read_exact(&mut buf) {
            Ok(()) => {
                match bincode::deserialize(&buf) {
                    Ok(value) => Some(value),
                    Err(_) => None,
                }
            }
            Err(_) => None,
        }
    }
}

impl From<DeletedFileReader> for HashSet<u32> {
    fn from(reader: DeletedFileReader) -> Self {
        let mut set = HashSet::with_capacity(reader.len());
        for idx in reader {
            set.insert(idx);
        }
        set
    }
}

impl DeletedFileWriter {
    pub fn open<T: Into<PathBuf>>(location: T) -> Result<Self, io::Error> {
        let location = location.into();

        debug!("Opening DeletedFile Writer at: {:?}", &location);
        let file = OpenOptions::new()
            .append(true)
            .read(true)
            .write(true)
            .create(true)
            .open(location.clone())?;

        Ok(DeletedFileWriter {
            file
        })
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
    use std::collections::HashSet;

    use log::LevelFilter;

    use super::{DeletedFileReader, DeletedFileWriter};

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
        let mut writer = DeletedFileWriter::open(tempfile.path()).unwrap();
        let mut reader = DeletedFileReader::open(tempfile.path()).unwrap();

        writer.append(1).unwrap();
        writer.append(2).unwrap();
        writer.append(3).unwrap();
        writer.append(256).unwrap();

        assert!(reader.contains(1).unwrap());
        assert!(reader.contains(256).unwrap());
    }

    #[test]
    fn concurrent_read_and_write() {
        init();
        let tempfile = tempfile::NamedTempFile::new().unwrap();
        let mut writer = DeletedFileWriter::open(tempfile.path()).unwrap();
        let mut reader = DeletedFileReader::open(tempfile.path()).unwrap();

        let j1 = std::thread::spawn(move || {
            for i in 0..1_000 {
                writer.append(i).unwrap();
            }
        });

        let j2 = std::thread::spawn(move || {
            for i in 0..1_000 {
                assert!(reader.contains(i).unwrap());
            }
        });

        j1.join().unwrap();
        j2.join().unwrap();
    }

    #[test]
    fn find_in_set() {
        init();
        let tempfile = tempfile::NamedTempFile::new().unwrap();
        let mut writer = DeletedFileWriter::open(tempfile.path()).unwrap();
        let reader = DeletedFileReader::open(tempfile.path()).unwrap();

        writer.append(1).unwrap();
        writer.append(2).unwrap();
        writer.append(3).unwrap();
        writer.append(256).unwrap();

        let set = HashSet::from(reader);
        assert!(set.contains(&1));
        assert!(set.contains(&2));
        assert!(set.contains(&3));
        assert!(set.contains(&256));

        info!("Contains 256: {}", set.contains(&256));
    }
}