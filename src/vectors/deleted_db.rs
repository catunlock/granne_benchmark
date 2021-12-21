use std::path::PathBuf;

use lmdb::{Database, Environment};
extern crate lmdb_zero as lmdb;


pub struct DeletedDBReader<'a> {
    db: Database<'a>
}

impl<'a> DeletedDBReader<'a> {
    pub fn open(path: &str) -> Result<Self, lmdb::Error> {
        std::fs::create_dir_all(path).unwrap();
        let env = unsafe {
            lmdb::EnvBuilder::new()
                .unwrap()
                .open(path, lmdb::open::Flags::empty(), 0o444)
                .unwrap()
        };

        let db = lmdb::Database::open(env, None, &lmdb::DatabaseOptions::defaults())?;

        Ok(DeletedDBReader {
            db
        })
    }

    pub fn contains(&self, idx: u32) -> Result<bool, lmdb::Error> {
        trace!("Check if contains: {}", idx);
        let key = bincode::serialize(&idx).unwrap();
        let env = self.db.env();

        let txn = lmdb::ReadTransaction::new(env).unwrap();
        let access = txn.access();

        match access.get::<[u8], [u8]>(&self.db, &key) {
            Ok(_) => Ok(true),
            Err(e) => {
                error!("Error access {:?}: {}", key, e.to_string());
                Ok(false)
            },
        }
    }
}

pub struct DeletedDBWriter<'a> {
    db: Database<'a>
}

impl<'a> DeletedDBWriter<'a> {
    pub fn open(path: &str) -> Result<Self, lmdb::Error> {
        std::fs::create_dir_all(path).unwrap();
        let env = unsafe {
            lmdb::EnvBuilder::new()
                .unwrap()
                .open(path, lmdb::open::Flags::empty(), 0o666)
                .unwrap()
        };

        let db = lmdb::Database::open(env, None, &lmdb::DatabaseOptions::defaults())?;

        Ok(DeletedDBWriter {
            db
        })
    }

    pub fn add(&self, idx: u32) -> Result<(), lmdb::Error> {
        trace!("Add: {:?}", idx);
        let env = self.db.env();
        let txn = lmdb::WriteTransaction::new(env)?;
        {
            let mut access = txn.access();
            let key = bincode::serialize(&idx).unwrap();
            access.put(&self.db, &key, &1, lmdb::put::Flags::empty())?;
        }
        txn.commit()?;
        Ok(())
    }

    pub fn add_batch(&self, idxs: impl Iterator<Item = u32>) -> Result<(), lmdb::Error> {
        trace!("Adding batch");
        let env = self.db.env();
        let txn = lmdb::WriteTransaction::new(env)?;
        {
            let mut access = txn.access();
            for idx in idxs {
                trace!("\tAdd: {:?}", idx);
                let key = bincode::serialize(&idx).unwrap();
                access.put(&self.db, &key, &1, lmdb::put::Flags::empty())?;
            }
            
        }
        txn.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use log::LevelFilter;
    use tempfile::tempdir;

    use super::{DeletedDBReader, DeletedDBWriter};

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
        let tempdir = tempdir().unwrap();
        let path = tempdir.path().to_str().unwrap();
        let mut writer = DeletedDBWriter::open("db2").unwrap();
        let mut reader = DeletedDBReader::open("db2").unwrap();

        writer.add(1).unwrap();
        writer.add(2).unwrap();
        writer.add(3).unwrap();
        writer.add(256).unwrap();

        assert!(reader.contains(1).unwrap());
        assert!(reader.contains(256).unwrap());
    }

    #[test]
    fn thousand_read_and_write() {
        init();
        let tempdir = tempdir().unwrap();
        let path = tempdir.path().to_str().unwrap();
        let mut writer = DeletedDBWriter::open(path).unwrap();
        let mut reader = DeletedDBReader::open(path).unwrap();

 
        std::thread::spawn(move || {
            for i in 0..1_000 {
                writer.add(i).unwrap();
            }  
        }).join().unwrap();

        std::thread::spawn(move || {
            for i in 0..1_000 {
                assert!(reader.contains(i).unwrap());
            }
        }).join().unwrap();
    }

    #[test]
    fn batch_insert() {
        init();
        let tempdir = tempdir().unwrap();
        let path = tempdir.path().to_str().unwrap();
        let mut writer = DeletedDBWriter::open(path).unwrap();
        let mut reader = DeletedDBReader::open(path).unwrap();

        std::thread::spawn(move || {
            writer.add_batch(0..1000).unwrap();  
        }).join().unwrap();

        std::thread::spawn(move || {
            for i in 0..1_000 {
                assert!(reader.contains(i).unwrap());
            }
        }).join().unwrap();
    }
}