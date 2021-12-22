use lmdb::{Database, Environment};
extern crate lmdb_zero as lmdb;

pub struct IndexMap<'a> {
    db: Database<'a>
}

impl<'a> IndexMap<'a> {
    pub fn open(path: &str) -> Result<Self, lmdb::Error> {
        std::fs::create_dir_all(path).unwrap();

        let flags = lmdb::open::Flags::empty();
        let database_options = lmdb::DatabaseOptions::new(lmdb::db::DUPSORT);

        let env = unsafe {
            lmdb::EnvBuilder::new()
                .unwrap()
                .open(path, flags, 0o666)
                .unwrap()
        };

        let db = lmdb::Database::open(env, None, &database_options)?;

        Ok(IndexMap {
            db
        })
    }

    /// Returns all the internal vectors ids for a document.
    pub fn get(&self, doc_id: usize) -> Result<Vec<usize>, lmdb::Error> {
        trace!("Obtaining all vector idxs for document: {}", doc_id);
        let key = bincode::serialize(&doc_id).unwrap();
        let env = self.db.env();

        let txn = lmdb::ReadTransaction::new(env).unwrap();
        let access = txn.access();

        let mut cursor = txn.cursor(&self.db).unwrap();

        let mut results = Vec::new();
        match cursor.seek_k::<[u8], [u8]>(&access, &key) {
            Ok(v) => {
                let v: usize = bincode::deserialize(&v).unwrap();
                results.push(v);
                while let Ok((_,v)) = cursor.next_dup::<[u8],[u8]>(&access) {
                    let v: usize = bincode::deserialize(&v).unwrap();
                    results.push(v);
                }
            },
            Err(e) => {
                error!("Error looking for key {}: {}", doc_id, e.to_string());
            },
        }
        Ok(results)
    }

    /// Adds a new internal vec_id to the list of associated vectors of a document.
    pub fn insert(&self, doc_id: usize, vec_id: usize) -> Result<(), lmdb::Error> {
        trace!("Add doc_id {} -> vec_id {}", doc_id, vec_id);
        let env = self.db.env();
        let txn = lmdb::WriteTransaction::new(env)?;
        let flags = lmdb::put::Flags::empty();
        {
            let mut access = txn.access();
            let key = bincode::serialize(&doc_id).unwrap();
            let value = bincode::serialize(&vec_id).unwrap();
            access.put(&self.db, &key, &value, flags)?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Deletes all the entries of a doc_id in the database.
    pub fn delete(&self, doc_id: usize) -> Result<(), lmdb::Error> {
        let env = self.db.env();
        let txn = lmdb::WriteTransaction::new(env)?;
        {
            let mut access = txn.access();
            let key = bincode::serialize(&doc_id).unwrap();
            access.del_key(&self.db, &key)?;
        }
        txn.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use log::LevelFilter;
    use tempfile::tempdir;

    use super::IndexMap;
    
    fn init() {
        let _ = env_logger::builder()
            .filter_level(LevelFilter::Trace)
            .is_test(true)
            .try_init();
    }

    #[test]
    fn insert() {
        init();

        let tempdir = tempdir().unwrap();
        let path = tempdir.path().to_str().unwrap();
        let map = IndexMap::open(path).unwrap();

        map.insert(0, 0).unwrap();
        map.insert(1, 4).unwrap();

        assert_eq!(map.get(0).unwrap(), vec![0]);
        assert_eq!(map.get(1).unwrap(), vec![4]);
    }


    #[test]
    fn insert_dup() {
        init();

        let tempdir = tempdir().unwrap();
        let path = tempdir.path().to_str().unwrap();
        let map = IndexMap::open(path).unwrap();

        map.insert(0, 0).unwrap();
        map.insert(0, 1).unwrap();
        map.insert(0, 2).unwrap();

        map.insert(1, 3).unwrap();
        map.insert(1, 4).unwrap();

        assert_eq!(map.get(0).unwrap(), vec![0,1,2]);
        assert_eq!(map.get(1).unwrap(), vec![3,4]);
    }

    #[test]
    fn delete() {
        init();

        let tempdir = tempdir().unwrap();
        let path = tempdir.path().to_str().unwrap();
        let map = IndexMap::open(path).unwrap();

        map.insert(0, 0).unwrap();
        map.insert(0, 1).unwrap();
        map.insert(0, 2).unwrap();

        assert_eq!(map.get(0).unwrap(), vec![0,1,2]);

        map.delete(0).unwrap();
        assert!(map.get(0).unwrap().is_empty());
    }
}