use lmdb::Database;
extern crate lmdb_zero as lmdb;

#[derive(Debug)]
pub struct IndexMap<'a> {
    db: Database<'a>,
    db_inverted: Database<'a>,
}

impl<'a> IndexMap<'a> {
    pub fn open(path: &str) -> Result<Self, lmdb::Error> {
        let inverted_path = path.to_string() + "inverted";
        std::fs::create_dir_all(path).unwrap();
        std::fs::create_dir_all(&inverted_path).unwrap();

        let flags = lmdb::open::Flags::empty();
        let database_options = lmdb::DatabaseOptions::new(lmdb::db::DUPSORT);
        let database_options_inverted = lmdb::DatabaseOptions::defaults();

        let env = unsafe {
            lmdb::EnvBuilder::new()
                .unwrap()
                .open(path, flags, 0o666)
                .unwrap()
        };

        let env_inverted = unsafe {
            lmdb::EnvBuilder::new()
                .unwrap()
                .open(&inverted_path, flags, 0o666)
                .unwrap()
        };

        let db = lmdb::Database::open(env, None, &database_options)?;
        let db_inverted = lmdb::Database::open(env_inverted, None, &database_options_inverted)?;

        Ok(IndexMap { db, db_inverted })
    }

    /// Returns all the internal vectors ids for a document.
    pub fn get_vec_ids(&self, doc_id: usize) -> Result<Vec<usize>, lmdb::Error> {
        trace!("Obtaining all vector idxs for document: {}", doc_id);
        let key = bincode::serialize(&doc_id).unwrap();

        let env = self.db.env();
        let txn = lmdb::ReadTransaction::new(env).unwrap();
        let access = txn.access();

        let mut cursor = txn.cursor(&self.db).unwrap();

        let mut results = Vec::new();
        match cursor.seek_k::<[u8], [u8]>(&access, &key) {
            Ok(v) => {
                let v: usize = bincode::deserialize(v).unwrap();
                results.push(v);
                while let Ok((_, v)) = cursor.next_dup::<[u8], [u8]>(&access) {
                    let v: usize = bincode::deserialize(v).unwrap();
                    results.push(v);
                }
            }
            Err(e) => {
                error!("Error looking for key {}: {}", doc_id, e.to_string());
            }
        }
        Ok(results)
    }

    /// Returns all the internal vectors ids for a document.
    pub fn get_doc_id(&self, vec_id: usize) -> Result<usize, lmdb::Error> {
        let key = bincode::serialize(&vec_id).unwrap();

        let env = self.db_inverted.env();
        let txn = lmdb::ReadTransaction::new(env).unwrap();
        let access = txn.access();

        match access.get::<[u8], [u8]>(&self.db_inverted, &key) {
            Ok(v) => Ok(bincode::deserialize(v).unwrap()),
            Err(e) => Err(e),
        }
    }

    fn insert_at(db: &Database, key: &[u8], val: &[u8]) -> Result<(), lmdb::Error> {
        let env = db.env();
        let txn = lmdb::WriteTransaction::new(env)?;
        let flags = lmdb::put::Flags::empty();
        {
            let mut access = txn.access();
            access.put::<[u8], [u8]>(db, key, val, flags)?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Adds a new internal vec_id to the list of associated vectors of a document.
    pub fn insert(&self, doc_id: usize, vec_id: usize) -> Result<(), lmdb::Error> {
        trace!("Add doc_id {} <-> vec_id {}", doc_id, vec_id);

        let key = bincode::serialize(&doc_id).unwrap();
        let val = bincode::serialize(&vec_id).unwrap();

        IndexMap::insert_at(&self.db, &key, &val)?;
        IndexMap::insert_at(&self.db_inverted, &val, &key)?;

        Ok(())
    }

    fn insert_at_batch(db: &Database, key: &[usize], val: &[usize]) -> Result<(), lmdb::Error> {
        let env = db.env();
        let txn = lmdb::WriteTransaction::new(env)?;
        let flags = lmdb::put::Flags::empty();
        {
            let mut access = txn.access();

            for i in 0..key.len() {
                let key = bincode::serialize(&key[i]).unwrap();
                let val = bincode::serialize(&val[i]).unwrap();
                access.put::<[u8], [u8]>(db, &key, &val, flags)?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Adds a new internal vec_id to the list of associated vectors of a document.
    pub fn insert_batch(&self, doc_ids: &[usize], vec_ids: &[usize]) -> Result<(), lmdb::Error> {
        assert_eq!(doc_ids.len(), vec_ids.len());
        IndexMap::insert_at_batch(&self.db, doc_ids, vec_ids)?;
        IndexMap::insert_at_batch(&self.db_inverted, vec_ids, doc_ids)?;

        Ok(())
    }

    /// Deletes all the entries of a doc_id in the database.
    ///
    /// The inverted index is not modified since these elements still exists in granne vectors
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

        assert_eq!(map.get_vec_ids(0).unwrap(), vec![0]);
        assert_eq!(map.get_vec_ids(1).unwrap(), vec![4]);

        assert_eq!(map.get_doc_id(0).unwrap(), 0);
        assert_eq!(map.get_doc_id(4).unwrap(), 1);
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

        assert_eq!(map.get_vec_ids(0).unwrap(), vec![0, 1, 2]);
        assert_eq!(map.get_vec_ids(1).unwrap(), vec![3, 4]);

        assert_eq!(map.get_doc_id(0).unwrap(), 0);
        assert_eq!(map.get_doc_id(1).unwrap(), 0);
        assert_eq!(map.get_doc_id(2).unwrap(), 0);

        assert_eq!(map.get_doc_id(3).unwrap(), 1);
        assert_eq!(map.get_doc_id(4).unwrap(), 1);
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
        map.insert(1, 3).unwrap();
        map.insert(1, 4).unwrap();
        map.insert(1, 5).unwrap();

        assert_eq!(map.get_vec_ids(0).unwrap(), vec![0, 1, 2]);

        map.delete(0).unwrap();
        assert!(map.get_vec_ids(0).unwrap().is_empty());
        assert_eq!(map.get_vec_ids(1).unwrap(), vec![3, 4, 5]);

        // Vectors still exist in granne vectors container, so we don't delete them from the inverse
        // index
        assert_eq!(map.get_doc_id(0).unwrap(), 0);
        assert_eq!(map.get_doc_id(1).unwrap(), 0);
        assert_eq!(map.get_doc_id(2).unwrap(), 0);

        map.delete(1).unwrap();
        assert!(map.get_vec_ids(1).unwrap().is_empty());
    }

    #[test]
    fn insert_batch() {
        init();

        let tempdir = tempdir().unwrap();
        let path = tempdir.path().to_str().unwrap();
        let map = IndexMap::open(path).unwrap();

        /*
        map.insert(0, 0).unwrap();
        map.insert(0, 1).unwrap();
        map.insert(0, 2).unwrap();
        map.insert(1, 3).unwrap();
        map.insert(1, 4).unwrap();
        */
        map.insert_batch(&[0, 0, 0, 1, 1], &[0, 1, 2, 3, 4])
            .unwrap();

        assert_eq!(map.get_vec_ids(0).unwrap(), vec![0, 1, 2]);
        assert_eq!(map.get_vec_ids(1).unwrap(), vec![3, 4]);

        assert_eq!(map.get_doc_id(0).unwrap(), 0);
        assert_eq!(map.get_doc_id(1).unwrap(), 0);
        assert_eq!(map.get_doc_id(2).unwrap(), 0);

        assert_eq!(map.get_doc_id(3).unwrap(), 1);
        assert_eq!(map.get_doc_id(4).unwrap(), 1);
    }
}
