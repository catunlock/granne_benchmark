use std::{path::PathBuf, io::Read};

use fslock::LockFile;
use granne::{angular::{self, Vectors, Vector}, Granne};

use crate::vectors::{ELEMENTS_PATH, INDEX_PATH};

use super::{COMMIT_LOCK_PATH, DIRTY_PATH, directory::Location};

/*
    // max_search controls how extensive the search is
    eprintln!("Querying a random vector - {:?}", t0.elapsed());
    let query_vector = random_vector(n_dim);
    let max_search = 200;
    let res = index.search(&query_vector, max_search, n_results);

    eprintln!("Found {} - {:?}", res.len(), t0.elapsed());
*/

pub struct Reader<'a> {
    location: Location,
    commit_lock: LockFile,
    index: Granne<'a, Vectors<'a>>,
    max_search: usize,
    num_neighbors: usize
}

impl<'a> Reader<'a> {

    pub fn open<T: Into<PathBuf>>(location: T) -> Result<Self, String> {
        let location = Location(location.into());     
        let commit_lock = LockFile::open(&location.commit_lock_path()).unwrap();

        let index = Reader::load_index(location.index_path(), location.elements_path());

        Ok(Reader{
            location,
            commit_lock,
            index,
            max_search: 200,
            num_neighbors: 30
        })
    }

    pub fn search<T: Into<Vec<f32>>>(&self, query_vector: T) -> Vec<(usize, f32)>{
        debug!("Search for vector");

        if self.is_dirty() {
            self.reload();
        }

        let v = Vector::from(query_vector.into());
        self.index.search(&v, self.max_search, self.num_neighbors)
    }

    fn load_index<T: Into<PathBuf>>(index_path: T, elements_path: T) -> Granne<'a, Vectors<'a>> {
        debug!("Loading (memory-mapping) index and vectors.");
        let index_file = std::fs::File::open(index_path.into()).unwrap();
        let elements_file = std::fs::File::open(elements_path.into()).unwrap();

        let elements = unsafe { angular::Vectors::from_file(&elements_file).unwrap() };
        unsafe { Granne::from_file(&index_file, elements.clone()).unwrap() }
    }

    fn is_dirty(&self) -> bool {
        self.location.dirty_path().exists()
    }

    fn clean_dirty(&self) {
        match std::fs::remove_file(self.location.dirty_path()) {
            Ok(_) => debug!("Cleaned dirty file"),
            Err(_) => trace!("Remove ignored, {:?} doesn't exist", self.location.dirty_path()),
        }
    }

    fn reload(&mut self) {
        debug!("Reloading!");

        self.commit_lock.lock().unwrap();
        self.index = Reader::load_index(self.location.index_path(), self.location.elements_path());
        self.commit_lock.unlock().unwrap();
    }
}