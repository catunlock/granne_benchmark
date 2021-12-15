use std::{path::PathBuf, cell::{RefCell}};
use granne::{angular::{self, Vectors, Vector}, Granne};

use super::{directory::Location, Lock};

pub struct Reader<'a> {
    location: Location,
    commit_lock: Lock,
    index: RefCell<Granne<'a, Vectors<'a>>>,
    max_search: usize,
    num_neighbors: usize
}

impl<'a> Reader<'a> {

    pub fn open<T: Into<PathBuf>>(location: T) -> Result<Self, String> {
        let location = Location(location.into());     
        let commit_lock = Lock::open(&location.commit_lock_path()).unwrap();

        let index = RefCell::new(
            Reader::load_index(location.index_path(), location.elements_path())
        );

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
            self.clean_dirty();
        }

        let v = Vector::from(query_vector.into());
        self.index.borrow().search(&v, self.max_search, self.num_neighbors)
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

    fn reload(&self) {
        debug!("Reloading!");

        self.commit_lock.lock();
        self.index.replace(
            Reader::load_index(self.location.index_path(), self.location.elements_path())
        );
        self.commit_lock.unlock();
    }
}