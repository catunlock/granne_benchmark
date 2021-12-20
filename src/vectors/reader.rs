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

    pub fn search(&self, query_vector: &Vector<'static>) -> Vec<(usize, f32)>{
        debug!("Search for vector");

        if self.is_dirty() {
            self.reload();
            self.clean_dirty();
        }
        self.index.borrow().search(query_vector, self.max_search, self.num_neighbors)
    }

    pub fn search_vec(&self, query_vector: Vec<f32>) -> Vec<(Vector, f32)> {
        let query_vector = Vector::from_iter(query_vector.into_iter());
        self.get_vectors(&self.search(&query_vector))
    }

    fn get_vectors(&self, results: &Vec<(usize, f32)>) -> Vec<(Vector, f32)> {
         results.iter().map(|(vec_id, score)| {
            (self.index.borrow().get_element(*vec_id), *score)
         }).collect()   
    }

    fn load_index<T: Into<PathBuf>>(index_path: T, elements_path: T) -> Granne<'a, Vectors<'a>> {        
        debug!("Loading (memory-mapping) index and vectors.");
        let index_file = std::fs::File::open(index_path.into()).unwrap();
        let elements_file = std::fs::File::open(elements_path.into()).unwrap();

        let elements = unsafe { angular::Vectors::from_file(&elements_file).unwrap() };
        unsafe { Granne::from_file(&index_file, elements.clone()).unwrap() }
    }

    pub fn is_dirty(&self) -> bool {
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