use granne::{
    angular::{self, Vector, Vectors},
    Granne,
};
use serde::{Serialize, Deserialize};
use std::{cell::RefCell, collections::HashMap, path::PathBuf, fmt, io};

use super::{directory::Location, DeletedDBReader, IndexMap, Lock};
use std::fmt::Debug;

pub struct Reader<'a, 'b, T: Default + Debug + Serialize + Deserialize<'static>> {
    location: Location,
    commit_lock: Lock,
    index: RefCell<Granne<'a, Vectors<'a>>>,
    max_search: usize,
    num_neighbors: usize,
    deleted: DeletedDBReader<'a>,
    index_map: IndexMap<'a,'b, T>,
    _d: T
}

impl<'a, 'b, T: Default + Debug + Serialize + Deserialize<'static>> fmt::Debug  for Reader<'a, 'b, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reader")
        .field("location", &self.location)
        .field("commit_lock", &self.commit_lock)
        .field("max_search", &self.max_search)
        .field("num_neighbors", &self.num_neighbors)
        .field("deleted", &self.deleted)
        .finish()
    }
}


impl<'a, 'b, T: Default + Debug + Serialize + Deserialize<'static>> Reader<'a, 'b, T> {
    pub fn open<P: Into<PathBuf>>(location: P) -> Result<Self, String> {
        let location = Location(location.into());
        let commit_lock = Lock::open(&location.commit_lock_path()).unwrap();

        let index = match Reader::<T>::load_index(
            location.index_path(),
            location.elements_path(),
        ) {
            Ok(index) => index,
            Err(e) => {
                let message = format!("{}", e.to_string());
                return Err(message)
            },
        };

        let index = RefCell::new(index);
        let deleted_path = location.deleted_path();
        let deleted = DeletedDBReader::open(deleted_path.to_str().unwrap()).unwrap();

        let index_map_path = location.index_map_path();
        let index_map = IndexMap::open(index_map_path.to_str().unwrap()).unwrap();

        Ok(Reader {
            location,
            commit_lock,
            index,
            max_search: 200,
            num_neighbors: 30,
            deleted,
            index_map,
            _d: Default::default()
        })
    }

    pub fn search(&self, query_vector: &Vector<'static>) -> Vec<(T, f32)> {
        debug!("Search for vector");

        if self.is_dirty() {
            self.reload();
            self.clean_dirty();
        }

        let raw_results =
            self.index
                .borrow()
                .search(query_vector, self.max_search, self.num_neighbors);
        let idxs: Vec<usize> = raw_results.iter().map(|(idx, _score)| *idx).collect();
        let idxs = self.deleted.filter(&idxs).unwrap();

        let raw_results: HashMap<usize, f32> = raw_results.into_iter().collect();

        idxs.into_iter()
            .map(|idx| {
                let score = raw_results.get(&idx).unwrap();
                let doc_id = self.index_map.get_doc_id(idx).unwrap();
                (doc_id, *score)
            })
            .collect()
    }

    pub fn search_vec(&self, query_vector: Vec<f32>) -> Vec<(T, f32)> {
        let query_vector = Vector::from_iter(query_vector.into_iter());
        self.search(&query_vector)
    }

    fn load_index<P: Into<PathBuf>>(index_path: P, elements_path: P) -> Result<Granne<'a, Vectors<'a>>, io::Error> {
        debug!("Loading (memory-mapping) index and vectors.");
        let index_file = std::fs::File::open(index_path.into())?;
        let elements_file = std::fs::File::open(elements_path.into())?;

        let elements = unsafe { angular::Vectors::from_file(&elements_file)? };
        Ok(unsafe { Granne::from_file(&index_file, elements.clone()).unwrap() })
    }

    pub fn is_dirty(&self) -> bool {
        self.location.dirty_path().exists()
    }

    fn clean_dirty(&self) {
        match std::fs::remove_file(self.location.dirty_path()) {
            Ok(_) => debug!("Cleaned dirty file"),
            Err(_) => trace!(
                "Remove ignored, {:?} doesn't exist",
                self.location.dirty_path()
            ),
        }
    }

    fn reload(&self) {
        debug!("Reloading!");

        self.commit_lock.lock();
        self.index.replace(Reader::<T>::load_index(
            self.location.index_path(),
            self.location.elements_path(),
        ).unwrap());
        self.commit_lock.unlock();
    }
}

unsafe impl<'a, 'b, T: Default + Debug + Serialize + Deserialize<'static>> Send for Reader<'a, 'b, T> {}
unsafe impl<'a, 'b, T: Default + Debug + Serialize + Deserialize<'static>> Sync for Reader<'a, 'b, T> {}