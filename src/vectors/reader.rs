use granne::{
    angular::{self, Vector, Vectors},
    Granne,
};
use std::{cell::RefCell, collections::HashMap, fmt, io, path::PathBuf};

use super::{directory::Location, DeletedDBReader, IndexMap, Lock, VectorIdentifier};

pub struct Reader<'a> {
    location: Location,
    commit_lock: Lock,
    index: RefCell<Granne<'a, Vectors<'a>>>,
    max_search: usize,
    num_neighbors: usize,
    deleted: DeletedDBReader<'a>,
    index_map: IndexMap<'a>,
}

impl fmt::Debug for Reader<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reader")
            .field("location", &self.location)
            .field("commit_lock", &self.commit_lock)
            .field("max_search", &self.max_search)
            .field("num_neighbors", &self.num_neighbors)
            .field("deleted", &self.deleted)
            .field("index_map", &self.index_map)
            .finish()
    }
}

impl<'a> Reader<'a> {
    pub fn open<T: Into<PathBuf>>(location: T) -> Result<Self, String> {
        let location = Location(location.into());
        let commit_lock = Lock::open(&location.commit_lock_path()).unwrap();

        let index = match Reader::load_index(location.index_path(), location.elements_path()) {
            Ok(index) => index,
            Err(e) => {
                let message = format!("{}", e.to_string());
                return Err(message);
            }
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
        })
    }

    pub fn search(&self, query_vector: &Vector<'static>) -> Vec<(VectorIdentifier, f32)> {
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
                (self.index_map.get_doc_id(idx).unwrap(), *score)
            })
            .collect()
    }

    pub fn search_vec(&self, query_vector: Vec<f32>) -> Vec<(VectorIdentifier, f32)> {
        let query_vector = Vector::from_iter(query_vector.into_iter());
        self.search(&query_vector)
    }

    fn load_index<T: Into<PathBuf>>(
        index_path: T,
        elements_path: T,
    ) -> Result<Granne<'a, Vectors<'a>>, io::Error> {
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
        self.index.replace(
            Reader::load_index(self.location.index_path(), self.location.elements_path()).unwrap(),
        );
        self.commit_lock.unlock();
    }
}

unsafe impl Send for Reader<'_> {}
unsafe impl Sync for Reader<'_> {}
