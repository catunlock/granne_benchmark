use std::{fs::File, path::PathBuf, time::Instant, fmt};

use granne::{
    angular::{self, Vector, Vectors},
    BuildConfig, Builder, GranneBuilder, Index,
};
use log::{debug, error, trace};
use serde::{Serialize, Deserialize};
use tempfile::NamedTempFile;

use super::{directory::Location, DeletedDBWriter, IndexMap, Lock};
use std::fmt::Debug;

pub struct Writer<'a, 'b, T: Default + Debug + Serialize + Deserialize<'static>> {
    location: Location,
    elements: angular::Vectors<'a>,
    build_config: BuildConfig,
    commit_lock: Lock,
    writer_lock: Lock,
    deleted: DeletedDBWriter<'a>,
    index_map: IndexMap<'a, 'b, T>,
    _d: T
}

impl<'a, 'b, T: Default + Debug + Serialize + Deserialize<'static>> fmt::Debug for Writer<'a, 'b, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Writer")
        .field("location", &self.location)
        .field("build_config", &self.build_config)
        .field("commit_lock", &self.commit_lock)
        .field("_writer_lock", &self.writer_lock)
        .field("deleted", &self.deleted)
        .field("index_map", &self.index_map)
        .finish()
    }
}

impl<'a, 'b, T: Default + Debug + Serialize + Deserialize<'static>> Drop for Writer<'a, 'b, T> {
    fn drop(&mut self) {
        debug!("Dropping writer");
        self.writer_lock.unlock();
    }
}

impl<'a, 'b, T: Default + Debug + Serialize + Deserialize<'static>> Writer<'a, 'b, T> {
    pub fn open<P: Into<PathBuf>>(location: P) -> Result<Self, String> {
        let location = Location(location.into());
        std::fs::create_dir_all(location.path()).unwrap();
        let commit_lock = Lock::open(&location.commit_lock_path()).unwrap();
        let writer_lock = Lock::open(&location.writer_lock_path()).unwrap();

        if let Err(e) = writer_lock.try_lock() {
            let message = format!("Adquiring lock for Writer: {}.\nCheck if another instance of nucliadb_node is running.", e);
            error!("{}", message);
        }

        let elements = Writer::<T>::open_elements(location.elements_path());

        let build_config = BuildConfig::default();

        let deleted_path = location.deleted_path();
        let deleted = DeletedDBWriter::open(deleted_path.to_str().unwrap()).unwrap();

        let index_map_path = location.index_map_path();
        let index_map = IndexMap::open(index_map_path.to_str().unwrap()).unwrap();

        Ok(Writer {
            location,
            elements,
            build_config,
            commit_lock,
            writer_lock,
            deleted,
            index_map,
            _d: Default::default()
        })
    }

    fn open_elements<'c, P: Into<PathBuf>>(elements_path: P) -> angular::Vectors<'c> {
        match File::open(elements_path.into()) {
            Ok(file) => unsafe { angular::Vectors::from_file(&file).unwrap() },
            Err(_) => angular::Vectors::new(),
        }
    }

    pub fn push(&mut self, doc_id: T, vector: &Vector) -> Result<(), String> {
        trace!("Pushing vector for doc: {:?}", doc_id);
        match self.index_map.insert(doc_id, self.next_idx()) {
            Ok(()) => {
                self.elements.push(vector);
                Ok(())
            }
            Err(e) => {
                error!("Error maping vector for document: {}", e.to_string());
                Err(e.to_string())
            }
        }
    }

    pub fn push_vec(&mut self, doc_id: T, vector: Vec<f32>) -> Result<(), String> {
        let vector = Vector::from_iter(vector.into_iter());
        self.push(doc_id, &vector)
    }

    pub fn push_batch(&mut self, doc_ids: &[T], vectors: &[Vector]) -> Result<(), String> {
        trace!("Pushing batch of {} docs", doc_ids.len());

        let start_id = self.next_idx();
        let end_id = start_id + doc_ids.len();

        let id_list: Vec<_> = (start_id..end_id).collect();

        let step = 5000;
        for i in 0..doc_ids.len()/step {
            let start = i*step;
            let end = (i+1)*step;
            println!("map batch {} - {}", start, end);
            self.map_batch(&doc_ids[start..end], &id_list[start..end], vectors)?;
        }

        Ok(())
    }

    fn map_batch(&mut self, doc_ids: &[T], vec_ids: &[usize], vectors: &[Vector]) -> Result<(), String> {
        match self.index_map.insert_batch(doc_ids, &vec_ids) {
            Ok(()) => {
                for v in vectors {
                    self.elements.push(v);
                }
                Ok(())
            }
            Err(e) => {
                error!("Error maping vector for document: {}", e.to_string());
                Err(e.to_string())
            }
        }
    }

    pub fn delete(&self, doc_id: T) -> Result<(), String> {
        trace!("Marking all vectors of doc {:?} as deleted", doc_id);
        match self.index_map.get_vec_ids(doc_id) {
            Ok(vec_ids) => match self.deleted.add_batch(vec_ids.into_iter()) {
                Ok(()) => Ok(()),
                Err(e) => {
                    error!(
                        "Error adding vectors to deleted indexes database: {}",
                        e.to_string()
                    );
                    Err(e.to_string())
                }
            },
            Err(e) => {
                error!(
                    "Error obtaining the indexes of the vectors of the document {:?}: {}",
                    doc_id,
                    e.to_string()
                );
                Err(e.to_string())
            }
        }
    }

    pub fn commit(&mut self) {
        let t0 = Instant::now();
        let mut builder = GranneBuilder::new(self.build_config, self.elements.clone());
        debug!("Builder made in {:?}", t0.elapsed());

        debug!("Start building index!");
        let t0 = Instant::now();
        builder.build();
        debug!("Index built in {:?}", t0.elapsed());

        let tmp_elements = self.save_elements(&builder);
        let tmp_index = self.save_index(&builder);

        self.commit_files(tmp_elements, tmp_index);
        self.set_dirty();
    }

    fn set_dirty(&self) {
        match File::create(self.location.dirty_path()) {
            Ok(_) => debug!("Set dirty file"),
            Err(e) => error!("Error setting dirty file: {}", e.to_string()),
        }
    }

    fn commit_files(&mut self, tmp_elements: NamedTempFile, tmp_index: NamedTempFile) {
        debug!("Adquiring commit lock");
        self.commit_lock.lock();
        std::fs::create_dir_all(&self.location.path()).unwrap();
        self.swap_files(tmp_elements.path(), &self.location.elements_path());
        self.swap_files(tmp_index.path(), &self.location.index_path());
        debug!("Releasing commit lock");
        self.commit_lock.unlock();
    }

    fn swap_files<P: Into<PathBuf>>(&self, orig: P, dest: P) {
        let orig = orig.into();
        let dest = dest.into();
        Writer::<T>::remove_file(dest.clone());
        debug!("Moving {:?} -> {:?}", orig, dest);
        std::fs::rename(orig, dest).unwrap();
    }

    fn remove_file<P: Into<PathBuf>>(path: P) {
        let path = path.into();
        match std::fs::remove_file(path.clone()) {
            Ok(_) => debug!("Removed {:?}", path),
            Err(_) => trace!("Remove ignored, {:?} doesn't exist", path),
        }
    }

    fn save_index(&self, builder: &GranneBuilder<Vectors>) -> NamedTempFile {
        let mut tmpfile = NamedTempFile::new().unwrap();

        let t0 = Instant::now();
        debug!("Writing index to file...");
        builder.write_index(&mut tmpfile).unwrap();
        trace!("Index wrote in {:?}", t0.elapsed());

        tmpfile
    }

    fn save_elements(&self, builder: &GranneBuilder<Vectors>) -> NamedTempFile {
        let mut tmpfile = NamedTempFile::new().unwrap();

        let t0 = Instant::now();
        debug!("Writing elements to file...");
        builder.write_elements(&mut tmpfile).unwrap();
        trace!("Elements wrote in {:?}", t0.elapsed());

        tmpfile
    }

    fn next_idx(&self) -> usize {
        self.elements.len()
    }
}

unsafe impl<'a, 'b, T: Default + Debug + Serialize + Deserialize<'static>> Send for Writer<'a, 'b, T> {}
unsafe impl<'a, 'b, T: Default + Debug+ Serialize + Deserialize<'static>> Sync for Writer<'a, 'b, T> {}

#[cfg(test)]
mod test {
    use granne::angular::{self, Vector};

    fn create_vector(n_dim: usize, u: f32) -> Vector<'static> {
        Vector((0..n_dim).map(|_| u).collect())
    }

    #[test]
    fn indices() {
        // This test show and checks the behaviour of the elements collection regarding the id matching
        // of the elements inserted.
        let mut elements = angular::Vectors::new();
        let mut idxs: Vec<usize> = vec![elements.len()];

        elements.push(&create_vector(3, 0.0));
        idxs.push(elements.len());
        elements.push(&create_vector(3, 1.0));
        idxs.push(elements.len());
        elements.push(&create_vector(3, 2.0));

        assert_eq!(elements.get_element(0).0[0], 0.0);
        assert_eq!(elements.get_element(1).0[1], 1.0);
        assert_eq!(elements.get_element(2).0[2], 2.0);
    }
}


