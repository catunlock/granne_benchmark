use std::{
    fs::File,
    path::{Path, PathBuf},
    time::Instant,
};

use granne::{
    angular::{self, Vector, Vectors},
    BuildConfig, Builder, GranneBuilder, Index,
};
use log::{debug, error, trace};
use tempfile::NamedTempFile;

use super::{directory::Location, DeletedDBWriter, IndexMap, Lock};

pub struct Writer<'a> {
    location: Location,
    elements: angular::Vectors<'a>,
    build_config: BuildConfig,
    commit_lock: Lock,
    _writer_lock: Lock,
    deleted: DeletedDBWriter<'a>,
    index_map: IndexMap<'a>,
}

impl<'a> Writer<'a> {
    pub fn open<T: Into<PathBuf>>(location: T) -> Result<Self, String> {
        let location = Location(location.into());
        let commit_lock = Lock::open(&location.commit_lock_path()).unwrap();
        let mut _writer_lock = Lock::open(&location.writer_lock_path()).unwrap();

        if let Err(e) = _writer_lock.try_lock() {
            let message = format!("Adquiring lock for Writer: {}", e.to_string());
            error!("{}", message);
            return Err(message);
        }

        let elements = Writer::open_elements(location.elements_path());

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
            _writer_lock,
            deleted,
            index_map,
        })
    }

    fn open_elements<'b, T: Into<PathBuf>>(elements_path: T) -> angular::Vectors<'b> {
        match File::open(elements_path.into()) {
            Ok(file) => unsafe { angular::Vectors::from_file(&file).unwrap() },
            Err(_) => angular::Vectors::new(),
        }
    }

    pub fn push(&mut self, doc_id: usize, vector: &Vector) -> Result<(), String> {
        trace!("Pushing vector for doc: {}", doc_id);
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

    pub fn push_batch(&mut self, doc_ids: &[usize], vectors: &[Vector]) -> Result<(), String> {
        trace!("Pushing batch of {} docs", doc_ids.len());

        let start_id = self.next_idx();
        let end_id = start_id + doc_ids.len();

        let id_list: Vec<_> = (start_id..end_id).collect();

        match self.index_map.insert_batch(doc_ids, &id_list) {
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

    pub fn delete(&self, doc_id: usize) -> Result<(), String> {
        trace!("Marking all vectors of doc {} as deleted", doc_id);
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
                    "Error obtaining the indexes of the vectors of the document {}: {}",
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

    fn swap_files<T: Into<PathBuf>>(&self, orig: T, dest: T) {
        let orig = orig.into();
        let dest = dest.into();
        Writer::remove_file(dest.clone());
        debug!("Moving {:?} -> {:?}", orig, dest);
        std::fs::rename(orig, dest).unwrap();
    }

    fn remove_file<T: Into<PathBuf>>(path: T) {
        let path = path.into();
        match std::fs::remove_file(path.clone()) {
            Ok(_) => debug!("Removed {:?}", path.clone()),
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
        let mut idxs: Vec<usize> = Vec::new();

        idxs.push(elements.len());
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
