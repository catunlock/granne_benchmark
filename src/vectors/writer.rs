use std::{fmt, fs::File, path::PathBuf, time::Instant};

use granne::{
    angular::{self, Vector, Vectors},
    BuildConfig, Builder, GranneBuilder, Index,
};
use log::{debug, error, trace};
use tempfile::NamedTempFile;

use super::{directory::Location, DeletedDBWriter, IndexMap, Lock, VectorIdentifier};

pub struct Writer<'a> {
    location: Location,
    elements: angular::Vectors<'a>,
    build_config: BuildConfig,
    commit_lock: Lock,
    writer_lock: Lock,
    deleted: DeletedDBWriter<'a>,
    index_map: IndexMap<'a>,
}

impl fmt::Debug for Writer<'_> {
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

impl<'a> Drop for Writer<'a> {
    fn drop(&mut self) {
        debug!("Dropping writer");
        self.writer_lock.unlock();
    }
}

impl<'a> Writer<'a> {
    pub fn open<T: Into<PathBuf>>(location: T) -> Result<Self, String> {
        let location = Location(location.into());
        std::fs::create_dir_all(location.path()).unwrap();
        let commit_lock = Lock::open(&location.commit_lock_path()).unwrap();
        let writer_lock = Lock::open(&location.writer_lock_path()).unwrap();

        if let Err(e) = writer_lock.try_lock() {
            let message = format!("Adquiring lock for Writer: {}.\nCheck if another instance of nucliadb_node is running.", e);
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
            writer_lock,
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

    pub fn push(&mut self, doc_id: &VectorIdentifier, vector: &Vector) -> Result<(), String> {
        trace!("Pushing vector for doc: {}", doc_id);
        match self.index_map.insert(&doc_id, self.next_idx()) {
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

    pub fn push_vec(&mut self, doc_id: &VectorIdentifier, vector: Vec<f32>) -> Result<(), String> {
        let vector = Vector::from_iter(vector.into_iter());
        self.push(doc_id, &vector)
    }

    pub fn push_batch(&mut self, batch: &[(VectorIdentifier, Vec<f32>)]) -> Result<(), String> {
        trace!("Pushing batch of {} docs", batch.len());

        let start_id = self.next_idx();
        let end_id = start_id + batch.len();

        let vec_ids: Vec<_> = (start_id..end_id).collect();

        self.map_batch(&batch, &vec_ids)?;

        Ok(())
    }

    fn map_batch(
        &mut self,
        batch: &[(VectorIdentifier, Vec<f32>)],
        vec_ids: &[usize],
    ) -> Result<(), String> {
        let doc_ids: Vec<_> = batch.into_iter().map(|(vi, _)| vi.clone()).collect();

        match self.index_map.insert_batch(&doc_ids, &vec_ids) {
            Ok(()) => {
                for (_, v) in batch {
                    let v = Vector::from_iter(v.clone().into_iter());
                    self.elements.push(&v);
                }
                Ok(())
            }
            Err(e) => {
                error!("Error maping vector for document: {}", e.to_string());
                Err(e.to_string())
            }
        }
    }

    pub fn delete(&self, doc_id: VectorIdentifier) -> Result<(), String> {
        trace!("Marking all vectors of doc {} as deleted", doc_id);
        match self.index_map.get_vec_ids(&doc_id) {
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

unsafe impl Send for Writer<'_> {}
unsafe impl Sync for Writer<'_> {}

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
