use std::{path::{PathBuf, Path}, time::Instant, fs::File};

use granne::{angular::{self, Vector, Vectors}, GranneBuilder, BuildConfig, Builder, Index};
use log::{trace, debug, error};
use tempfile::NamedTempFile;
use fslock::LockFile;

use super::{COMMIT_LOCK_PATH, WRITER_LOCK_PATH, ELEMENTS_PATH, INDEX_PATH, DIRTY_PATH, directory::Location};

pub struct Writer<'a> {
    location: Location,
    elements: angular::Vectors<'a>,
    build_config: BuildConfig,
    commit_lock: LockFile,
    writer_lock: LockFile,
}



impl<'a> Writer<'a> {

    pub fn open<T: Into<PathBuf>>(location: T) -> Result<Self, String> {

        let location = Location(location.into());        
        let commit_lock = LockFile::open(&location.commit_lock_path()).unwrap();
        let mut writer_lock = LockFile::open(&location.writer_lock_path()).unwrap();

        if !writer_lock.try_lock().unwrap() {
            let message = format!("Adquiring lock for Writer on this location: {:?}", &location.path());
            return Err(message);
        }

        let elements = Writer::open_elements(location.elements_path());

        let build_config = BuildConfig::new()
            .num_neighbors(30)
            .layer_multiplier(15.0)
            .max_search(200);
        
        Ok(Writer { 
            location,
            elements,
            build_config,
            commit_lock,
            writer_lock
        })
    }

    fn open_elements<'b, T: Into<PathBuf>>(elements_path: T) -> angular::Vectors<'b> {
        match File::open(elements_path.into()) {
            Ok(file) => unsafe { angular::Vectors::from_file(&file).unwrap() },
            Err(_) => angular::Vectors::new(),
        }
    }

    pub fn push(&mut self, vector: &Vector) {
        trace!("Pushing vector");
        self.elements.push(vector);
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
        self.commit_lock.lock().unwrap();
        std::fs::create_dir_all(&self.location.path()).unwrap();
        self.swap_files(tmp_elements.path(), &self.location.elements_path());
        self.swap_files(tmp_index.path(), &self.location.index_path());
        debug!("Releasing commit lock");
        self.commit_lock.unlock().unwrap()
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


}