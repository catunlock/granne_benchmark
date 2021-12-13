use std::{path::{PathBuf, Path}, time::Instant, fs::File};

use granne::{angular::{self, Vector, Vectors}, GranneBuilder, BuildConfig, Builder, Index};
use log::{trace, debug};
use tempfile::NamedTempFile;
use fslock::LockFile;

/*
        let index_file = std::fs::File::open("data/index.dat").unwrap();
        let elements_file = std::fs::File::open("data/elements.dat").unwrap();
    
        let mut elements = unsafe { angular::Vectors::from_file(&elements_file).unwrap() };

        let vectors: Vec<_> = (0..n_vectors).into_par_iter().map(|_| random_vector(n_dim)).collect();
    
        eprintln!("Inserting vectors into the collection");
        for v in vectors {
            elements.push(&v);
        }
    
        eprintln!("Building index");
        let mut builder = GranneBuilder::new(BuildConfig::default(), elements);
        builder.build()
*/

pub struct Writer<'a> {
    location: PathBuf,
    elements: angular::Vectors<'a>,
    build_config: BuildConfig,
    lock: LockFile
}

impl<'a> Writer<'a> {

    pub fn open<T: Into<PathBuf>>(location: T) -> Self {

        let location = location.into();
        let elements_path = location.join("elements.dat");
        let elements_file = std::fs::File::open(elements_path).unwrap();

        let elements = unsafe { angular::Vectors::from_file(&elements_file).unwrap() };

        let build_config = BuildConfig::new()
            .num_neighbors(30)
            .layer_multiplier(15.0)
            .max_search(200);
        
        let lock = LockFile::open(&location.join("LOCK")).unwrap();

        Writer { 
            location,
            elements,
            build_config,
            lock
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
    }

    fn commit_files(&mut self, tmp_elements: NamedTempFile, tmp_index: NamedTempFile) {
        self.lock.lock().unwrap();
        std::fs::create_dir_all(&self.location).unwrap();
        self.swap_files(tmp_elements.path(), &self.elements_path());
        self.swap_files(tmp_index.path(), &self.index_path());
        self.lock.unlock().unwrap()
    }

    fn swap_files<T: Into<PathBuf>>(&self, orig: T, dest: T) {
        let orig = orig.into();
        let dest = dest.into();
        std::fs::remove_file(dest.clone()).unwrap();
        std::fs::rename(orig, dest).unwrap();
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

    pub fn elements_path(&self) -> PathBuf {
        self.location.join("elements.dat")
    }

    pub fn index_path(&self) -> PathBuf {
        self.location.join("index.dat")
    }
}