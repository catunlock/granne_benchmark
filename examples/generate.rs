use granne::{
    angular::{self, Vector},
    BuildConfig, Granne, GranneBuilder, Index,
};
use rand::prelude::*;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::{env, time::Instant};

fn random_vector(n_dim: usize) -> Vector<'static> {
    let mut rng = rand::thread_rng();
    Vector((0..n_dim).map(|_| rng.gen()).collect())
}

fn main() {
    let t0 = Instant::now();
    eprintln!("Granne benchmark start!");
    let n_dim: usize = env::var("DIMENSIONS")
        .unwrap_or("800".to_string())
        .parse()
        .unwrap();
    let n_vectors: usize = env::var("N_VECTORS")
        .unwrap_or("8000000".to_string())
        .parse()
        .unwrap();
    let n_results: usize = env::var("N_RESULTS")
        .unwrap_or("200".to_string())
        .parse()
        .unwrap();

    eprintln!(
        "n_dim: {}, n_vectors: {}, n_results: {}",
        n_dim, n_vectors, n_results
    );

    let n_chunks = std::cmp::max(1, n_vectors / 200000);
    eprintln!("Splitting into {} chunks", n_chunks);

    {
        let mut elements: angular::Vectors = granne::angular::Vectors::new();

        eprintln!("Inserting vectors into the collection - {:?}", t0.elapsed());
        let vectors: Vec<_> = (0..100)
            .into_par_iter()
            .map(|_| random_vector(n_dim))
            .collect();

        for v in vectors {
            elements.push(&v);
        }

        eprintln!("Building the index - {:?}", t0.elapsed());
        let builder = GranneBuilder::new(BuildConfig::default(), elements);

        std::fs::create_dir_all("data").unwrap();
        let mut index_file = std::fs::OpenOptions::new()
            .write(true)
            .open("data/index.dat")
            .unwrap();
        let mut elements_file = std::fs::OpenOptions::new()
            .write(true)
            .open("data/elements.dat")
            .unwrap();

        eprintln!("Writing index to file - {:?}", t0.elapsed());
        builder.write_index(&mut index_file).unwrap();

        eprintln!("Writing elemetns to file - {:?}", t0.elapsed());
        builder.write_elements(&mut elements_file).unwrap();
    }

    for i_chunk in 0..n_chunks {
        eprintln!("Chunk {}/{}", i_chunk, n_chunks);

        let elements_file = std::fs::File::open("data/elements.dat").unwrap();

        eprintln!("loading (memory-mapping) vectors - {:?}", t0.elapsed());
        let mut elements = unsafe { angular::Vectors::from_file(&elements_file).unwrap() };

        eprintln!("Generating vectors in parallel");
        let vectors: Vec<_> = (0..n_vectors / n_chunks)
            .into_par_iter()
            .map(|_| random_vector(n_dim))
            .collect();

        eprintln!("Inserting vectors into the collection - {:?}", t0.elapsed());
        for v in vectors {
            elements.push(&v);
        }

        let mut elements_file = std::fs::File::create("data/elements.dat").unwrap();
        let builder = GranneBuilder::new(BuildConfig::default(), elements);
        eprintln!("Writing elements to file - {:?}", t0.elapsed());
        builder.write_elements(&mut elements_file).unwrap();
    }
}
