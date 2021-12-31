use futures::{future::join_all};
use granne::{
    angular::{self, Vector},
    BuildConfig, Builder, Granne, GranneBuilder,
};
use rand::prelude::*;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tokio::time::Instant;

fn random_vector(n_dim: usize) -> Vector<'static> {
    let mut rng = rand::thread_rng();
    Vector((0..n_dim).map(|_| rng.gen()).collect())
}

#[tokio::main]
async fn main() {
    let n_vectors = 1000;
    let _n_chunks = 4;
    let n_dim = 800;

    let t1 = tokio::spawn(async move {
        //Writer::open("data");

        let _index_file = std::fs::File::open("data/index.dat").unwrap();
        let elements_file = std::fs::File::open("data/elements.dat").unwrap();

        let mut elements = unsafe { angular::Vectors::from_file(&elements_file).unwrap() };

        let vectors: Vec<_> = (0..n_vectors)
            .into_par_iter()
            .map(|_| random_vector(n_dim))
            .collect();

        eprintln!("Inserting vectors into the collection");
        for v in vectors {
            elements.push(&v);
        }

        eprintln!("Building index");
        let mut builder = GranneBuilder::new(BuildConfig::default(), elements);
        builder.build()
    });

    let mut tasks: Vec<_> = (1..1000)
        .map(|_| {
            tokio::spawn(async move {
                let t0 = Instant::now();
                let index_file = std::fs::File::open("data/index.dat").unwrap();
                eprintln!("Opened index in {:?}", t0.elapsed());

                let t0 = Instant::now();
                let elements_file = std::fs::File::open("data/elements.dat").unwrap();
                eprintln!("Opened elements in {:?}", t0.elapsed());

                // Check if I can open several elements from the same file.
                let elements = unsafe { angular::Vectors::from_file(&elements_file).unwrap() };
                let index = unsafe { Granne::from_file(&index_file, elements).unwrap() };

                // max_search controls how extensive the search is
                eprintln!("Querying a random vector - {:?}", t0.elapsed());
                let query_vector = random_vector(n_dim);
                let max_search = 200;
                let res = index.search(&query_vector, max_search, 200);

                eprintln!("Found {} - {:?}", res.len(), t0.elapsed());
            })
        })
        .collect();

    tasks.push(t1);

    join_all(tasks).await;
}
