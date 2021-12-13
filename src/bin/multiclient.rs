use granne::{GranneBuilder, BuildConfig, angular, Builder};

#[tokio::main]
async fn main() {
    let index_file = std::fs::File::open("data/index.dat").unwrap();
    let elements_file = std::fs::File::open("data/elements.dat").unwrap();

    let elements = unsafe { angular::Vectors::from_file(&elements_file).unwrap() };

    let mut builder = GranneBuilder::new(BuildConfig::default(), elements);

    let t1 = tokio::spawn(async move {
        builder.build()
    });

    t1.await.unwrap();
}