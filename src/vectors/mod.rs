
pub mod reader;
pub mod writer;
pub mod directory;
pub mod lock;

pub use reader::*;
pub use writer::*;
pub use lock::*;

const COMMIT_LOCK_PATH: &str = "LOCK";
const WRITER_LOCK_PATH: &str = "WRITER";
const ELEMENTS_PATH: &str = "elements.dat";
const INDEX_PATH: &str = "index.dat";
const DIRTY_PATH: &str = "DIRTY";
#[cfg(test)]
mod tests {
    use granne::angular::Vector;
    use log::LevelFilter;
    use tempfile::TempDir;
    use rand::prelude::*;

    use crate::vectors::Writer;

    fn init() {
        let _ = env_logger::builder()
            .filter_level(LevelFilter::Trace)
            .is_test(true)
            .try_init();
    }

    fn random_vector(n_dim: usize) -> Vector<'static> {
        let mut rng = rand::thread_rng();
        Vector((0..n_dim).map(|_| rng.gen()).collect())
    }

    fn create_vector(n_dim: usize, u: f32) -> Vector<'static> {
        Vector((0..n_dim).map(|_| u).collect())
    }

    #[test]
    fn only_one_writer() {
        init();

        let tmpdir = TempDir::new().unwrap();
        let w1 = Writer::open(tmpdir.path());
        let w2 = Writer::open(tmpdir.path());

        assert!(w1.is_ok());
        assert!(w2.is_err());
    }

    #[test]
    fn push() {
        init();

        let tmpdir = TempDir::new().unwrap();
        let mut writer = Writer::open(tmpdir.path()).unwrap();

        writer.push(&create_vector(3, 1.0));
        writer.push(&create_vector(3, 2.0));
        writer.push(&create_vector(3, 3.0));

        writer.commit();

        writer.push(&create_vector(3, 4.0));
        writer.push(&create_vector(3, 5.0));

        writer.commit();

        writer.push(&create_vector(3, 6.0));
        writer.push(&create_vector(3, 7.0));

        writer.commit();
    }
}