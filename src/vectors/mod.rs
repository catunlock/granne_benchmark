pub mod reader;
pub mod writer;
pub mod directory;
pub mod lock;
pub mod deleted;
pub mod deleted_db;

pub use reader::*;
pub use writer::*;
pub use lock::*;
pub use deleted::*;
pub use deleted_db::*;

const COMMIT_LOCK_PATH: &str = "COMMIT_LOCK";
const WRITER_LOCK_PATH: &str = "WRITER_LOCK";
const ELEMENTS_PATH: &str = "elements.dat";
const INDEX_PATH: &str = "index.dat";
const DIRTY_PATH: &str = "DIRTY_BIT";
const DELETED_PATH: &str = "deleted.dat";

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use granne::angular::Vector;
    use log::LevelFilter;
    use tempfile::TempDir;

    use crate::vectors::Writer;

    use super::Reader;

    fn init() {
        let _ = env_logger::builder()
            .filter_level(LevelFilter::Trace)
            .is_test(true)
            .try_init();
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

    #[test]
    fn reader_and_writer() {
        init();

        let tmpdir = TempDir::new().unwrap();
        let mut writer = Writer::open(tmpdir.path()).unwrap();

        writer.push(&create_vector(3, 1.0));
        writer.push(&create_vector(3, 2.0));
        writer.push(&create_vector(3, 3.0));

        writer.commit();

        let reader = Reader::open(tmpdir.path()).unwrap();
        let res = reader.search(&create_vector(3, 1.0));
        info!("Results: {:?}", res);

        writer.push(&create_vector(3, 4.0));
        writer.push(&create_vector(3, 5.0));
        writer.push(&create_vector(3, 6.0));

        writer.commit();

        let res = reader.search(&create_vector(3, 3.0));
        info!("Results: {:?}", res);

    }

    #[test]
    fn multithreaded_reader_and_writer() {
        init();
        let tmpdir = TempDir::new().unwrap();
        let tmp1 = tmpdir.path().to_path_buf().clone();
        let tmp2 = tmpdir.path().to_path_buf().clone();

        let t_writer = std::thread::spawn( || {
            let mut writer = Writer::open(tmp1).unwrap();
            for i in 0..500 {
                writer.push(&create_vector(3, i as f32));
                writer.commit();
            }
        });

        let t_reader = std::thread::spawn( || {
            std::thread::sleep(Duration::from_millis(100));
            let reader = Reader::open(tmp2).unwrap();
            for i in 0..500 {
                reader.search(&create_vector(3, 3.0));
            }
        });

        t_writer.join();
        t_reader.join();
    }
}