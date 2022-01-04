pub mod deleted_db;
pub mod directory;
pub mod index_map;
pub mod lock;
pub mod reader;
pub mod vector_identifier;
pub mod writer;

pub use deleted_db::*;
pub use index_map::*;
pub use lock::*;
pub use reader::*;
pub use vector_identifier::*;
pub use writer::*;

const COMMIT_LOCK_PATH: &str = "COMMIT_LOCK";
const WRITER_LOCK_PATH: &str = "WRITER_LOCK";
const ELEMENTS_PATH: &str = "elements.dat";
const INDEX_PATH: &str = "index.dat";
const DIRTY_PATH: &str = "DIRTY_BIT";
const DELETED_PATH: &str = "deleted.dat";
const INDEX_MAP_PATH: &str = "index_map";

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use granne::angular::Vector;
    use log::LevelFilter;
    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::vectors::{VectorIdentifier, Writer};

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

        let doc_id1 = VectorIdentifier {
            doc_id: Uuid::from_u128(0),
            field: "body".to_string(),
            paragraph_id: Uuid::from_u128(0),
            start: 0,
            end: 100,
        };

        let doc_id2 = VectorIdentifier {
            doc_id: Uuid::from_u128(1),
            field: "body".to_string(),
            paragraph_id: Uuid::from_u128(1),
            start: 100,
            end: 200,
        };

        let doc_id3 = VectorIdentifier {
            doc_id: Uuid::from_u128(2),
            field: "body".to_string(),
            paragraph_id: Uuid::from_u128(0),
            start: 0,
            end: 100,
        };

        writer.push(&doc_id1, &create_vector(3, 1.0)).unwrap();
        writer.push(&doc_id1, &create_vector(3, 2.0)).unwrap();
        writer.push(&doc_id1, &create_vector(3, 3.0)).unwrap();

        writer.commit();

        writer.push(&doc_id2, &create_vector(3, 4.0)).unwrap();
        writer.push(&doc_id2, &create_vector(3, 5.0)).unwrap();

        writer.commit();

        writer.push(&doc_id3, &create_vector(3, 6.0)).unwrap();
        writer.push(&doc_id3, &create_vector(3, 7.0)).unwrap();

        writer.commit();
    }

    #[test]
    fn reader_and_writer() {
        init();

        let tmpdir = TempDir::new().unwrap();
        let mut writer = Writer::open(tmpdir.path()).unwrap();

        let doc_id1 = VectorIdentifier {
            doc_id: Uuid::from_u128(0),
            field: "body".to_string(),
            paragraph_id: Uuid::from_u128(0),
            start: 0,
            end: 100,
        };

        writer.push(&doc_id1, &create_vector(3, 1.0)).unwrap();
        writer.push(&doc_id1, &create_vector(3, 2.0)).unwrap();
        writer.push(&doc_id1, &create_vector(3, 3.0)).unwrap();

        writer.commit();

        let reader = Reader::open(tmpdir.path()).unwrap();
        let res = reader.search(&create_vector(3, 1.0));

        let doc_ids: Vec<_> = res.iter().map(|(doc_id, _score)| doc_id.clone()).collect();
        assert_eq!(
            doc_ids,
            vec![doc_id1.clone(), doc_id1.clone(), doc_id1.clone()]
        );

        info!("Results: {:?}", res);

        let doc_id2 = VectorIdentifier {
            doc_id: Uuid::from_u128(1),
            field: "body".to_string(),
            paragraph_id: Uuid::from_u128(1),
            start: 100,
            end: 200,
        };

        writer.push(&doc_id2, &create_vector(3, 4.0)).unwrap();
        writer.push(&doc_id2, &create_vector(3, 5.0)).unwrap();
        writer.push(&doc_id2, &create_vector(3, 6.0)).unwrap();

        writer.commit();

        let res = reader.search(&create_vector(3, 3.0));

        let doc_ids: Vec<_> = res.iter().map(|(doc_id, _score)| doc_id.clone()).collect();
        assert_eq!(
            doc_ids,
            vec![
                doc_id1.clone(),
                doc_id1.clone(),
                doc_id1.clone(),
                doc_id2.clone(),
                doc_id2.clone(),
                doc_id2.clone()
            ]
        );

        info!("Results: {:?}", res);
    }

    #[test]
    fn multithreaded_reader_and_writer() {
        init();
        let tmpdir = TempDir::new().unwrap();
        let tmp1 = tmpdir.path().to_path_buf();
        let tmp2 = tmpdir.path().to_path_buf();

        let t_writer = std::thread::spawn(|| {
            let doc_id1 = VectorIdentifier {
                doc_id: Uuid::from_u128(0),
                field: "body".to_string(),
                paragraph_id: Uuid::from_u128(0),
                start: 0,
                end: 100,
            };

            let mut writer = Writer::open(tmp1).unwrap();
            for i in 0..500 {
                writer.push(&doc_id1, &create_vector(3, i as f32)).unwrap();
                writer.commit();
            }
        });

        let t_reader = std::thread::spawn(|| {
            std::thread::sleep(Duration::from_millis(100));
            let reader = Reader::open(tmp2).unwrap();
            for _ in 0..500 {
                reader.search(&create_vector(3, 3.0));
            }
        });

        t_writer.join().unwrap();
        t_reader.join().unwrap();
    }

    #[test]
    fn search() {
        init();

        let tmpdir = TempDir::new().unwrap();
        let mut writer = Writer::open(tmpdir.path()).unwrap();

        for i in 1..100 {
            let doc_id = VectorIdentifier {
                doc_id: Uuid::from_u128(0),
                field: "body".to_string(),
                paragraph_id: Uuid::from_u128(0),
                start: i,
                end: i + 30,
            };
            writer.push(&doc_id, &create_vector(700, i as f32)).unwrap();
        }
        writer.commit();

        let batch: Vec<_> = (100..10000)
            .into_iter()
            .map(|i| {
                let vi = VectorIdentifier {
                    doc_id: Uuid::from_u128(i as u128),
                    field: "body".to_string(),
                    paragraph_id: Uuid::from_u128(0),
                    start: 0,
                    end: 0,
                };

                let vector = create_vector(700, i as f32).into_vec();

                (vi, vector)
            })
            .collect();

        writer.push_batch(&batch).unwrap();
        writer.commit();

        let reader = Reader::open(tmpdir.path()).unwrap();
        let res = reader.search(&create_vector(3, 700.0));
        println!("Res: {:?}", res);
    }
}
