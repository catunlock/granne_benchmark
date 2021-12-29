use nuclia_vectors::vectors::{Writer, Reader};
use serde::{Serialize, Deserialize};
use tempfile::TempDir;

#[derive(Debug, Serialize, Deserialize)]
pub struct Embedding {
    num: usize,
    text: String,
    start: usize,
    end: usize,
    encoding: Vec<f32>
}

fn main() {
    let file = std::fs::read_to_string("salida.json").unwrap();
    let vecs: Vec<Embedding> = serde_json::from_str(&file).unwrap();

    let tmpdir = TempDir::new().unwrap();
    let mut writer = Writer::open(tmpdir.path()).unwrap();

    for (i, v) in vecs.iter().enumerate() {
        println!("{}", v.text);
        writer.push_vec(i, v.encoding.clone()).unwrap();
    }
    writer.commit();
    println!("==============================");


    let reader = Reader::open(tmpdir.path()).unwrap();
    let res = reader.search_vec(vecs[0].encoding.clone());

    for (doc_id, score) in res {
        let doc = &vecs[doc_id].text;
        println!("{} - {}", doc, score);
    }
}