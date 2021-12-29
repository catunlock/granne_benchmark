/*!
This example shows how to read glove vectors and build a Granne index.
# Example
```
cargo run --release --example glove glove.6B.50d.txt
```
# Input data
Input data needs to be downloaded and unzipped from:
https://nlp.stanford.edu/data/wordvecs/glove.6B.zip
This data is made available under the Public Domain Dedication and License v1.0 whose
full text can be found at: http://www.opendatacommons.org/licenses/pddl/1.0/
*/

use granne::{self, Builder, ElementContainer};
use nuclia_vectors::vectors::{Writer, Reader};
use rand::distributions::uniform::SampleBorrow;
use tempfile::TempDir;
use std::fs::File;
use std::io::{BufRead, BufReader};

fn parse_line(line: &str) -> std::io::Result<(String, granne::angular::Vector<'static>)> {
    let mut line_iter = line.split_whitespace();
    let token = line_iter.next().ok_or(std::io::ErrorKind::InvalidData)?;
    let vec: granne::angular::Vector = line_iter.map(|d| d.parse::<f32>().unwrap()).collect();

    Ok((token.to_string(), vec))
}

fn main() -> std::io::Result<()> {
    let file = BufReader::new(File::open("glove.6B.50d.txt")?);

    // reading the input data
    let mut tokens = Vec::new();
    let mut vectors = Vec::new();
    let tmpdir = TempDir::new().unwrap();
    let mut writer = Writer::open(tmpdir.path()).unwrap();

    for line in file.lines() {
        let (token, vector) = parse_line(&line?)?;

        tokens.push(token);
        vectors.push(vector);
        
    }
    let idxs: Vec<_> = (0..vectors.len()).collect();


    writer.push_batch(&idxs[0..10000], &vectors[0..10000]).unwrap();
    writer.commit();

    let reader = Reader::open(tmpdir.path()).unwrap();
    for &i in &[0, 134, 5555, 9999] {
        let res = reader.search(&vectors[i]);
        let res: Vec<_> = res.into_iter().map(|(j, d)| (&tokens[j], d)).collect();

        println!("\nThe closest words to \"{}\" are: \n{:?}", &tokens[i], res);
    }

    Ok(())
}