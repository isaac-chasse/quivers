use std::{fs::File, io::{self, BufReader}};
use arrow::datatypes::DataType;
use crate::datatypes::{schema::{Schema, Field}, record_batch::RecordBatch};

pub struct CsvDataSource {
    pub filename: String,
    pub schema: Option<Schema>,
    pub has_headers: bool,
    pub batch_size: usize,
}

impl CsvDataSource {
    pub fn new(filename: String, schema: Option<Schema>, has_headers: bool, batch_size: usize) -> Self {
        CsvDataSource { filename, schema, has_headers, batch_size }
    }

    pub fn infer_schema(&self) -> Result<Schema, csv::Error> {
        // might want to specify error here if read fails
        let file = File::open(&self.filename)?; 
        let mut rdr = csv::Reader::from_reader(file);

        // if we have headers, read them
        let headers: Vec<String> = if self.has_headers {
            rdr.headers()?.iter().map(|h| h.to_string()).collect()
        } else {
            // if we don't have headers default to "field_n" where n is field index
            // e.g. [field_0, field_1, field_2, ..., field_n]
            let field_count = rdr.records().next().unwrap()?.len();
            (0..field_count).map(|n| format!("field_{}", n + 1)).collect()
        };

        let fields = headers
            .into_iter()
            .map(|name| Field::new(name, DataType::Utf8))
            .collect();

        Ok(Schema { fields })
    }

    pub fn schema(&self) -> Result<Schema, csv::Error> {
        match &self.schema {
            Some(schema) => Ok(schema.clone()),
            None => self.infer_schema(),
        }
    }

    pub fn scan(&self, projection: Vec<String>) -> Result<Box<dyn Iterator<Item = Result<RecordBatch, csv::Error>>>, csv::Error> {
        // might want to specify error here if read fails
        let file = File::open(&self.filename)?; 
        let rdr = BufReader::new(file);

        // Here we need to implement our logic for handling a projection, creating RecordBatches,
        // and managing batch sizes accorinding to the requirements defined by an instance of
        // [`CsvDataSource`].
        //
        // We are returning a dummy variable right now for temporary completeness.
        Ok(Box::new(std::iter::once(Err(csv::Error::from(io::Error::new(
            io::ErrorKind::Other,
            "Not implemented",
        ))))))
    } 
}

// make an iterator of RecordBatches 
fn reader_as_sequence() {} 

// make a reader an iterator 
fn reader_iterator() {}

