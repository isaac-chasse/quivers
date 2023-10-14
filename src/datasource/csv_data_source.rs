use std::fs::File;
use arrow::datatypes::DataType;
use crate::datatypes::{schema::{Schema, Field}, record_batch::RecordBatch};

use super::DataSource;

pub struct CsvDataSource {
    pub filename: String,
    pub schema: Option<Schema>,
    pub has_headers: bool,
    pub batch_size: usize,
}

impl DataSource for CsvDataSource {
    fn schema(&self) -> Schema {
        match &self.schema {
            Some(existing_schema) => existing_schema.clone(),
            None => {
                let inferred_schema = self.infer_schema().expect("Failed to infer schema from CsvDataSource");
                // NOTE: Could store the inferred schema to self.schema if it makes sense later
                inferred_schema
            }
        }
    }

    fn scan(&self, projection: Vec<String>) -> Box<dyn Iterator<Item = RecordBatch>> {
        todo!()        
    }
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
}

// make an iterator of RecordBatches 
fn reader_as_sequence() {} 

// make a reader an iterator 
fn reader_iterator() {}

