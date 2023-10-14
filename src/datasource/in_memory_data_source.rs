use crate::datatypes::{schema::Schema, record_batch::RecordBatch};

use super::DataSource;

pub struct InMemoryDataSource {
    pub schema: Schema,
    pub data: Vec<RecordBatch>,
}

impl InMemoryDataSource {
    pub fn new(schema: Schema, data: Vec<RecordBatch>) -> Self {
        InMemoryDataSource { schema, data }
    }
}

impl DataSource for InMemoryDataSource {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn scan(&self, projection: Vec<String>) -> Box<dyn Iterator<Item = RecordBatch>> {
        let schema = self.schema.clone();
        let projection_indicies = projection
            .iter()
            .map(|name| schema.fields.iter().position(|f| f.name == *name).unwrap())
            .collect::<Vec<_>>();

        let data = self.data.clone(); 
        let iter = data.into_iter().map(move |batch| {
            let columns = projection_indicies.iter().map(|&i| batch.field(i).clone()).collect();
            RecordBatch::new(schema.clone(), columns)
        });

        Box::new(iter)
    }
}
