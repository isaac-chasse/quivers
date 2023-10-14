// A [`RecordBatch`] represents a batch of columnar data.
use crate::datatypes::{column_vector::ColumnVector, schema::Schema};

#[derive(Clone)]
pub struct RecordBatch {
    pub schema: Schema,
    pub fields: Vec<Box<dyn ColumnVector>>,
}

impl RecordBatch {
    /// Constructor for [`RecordBatch`].
    pub fn new(schema: Schema, fields: Vec<Box<dyn ColumnVector>>) -> Self {
        RecordBatch { schema, fields }
    }

    /// Returns the row count for a [`RecordBatch`]
    pub fn row_count(&self) -> usize {
        // Since we unwrap_or(0) here we can possibly experience cases where there is no first value
        // within [`RecordBatch::fields`]. We need to handle this case and possibly create a
        // `EmptyFieldError`. Currently we just throw a 0 row count.
        self.fields.first().map(|column| column.len()).unwrap_or(0)
    }

    /// Returns the column count for a [`RecordBatch`]
    pub fn column_count(&self) -> usize {
        self.fields.len()
    }

    /// Access one columnn by index within a [`RecordBatch`]
    pub fn field(&self, i: usize) -> &Box<dyn ColumnVector> {
        &self.fields[i]
    }
}
