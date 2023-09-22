use crate::datatypes::{schema::Schema, record_batch::RecordBatch};

/// Defines a [`CsvDataSource`] built on the [`DataSource`] trait. Infers a
/// [`Schema`] if one is not provided during read.
pub mod csv_data_source;

/// Defines an [`InMemoryDataSource`] built on the [`DataSource`] trait. The simplest method for
/// working with a [`DataSource`] that doesn't need to define any I/O. 
pub mod in_memory_data_source;

/// Establishes a trait that defines a [`DataSource`].
pub trait DataSource {
    /// Returns the [`Schema`] for the underlying [`DataSource`].
    fn schema(&self) -> Schema;

    /// Scans the [`DataSource`] and selects the specified columns from a [`RecordBatch`].
    fn scan(&self, projection: Vec<String>) -> Box<dyn Iterator<Item = RecordBatch>>;
}

