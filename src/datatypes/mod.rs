/// Provides an [`ArrowTypes`] struct containg constants that can be referenced for the supported
/// Arrow data types.
pub mod arrow_types;

/// Provides a [`ColumnVector`] trait which serves as an abstracted interface to provide more
/// conveniennt accessor methods. This avoids the need for a case-by-case [`FieldVector`]
/// implementation for each data type defined by [`ArrowTypes`].
pub mod column_vector;

/// [`LiteralValueVector`] abstraction makes it possible to have an implementation for scalar
/// values, avoiding the need to create and populate a [`FieldVector`] with a literal value
/// repeated for every index in the column.
pub mod literal_value_vector;

pub mod record_batch;

pub mod schema;
