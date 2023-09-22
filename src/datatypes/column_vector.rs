// Interface as an abstraction over each datatype
use arrow::datatypes::DataType;
use dyn_clone::DynClone;

/// [`ColumnVector`] serves as an abstraction over different implementations of a column vector.
pub trait ColumnVector: DynClone {
    fn get_type(&self) -> DataType;
    fn get_value(&self, i: usize) -> Option<Box<dyn std::any::Any>>;
    fn len(&self) -> usize;
}
