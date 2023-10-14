use arrow::{
    array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array},
    datatypes::DataType,
};

pub struct ArrowFieldVector {
    pub field: ArrayRef,
}

impl ArrowFieldVector {
    pub fn new(field: ArrayRef) -> Self {
        ArrowFieldVector { field }
    }

    pub fn get_type(&self) -> DataType {
        self.field.data_type().clone()
    }

    pub fn get_value(&self, i: usize) -> Option<ArrowPrimitive> {
        if self.field.is_null(i) {
            return None;
        }

        Some(match self.field.data_type() {
            DataType::Boolean => ArrowPrimitive::Bool(
                self.field
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .value(i),
            ),
            DataType::Int64 => ArrowPrimitive::I64(
                self.field
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(i),
            ),
            DataType::UInt64 => ArrowPrimitive::U64(
                self.field
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(i),
            ),
            DataType::Float64 => ArrowPrimitive::F64(
                self.field
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(i),
            ),
            DataType::Utf8 => ArrowPrimitive::String(
                self.field
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(i)
                    .to_string(),
            ),
            _ => unimplemented!("Found an unimplemented Arrow primitive type!"),
        })
    }

    pub fn len(&self) -> usize {
        self.field.len()
    }
}

pub enum ArrowPrimitive {
    // Access to the `bool` primitive type
    Bool(bool),
    // Access to the `i64` primitive type
    I64(i64),
    // Access to the `u64` primitive type
    U64(u64),
    // Access to the `f64` primitive type
    F64(f64),
    // Access to the `String` primitive type
    String(String),
}

// This code is going to be used later for when we are performing ops on
// Arrow fields (e.g. String ops, math ops on ints/float, etc.)
// struct FieldVectorFactory;
