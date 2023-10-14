use crate::datatypes::column_vector::ColumnVector;
use arrow::datatypes::DataType;
use std::any::Any;
use dyn_clone::clone_trait_object;

// Generates code to implement the standard libs Clone for Box<dyn ColumnVector>
clone_trait_object!(ColumnVector);

pub struct LiteralValueVector {
    arrow_type: DataType,
    value: Option<Box<dyn Any>>,
    size: usize,
}

impl LiteralValueVector {
    /// Constructor for [`LiteralValueVector`].
    pub fn new(arrow_type: DataType, value: Option<Box<dyn Any>>, size: usize) -> Self {
        LiteralValueVector {
            arrow_type,
            value,
            size,
        }
    }
}

impl ColumnVector for LiteralValueVector {
    fn get_type(&self) -> DataType {
        self.arrow_type.clone()
    }

    fn get_value(&self, i: usize) -> Option<Box<dyn Any>> {
        if i >= self.size {
            panic!("Index out of bounds!");
        }
        // Cloning is tricky here due to Box<dyn Any>. 
        // This is an inncredibly hacky solution. I don't like this. 
        // Like want to create some kind of macro or something that can auto generate this kind of code for a [`ColumnVector::value`]
        self.value.as_ref().map(|v|
            match &self.arrow_type {
                DataType::Boolean => {
                    let val: &bool = v.downcast_ref().expect("Invalid value type, expected bool");
                    Box::new(*val) as Box<dyn Any>
                }
                DataType::Int64 => {
                    let val: &i64 = v.downcast_ref().expect("Invalid value type, expected i64");
                    Box::new(*val) as Box<dyn Any>
                }
                DataType::UInt64 => {
                    let val: &u64 = v.downcast_ref().expect("Invalid value type, expected u64");
                    Box::new(*val) as Box<dyn Any>
                }
                DataType::Float64 => {
                    let val: &f64 = v.downcast_ref().expect("Invalid value type, expected f64");
                    Box::new(*val) as Box<dyn Any>
                }
                DataType::Utf8 => {
                    let val: &String = v.downcast_ref().expect("Invalid value type, expected String");
                    Box::new(val.clone()) as Box<dyn Any>
                }
                _ => panic!("Unsupported Arrow data type found!")
            }
        )
    }

    fn len(&self) -> usize {
        self.size
    }
}

impl Clone for LiteralValueVector {
    fn clone(&self) -> Self {
        Self {
            arrow_type: self.arrow_type.clone(),
            value: self.value.as_deref().map(|v| {
                match &self.arrow_type {
                    DataType::Boolean => {
                        let val: &bool = v.downcast_ref().expect("Invalid value type");
                        Box::new(*val) as Box<dyn Any>
                    }
                    DataType::Int64 => {
                        let val: &i64 = v.downcast_ref().expect("Invalid value type");
                        Box::new(*val)
                    }
                    DataType::UInt64 => {
                        let val: &u64 = v.downcast_ref().expect("Invalid value type");
                        Box::new(*val) as Box<dyn Any>
                    }
                    DataType::Float64 => {
                        let val: &f64 = v.downcast_ref().expect("Invalid value type");
                        Box::new(*val) as Box<dyn Any>
                    }
                    // similar branches for other data types...
                    DataType::Utf8 => {
                        let val: &String = v.downcast_ref().expect("Invalid value type");
                        Box::new(val.clone()) as Box<dyn Any>
                    }
                    _ => panic!("Unsupported Arrow data type!"),
                }
            }),
            size: self.size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_type() {
        let vector = LiteralValueVector::new(DataType::Int64, Some(Box::new(42i64) as Box<dyn Any>), 1);
        assert_eq!(vector.get_type(), DataType::Int64);
    }

    #[test]
    fn test_get_value_at_valid_index() {
        let value = 42i64;
        let vector = LiteralValueVector::new(DataType::Int64, Some(Box::new(value) as Box<dyn Any>), 1);
        
        let result = vector.get_value(0);
        assert!(result.is_some());
        
        let boxed_value = result.unwrap();
        let retrieved_value = *boxed_value.downcast_ref::<i64>().unwrap();
        assert_eq!(retrieved_value, value);
    }

    #[test]
    #[should_panic]
    fn test_get_value_at_invalid_index() {
        let vector = LiteralValueVector::new(DataType::Int64, Some(Box::new(42i64) as Box<dyn Any>), 1);
        
        // this *should* panic since we are grabbing a value from idx[1] when max(idx) = 0
        let _result = vector.get_value(1);
    }
}
