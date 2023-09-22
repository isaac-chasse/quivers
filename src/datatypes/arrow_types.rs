// Defining the type system based on Apache Arrow columnar memory model
use arrow::datatypes::DataType;

/// [`ArrowTypes`] is a type system for our `quivers` query engine.
pub struct ArrowTypes;

/// A collection of constants representing various `arrow` data types.
impl ArrowTypes {
    /// Represents an Arrow Null data type.
    // pub const NULL_TYPE: DataType = DataType::Null;

    /// Represents an Arrow Boolean data type. 
    pub const BOOLEAN_TYPE: DataType = DataType::Boolean;
    
    /// Represents an Arrow signed 8-bit integer data type. 
    // pub const INT8_TYPE: DataType = DataType::Int8;
    
    /// Represents an Arrow signed 16-bit integer data type. 
    // pub const INT16_TYPE: DataType = DataType::Int16;
    
    /// Represents an Arrow signed 32-bit integer data type. 
    // pub const INT32_TYPE: DataType = DataType::Int32;
    
    /// Represents an Arrow signed 64-bit integer data type. 
    pub const INT64_TYPE: DataType = DataType::Int64;
    
    /// Represents an Arrow unisgned 8-bit integer data type. 
    // pub const UINT8_TYPE: DataType = DataType::UInt8;
    
    /// Represents an Arrow unsigned 16-bit integer data type. 
    // pub const UINT16_TYPE: DataType = DataType::UInt16;
    
    /// Represents an Arrow unsigned 32-bit integer data type. 
    // pub const UINT32_TYPE: DataType = DataType::UInt32;
    
    /// Represents an Arrow unsigned 64-bit integer data type. 
    pub const UINT64_TYPE: DataType = DataType::UInt64;
    
    /// Represents an Arrow 32-bit floating point data type. 
    // pub const FLOAT_TYPE: DataType = DataType::Float32;
    
    /// Represents an Arrow 64-bit floating point data type. 
    pub const DOUBLE_TYPE: DataType = DataType::Float64;
    
    /// Represents an Arrow UTF-8 encoded string data type. 
    pub const STRING_TYPE: DataType = DataType::Utf8;
}
