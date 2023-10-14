use std::collections::HashSet;

use arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};

#[derive(Debug, Clone)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    pub fn from_arrow(arrow_schema: &ArrowSchema) -> Self {
        let fields = arrow_schema
            .fields()
            .iter()
            .map(|f| Field {
                name: f.name().to_string(),
                data_type: f.data_type().clone(),
            })
            .collect();
        Schema { fields }
    }

    pub fn to_arrow(&self) -> ArrowSchema {
        let arrow_fields: Vec<ArrowField> = self
            .fields
            .iter()
            .map(|f| ArrowField::new(f.name.as_str(), f.data_type.clone(), true))
            .collect();
        ArrowSchema::new(arrow_fields)
    }

    pub fn project(&self, indicies: &[usize]) -> Schema {
        let fields = indicies.iter().map(|&i| self.fields[i].clone()).collect();
        Schema { fields }
    }

    pub fn select(&self, names: &[String]) -> Result<Schema, &'static str> {
        let mut fields = Vec::new();
        let available_names: HashSet<_> = self.fields.iter().map(|f| &f.name).collect();

        for name in names {
            if available_names.contains(name) {
                fields.push(
                    self.fields
                        .iter()
                        .find(|f| &f.name == name)
                        .unwrap()
                        .clone(),
                );
            } else {
                return Err("Field not found in schema");
            }
        }

        Ok(Schema { fields })
    }
}

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
}

impl Field {
    pub fn new(name: String, data_type: DataType) -> Self {
        Field { name, data_type }
    }

    pub fn to_arrow(&self) -> ArrowField {
        ArrowField::new(self.name.as_str(), self.data_type.clone(), true)
    }
}
