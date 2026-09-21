//! Owned snapshots from the external legacy parser, for explicit parity tests.
use crate::{cif::CategoryBlock, error, reference_process};
use datafusion::common::Result;
use std::collections::HashMap;

struct Block {
    name: String,
    columns: HashMap<String, Vec<Option<String>>>,
}
pub struct Document(Vec<Block>);
impl Document {
    pub fn parse(data: &[u8]) -> Result<Self> {
        let raw = reference_process::query("cif", data, 0)?;
        let mut blocks = Vec::new();
        for block in raw["blocks"].as_array().unwrap() {
            let mut columns = HashMap::new();
            for column in block["columns"].as_array().unwrap() {
                let values = column["values_hex"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| {
                        (!v.is_null())
                            .then(|| reference_process::text(v))
                            .transpose()
                    })
                    .collect::<Result<Vec<_>>>()?;
                columns.insert(reference_process::text(&column["name_hex"])?, values);
            }
            blocks.push(Block {
                name: reference_process::text(&block["name_hex"])?,
                columns,
            });
        }
        Ok(Self(blocks))
    }
    pub fn block_count(&self) -> usize {
        self.0.len()
    }
    pub fn block(&self, index: usize) -> Result<CategoryBlock<'_>> {
        let block = self
            .0
            .get(index)
            .ok_or_else(|| error("reference block index"))?;
        Ok(CategoryBlock {
            name: &block.name,
            columns: block
                .columns
                .iter()
                .map(|(name, values)| {
                    (name.as_str(), values.iter().map(|v| v.as_deref()).collect())
                })
                .collect(),
        })
    }
}
