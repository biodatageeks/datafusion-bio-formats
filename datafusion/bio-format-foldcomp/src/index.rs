//! Pure parsing and checked ranges for Foldcomp index rows.
use datafusion::common::Result;
use datafusion_bio_format_structure::error;

pub(crate) fn integer(s: &str) -> Result<u64> {
    s.parse()
        .map_err(|_| error(format!("invalid unsigned integer {s:?}")))
}

pub(crate) struct IndexRow {
    pub key: u64,
    pub offset: u64,
    pub len: u64,
}
impl IndexRow {
    pub fn parse(line: &str, previous: Option<u64>) -> Result<Self> {
        let mut fields = line.split_whitespace();
        let (Some(key), Some(offset), Some(len), None) =
            (fields.next(), fields.next(), fields.next(), fields.next())
        else {
            return Err(error("index requires key, offset, length"));
        };
        let row = Self {
            key: integer(key)?,
            offset: integer(offset)?,
            len: integer(len)?,
        };
        if previous.is_some_and(|p| p >= row.key) {
            return Err(error("Foldcomp index keys must be unique and increasing"));
        }
        Ok(row)
    }

    pub fn validate_selected(&self, file_len: u64, max_input_bytes: u64) -> Result<()> {
        if self.len < 2
            || self.len > max_input_bytes
            || self
                .offset
                .checked_add(self.len)
                .is_none_or(|end| end > file_len)
        {
            return Err(error(format!(
                "invalid selected payload range for key {}",
                self.key
            )));
        }
        Ok(())
    }
}
