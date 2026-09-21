//! Explicit FCZ byte/bit decoding. No ABI struct casts or unchecked pointers.
//! Packing adapted from Foldcomp (MIT); see LICENSE-FOLDCOMP.
use super::{discretize::Discretizer, residue::Residue};
use datafusion::common::Result;
use datafusion_bio_format_structure::error;

pub(super) struct Reader<'a> {
    data: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, position: 0 }
    }

    pub fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| error("FCZ section length overflow"))?;
        let bytes = self
            .data
            .get(self.position..end)
            .ok_or_else(|| error(format!("truncated FCZ at byte {}", self.position)))?;
        self.position = end;
        Ok(bytes)
    }

    pub fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.take(N)?
            .try_into()
            .map_err(|_| error("invalid FCZ field width"))
    }

    pub fn byte(&mut self) -> Result<u8> {
        Ok(self.array::<1>()?[0])
    }
    pub fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.array()?))
    }
    pub fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }
    pub fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_le_bytes(self.array()?))
    }

    pub fn finite_float(&mut self, field: &str) -> Result<f32> {
        let value = f32::from_le_bytes(self.array()?);
        if !value.is_finite() {
            return Err(error(format!(
                "non-finite FCZ {field} at byte {}",
                self.position - 4
            )));
        }
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Backbone {
    pub residue: Residue,
    /// phi, psi, omega, N-CA-C, CA-C-N, C-N-CA.
    pub angles: [u16; 6],
}

impl Backbone {
    pub fn read(reader: &mut Reader<'_>) -> Result<Self> {
        let b = reader.array::<8>()?;
        Ok(Self {
            residue: Residue::from_code(b[0] >> 3)?,
            angles: [
                (u16::from(b[3] & 15) << 8) | u16::from(b[4]),
                (u16::from(b[2]) << 4) | u16::from(b[3] >> 4),
                (u16::from(b[0] & 7) << 8) | u16::from(b[1]),
                u16::from(b[7]),
                u16::from(b[5]),
                u16::from(b[6]),
            ],
        })
    }

    pub fn parameters(self, discretizers: &[Discretizer; 6]) -> [f32; 6] {
        std::array::from_fn(|index| discretizers[index].restore(self.angles[index]))
    }
}
