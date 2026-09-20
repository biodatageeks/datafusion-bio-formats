//! Checked FCZ legacy layout. See the migration BASELINE.md for field offsets.
use super::{
    bitstream::{Backbone, Reader},
    discretize::Discretizer,
};
use datafusion::common::Result;
use datafusion_bio_format_structure::error;

#[derive(Debug)]
pub(super) struct Header {
    pub residue_count: u16,
    pub atom_count: u16,
    pub first_residue: u16,
    pub first_atom: u16,
    pub anchor_count: u8,
    pub chain: u8,
    pub sidechain_count: u32,
    pub first_letter: u8,
    pub last_letter: u8,
    pub title_length: u32,
    pub discretizers: [Discretizer; 6],
}

impl Header {
    fn read(reader: &mut Reader<'_>) -> Result<Self> {
        let residue_count = reader.u16()?;
        let atom_count = reader.u16()?;
        let first_residue = reader.u16()?;
        let first_atom = reader.u16()?;
        let anchor_count = reader.byte()?;
        let chain = reader.byte()?;
        reader.take(2)?; // C ABI padding, not a format version.
        let sidechain_count = reader.u32()?;
        let first_letter = reader.byte()?;
        let last_letter = reader.byte()?;
        reader.take(2)?;
        let title_length = reader.u32()?;
        let mut discretizers = [Discretizer {
            minimum: 0.0,
            factor: 0.0,
        }; 6];
        for item in &mut discretizers {
            item.minimum = reader.finite_float("minimum")?;
        }
        for item in &mut discretizers {
            item.factor = reader.finite_float("continuation factor")?;
        }
        Ok(Self {
            residue_count,
            atom_count,
            first_residue,
            first_atom,
            anchor_count,
            chain,
            sidechain_count,
            first_letter,
            last_letter,
            title_length,
            discretizers,
        })
    }
}

#[derive(Debug)]
pub(super) struct Anchor {
    pub residue: usize,
    pub coordinates: [[f32; 3]; 3],
}

/// Constructible only after bounds/section/count validation. Borrowed payloads
/// refer to the caller's input; reconstructed coordinates are not allocated here.
pub(super) struct EncodedEntry<'a> {
    header: Header,
    anchors: Vec<Anchor>,
    title: &'a [u8],
    chain: &'a [u8],
    backbone: Vec<Backbone>,
    sidechain: &'a [u8],
    has_oxt: bool,
    oxt: [f32; 3],
    bfactor_discretizer: Discretizer,
    bfactors: &'a [u8],
    reconstructed_atoms: usize,
}

fn require(condition: bool, message: &str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(error(message))
    }
}

fn size(value: u32) -> Result<usize> {
    usize::try_from(value).map_err(|_| error("FCZ size does not fit address space"))
}

fn total(parts: &[usize]) -> Result<usize> {
    parts.iter().try_fold(0usize, |sum, &part| {
        sum.checked_add(part)
            .ok_or_else(|| error("FCZ section length overflow"))
    })
}

fn legacy_string(bytes: &[u8]) -> Result<&str> {
    let end = bytes
        .iter()
        .position(|&byte| byte == 0)
        .unwrap_or(bytes.len());
    std::str::from_utf8(&bytes[..end]).map_err(|e| error(format!("invalid FCZ UTF-8: {e}")))
}

impl<'a> EncodedEntry<'a> {
    pub fn parse(data: &'a [u8], max_atoms: usize) -> Result<Self> {
        require(
            data.len() >= 76 && data.get(..4) == Some(b"FCMP"),
            "invalid FCZ magic/header",
        )?;
        let mut reader = Reader::new(data);
        reader.take(4)?;
        let header = Header::read(&mut reader)?;
        let residues = usize::from(header.residue_count);
        let anchors_count = usize::from(header.anchor_count);
        require(
            residues >= 2 && usize::from(header.atom_count) >= 3 * residues,
            "unsupported FCZ residue/atom count",
        )?;
        require(
            usize::from(header.atom_count) <= max_atoms,
            "FCZ header atom count exceeds max_atoms",
        )?;
        require(
            anchors_count >= 2 && anchors_count <= residues,
            "invalid FCZ anchor count",
        )?;
        // Multiplications are bounded by u8/u16 header fields. u32 lengths are
        // converted and summed with checks before any count-dependent allocation.
        let expected = total(&[
            76,
            4 * anchors_count,
            size(header.title_length)?,
            36 * anchors_count,
            13,
            9 * residues,
            size(header.sidechain_count)?,
            8,
        ])?;
        require(expected == data.len(), "FCZ length does not match header")?;
        let mut anchors = Vec::with_capacity(anchors_count);
        let mut previous = None;
        for index in 0..anchors_count {
            let residue =
                usize::try_from(reader.i32()?).map_err(|_| error("negative FCZ anchor index"))?;
            require(
                residue < residues && previous.is_none_or(|last| residue > last),
                "invalid FCZ anchor indices",
            )?;
            require(index != 0 || residue == 0, "FCZ first anchor must be zero")?;
            require(
                index + 1 != anchors_count || residue == residues - 1,
                "FCZ last anchor must be final residue",
            )?;
            previous = Some(residue);
            anchors.push(Anchor {
                residue,
                coordinates: [[0.0; 3]; 3],
            });
        }
        let title = reader.take(size(header.title_length)?)?;
        for anchor in &mut anchors {
            for point in &mut anchor.coordinates {
                for value in point {
                    *value = reader.finite_float("anchor coordinate")?;
                }
            }
        }
        let flag = reader.byte()?;
        require(flag <= 1, "invalid FCZ OXT flag")?;
        let has_oxt = flag == 1;
        let mut oxt = [0.0; 3];
        for value in &mut oxt {
            *value = reader.finite_float("OXT coordinate")?;
        }
        let mut backbone = Vec::with_capacity(residues);
        let mut sidechains = 0;
        let mut reconstructed_atoms = usize::from(has_oxt);
        for index in 0..residues {
            let record = Backbone::read(&mut reader)?;
            if index == 0 {
                require(
                    record.residue.letter() == header.first_letter,
                    "inconsistent FCZ first residue",
                )?;
            }
            if index + 1 == residues {
                require(
                    record.residue.letter() == header.last_letter,
                    "inconsistent FCZ last residue",
                )?;
            }
            sidechains += record.residue.sidechain_count();
            reconstructed_atoms += record.residue.atom_count();
            backbone.push(record);
        }
        require(
            sidechains == size(header.sidechain_count)?,
            "invalid FCZ sidechain count",
        )?;
        require(
            reconstructed_atoms <= max_atoms,
            "FCZ reconstructed atom count exceeds max_atoms",
        )?;
        let sidechain = reader.take(sidechains)?;
        let bfactor_discretizer = Discretizer {
            minimum: reader.finite_float("B-factor minimum")?,
            factor: reader.finite_float("B-factor continuation factor")?,
        };
        let bfactors = reader.take(residues)?;
        Ok(Self {
            header,
            anchors,
            title,
            chain: &data[13..14],
            backbone,
            sidechain,
            has_oxt,
            oxt,
            bfactor_discretizer,
            bfactors,
            reconstructed_atoms,
        })
    }

    pub fn header(&self) -> &Header {
        &self.header
    }
    pub fn anchors(&self) -> &[Anchor] {
        &self.anchors
    }
    pub fn title_bytes(&self) -> &[u8] {
        self.title
    }
    pub fn title(&self) -> Result<&str> {
        legacy_string(self.title)
    }
    pub fn chain(&self) -> Result<&str> {
        legacy_string(self.chain)
    }
    pub fn backbone(&self) -> &[Backbone] {
        &self.backbone
    }
    pub fn sidechain(&self) -> &[u8] {
        self.sidechain
    }
    pub fn has_oxt(&self) -> bool {
        self.has_oxt
    }
    pub fn oxt(&self) -> [f32; 3] {
        self.oxt
    }
    pub fn bfactor_discretizer(&self) -> Discretizer {
        self.bfactor_discretizer
    }
    pub fn bfactors(&self) -> &[u8] {
        self.bfactors
    }
    pub fn reconstructed_atoms(&self) -> usize {
        self.reconstructed_atoms
    }
}
