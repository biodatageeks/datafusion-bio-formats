use datafusion::common::Result;
use datafusion_bio_format_structure::{
    StructureOptions, error,
    model::{Atom, NormalizedEntry},
};
use std::{
    ffi::{CStr, c_char, c_void},
    ptr::NonNull,
};
#[repr(C)]
struct NativeAtom {
    name: *const c_char,
    residue: *const c_char,
    chain: *const c_char,
    atom_id: i32,
    residue_id: i32,
    x: f32,
    y: f32,
    z: f32,
    b_factor: f32,
}
unsafe extern "C" {
    fn bio_fc_decode(data: *const u8, len: usize, max_atoms: usize) -> *mut c_void;
    fn bio_fc_error(h: *const c_void) -> *const c_char;
    fn bio_fc_title(h: *const c_void) -> *const c_char;
    fn bio_fc_atoms(h: *const c_void, n: *mut usize) -> *const NativeAtom;
    fn bio_fc_free(h: *mut c_void);
}
struct Handle(NonNull<c_void>);
impl Drop for Handle {
    fn drop(&mut self) {
        // SAFETY: uniquely owned handle allocated by the native adapter.
        unsafe { bio_fc_free(self.0.as_ptr()) }
    }
}
// SAFETY: caller supplies a NUL-terminated string belonging to the live native handle, or null.
unsafe fn string(p: *const c_char) -> Result<String> {
    if p.is_null() {
        return Err(error("unexpected null string from FCZ codec"));
    }
    unsafe { CStr::from_ptr(p) }
        .to_str()
        .map(str::to_owned)
        .map_err(|e| error(format!("invalid FCZ UTF-8: {e}")))
}
/// Decode validated FCZ bytes directly into the common model; no PDB rounding step.
pub fn decode(data: &[u8], options: &StructureOptions) -> Result<NormalizedEntry> {
    // SAFETY: valid borrowed byte slice; the synchronous adapter catches C++ exceptions.
    let h = Handle(
        NonNull::new(unsafe { bio_fc_decode(data.as_ptr(), data.len(), options.max_atoms) })
            .ok_or_else(|| error("FCZ allocation failed"))?,
    );
    // SAFETY: all spans/strings below belong to h, which lives until they are copied.
    unsafe {
        let err = string(bio_fc_error(h.0.as_ptr()))?;
        if !err.is_empty() {
            return Err(error(err));
        }
        let mut n = 0;
        let ptr = bio_fc_atoms(h.0.as_ptr(), &mut n);
        let rows = if n == 0 {
            &[]
        } else {
            std::slice::from_raw_parts(ptr, n)
        };
        let mut entry = NormalizedEntry {
            entry_id: Some(string(bio_fc_title(h.0.as_ptr()))?),
            source_format: "foldcomp".into(),
            ..Default::default()
        };
        for (i, r) in rows.iter().enumerate() {
            let name = string(r.name)?;
            let comp = string(r.residue)?;
            entry.atoms.push(Atom {
                atom_index: i as u64,
                atom_id: Some(r.atom_id.to_string()),
                record_type: "ATOM".into(),
                model_id: 1,
                auth_asym_id: Some(string(r.chain)?),
                auth_seq_id: Some(r.residue_id.to_string()),
                auth_atom_id: Some(name.clone()),
                auth_comp_id: Some(comp.clone()),
                atom_name: name,
                residue_name: comp,
                position: [r.x as f64, r.y as f64, r.z as f64],
                b_factor: Some(r.b_factor as f64),
                peptide: true,
                ..Default::default()
            });
        }
        entry.normalize(options)?;
        Ok(entry)
    }
}
