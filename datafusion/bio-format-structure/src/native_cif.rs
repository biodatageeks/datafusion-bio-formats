//! Owner-scoped views over the native CIF parser. No native pointer escapes `Document`.
use crate::error;
use datafusion::common::Result;
use std::{
    collections::HashMap,
    ffi::{CStr, c_char, c_void},
    ptr::NonNull,
    slice, str,
};
#[repr(C)]
struct Cell {
    data: *const u8,
    len: usize,
}
#[repr(C)]
struct Column {
    name: Cell,
    cells: *const Cell,
    len: usize,
}
#[repr(C)]
struct Block {
    name: Cell,
    columns: *const Column,
    len: usize,
}
unsafe extern "C" {
    fn bio_cif_read(data: *const u8, len: usize) -> *mut c_void;
    fn bio_cif_error(h: *const c_void) -> *const c_char;
    fn bio_cif_blocks(h: *const c_void, len: *mut usize) -> *const Block;
    fn bio_cif_free(h: *mut c_void);
}
pub struct Document(NonNull<c_void>);
impl Drop for Document {
    fn drop(&mut self) {
        // SAFETY: sole owner of the handle returned by bio_cif_read.
        unsafe {
            bio_cif_free(self.0.as_ptr());
        }
    }
}
// SAFETY: callers keep the owner alive; C++ guarantees stable, valid spans until free.
unsafe fn span<'a, T>(p: *const T, n: usize) -> &'a [T] {
    if n == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(p, n) }
    }
}
impl Cell {
    fn text(&self) -> Result<Option<&str>> {
        if self.data.is_null() {
            Ok(None)
        } else {
            // SAFETY: this cell is borrowed from a live document.
            str::from_utf8(unsafe { span(self.data, self.len) })
                .map(Some)
                .map_err(|e| error(format!("invalid CIF UTF-8: {e}")))
        }
    }
}
pub struct CategoryBlock<'a> {
    pub name: &'a str,
    pub columns: HashMap<&'a str, Vec<Option<&'a str>>>,
}
impl Document {
    pub fn parse(data: &[u8]) -> Result<Self> {
        // SAFETY: input valid for this synchronous call; native code catches exceptions.
        let h = NonNull::new(unsafe { bio_cif_read(data.as_ptr(), data.len()) })
            .ok_or_else(|| error("CIF allocation failed"))?;
        let doc = Self(h); // SAFETY: native error string lives as long as doc.
        let err = unsafe { CStr::from_ptr(bio_cif_error(h.as_ptr())) }.to_string_lossy();
        if !err.is_empty() {
            return Err(error(err.into_owned()));
        }
        Ok(doc)
    }
    pub fn blocks(&self) -> Result<Vec<CategoryBlock<'_>>> {
        let mut len = 0; // SAFETY: handle is valid and C++ fills length for its block span.
        let blocks = unsafe { span(bio_cif_blocks(self.0.as_ptr(), &mut len), len) };
        blocks
            .iter()
            .map(|b| {
                let mut columns = HashMap::new(); // SAFETY: all column/cell spans belong to self.
                for col in unsafe { span(b.columns, b.len) } {
                    let values = unsafe { span(col.cells, col.len) }
                        .iter()
                        .map(Cell::text)
                        .collect::<Result<Vec<_>>>()?;
                    columns.insert(col.name.text()?.unwrap_or(""), values);
                }
                Ok(CategoryBlock {
                    name: b.name.text()?.unwrap_or(""),
                    columns,
                })
            })
            .collect()
    }
}
