//! Explicit arithmetic profiles measured from the legacy supported targets.
//! ARM64 contracts multiply/add; baseline x86-64 builds use separate operations.
//! This preserves decoding behavior, without fast-math or tolerance changes.
#[inline]
pub(super) fn multiply_add(a: f32, b: f32, c: f32) -> f32 {
    if cfg!(target_arch = "aarch64") {
        a.mul_add(b, c)
    } else {
        a * b + c
    }
}
