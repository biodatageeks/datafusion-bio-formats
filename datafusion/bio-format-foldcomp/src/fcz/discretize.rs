//! Float32 inverse discretization matching the pinned reference's contraction.
//! Adapted from Foldcomp (MIT); see LICENSE-FOLDCOMP.

#[derive(Clone, Copy, Debug)]
pub(super) struct Discretizer {
    pub minimum: f32,
    pub factor: f32,
}

impl Discretizer {
    pub fn restore(self, code: u16) -> f32 {
        // The captured Clang/arm64 reference emits FMADD for code*factor+min.
        // Explicit fusion reproduces those bits, independently of Rust's target
        // instruction selection. This is compatibility, not a speed optimization.
        f32::from(code).mul_add(self.factor, self.minimum)
    }

    pub fn sidechain() -> Self {
        Self {
            minimum: -180.0,
            factor: 360.0 / 255.0,
        }
    }
}
