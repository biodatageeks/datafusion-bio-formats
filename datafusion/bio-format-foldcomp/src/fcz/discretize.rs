//! Float32 inverse discretization matching the target reference contraction.
//! Adapted from Foldcomp (MIT); see LICENSE-FOLDCOMP.

#[derive(Clone, Copy, Debug)]
pub(super) struct Discretizer {
    pub minimum: f32,
    pub factor: f32,
}

impl Discretizer {
    pub fn restore(self, code: u16) -> f32 {
        // ARM64 uses FMADD; baseline x86-64 rounds multiply and add separately.
        super::numeric::multiply_add(f32::from(code), self.factor, self.minimum)
    }

    pub fn sidechain() -> Self {
        Self {
            minimum: -180.0,
            factor: 360.0 / 255.0,
        }
    }
}
