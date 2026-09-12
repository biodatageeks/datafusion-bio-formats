//! Float64 geometry on Angstrom coordinates. Degenerate geometry is null.
use crate::model::Point;
fn sub(a: Point, b: Point) -> Point {
    [a[0] - b[0], a[1] - b[1], a[2] - b[2]]
}
fn dot(a: Point, b: Point) -> f64 {
    a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
}
fn cross(a: Point, b: Point) -> Point {
    [
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    ]
}
fn unit(a: Point) -> Option<Point> {
    let n = dot(a, a).sqrt();
    (n.is_finite() && n > 1e-12).then(|| a.map(|x| x / n))
}
pub fn distance(a: Point, b: Point) -> f64 {
    let d = sub(a, b);
    dot(d, d).sqrt()
}
pub fn bond_angle(a: Point, b: Point, c: Point) -> Option<f64> {
    let u = unit(sub(a, b))?;
    let v = unit(sub(c, b))?;
    Some(dot(u, v).clamp(-1., 1.).acos().to_degrees())
}
pub fn dihedral(a: Point, b: Point, c: Point, d: Point) -> Option<f64> {
    let u = unit(sub(b, a))?;
    let v = unit(sub(c, b))?;
    let w = unit(sub(d, c))?;
    let n = unit(cross(u, v))?;
    let m = unit(cross(v, w))?;
    let angle = dot(cross(n, m), v).atan2(dot(n, m)).to_degrees();
    Some((angle + 180.).rem_euclid(360.) - 180.)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn signed_and_degenerate() {
        let a = [1., 0., 0.];
        let b = [0., 0., 0.];
        let c = [0., 1., 0.];
        assert_eq!(dihedral(a, b, c, [0., 1., 1.]), Some(-90.));
        assert_eq!(dihedral(a, b, c, [0., 1., -1.]), Some(90.));
        assert_eq!(dihedral(a, b, c, [-1., 1., 0.]), Some(-180.));
        assert_eq!(dihedral(b, b, c, a), None);
        assert_eq!(bond_angle(a, b, [-1., 0., 0.]), Some(180.));
    }
}
