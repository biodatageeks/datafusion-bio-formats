//! NeRF primitives adapted from Foldcomp (MIT); see LICENSE-FOLDCOMP.
//! Float32 rounding and reference contraction are explicit compatibility choices.
use super::numeric::multiply_add;

pub(super) type Point = [f32; 3];

fn subtract(a: Point, b: Point) -> Point {
    std::array::from_fn(|i| a[i] - b[i])
}

fn cross(a: Point, b: Point) -> Point {
    [
        multiply_add(a[1], b[2], -(b[1] * a[2])),
        multiply_add(a[2], b[0], -(b[2] * a[0])),
        multiply_add(a[0], b[1], -(b[0] * a[1])),
    ]
}

fn norm(a: Point) -> f32 {
    // C++ pow(float, int) promotes to double before summing/square-rooting.
    let [x, y, z] = a.map(f64::from);
    ((x * x + y * y) + z * z).sqrt() as f32
}

fn unit(a: Point) -> Point {
    let length = norm(a);
    a.map(|v| v / length)
}

/// Check an anchor using the same rounded basis arithmetic as `place`.
pub(super) fn finite_frame([a, b, c]: [Point; 3]) -> bool {
    let bc = unit(subtract(c, b));
    let normal = unit(cross(subtract(b, a), bc));
    bc.into_iter().chain(normal).all(f32::is_finite)
}

pub(super) fn place(previous: [Point; 3], length: f32, bond: f32, torsion: f32) -> Point {
    let [a, b, c] = previous;
    let ab = subtract(b, a);
    let bc = unit(subtract(c, b));
    let bond = (f64::from(bond) * std::f64::consts::PI / 180.0) as f32;
    let torsion = (f64::from(torsion) * std::f64::consts::PI / 180.0) as f32;
    let local = [
        -length * bond.cos(),
        length * torsion.cos() * bond.sin(),
        length * torsion.sin() * bond.sin(),
    ];
    let n = unit(cross(ab, bc));
    let nbc = cross(n, bc);
    std::array::from_fn(|axis| {
        let first = bc[axis] * local[0];
        let second = multiply_add(nbc[axis], local[1], first);
        let third = multiply_add(n[axis], local[2], second);
        third + c[axis]
    })
}

fn dot(a: Point, b: Point) -> f32 {
    multiply_add(a[2], b[2], multiply_add(a[0], b[0], a[1] * b[1]))
}

pub(super) fn angle(a: Point, b: Point, c: Point) -> f32 {
    let ab = subtract(a, b);
    let cb = subtract(c, b);
    let inner = dot(ab, cb);
    let squared_sizes = dot(ab, ab) * dot(cb, cb);
    // Measured legacy profiles: GNU/Linux GCC promotes sqrt/acos to double;
    // Apple Clang and Windows x64/MSVC select Float32 overloads. All five CI
    // targets compare against their own optimized native reference, including
    // Windows (<= 1e-4 coordinate error). Preserve intermediate narrowing.
    // This OS mapping describes those tested toolchains, not every C++ library:
    // musl, MinGW and BSD are uncharacterized and need separate reference checks
    // before claiming legacy numerical compatibility on those targets.
    let radians = if cfg!(target_os = "linux") {
        let cosine = (f64::from(inner) / f64::from(squared_sizes).sqrt()) as f32;
        f64::from(cosine).acos()
    } else {
        let cosine = inner / squared_sizes.sqrt();
        f64::from(cosine.acos())
    };
    (radians * 180.0 / std::f64::consts::PI) as f32
}

pub(super) fn blend(forward: Point, reverse: Point, index: usize, count: usize) -> Point {
    std::array::from_fn(|axis| {
        multiply_add(
            forward[axis],
            (count - index) as f32,
            reverse[axis] * index as f32,
        ) / count as f32
    })
}
