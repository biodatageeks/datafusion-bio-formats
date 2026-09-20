//! NeRF primitives adapted from Foldcomp (MIT); see LICENSE-FOLDCOMP.
//! Float32 rounding and reference contraction are explicit compatibility choices.
pub(super) type Point = [f32; 3];

fn subtract(a: Point, b: Point) -> Point {
    std::array::from_fn(|i| a[i] - b[i])
}

fn cross(a: Point, b: Point) -> Point {
    [
        a[1].mul_add(b[2], -(b[1] * a[2])),
        a[2].mul_add(b[0], -(b[2] * a[0])),
        a[0].mul_add(b[1], -(b[0] * a[1])),
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
        let second = nbc[axis].mul_add(local[1], first);
        let third = n[axis].mul_add(local[2], second);
        third + c[axis]
    })
}

fn dot(a: Point, b: Point) -> f32 {
    a[2].mul_add(b[2], a[0].mul_add(b[0], a[1] * b[1]))
}

pub(super) fn angle(a: Point, b: Point, c: Point) -> f32 {
    let ab = subtract(a, b);
    let cb = subtract(c, b);
    let cosine = dot(ab, cb) / (dot(ab, ab) * dot(cb, cb)).sqrt();
    (f64::from(cosine.acos()) * 180.0 / std::f64::consts::PI) as f32
}

pub(super) fn blend(forward: Point, reverse: Point, index: usize, count: usize) -> Point {
    std::array::from_fn(|axis| {
        forward[axis].mul_add((count - index) as f32, reverse[axis] * index as f32) / count as f32
    })
}
