//! Forward/reverse anchor reconstruction adapted from Foldcomp (MIT).
//! See LICENSE-FOLDCOMP. EncodedEntry has already checked every section/count.
use super::{
    geometry::{Point, angle, blend, finite_frame, place},
    header::EncodedEntry,
};
use datafusion::common::Result;
use datafusion_bio_format_structure::error;

const C_TO_N: f32 = 1.3311;
const N_TO_CA: f32 = 1.4581;
const PRO_N_TO_CA: f32 = 1.353;
const CA_TO_C: f32 = 1.5281;

pub(super) fn reconstruct(entry: &EncodedEntry<'_>) -> Result<Vec<Point>> {
    for (index, anchor) in entry.anchors().iter().enumerate() {
        // The initial seed is used forward; subsequent anchors seed reverse
        // reconstruction. Check the actual orientation's rounded arithmetic.
        let [n, ca, c] = anchor.coordinates;
        let seed = if index == 0 { [n, ca, c] } else { [c, ca, n] };
        if !finite_frame(seed) {
            return Err(error(format!(
                "FCZ anchor {index} at residue index {} cannot define a finite reconstruction frame",
                anchor.residue
            )));
        }
    }
    let records = entry.backbone();
    let parameters = records
        .iter()
        .map(|r| r.parameters(&entry.header().discretizers))
        .collect::<Vec<_>>();
    let mut output = Vec::with_capacity(records.len() * 3);
    let mut seed = entry.anchors()[0].coordinates;
    for (segment, anchors) in entry.anchors().windows(2).enumerate() {
        let first = anchors[0].residue;
        let last = anchors[1].residue;
        let count = (last - first + 1) * 3;
        let mut forward = Vec::with_capacity(count);
        forward.extend_from_slice(&seed);
        let mut torsions = Vec::with_capacity(count - 3);
        for index in first..last {
            let [phi, psi, omega, n_ca_c, ca_c_n, c_n_ca] = parameters[index];
            let ca_length = if records[index].residue.letter() == b'P' {
                PRO_N_TO_CA
            } else {
                N_TO_CA
            };
            // Legacy chooses the CA length from the preceding residue.
            for (length, bond, torsion) in [
                (C_TO_N, ca_c_n, psi),
                (ca_length, c_n_ca, omega),
                (CA_TO_C, n_ca_c, phi),
            ] {
                let n = forward.len();
                let next = place(
                    [forward[n - 3], forward[n - 2], forward[n - 1]],
                    length,
                    bond,
                    torsion,
                );
                forward.push(next);
                torsions.push(torsion);
            }
        }
        let angles = forward
            .windows(3)
            .map(|p| angle(p[0], p[1], p[2]))
            .collect::<Vec<_>>();
        let mut reverse = Vec::with_capacity(count);
        reverse.extend(anchors[1].coordinates.iter().rev().copied());
        for index in 0..count - 3 {
            let n = reverse.len();
            // Legacy's reverse pass deliberately uses N_TO_CA even for proline;
            // only the forward pass selects PRO_N_TO_CA from the prior residue.
            let length = [C_TO_N, CA_TO_C, N_TO_CA][index % 3];
            let next = place(
                [reverse[n - 3], reverse[n - 2], reverse[n - 1]],
                length,
                angles[angles.len() - 2 - index],
                torsions[torsions.len() - 1 - index],
            );
            reverse.push(next);
        }
        for (index, (f, r)) in forward.iter_mut().zip(reverse.iter().rev()).enumerate() {
            *f = blend(*f, *r, index, count);
        }
        seed.copy_from_slice(&forward[count - 3..]);
        let final_segment = segment + 2 == entry.anchors().len();
        output.extend_from_slice(&forward[..if final_segment { count } else { count - 3 }]);
    }
    Ok(output)
}
