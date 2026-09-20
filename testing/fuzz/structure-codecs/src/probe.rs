//! Standalone source-identical candidate for separate-process platform checks.
use serde_json::json;
use structure_codecs_fuzz::{Document, decode_entry};
fn hex(value: &str) -> String {
    value.bytes().map(|b| format!("{b:02x}")).collect()
}
fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let data = std::fs::read(&args[2]).unwrap();
    let output = if args[1] == "cif" {
        match Document::parse(&data) {
            Err(_) => json!({"status":"error"}),
            Ok(document) => {
                let blocks = (0..document.block_count())
                    .map(|i| document.block(i))
                    .collect::<Result<Vec<_>, _>>();
                match blocks {
                    Err(_) => json!({"status":"error"}),
                    Ok(blocks) => {
                        json!({"status":"ok", "blocks":blocks.iter().map(|b| json!({"name":b.name,"columns":b.columns})).collect::<Vec<_>>()})
                    }
                }
            }
        }
    } else {
        assert_eq!(args[1], "fcz");
        match decode_entry(&data, args[3].parse().unwrap()) {
            Err(_) => json!({"status":"error"}),
            Ok(entry) => json!({
                "status":"ok", "decoded_title_hex":hex(entry.entry_id.as_deref().unwrap()),
                "atoms":entry.atoms.iter().map(|a| json!([
                    hex(&a.atom_name),hex(&a.residue_name),hex(a.auth_asym_id.as_deref().unwrap()),
                    a.atom_id.as_deref().unwrap().parse::<i32>().unwrap(),a.auth_seq_id.as_deref().unwrap().parse::<i32>().unwrap(),
                    (a.position[0] as f32).to_bits(),(a.position[1] as f32).to_bits(),(a.position[2] as f32).to_bits(),
                    (a.b_factor.unwrap() as f32).to_bits()
                ])).collect::<Vec<_>>()
            }),
        }
    };
    println!("{output}");
}
