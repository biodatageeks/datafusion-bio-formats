use crate::error;
use datafusion::common::Result;
#[derive(Clone, Copy, Debug)]
pub enum TextFormat {
    Pdb,
    Mmcif,
}
#[derive(Clone, Debug)]
pub struct Source {
    pub path: String,
    pub source_index: u64,
    pub format: TextFormat,
}
pub fn expand(paths: Vec<String>, format: Option<TextFormat>) -> Result<Vec<Source>> {
    let mut result = Vec::new();
    for path in paths {
        let remote = path.contains("://");
        let expanded = if !remote && path.contains(['*', '?', '[']) {
            let mut v = glob::glob(&path)
                .map_err(|e| error(e.to_string()))?
                .map(|p| {
                    p.map(|p| p.to_string_lossy().into_owned())
                        .map_err(|e| error(e.to_string()))
                })
                .collect::<Result<Vec<_>>>()?;
            v.sort();
            if v.is_empty() {
                return Err(error(format!("glob matched no structures: {path}")));
            }
            v
        } else {
            vec![path]
        };
        for path in expanded {
            let inferred = match format {
                Some(f) => f,
                None => {
                    let p = path.split('?').next().unwrap_or(&path).to_ascii_lowercase();
                    let p = p.strip_suffix(".gz").unwrap_or(&p);
                    if p.ends_with(".pdb") || p.ends_with(".ent") {
                        TextFormat::Pdb
                    } else if p.ends_with(".cif") || p.ends_with(".mmcif") {
                        TextFormat::Mmcif
                    } else {
                        return Err(error(format!("cannot infer structure format: {path}")));
                    }
                }
            };
            result.push(Source {
                path,
                source_index: result.len() as u64,
                format: inferred,
            });
        }
    }
    if result.is_empty() {
        return Err(error("structure sources cannot be empty"));
    }
    Ok(result)
}
