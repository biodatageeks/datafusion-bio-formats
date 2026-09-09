use crate::{FoldcompOptions, codec};
use async_trait::async_trait;
use datafusion::common::Result;
use datafusion_bio_format_structure::{
    EntrySource, StructureOptions, error, model::NormalizedEntry,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, Metadata},
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
    path::Path,
    time::SystemTime,
};
#[derive(Debug, Clone, PartialEq, Eq)]
struct Identity {
    len: u64,
    modified: Option<SystemTime>,
}
impl From<Metadata> for Identity {
    fn from(m: Metadata) -> Self {
        Self {
            len: m.len(),
            modified: m.modified().ok(),
        }
    }
}
#[derive(Debug, Clone)]
pub struct SelectedEntry {
    path: String,
    key: Option<u64>,
    name: Option<String>,
    ordinal: u64,
    offset: u64,
    len: u64,
    database: bool,
    identities: Vec<(String, Identity)>,
}
fn identity(path: &str) -> Result<Identity> {
    Ok(std::fs::metadata(path)?.into())
}
fn each_line(path: &str, mut visit: impl FnMut(usize, &str) -> Result<()>) -> Result<()> {
    let mut input = BufReader::new(File::open(path)?);
    let mut bytes = Vec::new();
    let mut row = 0;
    loop {
        bytes.clear();
        let n = input
            .by_ref()
            .take(1024 * 1024 + 1)
            .read_until(b'\n', &mut bytes)?;
        if n == 0 {
            break;
        }
        if n > 1024 * 1024 {
            return Err(error(format!("{path}: oversized metadata line")));
        }
        row += 1;
        let line = std::str::from_utf8(&bytes).map_err(|e| error(format!("{path}:{row}: {e}")))?;
        visit(row, line.trim_end()).map_err(|e| error(format!("{path}:{row}: {e}")))?;
    }
    Ok(())
}
fn integer(s: &str) -> Result<u64> {
    s.parse()
        .map_err(|_| error(format!("invalid unsigned integer {s:?}")))
}
pub fn select(path: &str, options: &FoldcompOptions) -> Result<Vec<SelectedEntry>> {
    if path.contains("://") {
        return Err(error(
            "Foldcomp currently requires a local database or FCZ file",
        ));
    }
    let index = format!("{path}.index");
    let lookup = format!("{path}.lookup");
    let dbtype = format!("{path}.dbtype");
    let mut identities = vec![(path.to_owned(), identity(path)?)];
    if !Path::new(&index).exists() {
        if options.ids.is_some() || options.entry_keys.is_some() {
            return Err(error("selectors require an indexed Foldcomp database"));
        }
        let len = identities[0].1.len;
        if len > options.structure.max_input_bytes as u64 {
            return Err(error("FCZ exceeds max_input_bytes"));
        }
        return Ok(vec![SelectedEntry {
            path: path.into(),
            key: None,
            name: None,
            ordinal: 0,
            offset: 0,
            len,
            database: false,
            identities,
        }]);
    }
    let mut kind = Vec::new();
    File::open(&dbtype)?.take(5).read_to_end(&mut kind)?;
    if kind != 12u32.to_le_bytes() {
        return Err(error(
            "unsupported Foldcomp dbtype (expected uncompressed type 12)",
        ));
    }
    identities.push((index.clone(), identity(&index)?));
    identities.push((dbtype.clone(), identity(&dbtype)?));
    if Path::new(&lookup).exists() {
        identities.push((lookup.clone(), identity(&lookup)?));
    }
    let requested_names = options
        .ids
        .as_ref()
        .map(|v| v.iter().cloned().collect::<BTreeSet<_>>());
    let mut keys = options
        .entry_keys
        .as_ref()
        .map(|v| v.iter().copied().collect::<BTreeSet<_>>());
    let mut names = BTreeMap::new();
    if let Some(wanted) = &requested_names {
        if !Path::new(&lookup).exists() {
            return Err(error("name selection requires .lookup"));
        }
        let mut found = BTreeSet::new();
        let mut selected = BTreeSet::new();
        each_line(&lookup, |_, line| {
            let v: Vec<_> = line.split('\t').collect();
            if v.len() != 3 {
                return Err(error("lookup requires key, name, source"));
            }
            let key = integer(v[0])?;
            if wanted.contains(v[1]) {
                if !found.insert(v[1].to_owned()) || !selected.insert(key) {
                    return Err(error(format!("ambiguous Foldcomp name/key: {}", v[1])));
                }
                names.insert(key, v[1].to_owned());
            }
            Ok(())
        })?;
        if found.len() != wanted.len() {
            return Err(error(format!(
                "missing Foldcomp IDs: {:?}",
                wanted.difference(&found).collect::<Vec<_>>()
            )));
        }
        keys = Some(selected);
    }
    let mut result = Vec::new();
    let mut previous = None;
    each_line(&index, |row, line| {
        let v: Vec<_> = line.split_whitespace().collect();
        if v.len() != 3 {
            return Err(error("index requires key, offset, length"));
        }
        let key = integer(v[0])?;
        let offset = integer(v[1])?;
        let len = integer(v[2])?;
        if previous.is_some_and(|p| p >= key) {
            return Err(error("Foldcomp index keys must be unique and increasing"));
        }
        previous = Some(key);
        if keys.as_ref().is_none_or(|k| k.contains(&key)) {
            if len < 2
                || len > options.structure.max_input_bytes as u64
                || offset
                    .checked_add(len)
                    .is_none_or(|end| end > identities[0].1.len)
            {
                return Err(error(format!(
                    "invalid selected payload range for key {key}"
                )));
            }
            result.push(SelectedEntry {
                path: path.into(),
                key: Some(key),
                name: names.remove(&key),
                ordinal: (row - 1) as u64,
                offset,
                len,
                database: true,
                identities: vec![],
            });
        }
        Ok(())
    })?;
    if keys.as_ref().is_some_and(|k| k.len() != result.len()) {
        return Err(error("selected Foldcomp key is absent from index"));
    }
    if Path::new(&lookup).exists() && requested_names.is_none() {
        let positions: BTreeMap<_, _> = result
            .iter()
            .enumerate()
            .map(|(i, r)| (r.key.unwrap_or_default(), i))
            .collect();
        each_line(&lookup, |_, line| {
            let v: Vec<_> = line.split('\t').collect();
            if v.len() != 3 {
                return Err(error("invalid lookup row"));
            }
            let key = integer(v[0])?;
            if let Some(i) = positions.get(&key)
                && result[*i].name.replace(v[1].into()).is_some()
            {
                return Err(error("duplicate selected lookup key"));
            }
            Ok(())
        })?;
    }
    for (file, expected) in &identities {
        if identity(file)? != *expected {
            return Err(error(format!(
                "Foldcomp file changed during selection: {file}"
            )));
        }
    }
    for r in &mut result {
        r.identities = identities.clone();
    }
    Ok(result)
}
#[async_trait]
impl EntrySource for SelectedEntry {
    async fn load(&self, options: &StructureOptions) -> Result<Vec<NormalizedEntry>> {
        let result = (|| {
            for (path, expected) in &self.identities {
                if identity(path)? != *expected {
                    return Err(error(format!(
                        "Foldcomp file changed after selection: {path}"
                    )));
                }
            }
            let mut file = File::open(&self.path)?;
            if Identity::from(file.metadata()?) != self.identities[0].1 {
                return Err(error("Foldcomp payload changed while opening"));
            }
            file.seek(SeekFrom::Start(self.offset))?;
            let mut data =
                vec![0; usize::try_from(self.len).map_err(|_| error("FCZ length overflow"))?];
            file.read_exact(&mut data)?;
            if self.database && data.pop() != Some(0) {
                return Err(error("Foldcomp record lacks NUL terminator"));
            }
            let mut entry = codec::decode(&data, options)?;
            entry.encoded_bytes = self.len as usize;
            entry.source_path = self.path.clone();
            entry.entry_index = self.ordinal;
            entry.entry_key = self.key;
            entry.entry_name = self.name.clone();
            Ok(vec![entry])
        })();
        result.map_err(|e: datafusion::common::DataFusionError| {
            error(format!("{} key {:?}: {e}", self.path, self.key))
        })
    }
}
