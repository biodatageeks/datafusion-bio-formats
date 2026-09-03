## 1. Serialization
- [ ] 1.1 Define the sidecar layout and version; `PvarTable::write_to(path)` atomic via temp file and rename.
- [ ] 1.2 `PvarTable::map(path, expected_key)` validating magic, version, key, checksum, and lengths.
- [ ] 1.3 Make `PvarTable` columns storage-generic; accessors unchanged.

## 2. Provider integration
- [ ] 2.1 `PgenReadOptions::companion_cache` (`Off`/`ReadOnly`/`ReadWrite`) and `cache_dir`; resolve the sidecar path from the PVAR location or `cache_dir`.
- [ ] 2.2 Open path: try map, validate key, fall back to parse; write when `ReadWrite`.

## 3. Tests
- [ ] 3.1 Round trip: parse, write, map; every accessor equal on the fixtures.
- [ ] 3.2 Stale key (size or mtime changed) and truncated file are ignored and rebuilt.
- [ ] 3.3 `Off` never touches the sidecar; `ReadOnly` never writes.
- [ ] 3.4 Opt-in real-panel run: second open time and resident memory recorded in `PERF_HANDOVER.md`.
