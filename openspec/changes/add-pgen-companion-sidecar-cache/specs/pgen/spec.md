## ADDED Requirements

### Requirement: PGEN Parsed Companion Cache

The system SHALL be able to persist the parsed variant table as a sidecar
keyed to the source PVAR, memory-map it on later opens, and SHALL ignore a
sidecar whose key or integrity check fails.

#### Scenario: Cached open
- **WHEN** a valid sidecar exists beside the PVAR or in the cache directory
- **THEN** the fileset opens without decoding the PVAR
- **AND** the variant table is memory-mapped rather than allocated.

#### Scenario: Stale sidecar
- **WHEN** the PVAR's size or modification key differs from the sidecar's
- **THEN** the sidecar is ignored and the PVAR is parsed.

#### Scenario: Write is opt-in
- **WHEN** the cache mode is not read-write
- **THEN** no sidecar is written.
