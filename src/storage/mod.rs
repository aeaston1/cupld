use std::fs;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::engine::{
    ConstraintState, CupldEngine, EdgeState, EngineState, GraphError, IndexKind, IndexState,
    IndexStatus, NodeState, PropertyMap, SchemaObjectState, SchemaState, Value,
};

const MAGIC: &[u8; 8] = b"CUPLD01\0";
const FORMAT_VERSION: u32 = 3;
const COMPAT_VERSION: u32 = 1;
const PREVIOUS_FORMAT_VERSION: u32 = 2;
const OLDER_FORMAT_VERSION: u32 = 1;
const LEGACY_FORMAT_VERSION: u32 = 0;
const LEGACY_COMPAT_VERSION: u32 = 0;
const HEADER_SIZE: usize = 128;
const WAL_RECORD_MAGIC: &[u8; 4] = b"WALR";
const WAL_HEADER_SIZE: usize = 48;
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Identifies the exact persisted bytes observed by a reader.
/// Pass the latest revision to each write; do not reuse it after a successful write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageRevision {
    byte_len: usize,
    checksum: u64,
}

impl StorageRevision {
    fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            byte_len: bytes.len(),
            checksum: checksum(bytes),
        }
    }
}

#[derive(Clone, Debug)]
pub struct IntegrityReport {
    pub db_uuid: [u8; 16],
    pub last_tx_id: u64,
    pub wal_records: usize,
    pub recovered_tail: bool,
    pub revision: StorageRevision,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MigrationPolicy {
    MigrateInPlace,
    ReadOnlyProbe,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StorageFormatVersion {
    version: u32,
    compat: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum StorageErrorKind {
    Io,
    DatabaseBusy,
    DatabaseChanged,
    DatabaseExists,
    PersistenceUncertain,
    Graph(GraphError),
    FileHeader,
    FileLayout,
    FileMagic,
    FileVersion,
    DecodeEof,
    IndexKind,
    IndexStatus,
    PropertyType,
    SchemaTarget,
    SnapshotChecksum,
    Utf8,
    ValueTag,
    WalChecksum,
}

impl StorageErrorKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Io => "io_error",
            Self::DatabaseBusy => "database_busy",
            Self::DatabaseChanged => "database_changed",
            Self::DatabaseExists => "database_exists",
            Self::PersistenceUncertain => "persistence_uncertain",
            Self::Graph(error) => error.code(),
            Self::FileHeader => "file_header",
            Self::FileLayout => "file_layout",
            Self::FileMagic => "file_magic",
            Self::FileVersion => "file_version",
            Self::DecodeEof => "decode_eof",
            Self::IndexKind => "index_kind",
            Self::IndexStatus => "index_status",
            Self::PropertyType => "property_type",
            Self::SchemaTarget => "schema_target",
            Self::SnapshotChecksum => "snapshot_checksum",
            Self::Utf8 => "utf8",
            Self::ValueTag => "value_tag",
            Self::WalChecksum => "wal_checksum",
        }
    }
}

impl From<&'static str> for StorageErrorKind {
    fn from(value: &'static str) -> Self {
        match value {
            "io_error" => Self::Io,
            "file_header" => Self::FileHeader,
            "file_layout" => Self::FileLayout,
            "file_magic" => Self::FileMagic,
            "file_version" => Self::FileVersion,
            "decode_eof" => Self::DecodeEof,
            "index_kind" => Self::IndexKind,
            "index_status" => Self::IndexStatus,
            "property_type" => Self::PropertyType,
            "schema_target" => Self::SchemaTarget,
            "snapshot_checksum" => Self::SnapshotChecksum,
            "utf8" => Self::Utf8,
            "value_tag" => Self::ValueTag,
            "wal_checksum" => Self::WalChecksum,
            _ => panic!("unknown storage error code: {value}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageError {
    kind: StorageErrorKind,
    message: String,
}

impl StorageError {
    fn new(kind: impl Into<StorageErrorKind>, message: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            message: message.into(),
        }
    }

    pub fn code(&self) -> &'static str {
        self.kind.as_str()
    }
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code(), self.message)
    }
}

impl std::error::Error for StorageError {}

impl From<io::Error> for StorageError {
    fn from(value: io::Error) -> Self {
        Self::new(StorageErrorKind::Io, value.to_string())
    }
}

impl From<GraphError> for StorageError {
    fn from(value: GraphError) -> Self {
        let message = value.to_string();
        Self::new(StorageErrorKind::Graph(value), message)
    }
}

/// Create a new database. Existing destinations are never overwritten.
pub fn save_compacted(path: &Path, engine: &CupldEngine) -> Result<StorageRevision, StorageError> {
    let path = canonical_database_path(path)?;
    // Reject before acquiring the writer so a refused destination gains no
    // `.lock` sidecar; check again under the lock for concurrent creators.
    require_new_destination(&path)?;
    let writer = DatabaseWriter::acquire(&path)?;
    require_new_destination(&writer.path)?;
    let bytes = compacted_bytes(engine, file_uuid())?;
    write_durable(&writer.path, &bytes)?;
    Ok(StorageRevision::from_bytes(&bytes))
}

/// Reject destinations that already exist, or cannot name a database file, so
/// `SAVE AS` never replaces or locks an unrelated path.
fn require_new_destination(path: &Path) -> Result<(), StorageError> {
    database_file_name(path)?;
    match fs::symlink_metadata(path) {
        Ok(_) => Err(StorageError::new(
            StorageErrorKind::DatabaseExists,
            "SAVE AS destination already exists; open it before saving",
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub fn append_commit(
    path: &Path,
    engine: &CupldEngine,
    expected: StorageRevision,
) -> Result<StorageRevision, StorageError> {
    let writer = DatabaseWriter::acquire(path)?;
    let existing = writer.read_expected(expected)?;
    let parsed = parse_file(&existing)?;
    let tx_id = engine.snapshot().tx_id().get();
    let state = encode_state(&engine.to_state())?;
    let seq_no = parsed.wal_records.len() as u64 + 1;
    let record = encode_wal_record(seq_no, tx_id, &state);
    let mut wal = parsed.valid_wal_bytes;
    wal.extend(record);
    let header = FileHeader {
        clean: true,
        db_uuid: parsed.header.db_uuid,
        snapshot_offset: HEADER_SIZE as u64,
        snapshot_len: parsed.snapshot_bytes.len() as u64,
        wal_offset: (HEADER_SIZE + parsed.snapshot_bytes.len()) as u64,
        wal_len: wal.len() as u64,
        last_tx_id: tx_id,
        snapshot_checksum: checksum(&parsed.snapshot_bytes),
        wal_checksum: checksum(&wal),
    };
    // A read-only probe may supply a legacy revision. Never label legacy snapshot
    // bytes with the current format when committing through that session.
    let bytes = if parsed.format.version == FORMAT_VERSION {
        assemble_file(&header, &parsed.snapshot_bytes, &wal)
    } else {
        compacted_bytes(engine, parsed.header.db_uuid)?
    };
    write_durable(&writer.path, &bytes)?;
    Ok(StorageRevision::from_bytes(&bytes))
}

pub fn load(path: &Path) -> Result<(CupldEngine, IntegrityReport), StorageError> {
    load_with_migration_policy(path, MigrationPolicy::MigrateInPlace)
}

pub(crate) fn load_without_migration(
    path: &Path,
) -> Result<(CupldEngine, IntegrityReport), StorageError> {
    load_with_migration_policy(path, MigrationPolicy::ReadOnlyProbe)
}

fn load_with_migration_policy(
    path: &Path,
    migration_policy: MigrationPolicy,
) -> Result<(CupldEngine, IntegrityReport), StorageError> {
    let bytes = fs::read(path)?;
    let parsed = parse_file(&bytes)?;
    if migration_policy == MigrationPolicy::MigrateInPlace
        && plan_migration(parsed.format)?.rewrite_header
    {
        // Re-read under the stable sidecar lock: a writer could have committed
        // between the initial read and lock acquisition.
        let writer = DatabaseWriter::acquire(path)?;
        let bytes = fs::read(&writer.path)?;
        let parsed = parse_file(&bytes)?;
        let engine = decode_engine(&parsed)?;
        let revision = if plan_migration(parsed.format)?.rewrite_header {
            let migrated = compacted_bytes(&engine, parsed.header.db_uuid)?;
            write_durable(&writer.path, &migrated)?;
            StorageRevision::from_bytes(&migrated)
        } else {
            StorageRevision::from_bytes(&bytes)
        };
        let report = integrity_report(&parsed, &engine, revision);
        return Ok((engine, report));
    }
    let engine = decode_engine(&parsed)?;
    let report = integrity_report(&parsed, &engine, StorageRevision::from_bytes(&bytes));
    Ok((engine, report))
}

fn decode_engine(parsed: &ParsedFile) -> Result<CupldEngine, StorageError> {
    let mut state = decode_state(&parsed.snapshot_bytes, parsed.format)?;
    for record in &parsed.wal_records {
        state = decode_state(&record.payload, parsed.format)?;
    }
    Ok(CupldEngine::from_state(state)?)
}

fn integrity_report(
    parsed: &ParsedFile,
    engine: &CupldEngine,
    revision: StorageRevision,
) -> IntegrityReport {
    IntegrityReport {
        db_uuid: parsed.header.db_uuid,
        // The header may name an interrupted transaction that was not recovered.
        last_tx_id: engine.snapshot().tx_id().get(),
        wal_records: parsed.wal_records.len(),
        recovered_tail: parsed.recovered_tail,
        revision,
    }
}

fn compacted_bytes(engine: &CupldEngine, db_uuid: [u8; 16]) -> Result<Vec<u8>, StorageError> {
    let snapshot = encode_state(&engine.to_state())?;
    let header = FileHeader {
        clean: true,
        db_uuid,
        snapshot_offset: HEADER_SIZE as u64,
        snapshot_len: snapshot.len() as u64,
        wal_offset: (HEADER_SIZE + snapshot.len()) as u64,
        wal_len: 0,
        last_tx_id: engine.snapshot().tx_id().get(),
        snapshot_checksum: checksum(&snapshot),
        wal_checksum: checksum(&[]),
    };
    Ok(assemble_file(&header, &snapshot, &[]))
}

pub fn compact(
    path: &Path,
    engine: &CupldEngine,
    expected: StorageRevision,
) -> Result<StorageRevision, StorageError> {
    let writer = DatabaseWriter::acquire(path)?;
    let existing = writer.read_expected(expected)?;
    let parsed = parse_file(&existing)?;
    let bytes = compacted_bytes(engine, parsed.header.db_uuid)?;
    write_durable(&writer.path, &bytes)?;
    Ok(StorageRevision::from_bytes(&bytes))
}

pub fn check(path: &Path) -> Result<IntegrityReport, StorageError> {
    load(path).map(|(_, report)| report)
}

pub(crate) fn check_without_migration(path: &Path) -> Result<IntegrityReport, StorageError> {
    load_without_migration(path).map(|(_, report)| report)
}

#[derive(Clone, Copy)]
struct FileHeader {
    clean: bool,
    db_uuid: [u8; 16],
    snapshot_offset: u64,
    snapshot_len: u64,
    wal_offset: u64,
    wal_len: u64,
    last_tx_id: u64,
    snapshot_checksum: u64,
    wal_checksum: u64,
}

struct ParsedFile {
    format: StorageFormatVersion,
    header: FileHeader,
    snapshot_bytes: Vec<u8>,
    wal_records: Vec<WalRecord>,
    valid_wal_bytes: Vec<u8>,
    recovered_tail: bool,
}

#[derive(Clone)]
struct WalRecord {
    payload: Vec<u8>,
}

fn parse_file(bytes: &[u8]) -> Result<ParsedFile, StorageError> {
    if bytes.len() < HEADER_SIZE {
        return Err(StorageError::new("file_header", "file too small"));
    }
    let decoded = decode_header(&bytes[..HEADER_SIZE])?;
    let _migration = plan_migration(decoded.format)?;
    let header = decoded.header;
    let snapshot_end = header
        .snapshot_offset
        .checked_add(header.snapshot_len)
        .ok_or_else(|| StorageError::new("file_layout", "invalid snapshot length"))?;
    let wal_end = header
        .wal_offset
        .checked_add(header.wal_len)
        .ok_or_else(|| StorageError::new("file_layout", "invalid wal length"))?;
    if snapshot_end > bytes.len() as u64
        || header.snapshot_offset != HEADER_SIZE as u64
        || header.wal_offset != snapshot_end
        || header.wal_offset > bytes.len() as u64
    {
        return Err(StorageError::new(
            "file_layout",
            "section offsets exceed file size",
        ));
    }

    let snapshot_bytes = bytes[header.snapshot_offset as usize..snapshot_end as usize].to_vec();
    if checksum(&snapshot_bytes) != header.snapshot_checksum {
        return Err(StorageError::new(
            "snapshot_checksum",
            "snapshot checksum mismatch",
        ));
    }

    let wal_bytes = &bytes[header.wal_offset as usize..wal_end.min(bytes.len() as u64) as usize];
    let (wal_records, valid_wal_len, recovered_tail) = parse_wal(wal_bytes)?;
    let recovered_tail = recovered_tail || wal_end > bytes.len() as u64;
    let valid_wal_bytes = wal_bytes[..valid_wal_len].to_vec();
    if !recovered_tail && checksum(&valid_wal_bytes) != header.wal_checksum {
        return Err(StorageError::new("wal_checksum", "wal checksum mismatch"));
    }

    Ok(ParsedFile {
        format: decoded.format,
        header,
        snapshot_bytes,
        wal_records,
        valid_wal_bytes,
        recovered_tail,
    })
}

fn parse_wal(bytes: &[u8]) -> Result<(Vec<WalRecord>, usize, bool), StorageError> {
    let mut cursor = 0usize;
    let mut valid_end = 0usize;
    let mut records = Vec::new();

    while cursor < bytes.len() {
        if bytes.len() - cursor < WAL_HEADER_SIZE || &bytes[cursor..cursor + 4] != WAL_RECORD_MAGIC
        {
            break;
        }
        cursor += 4;
        let seq_no = read_u64(bytes, &mut cursor)?;
        let _tx_id = read_u64(bytes, &mut cursor)?;
        let record_count = read_u32(bytes, &mut cursor)?;
        let payload_len = read_u64(bytes, &mut cursor)?;
        let payload_checksum = read_u64(bytes, &mut cursor)?;
        let tx_checksum = read_u64(bytes, &mut cursor)?;
        if seq_no != records.len() as u64 + 1
            || record_count != 1
            || payload_len > (bytes.len() - cursor) as u64
        {
            break;
        }
        let payload = &bytes[cursor..cursor + payload_len as usize];
        if checksum(payload) != payload_checksum || tx_checksum != payload_checksum {
            break;
        }
        cursor += payload.len();
        records.push(WalRecord {
            payload: payload.to_vec(),
        });
        valid_end = cursor;
    }

    Ok((records, valid_end, valid_end != bytes.len()))
}

fn assemble_file(header: &FileHeader, snapshot: &[u8], wal: &[u8]) -> Vec<u8> {
    let mut output = vec![0; HEADER_SIZE];
    encode_header(header, &mut output);
    output.extend_from_slice(snapshot);
    output.extend_from_slice(wal);
    output
}

fn encode_header(header: &FileHeader, output: &mut [u8]) {
    output[..8].copy_from_slice(MAGIC);
    output[8..12].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    output[12..16].copy_from_slice(&COMPAT_VERSION.to_le_bytes());
    output[16] = u8::from(header.clean);
    output[24..40].copy_from_slice(&header.db_uuid);
    output[40..48].copy_from_slice(&header.snapshot_offset.to_le_bytes());
    output[48..56].copy_from_slice(&header.snapshot_len.to_le_bytes());
    output[56..64].copy_from_slice(&header.wal_offset.to_le_bytes());
    output[64..72].copy_from_slice(&header.wal_len.to_le_bytes());
    output[72..80].copy_from_slice(&header.last_tx_id.to_le_bytes());
    output[80..88].copy_from_slice(&header.snapshot_checksum.to_le_bytes());
    output[88..96].copy_from_slice(&header.wal_checksum.to_le_bytes());
}

struct DecodedHeader {
    format: StorageFormatVersion,
    header: FileHeader,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MigrationPlan {
    rewrite_header: bool,
    target: StorageFormatVersion,
}

fn decode_header(bytes: &[u8]) -> Result<DecodedHeader, StorageError> {
    if &bytes[..8] != MAGIC {
        return Err(StorageError::new("file_magic", "invalid .cupld header"));
    }
    Ok(DecodedHeader {
        format: StorageFormatVersion {
            version: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            compat: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
        },
        header: FileHeader {
            clean: bytes[16] == 1,
            db_uuid: bytes[24..40].try_into().unwrap(),
            snapshot_offset: u64::from_le_bytes(bytes[40..48].try_into().unwrap()),
            snapshot_len: u64::from_le_bytes(bytes[48..56].try_into().unwrap()),
            wal_offset: u64::from_le_bytes(bytes[56..64].try_into().unwrap()),
            wal_len: u64::from_le_bytes(bytes[64..72].try_into().unwrap()),
            last_tx_id: u64::from_le_bytes(bytes[72..80].try_into().unwrap()),
            snapshot_checksum: u64::from_le_bytes(bytes[80..88].try_into().unwrap()),
            wal_checksum: u64::from_le_bytes(bytes[88..96].try_into().unwrap()),
        },
    })
}

fn plan_migration(format: StorageFormatVersion) -> Result<MigrationPlan, StorageError> {
    if format.version == FORMAT_VERSION && format.compat == COMPAT_VERSION {
        return Ok(MigrationPlan {
            rewrite_header: false,
            target: format,
        });
    }
    if format.version == PREVIOUS_FORMAT_VERSION && format.compat == COMPAT_VERSION {
        return Ok(MigrationPlan {
            rewrite_header: true,
            target: StorageFormatVersion {
                version: FORMAT_VERSION,
                compat: COMPAT_VERSION,
            },
        });
    }
    if format.version == OLDER_FORMAT_VERSION && format.compat == COMPAT_VERSION {
        return Ok(MigrationPlan {
            rewrite_header: true,
            target: StorageFormatVersion {
                version: FORMAT_VERSION,
                compat: COMPAT_VERSION,
            },
        });
    }
    if format.version == LEGACY_FORMAT_VERSION && format.compat == LEGACY_COMPAT_VERSION {
        return Ok(MigrationPlan {
            rewrite_header: true,
            target: StorageFormatVersion {
                version: FORMAT_VERSION,
                compat: COMPAT_VERSION,
            },
        });
    }
    Err(StorageError::new(
        "file_version",
        "unsupported file format version",
    ))
}

fn encode_wal_record(seq_no: u64, tx_id: u64, payload: &[u8]) -> Vec<u8> {
    let payload_checksum = checksum(payload);
    let mut output = Vec::new();
    output.extend_from_slice(WAL_RECORD_MAGIC);
    output.extend_from_slice(&seq_no.to_le_bytes());
    output.extend_from_slice(&tx_id.to_le_bytes());
    output.extend_from_slice(&1u32.to_le_bytes());
    output.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    output.extend_from_slice(&payload_checksum.to_le_bytes());
    output.extend_from_slice(&payload_checksum.to_le_bytes());
    output.extend_from_slice(payload);
    output
}

fn encode_state(state: &EngineState) -> Result<Vec<u8>, StorageError> {
    let mut bytes = Vec::new();
    push_u64(&mut bytes, state.next_tx_id);
    push_u64(&mut bytes, state.next_node_id);
    push_u64(&mut bytes, state.next_edge_id);
    encode_schema_state(&mut bytes, &state.schema);
    push_u32(&mut bytes, state.nodes.len() as u32);
    for node in &state.nodes {
        push_u64(&mut bytes, node.id);
        push_strings(&mut bytes, &node.labels);
        encode_property_map(&mut bytes, node.properties.iter())?;
        push_optional_system_time(&mut bytes, node.valid_from);
        push_optional_system_time(&mut bytes, node.valid_to);
    }
    push_u32(&mut bytes, state.edges.len() as u32);
    for edge in &state.edges {
        push_u64(&mut bytes, edge.id);
        push_u64(&mut bytes, edge.from);
        push_u64(&mut bytes, edge.to);
        push_string(&mut bytes, &edge.edge_type);
        encode_property_map(&mut bytes, edge.properties.iter())?;
        push_optional_system_time(&mut bytes, edge.valid_from);
        push_optional_system_time(&mut bytes, edge.valid_to);
    }
    Ok(bytes)
}

fn decode_state(bytes: &[u8], format: StorageFormatVersion) -> Result<EngineState, StorageError> {
    let mut cursor = 0usize;
    let next_tx_id = read_u64(bytes, &mut cursor)?;
    let next_node_id = read_u64(bytes, &mut cursor)?;
    let next_edge_id = read_u64(bytes, &mut cursor)?;
    let schema = decode_schema_state(bytes, &mut cursor, format)?;
    let node_count = read_u32(bytes, &mut cursor)? as usize;
    let mut nodes = Vec::with_capacity(node_count);
    for _ in 0..node_count {
        nodes.push(NodeState {
            id: read_u64(bytes, &mut cursor)?,
            labels: read_strings(bytes, &mut cursor)?,
            properties: decode_property_map(bytes, &mut cursor)?,
            valid_from: if format.version >= FORMAT_VERSION {
                read_optional_system_time(bytes, &mut cursor)?
            } else {
                None
            },
            valid_to: if format.version >= FORMAT_VERSION {
                read_optional_system_time(bytes, &mut cursor)?
            } else {
                None
            },
        });
    }
    let edge_count = read_u32(bytes, &mut cursor)? as usize;
    let mut edges = Vec::with_capacity(edge_count);
    for _ in 0..edge_count {
        edges.push(EdgeState {
            id: read_u64(bytes, &mut cursor)?,
            from: read_u64(bytes, &mut cursor)?,
            to: read_u64(bytes, &mut cursor)?,
            edge_type: read_string(bytes, &mut cursor)?,
            properties: decode_property_map(bytes, &mut cursor)?,
            valid_from: if format.version >= FORMAT_VERSION {
                read_optional_system_time(bytes, &mut cursor)?
            } else {
                None
            },
            valid_to: if format.version >= FORMAT_VERSION {
                read_optional_system_time(bytes, &mut cursor)?
            } else {
                None
            },
        });
    }
    Ok(EngineState {
        next_tx_id,
        next_node_id,
        next_edge_id,
        nodes,
        edges,
        schema,
    })
}

fn encode_schema_state(output: &mut Vec<u8>, state: &SchemaState) {
    push_strings(output, &state.labels);
    push_strings(output, &state.edge_types);
    push_u32(output, state.object_options.len() as u32);
    for object in &state.object_options {
        push_schema_target(output, &object.target);
        push_optional_string(output, object.description.as_deref());
    }
    push_u32(output, state.indexes.len() as u32);
    for index in &state.indexes {
        push_string(output, &index.name);
        push_schema_target(output, &index.target);
        push_string(output, &index.property);
        push_u8(
            output,
            match index.kind {
                IndexKind::Equality => 0,
                IndexKind::Range => 1,
                IndexKind::ListMembership => 2,
                IndexKind::FullText => 3,
            },
        );
        push_bool(output, index.unique);
        push_u8(
            output,
            match index.status {
                IndexStatus::Ready => 0,
                IndexStatus::Building => 1,
                IndexStatus::Invalid => 2,
            },
        );
        push_optional_string(output, index.owned_by_constraint.as_deref());
    }
    push_u32(output, state.constraints.len() as u32);
    for constraint in &state.constraints {
        push_string(output, &constraint.name);
        push_schema_target(output, &constraint.target);
        push_string(output, &constraint.property);
        match &constraint.constraint_type {
            crate::engine::ConstraintType::Unique => push_u8(output, 0),
            crate::engine::ConstraintType::Required => push_u8(output, 1),
            crate::engine::ConstraintType::Type(kind) => {
                push_u8(output, 2);
                push_property_type(output, *kind);
            }
            crate::engine::ConstraintType::Endpoints {
                from_label,
                to_label,
            } => {
                push_u8(output, 3);
                push_string(output, from_label);
                push_string(output, to_label);
            }
            crate::engine::ConstraintType::MaxOutgoing(limit) => {
                push_u8(output, 4);
                push_u32(output, *limit as u32);
            }
        }
    }
}

fn decode_schema_state(
    bytes: &[u8],
    cursor: &mut usize,
    format: StorageFormatVersion,
) -> Result<SchemaState, StorageError> {
    let labels = read_strings(bytes, cursor)?;
    let edge_types = read_strings(bytes, cursor)?;
    let object_options = if format.version >= PREVIOUS_FORMAT_VERSION {
        let object_count = read_u32(bytes, cursor)? as usize;
        let mut object_options = Vec::with_capacity(object_count);
        for _ in 0..object_count {
            object_options.push(SchemaObjectState {
                target: read_schema_target(bytes, cursor)?,
                description: read_optional_string(bytes, cursor)?,
            });
        }
        object_options
    } else {
        Vec::new()
    };
    let index_count = read_u32(bytes, cursor)? as usize;
    let mut indexes = Vec::with_capacity(index_count);
    for _ in 0..index_count {
        indexes.push(IndexState {
            name: read_string(bytes, cursor)?,
            target: read_schema_target(bytes, cursor)?,
            property: read_string(bytes, cursor)?,
            kind: if format.version >= FORMAT_VERSION {
                match read_u8(bytes, cursor)? {
                    0 => IndexKind::Equality,
                    1 => IndexKind::Range,
                    2 => IndexKind::ListMembership,
                    3 => IndexKind::FullText,
                    _ => return Err(StorageError::new("index_kind", "invalid index kind")),
                }
            } else {
                IndexKind::Equality
            },
            unique: read_bool(bytes, cursor)?,
            status: match read_u8(bytes, cursor)? {
                0 => IndexStatus::Ready,
                1 => IndexStatus::Building,
                2 => IndexStatus::Invalid,
                _ => return Err(StorageError::new("index_status", "invalid index status")),
            },
            owned_by_constraint: read_optional_string(bytes, cursor)?,
        });
    }
    let constraint_count = read_u32(bytes, cursor)? as usize;
    let mut constraints = Vec::with_capacity(constraint_count);
    for _ in 0..constraint_count {
        let name = read_string(bytes, cursor)?;
        let target = read_schema_target(bytes, cursor)?;
        let property = read_string(bytes, cursor)?;
        let tag = read_u8(bytes, cursor)?;
        let constraint_type = match tag {
            0 => crate::engine::ConstraintType::Unique,
            1 => crate::engine::ConstraintType::Required,
            2 => crate::engine::ConstraintType::Type(read_property_type(bytes, cursor)?),
            3 => crate::engine::ConstraintType::Endpoints {
                from_label: read_string(bytes, cursor)?,
                to_label: read_string(bytes, cursor)?,
            },
            4 => crate::engine::ConstraintType::MaxOutgoing(read_u32(bytes, cursor)? as usize),
            _ => {
                return Err(StorageError::new(
                    "constraint_type",
                    "invalid constraint type",
                ));
            }
        };
        constraints.push(ConstraintState {
            name,
            target,
            property,
            constraint_type,
        });
    }
    Ok(SchemaState {
        labels,
        edge_types,
        object_options,
        indexes,
        constraints,
    })
}

fn encode_property_map<'a, I>(output: &mut Vec<u8>, map: I) -> Result<(), StorageError>
where
    I: IntoIterator<Item = (&'a str, &'a Value)>,
{
    let entries = map.into_iter().collect::<Vec<_>>();
    push_u32(output, entries.len() as u32);
    for (key, value) in entries {
        push_string(output, key);
        encode_value(output, value)?;
    }
    Ok(())
}

fn decode_property_map(bytes: &[u8], cursor: &mut usize) -> Result<PropertyMap, StorageError> {
    let count = read_u32(bytes, cursor)? as usize;
    let mut map = PropertyMap::new();
    for _ in 0..count {
        let key = read_string(bytes, cursor)?;
        let value = decode_value(bytes, cursor)?;
        map.insert(key, value);
    }
    Ok(map)
}

fn encode_value(output: &mut Vec<u8>, value: &Value) -> Result<(), StorageError> {
    match value {
        Value::Null => push_u8(output, 0),
        Value::Bool(value) => {
            push_u8(output, 1);
            push_bool(output, *value);
        }
        Value::Int(value) => {
            push_u8(output, 2);
            push_i64(output, *value);
        }
        Value::Float(value) => {
            push_u8(output, 3);
            output.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        Value::String(value) => {
            push_u8(output, 4);
            push_string(output, value);
        }
        Value::Bytes(value) => {
            push_u8(output, 5);
            push_bytes(output, value);
        }
        Value::Datetime(value) => {
            push_u8(output, 6);
            let (secs, nanos) = system_time_parts(*value);
            push_i64(output, secs);
            push_u32(output, nanos);
        }
        Value::List(values) => {
            push_u8(output, 7);
            push_u32(output, values.len() as u32);
            for value in values {
                encode_value(output, value)?;
            }
        }
        Value::Map(map) => {
            push_u8(output, 8);
            encode_property_map(output, map.iter().map(|(key, value)| (key.as_str(), value)))?;
        }
    }
    Ok(())
}

fn decode_value(bytes: &[u8], cursor: &mut usize) -> Result<Value, StorageError> {
    Ok(match read_u8(bytes, cursor)? {
        0 => Value::Null,
        1 => Value::Bool(read_bool(bytes, cursor)?),
        2 => Value::Int(read_i64(bytes, cursor)?),
        3 => Value::Float(f64::from_bits(read_u64(bytes, cursor)?)),
        4 => Value::String(read_string(bytes, cursor)?),
        5 => Value::Bytes(read_bytes(bytes, cursor)?),
        6 => Value::Datetime(system_time_from_parts(
            read_i64(bytes, cursor)?,
            read_u32(bytes, cursor)?,
        )),
        7 => {
            let count = read_u32(bytes, cursor)? as usize;
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                values.push(decode_value(bytes, cursor)?);
            }
            Value::List(values)
        }
        8 => Value::from(decode_property_map(bytes, cursor)?),
        _ => return Err(StorageError::new("value_tag", "invalid value tag")),
    })
}

fn push_schema_target(output: &mut Vec<u8>, target: &crate::engine::SchemaTarget) {
    push_u8(
        output,
        match target.kind() {
            crate::engine::TargetKind::Label => 0,
            crate::engine::TargetKind::EdgeType => 1,
        },
    );
    push_string(output, target.name());
}

fn read_schema_target(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<crate::engine::SchemaTarget, StorageError> {
    Ok(match read_u8(bytes, cursor)? {
        0 => crate::engine::SchemaTarget::label(read_string(bytes, cursor)?),
        1 => crate::engine::SchemaTarget::edge_type(read_string(bytes, cursor)?),
        _ => return Err(StorageError::new("schema_target", "invalid schema target")),
    })
}

fn push_property_type(output: &mut Vec<u8>, kind: crate::engine::PropertyType) {
    push_u8(
        output,
        match kind {
            crate::engine::PropertyType::String => 0,
            crate::engine::PropertyType::Int => 1,
            crate::engine::PropertyType::Float => 2,
            crate::engine::PropertyType::Bool => 3,
            crate::engine::PropertyType::Bytes => 4,
            crate::engine::PropertyType::Datetime => 5,
            crate::engine::PropertyType::List => 6,
            crate::engine::PropertyType::Map => 7,
            crate::engine::PropertyType::Null => 8,
        },
    );
}

fn read_property_type(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<crate::engine::PropertyType, StorageError> {
    Ok(match read_u8(bytes, cursor)? {
        0 => crate::engine::PropertyType::String,
        1 => crate::engine::PropertyType::Int,
        2 => crate::engine::PropertyType::Float,
        3 => crate::engine::PropertyType::Bool,
        4 => crate::engine::PropertyType::Bytes,
        5 => crate::engine::PropertyType::Datetime,
        6 => crate::engine::PropertyType::List,
        7 => crate::engine::PropertyType::Map,
        8 => crate::engine::PropertyType::Null,
        _ => return Err(StorageError::new("property_type", "invalid property type")),
    })
}

fn file_uuid() -> [u8; 16] {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let pid = std::process::id() as u128;
    (nanos ^ (pid << 64)).to_le_bytes()
}

fn push_optional_system_time(output: &mut Vec<u8>, value: Option<SystemTime>) {
    push_bool(output, value.is_some());
    if let Some(value) = value {
        let (secs, nanos) = system_time_parts(value);
        push_i64(output, secs);
        push_u32(output, nanos);
    }
}

fn read_optional_system_time(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<Option<SystemTime>, StorageError> {
    if !read_bool(bytes, cursor)? {
        return Ok(None);
    }
    Ok(Some(system_time_from_parts(
        read_i64(bytes, cursor)?,
        read_u32(bytes, cursor)?,
    )))
}

/// Resolve aliases before deriving the stable lock path or replacing a file.
pub(crate) fn canonical_database_path(path: &Path) -> Result<PathBuf, StorageError> {
    match fs::canonicalize(path) {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            // A dangling symlink is not a new database destination.
            if fs::symlink_metadata(path).is_ok() {
                return Err(error.into());
            }
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            let name = database_file_name(path)?;
            Ok(fs::canonicalize(parent)?.join(name))
        }
        Err(error) => Err(error.into()),
    }
}

fn database_file_name(path: &Path) -> io::Result<&std::ffi::OsStr> {
    path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "database path has no file name",
        )
    })
}

struct DatabaseWriter {
    path: PathBuf,
    // Never unlink the sidecar: unlinking would let another writer lock a new inode.
    _lock: fs::File,
}

impl Drop for DatabaseWriter {
    fn drop(&mut self) {
        // A concurrent process spawn can briefly inherit this descriptor before
        // exec closes it. Unlock explicitly so an inherited/duplicated handle
        // cannot extend the write scope after this guard is dropped.
        let _ = self._lock.unlock();
    }
}

impl DatabaseWriter {
    fn acquire(path: &Path) -> Result<Self, StorageError> {
        let path = canonical_database_path(path)?;
        let mut lock_name = database_file_name(&path)?.to_os_string();
        lock_name.push(".lock");
        let lock_path = path.with_file_name(lock_name);
        validate_lock_sidecar(&lock_path)?;
        // Owner-only, like the temporary file: an advisory lock needs only an
        // open descriptor, so a readable sidecar lets any local user block writes.
        let mut options = fs::OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let lock = options.open(&lock_path)?;
        validate_lock_sidecar(&lock_path)?;
        match lock.try_lock() {
            Ok(()) => Ok(Self { path, _lock: lock }),
            Err(fs::TryLockError::WouldBlock) => Err(StorageError::new(
                StorageErrorKind::DatabaseBusy,
                "another writer is updating this database; retry after it completes",
            )),
            Err(fs::TryLockError::Error(error)) => Err(error.into()),
        }
    }

    fn read_expected(&self, expected: StorageRevision) -> Result<Vec<u8>, StorageError> {
        let bytes = match fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(StorageError::new(
                    StorageErrorKind::DatabaseChanged,
                    "database was removed since it was opened; reopen before writing",
                ));
            }
            Err(error) => return Err(error.into()),
        };
        if StorageRevision::from_bytes(&bytes) != expected {
            return Err(StorageError::new(
                StorageErrorKind::DatabaseChanged,
                "database changed since it was opened; reopen before writing",
            ));
        }
        Ok(bytes)
    }
}

fn validate_lock_sidecar(path: &Path) -> Result<(), StorageError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let valid = metadata.is_file() && metadata.len() == 0;
    #[cfg(unix)]
    let valid = {
        use std::os::unix::fs::MetadataExt;
        valid && metadata.nlink() == 1
    };
    if !valid {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "database lock sidecar must be an empty regular file without aliases",
        )
        .into());
    }
    Ok(())
}

struct PendingFile(PathBuf);

impl Drop for PendingFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn write_durable(path: &Path, bytes: &[u8]) -> Result<(), StorageError> {
    let permissions = match fs::metadata(path) {
        Ok(metadata) => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if metadata.nlink() > 1 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "database has multiple hard links; use one database path",
                    )
                    .into());
                }
            }
            if metadata.permissions().readonly() {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "database file is read-only",
                )
                .into());
            }
            // Atomic rename depends on directory permissions; also respect the
            // existing file's OS write access before replacing it.
            fs::OpenOptions::new().write(true).open(path)?;
            Some(metadata.permissions())
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    let (temporary, mut file) = loop {
        let mut name = std::ffi::OsString::from(".");
        name.push(path.file_name().expect("canonical file name"));
        name.push(format!(
            ".{}-{}.tmp",
            std::process::id(),
            TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let temporary = path.with_file_name(name);
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&temporary) {
            Ok(file) => break (PendingFile(temporary), file),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    };
    if let Some(permissions) = permissions {
        file.set_permissions(permissions)?;
    }
    let middle = bytes.len() / 2;
    file.write_all(&bytes[..middle])?;
    #[cfg(test)]
    inject_write_failure(WriteStage::PartialWrite)?;
    file.write_all(&bytes[middle..])?;
    #[cfg(test)]
    inject_write_failure(WriteStage::FileSync)?;
    file.sync_all()?;
    drop(file);
    #[cfg(test)]
    inject_write_failure(WriteStage::Rename)?;
    fs::rename(&temporary.0, path)?;
    // From this point the new database is visible. A failed directory flush
    // cannot be treated as a rolled-back commit or safely retried blindly.
    sync_parent(path).map_err(|error| StorageError::new(StorageErrorKind::PersistenceUncertain,
        format!("database replacement is visible but directory sync failed ({error}); reopen before writing")))?;
    Ok(())
}

fn sync_parent(path: &Path) -> io::Result<()> {
    #[cfg(test)]
    inject_write_failure(WriteStage::DirectorySync)?;
    #[cfg(unix)]
    {
        let parent = path.parent().expect("canonical parent directory");
        match fs::File::open(parent)?.sync_all() {
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::InvalidInput | io::ErrorKind::Unsupported
                ) =>
            {
                Ok(())
            }
            result => result,
        }
    }
    #[cfg(not(unix))]
    {
        // std has no portable way to flush a directory on Windows.
        let _ = path;
        Ok(())
    }
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriteStage {
    PartialWrite,
    FileSync,
    Rename,
    DirectorySync,
}

#[cfg(test)]
thread_local! {
    static WRITE_FAILURE: std::cell::Cell<Option<WriteStage>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
pub(crate) fn fail_next_write_at(stage: WriteStage) {
    WRITE_FAILURE.set(Some(stage));
}

#[cfg(test)]
fn inject_write_failure(stage: WriteStage) -> io::Result<()> {
    if WRITE_FAILURE.get() == Some(stage) {
        WRITE_FAILURE.set(None);
        Err(io::Error::other("injected persistence failure"))
    } else {
        Ok(())
    }
}

fn system_time_parts(value: SystemTime) -> (i64, u32) {
    match value.duration_since(UNIX_EPOCH) {
        Ok(duration) => (duration.as_secs() as i64, duration.subsec_nanos()),
        Err(error) => {
            let duration = error.duration();
            (-(duration.as_secs() as i64), duration.subsec_nanos())
        }
    }
}

fn system_time_from_parts(secs: i64, nanos: u32) -> SystemTime {
    if secs >= 0 {
        UNIX_EPOCH + std::time::Duration::new(secs as u64, nanos)
    } else {
        UNIX_EPOCH - std::time::Duration::new((-secs) as u64, nanos)
    }
}

fn checksum(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn push_u8(output: &mut Vec<u8>, value: u8) {
    output.push(value);
}

fn push_bool(output: &mut Vec<u8>, value: bool) {
    push_u8(output, u8::from(value));
}

fn push_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn push_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn push_i64(output: &mut Vec<u8>, value: i64) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn push_string(output: &mut Vec<u8>, value: &str) {
    push_u32(output, value.len() as u32);
    output.extend_from_slice(value.as_bytes());
}

fn push_optional_string(output: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            push_bool(output, true);
            push_string(output, value);
        }
        None => push_bool(output, false),
    }
}

fn push_strings(output: &mut Vec<u8>, values: &[String]) {
    push_u32(output, values.len() as u32);
    for value in values {
        push_string(output, value);
    }
}

fn push_bytes(output: &mut Vec<u8>, value: &[u8]) {
    push_u32(output, value.len() as u32);
    output.extend_from_slice(value);
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8, StorageError> {
    if *cursor >= bytes.len() {
        return Err(StorageError::new("decode_eof", "unexpected end of input"));
    }
    let value = bytes[*cursor];
    *cursor += 1;
    Ok(value)
}

fn read_bool(bytes: &[u8], cursor: &mut usize) -> Result<bool, StorageError> {
    Ok(read_u8(bytes, cursor)? == 1)
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, StorageError> {
    if bytes.len() - *cursor < 4 {
        return Err(StorageError::new("decode_eof", "unexpected end of input"));
    }
    let value = u32::from_le_bytes(bytes[*cursor..*cursor + 4].try_into().unwrap());
    *cursor += 4;
    Ok(value)
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, StorageError> {
    if bytes.len() - *cursor < 8 {
        return Err(StorageError::new("decode_eof", "unexpected end of input"));
    }
    let value = u64::from_le_bytes(bytes[*cursor..*cursor + 8].try_into().unwrap());
    *cursor += 8;
    Ok(value)
}

fn read_i64(bytes: &[u8], cursor: &mut usize) -> Result<i64, StorageError> {
    if bytes.len() - *cursor < 8 {
        return Err(StorageError::new("decode_eof", "unexpected end of input"));
    }
    let value = i64::from_le_bytes(bytes[*cursor..*cursor + 8].try_into().unwrap());
    *cursor += 8;
    Ok(value)
}

fn read_string(bytes: &[u8], cursor: &mut usize) -> Result<String, StorageError> {
    let len = read_u32(bytes, cursor)? as usize;
    if bytes.len() - *cursor < len {
        return Err(StorageError::new("decode_eof", "unexpected end of input"));
    }
    let value = String::from_utf8(bytes[*cursor..*cursor + len].to_vec())
        .map_err(|_| StorageError::new("utf8", "invalid utf-8 string"))?;
    *cursor += len;
    Ok(value)
}

fn read_optional_string(bytes: &[u8], cursor: &mut usize) -> Result<Option<String>, StorageError> {
    if read_bool(bytes, cursor)? {
        Ok(Some(read_string(bytes, cursor)?))
    } else {
        Ok(None)
    }
}

fn read_strings(bytes: &[u8], cursor: &mut usize) -> Result<Vec<String>, StorageError> {
    let count = read_u32(bytes, cursor)? as usize;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(read_string(bytes, cursor)?);
    }
    Ok(values)
}

fn read_bytes(bytes: &[u8], cursor: &mut usize) -> Result<Vec<u8>, StorageError> {
    let len = read_u32(bytes, cursor)? as usize;
    if bytes.len() - *cursor < len {
        return Err(StorageError::new("decode_eof", "unexpected end of input"));
    }
    let value = bytes[*cursor..*cursor + len].to_vec();
    *cursor += len;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::{Path, PathBuf};

    use super::{
        COMPAT_VERSION, DatabaseWriter, FORMAT_VERSION, FileHeader, HEADER_SIZE, IndexStatus,
        StorageError, WriteStage, append_commit, assemble_file, check, check_without_migration,
        checksum, compact, encode_property_map, encode_wal_record, fail_next_write_at, file_uuid,
        load, load_without_migration, parse_file, push_bool, push_optional_string,
        push_property_type, push_schema_target, push_string, push_strings, push_u8, push_u32,
        push_u64, save_compacted,
    };
    use crate::engine::{EngineState, IndexKind};
    use crate::runtime::Session;

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "{}_{}_{}.cupld",
            name,
            std::process::id(),
            super::TEMP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ))
    }

    fn lock_path(path: &Path) -> PathBuf {
        let mut name = path.file_name().unwrap().to_os_string();
        name.push(".lock");
        path.with_file_name(name)
    }

    /// Remove a test database together with the `.lock` sidecar beside it.
    fn remove_database(path: &Path) {
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(lock_path(path));
    }

    fn copy_fixture(name: &str) -> PathBuf {
        let path = temp_path(&format!("cupld_storage_fixture_{name}"));
        fs::copy(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests")
                .join("fixtures")
                .join(name),
            &path,
        )
        .unwrap();
        path
    }

    fn encode_state_v2(state: &EngineState) -> Result<Vec<u8>, StorageError> {
        let mut bytes = Vec::new();
        push_u64(&mut bytes, state.next_tx_id);
        push_u64(&mut bytes, state.next_node_id);
        push_u64(&mut bytes, state.next_edge_id);
        push_strings(&mut bytes, &state.schema.labels);
        push_strings(&mut bytes, &state.schema.edge_types);
        push_u32(&mut bytes, state.schema.object_options.len() as u32);
        for object in &state.schema.object_options {
            push_schema_target(&mut bytes, &object.target);
            push_optional_string(&mut bytes, object.description.as_deref());
        }
        push_u32(&mut bytes, state.schema.indexes.len() as u32);
        for index in &state.schema.indexes {
            push_string(&mut bytes, &index.name);
            push_schema_target(&mut bytes, &index.target);
            push_string(&mut bytes, &index.property);
            push_bool(&mut bytes, index.unique);
            push_u8(
                &mut bytes,
                match index.status {
                    IndexStatus::Ready => 0,
                    IndexStatus::Building => 1,
                    IndexStatus::Invalid => 2,
                },
            );
            push_optional_string(&mut bytes, index.owned_by_constraint.as_deref());
        }
        push_u32(&mut bytes, state.schema.constraints.len() as u32);
        for constraint in &state.schema.constraints {
            push_string(&mut bytes, &constraint.name);
            push_schema_target(&mut bytes, &constraint.target);
            push_string(&mut bytes, &constraint.property);
            match &constraint.constraint_type {
                crate::engine::ConstraintType::Unique => push_u8(&mut bytes, 0),
                crate::engine::ConstraintType::Required => push_u8(&mut bytes, 1),
                crate::engine::ConstraintType::Type(kind) => {
                    push_u8(&mut bytes, 2);
                    push_property_type(&mut bytes, *kind);
                }
                crate::engine::ConstraintType::Endpoints {
                    from_label,
                    to_label,
                } => {
                    push_u8(&mut bytes, 3);
                    push_string(&mut bytes, from_label);
                    push_string(&mut bytes, to_label);
                }
                crate::engine::ConstraintType::MaxOutgoing(limit) => {
                    push_u8(&mut bytes, 4);
                    push_u32(&mut bytes, *limit as u32);
                }
            }
        }
        push_u32(&mut bytes, state.nodes.len() as u32);
        for node in &state.nodes {
            push_u64(&mut bytes, node.id);
            push_strings(&mut bytes, &node.labels);
            encode_property_map(&mut bytes, node.properties.iter())?;
        }
        push_u32(&mut bytes, state.edges.len() as u32);
        for edge in &state.edges {
            push_u64(&mut bytes, edge.id);
            push_u64(&mut bytes, edge.from);
            push_u64(&mut bytes, edge.to);
            push_string(&mut bytes, &edge.edge_type);
            encode_property_map(&mut bytes, edge.properties.iter())?;
        }
        Ok(bytes)
    }

    #[test]
    fn round_trips_snapshot_and_wal() {
        let path = temp_path("cupld_storage_round_trip");
        let mut session = Session::new_in_memory();
        session
            .execute_script("CREATE (n:Person {name: 'Ada'})", &BTreeMap::new())
            .unwrap();
        let uuid = save_compacted(&path, session.engine()).unwrap();

        session
            .execute_script("CREATE (n:Person {name: 'Grace'})", &BTreeMap::new())
            .unwrap();
        append_commit(&path, session.engine(), uuid).unwrap();

        let (engine, report) = load(&path).unwrap();
        assert_eq!(report.wal_records, 1);
        assert_eq!(engine.stats().node_count, 2);

        remove_database(&path);
    }

    #[test]
    fn compact_resets_wal() {
        let path = temp_path("cupld_storage_compact");
        let mut session = Session::new_in_memory();
        session
            .execute_script("CREATE (n:Person {name: 'Ada'})", &BTreeMap::new())
            .unwrap();
        let uuid = save_compacted(&path, session.engine()).unwrap();
        session
            .execute_script("CREATE (n:Person {name: 'Grace'})", &BTreeMap::new())
            .unwrap();
        let uuid = append_commit(&path, session.engine(), uuid).unwrap();
        compact(&path, session.engine(), uuid).unwrap();

        let report = check(&path).unwrap();
        assert_eq!(report.wal_records, 0);

        remove_database(&path);
    }

    #[test]
    fn check_migrates_legacy_header_versions_in_place() {
        let path = copy_fixture("person_v0_1_0.cupld");
        let report = check(&path).unwrap();
        assert_eq!(report.wal_records, 8);
        let bytes = fs::read(&path).unwrap();
        assert_eq!(
            u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            FORMAT_VERSION
        );
        assert_eq!(
            u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            COMPAT_VERSION
        );
        assert_eq!(load(&path).unwrap().0.stats().node_count, 4);
        remove_database(&path);
    }

    #[test]
    fn no_migration_load_and_check_preserve_legacy_fixture_bytes() {
        let path = copy_fixture("person_v0_1_0.cupld");
        let original = fs::read(&path).unwrap();
        assert_eq!(u32::from_le_bytes(original[8..12].try_into().unwrap()), 1);

        let (engine, report) = load_without_migration(&path).unwrap();
        assert_eq!(engine.stats().node_count, 4);
        assert_eq!(report.wal_records, 8);
        assert_eq!(fs::read(&path).unwrap(), original);

        let report = check_without_migration(&path).unwrap();
        assert_eq!(report.wal_records, 8);
        assert_eq!(fs::read(&path).unwrap(), original);

        remove_database(&path);
    }

    #[test]
    fn unsupported_future_header_versions_fail() {
        let path = temp_path("cupld_storage_future");
        let mut session = Session::new_in_memory();
        session
            .execute_script("CREATE (n:Person {name: 'Ada'})", &BTreeMap::new())
            .unwrap();
        save_compacted(&path, session.engine()).unwrap();

        let mut bytes = fs::read(&path).unwrap();
        bytes[8..12].copy_from_slice(&99u32.to_le_bytes());
        bytes[12..16].copy_from_slice(&99u32.to_le_bytes());
        fs::write(&path, &bytes).unwrap();

        let error = check(&path).unwrap_err();
        assert_eq!(error.code(), "file_version");

        remove_database(&path);
    }

    #[test]
    fn migrates_v2_defaults_for_index_kinds_and_temporal_validity() {
        let path = temp_path("cupld_storage_v2_defaults");
        let mut session = Session::new_in_memory();
        session
            .execute_script(
                "CREATE (:Doc {title: 'Legacy', published: 2024})",
                &BTreeMap::new(),
            )
            .unwrap();
        session
            .execute_script("CREATE INDEX ON :Doc(published)", &BTreeMap::new())
            .unwrap();

        let snapshot = encode_state_v2(&session.engine().to_state()).unwrap();
        let header = FileHeader {
            clean: true,
            db_uuid: file_uuid(),
            snapshot_offset: HEADER_SIZE as u64,
            snapshot_len: snapshot.len() as u64,
            wal_offset: (HEADER_SIZE + snapshot.len()) as u64,
            wal_len: 0,
            last_tx_id: session.engine().snapshot().tx_id().get(),
            snapshot_checksum: checksum(&snapshot),
            wal_checksum: checksum(&[]),
        };
        let bytes = assemble_file(&header, &snapshot, &[]);
        let mut bytes = bytes;
        bytes[8..12].copy_from_slice(&2u32.to_le_bytes());
        bytes[12..16].copy_from_slice(&1u32.to_le_bytes());
        fs::write(&path, &bytes).unwrap();

        let (engine, _) = load(&path).unwrap();
        let node = engine.nodes().next().unwrap();
        assert_eq!(node.valid_from(), None);
        assert_eq!(node.valid_to(), None);
        let index = engine.schema_catalog().indexes().next().unwrap();
        assert_eq!(index.kind(), IndexKind::Equality);

        let checked = check(&path).unwrap();
        assert_eq!(checked.wal_records, 0);
        let migrated = fs::read(&path).unwrap();
        assert_eq!(
            u32::from_le_bytes(migrated[8..12].try_into().unwrap()),
            FORMAT_VERSION
        );

        remove_database(&path);
    }
    fn create_committed_database(name: &str) -> (PathBuf, Session) {
        let path = temp_path(name);
        let mut session = Session::new_in_memory();
        session
            .execute_script("CREATE (:Doc {name: 'original'})", &BTreeMap::new())
            .unwrap();
        session.save_as(&path).unwrap();
        (path, session)
    }

    #[test]
    fn failed_replacements_preserve_disk_and_autocommit_state() {
        for stage in [
            WriteStage::PartialWrite,
            WriteStage::FileSync,
            WriteStage::Rename,
        ] {
            let (path, mut session) = create_committed_database("failed_autocommit");
            let original = fs::read(&path).unwrap();
            let original_tx = session.transaction_info().last_tx_id;
            fail_next_write_at(stage);
            let error = session
                .execute_script("CREATE (:Doc {name: 'failed'})", &BTreeMap::new())
                .unwrap_err();
            assert_eq!(error.code(), "io_error");
            assert_eq!(fs::read(&path).unwrap(), original);
            assert_eq!(session.engine().stats().node_count, 1);
            assert_eq!(session.transaction_info().last_tx_id, original_tx);
            assert!(!session.is_dirty());
            assert_eq!(load(&path).unwrap().0.stats().node_count, 1);
            let prefix = format!(".{}.", path.file_name().unwrap().to_string_lossy());
            assert!(!fs::read_dir(path.parent().unwrap()).unwrap().any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(&prefix)
            }));
            session
                .execute_script("CREATE (:Doc {name: 'retry'})", &BTreeMap::new())
                .unwrap();
            assert_eq!(load(&path).unwrap().0.stats().node_count, 2);
            remove_database(&path);
        }
    }

    #[test]
    fn failed_explicit_commit_retains_pending_transaction_for_retry() {
        let (path, mut session) = create_committed_database("failed_transaction");
        let original = fs::read(&path).unwrap();
        let original_tx = session.transaction_info().last_tx_id;
        session.execute_script("BEGIN", &BTreeMap::new()).unwrap();
        session
            .execute_script("CREATE (:Doc {name: 'pending'})", &BTreeMap::new())
            .unwrap();
        fail_next_write_at(WriteStage::FileSync);
        assert_eq!(
            session
                .execute_script("COMMIT", &BTreeMap::new())
                .unwrap_err()
                .code(),
            "io_error"
        );
        assert_eq!(fs::read(&path).unwrap(), original);
        assert!(session.transaction_info().active);
        assert_eq!(session.transaction_info().last_tx_id, original_tx);
        assert_eq!(session.engine().stats().node_count, 2);
        session.execute_script("COMMIT", &BTreeMap::new()).unwrap();
        assert_eq!(session.transaction_info().last_tx_id, original_tx + 1);
        assert_eq!(load(&path).unwrap().0.stats().node_count, 2);
        remove_database(&path);
    }

    #[test]
    fn failed_save_preserves_replacement_engine_and_prior_database() {
        let (path, mut session) = create_committed_database("failed_save");
        let original = fs::read(&path).unwrap();
        let mut replacement = Session::new_in_memory();
        replacement
            .execute_script("CREATE (:Doc {name: 'new'})", &BTreeMap::new())
            .unwrap();
        replacement
            .execute_script("CREATE (:Doc {name: 'another'})", &BTreeMap::new())
            .unwrap();
        session
            .replace_engine(replacement.engine().clone())
            .unwrap();
        fail_next_write_at(WriteStage::Rename);
        assert_eq!(session.save().unwrap_err().code(), "io_error");
        assert_eq!(fs::read(&path).unwrap(), original);
        assert!(session.is_dirty());
        assert_eq!(session.engine().stats().node_count, 2);
        session.save().unwrap();
        assert_eq!(load(&path).unwrap().0.stats().node_count, 2);
        remove_database(&path);
    }

    #[test]
    fn failed_directory_sync_requires_reopening_and_cannot_be_cleared() {
        let (path, mut session) = create_committed_database("uncertain_commit");
        session.execute_script("BEGIN", &BTreeMap::new()).unwrap();
        session
            .execute_script("SAVEPOINT pending", &BTreeMap::new())
            .unwrap();
        session
            .execute_script("CREATE (:Doc {name: 'published'})", &BTreeMap::new())
            .unwrap();
        fail_next_write_at(WriteStage::DirectorySync);
        let error = session
            .execute_script("COMMIT", &BTreeMap::new())
            .unwrap_err();
        assert_eq!(error.code(), "persistence_uncertain");
        assert_eq!(load(&path).unwrap().0.stats().node_count, 2);
        for query in [
            "ROLLBACK",
            "ROLLBACK TO SAVEPOINT pending",
            "COMMIT",
            "CREATE (:Doc)",
        ] {
            assert_eq!(
                session
                    .execute_script(query, &BTreeMap::new())
                    .unwrap_err()
                    .code(),
                "persistence_uncertain"
            );
        }
        assert_eq!(session.save().unwrap_err().code(), "persistence_uncertain");
        assert_eq!(
            session
                .save_as(temp_path("uncertain_copy"))
                .unwrap_err()
                .code(),
            "persistence_uncertain"
        );
        assert_eq!(
            session
                .replace_engine(Session::new_in_memory().engine().clone())
                .unwrap_err()
                .code(),
            "persistence_uncertain"
        );
        let mut reopened = Session::open(&path).unwrap();
        reopened
            .execute_script("CREATE (:Doc {name: 'next'})", &BTreeMap::new())
            .unwrap();
        assert_eq!(load(&path).unwrap().0.stats().node_count, 3);
        remove_database(&path);
    }

    #[test]
    fn invalid_wal_tails_are_removed_before_new_commits() {
        // Every short header length, including 40..47, plus a short payload
        // and a fully present payload with a bad checksum must recover safely.
        for tail_case in 0..=50 {
            let (path, mut session) = create_committed_database("wal_tail");
            session
                .execute_script("CREATE (:Doc {name: 'committed'})", &BTreeMap::new())
                .unwrap();
            let committed_tx = session.transaction_info().last_tx_id;
            let bytes = fs::read(&path).unwrap();
            let parsed = parse_file(&bytes).unwrap();
            let mut tail = encode_wal_record(2, committed_tx + 1, b"uncommitted payload");
            match tail_case {
                0..=47 => tail.truncate(tail_case),
                48 => tail.truncate(51),
                49 => *tail.last_mut().unwrap() ^= 1,
                _ => tail[40] ^= 1,
            }
            let mut wal = parsed.valid_wal_bytes.clone();
            wal.extend(&tail);
            let mut header = parsed.header;
            header.wal_len = wal.len() as u64;
            header.last_tx_id = committed_tx + 1;
            header.wal_checksum = checksum(&wal);
            let mut damaged = assemble_file(&header, &parsed.snapshot_bytes, &wal);
            if tail_case == 0 {
                // A crash can leave the header advertising bytes absent from disk.
                header.wal_len += 10;
                damaged = assemble_file(&header, &parsed.snapshot_bytes, &wal);
            }
            fs::write(&path, damaged).unwrap();
            let (engine, report) = load(&path).unwrap();
            assert!(report.recovered_tail, "case {tail_case}");
            assert_eq!(report.last_tx_id, committed_tx);
            assert_eq!(engine.stats().node_count, 2);
            let mut recovered = Session::open(&path).unwrap();
            recovered
                .execute_script("CREATE (:Doc {name: 'after recovery'})", &BTreeMap::new())
                .unwrap();
            let (engine, report) = load(&path).unwrap();
            assert!(!report.recovered_tail);
            assert_eq!(report.wal_records, 2);
            assert_eq!(engine.stats().node_count, 3);
            remove_database(&path);
        }
    }

    #[test]
    fn failed_migration_never_relabels_or_replaces_original_bytes() {
        for stage in [
            WriteStage::PartialWrite,
            WriteStage::FileSync,
            WriteStage::Rename,
        ] {
            let path = copy_fixture("person_v0_1_0.cupld");
            let original = fs::read(&path).unwrap();
            fail_next_write_at(stage);
            assert_eq!(load(&path).unwrap_err().code(), "io_error");
            assert_eq!(fs::read(&path).unwrap(), original);
            assert_eq!(
                load_without_migration(&path).unwrap().0.stats().node_count,
                4
            );
            assert_eq!(load(&path).unwrap().0.stats().node_count, 4);
            remove_database(&path);
        }
        let path = copy_fixture("person_v0_1_0.cupld");
        let parsed = parse_file(&fs::read(&path).unwrap()).unwrap();
        let invalid_state = [0u8; 1];
        let mut header = parsed.header;
        header.snapshot_len = invalid_state.len() as u64;
        header.wal_offset = (HEADER_SIZE + invalid_state.len()) as u64;
        header.wal_len = 0;
        header.snapshot_checksum = checksum(&invalid_state);
        header.wal_checksum = checksum(&[]);
        let mut invalid = assemble_file(&header, &invalid_state, &[]);
        invalid[8..12].copy_from_slice(&1u32.to_le_bytes());
        fs::write(&path, &invalid).unwrap();
        assert_eq!(check(&path).unwrap_err().code(), "decode_eof");
        assert_eq!(fs::read(&path).unwrap(), invalid);
        remove_database(&path);
    }

    #[test]
    fn writer_lock_is_exclusive_across_processes_and_released_on_exit() {
        use std::io::{BufRead, BufReader, Write};
        use std::process::{Command, Stdio};
        let (path, mut session) = create_committed_database("process_lock");
        let original = fs::read(&path).unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "storage::tests::lock_holder_process",
                "--nocapture",
            ])
            .env("CUPLD_TEST_STORAGE_LOCK_PATH", &path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        loop {
            let mut line = String::new();
            assert_ne!(
                output.read_line(&mut line).unwrap(),
                0,
                "lock holder exited before acquiring lock"
            );
            if line.trim() == "LOCKED" {
                break;
            }
        }
        assert_eq!(
            session
                .execute_script("CREATE (:Doc)", &BTreeMap::new())
                .unwrap_err()
                .code(),
            "database_busy"
        );
        assert_eq!(fs::read(&path).unwrap(), original);
        assert_eq!(Session::open(&path).unwrap().engine().stats().node_count, 1);
        child.stdin.take().unwrap().write_all(b"release\n").unwrap();
        assert!(child.wait().unwrap().success());
        session
            .execute_script("CREATE (:Doc)", &BTreeMap::new())
            .unwrap();
        assert_eq!(load(&path).unwrap().0.stats().node_count, 2);
        remove_database(&path);
    }

    /// Subprocess helper for the cross-process lock test; not a test itself.
    #[test]
    #[ignore]
    fn lock_holder_process() {
        use std::io::Write;
        let Some(path) = std::env::var_os("CUPLD_TEST_STORAGE_LOCK_PATH") else {
            return;
        };
        let _writer = DatabaseWriter::acquire(std::path::Path::new(&path)).unwrap();
        println!("LOCKED");
        std::io::stdout().flush().unwrap();
        std::io::stdin().read_line(&mut String::new()).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn replacement_preserves_permissions_and_refuses_readonly_database() {
        use std::os::unix::fs::PermissionsExt;
        let (path, mut session) = create_committed_database("permissions");
        // New databases and their lock sidecars start owner-only.
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(lock_path(&path)).unwrap().permissions().mode() & 0o777,
            0o600
        );
        fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
        session
            .execute_script("CREATE (:Doc)", &BTreeMap::new())
            .unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o640
        );
        session.compact().unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o640
        );
        fs::set_permissions(&path, fs::Permissions::from_mode(0o444)).unwrap();
        let original = fs::read(&path).unwrap();
        assert_eq!(
            session
                .execute_script("CREATE (:Doc)", &BTreeMap::new())
                .unwrap_err()
                .code(),
            "io_error"
        );
        assert_eq!(fs::read(&path).unwrap(), original);
        assert_eq!(session.engine().stats().node_count, 2);
        fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
        session
            .execute_script("CREATE (:Doc)", &BTreeMap::new())
            .unwrap();
        remove_database(&path);
    }

    #[cfg(unix)]
    #[test]
    fn aliased_or_nonempty_lock_sidecars_are_rejected_without_touching_targets() {
        use std::os::unix::fs::symlink;
        let (path, mut session) = create_committed_database("lock_aliases");
        let writer = DatabaseWriter::acquire(&path).unwrap();
        let lock_path = lock_path(&path);
        drop(writer);
        fs::remove_file(&lock_path).unwrap();
        let original = fs::read(&path).unwrap();
        symlink(&path, &lock_path).unwrap();
        assert_eq!(session.save().unwrap_err().code(), "io_error");
        assert_eq!(fs::read(&path).unwrap(), original);
        fs::remove_file(&lock_path).unwrap();
        fs::hard_link(&path, &lock_path).unwrap();
        assert_eq!(session.save().unwrap_err().code(), "io_error");
        assert_eq!(fs::read(&path).unwrap(), original);
        fs::remove_file(&lock_path).unwrap();
        // An empty hard-linked sidecar passes the length check, so only the
        // Unix alias rule can reject it.
        let empty_target = path.with_extension("empty");
        fs::write(&empty_target, b"").unwrap();
        fs::hard_link(&empty_target, &lock_path).unwrap();
        let error = session.save().unwrap_err();
        assert_eq!(error.code(), "io_error");
        assert!(error.to_string().contains("without aliases"), "{error}");
        assert_eq!(fs::read(&path).unwrap(), original);
        assert_eq!(fs::metadata(&empty_target).unwrap().len(), 0);
        fs::remove_file(&lock_path).unwrap();
        fs::remove_file(&empty_target).unwrap();
        fs::write(&lock_path, b"unrelated file").unwrap();
        assert_eq!(session.save().unwrap_err().code(), "io_error");
        assert_eq!(fs::read(&lock_path).unwrap(), b"unrelated file");
        assert_eq!(fs::read(&path).unwrap(), original);
        fs::remove_file(&lock_path).unwrap();
        session.save().unwrap();
        remove_database(&path);
    }
    #[test]
    fn duplicated_lock_descriptor_does_not_extend_writer_scope() {
        let (path, _) = create_committed_database("lock_lifetime");
        let writer = DatabaseWriter::acquire(&path).unwrap();
        let inherited = writer._lock.try_clone().unwrap();
        drop(writer);
        let next_writer = DatabaseWriter::acquire(&path).unwrap();
        drop(next_writer);
        drop(inherited);
        remove_database(&path);
    }

    #[test]
    fn save_as_root_destination_returns_error_without_losing_unsaved_state() {
        let mut session = Session::new_in_memory();
        session
            .execute_script("CREATE (:Doc)", &BTreeMap::new())
            .unwrap();
        let current = std::env::current_dir().unwrap();
        let root = current.ancestors().last().unwrap();
        assert_eq!(session.save_as(root).unwrap_err().code(), "io_error");
        assert!(session.path().is_none());
        assert!(session.is_dirty());
        assert_eq!(session.engine().stats().node_count, 1);
    }
}
