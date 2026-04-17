use std::fs;
use std::io;
use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::engine::{
    ConstraintState, CupldEngine, EdgeState, EngineState, GraphError, IndexState, IndexStatus,
    NodeState, PropertyMap, SchemaState, Value,
};

const MAGIC: &[u8; 8] = b"CUPLD01\0";
const FORMAT_VERSION: u32 = 1;
const COMPAT_VERSION: u32 = 1;
const LEGACY_FORMAT_VERSION: u32 = 0;
const LEGACY_COMPAT_VERSION: u32 = 0;
const HEADER_SIZE: usize = 128;
const WAL_RECORD_MAGIC: &[u8; 4] = b"WALR";

#[derive(Clone, Debug)]
pub struct IntegrityReport {
    pub db_uuid: [u8; 16],
    pub last_tx_id: u64,
    pub wal_records: usize,
    pub recovered_tail: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StorageFormatVersion {
    version: u32,
    compat: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum StorageErrorKind {
    Io,
    Graph(GraphError),
    FileHeader,
    FileLayout,
    FileMagic,
    FileVersion,
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
            Self::Graph(error) => error.code(),
            Self::FileHeader => "file_header",
            Self::FileLayout => "file_layout",
            Self::FileMagic => "file_magic",
            Self::FileVersion => "file_version",
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

pub fn save_compacted(path: &Path, engine: &CupldEngine) -> Result<[u8; 16], StorageError> {
    let db_uuid = file_uuid();
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
    let bytes = assemble_file(&header, &snapshot, &[]);
    write_durable(path, &bytes)?;
    Ok(db_uuid)
}

pub fn append_commit(
    path: &Path,
    engine: &CupldEngine,
    db_uuid: Option<[u8; 16]>,
) -> Result<[u8; 16], StorageError> {
    if !path.exists() {
        return save_compacted(path, engine);
    }
    let existing = fs::read(path)?;
    let parsed = parse_file(&existing)?;
    let tx_id = engine.snapshot().tx_id().get();
    let state = encode_state(&engine.to_state())?;
    let seq_no = parsed.wal_records.len() as u64 + 1;
    let record = encode_wal_record(seq_no, tx_id, &state);
    let mut wal = parsed.valid_wal_bytes;
    wal.extend(record);
    let header = FileHeader {
        clean: true,
        db_uuid: db_uuid.unwrap_or(parsed.header.db_uuid),
        snapshot_offset: HEADER_SIZE as u64,
        snapshot_len: parsed.snapshot_bytes.len() as u64,
        wal_offset: (HEADER_SIZE + parsed.snapshot_bytes.len()) as u64,
        wal_len: wal.len() as u64,
        last_tx_id: tx_id,
        snapshot_checksum: checksum(&parsed.snapshot_bytes),
        wal_checksum: checksum(&wal),
    };
    let bytes = assemble_file(&header, &parsed.snapshot_bytes, &wal);
    write_durable(path, &bytes)?;
    Ok(header.db_uuid)
}

pub fn load(path: &Path) -> Result<(CupldEngine, IntegrityReport), StorageError> {
    let bytes = fs::read(path)?;
    let parsed = parse_file(&bytes)?;
    maybe_migrate_file(path, parsed.format)?;
    let mut state = decode_state(&parsed.snapshot_bytes)?;
    for record in &parsed.wal_records {
        state = decode_state(&record.payload)?;
    }
    let engine = CupldEngine::from_state(state)?;
    let report = IntegrityReport {
        db_uuid: parsed.header.db_uuid,
        last_tx_id: parsed.header.last_tx_id,
        wal_records: parsed.wal_records.len(),
        recovered_tail: parsed.recovered_tail,
    };
    Ok((engine, report))
}

pub fn compact(path: &Path, engine: &CupldEngine, db_uuid: [u8; 16]) -> Result<(), StorageError> {
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
    let bytes = assemble_file(&header, &snapshot, &[]);
    write_durable(path, &bytes)?;
    Ok(())
}

pub fn check(path: &Path) -> Result<IntegrityReport, StorageError> {
    let bytes = fs::read(path)?;
    let parsed = parse_file(&bytes)?;
    maybe_migrate_file(path, parsed.format)?;
    Ok(IntegrityReport {
        db_uuid: parsed.header.db_uuid,
        last_tx_id: parsed.header.last_tx_id,
        wal_records: parsed.wal_records.len(),
        recovered_tail: parsed.recovered_tail,
    })
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
    if wal_end as usize > bytes.len() || snapshot_end as usize > bytes.len() {
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

    let wal_bytes = &bytes[header.wal_offset as usize..wal_end as usize];
    let (wal_records, valid_wal_len, recovered_tail) = parse_wal(wal_bytes)?;
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
    let mut records = Vec::new();
    let mut recovered_tail = false;

    while cursor < bytes.len() {
        if bytes.len() - cursor < 40 {
            recovered_tail = true;
            break;
        }
        if &bytes[cursor..cursor + 4] != WAL_RECORD_MAGIC {
            recovered_tail = true;
            break;
        }
        cursor += 4;
        let _seq_no = read_u64(bytes, &mut cursor)?;
        let _tx_id = read_u64(bytes, &mut cursor)?;
        let _record_count = read_u32(bytes, &mut cursor)?;
        let payload_len = read_u64(bytes, &mut cursor)? as usize;
        let payload_checksum = read_u64(bytes, &mut cursor)?;
        let _tx_checksum = read_u64(bytes, &mut cursor)?;
        if bytes.len() - cursor < payload_len {
            recovered_tail = true;
            break;
        }
        let payload = bytes[cursor..cursor + payload_len].to_vec();
        cursor += payload_len;
        if checksum(&payload) != payload_checksum {
            recovered_tail = true;
            break;
        }
        records.push(WalRecord { payload });
    }

    Ok((records, cursor, recovered_tail))
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

fn maybe_migrate_file(path: &Path, format: StorageFormatVersion) -> Result<(), StorageError> {
    let migration = plan_migration(format)?;
    if !migration.rewrite_header {
        return Ok(());
    }

    let mut bytes = fs::read(path)?;
    bytes[8..12].copy_from_slice(&migration.target.version.to_le_bytes());
    bytes[12..16].copy_from_slice(&migration.target.compat.to_le_bytes());
    write_durable(path, &bytes)
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

fn decode_state(bytes: &[u8]) -> Result<EngineState, StorageError> {
    let mut cursor = 0usize;
    let next_tx_id = read_u64(bytes, &mut cursor)?;
    let next_node_id = read_u64(bytes, &mut cursor)?;
    let next_edge_id = read_u64(bytes, &mut cursor)?;
    let schema = decode_schema_state(bytes, &mut cursor)?;
    let node_count = read_u32(bytes, &mut cursor)? as usize;
    let mut nodes = Vec::with_capacity(node_count);
    for _ in 0..node_count {
        nodes.push(NodeState {
            id: read_u64(bytes, &mut cursor)?,
            labels: read_strings(bytes, &mut cursor)?,
            properties: decode_property_map(bytes, &mut cursor)?,
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
    push_u32(output, state.indexes.len() as u32);
    for index in &state.indexes {
        push_string(output, &index.name);
        push_schema_target(output, &index.target);
        push_string(output, &index.property);
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
        }
    }
}

fn decode_schema_state(bytes: &[u8], cursor: &mut usize) -> Result<SchemaState, StorageError> {
    let labels = read_strings(bytes, cursor)?;
    let edge_types = read_strings(bytes, cursor)?;
    let index_count = read_u32(bytes, cursor)? as usize;
    let mut indexes = Vec::with_capacity(index_count);
    for _ in 0..index_count {
        indexes.push(IndexState {
            name: read_string(bytes, cursor)?,
            target: read_schema_target(bytes, cursor)?,
            property: read_string(bytes, cursor)?,
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

fn write_durable(path: &Path, bytes: &[u8]) -> Result<(), StorageError> {
    let mut file = fs::File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
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
    use std::path::PathBuf;

    use super::{
        COMPAT_VERSION, FORMAT_VERSION, append_commit, check, compact, load, save_compacted,
    };
    use crate::runtime::Session;

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("{}_{}.cupld", name, std::process::id()))
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
        append_commit(&path, session.engine(), Some(uuid)).unwrap();

        let (engine, report) = load(&path).unwrap();
        assert_eq!(report.wal_records, 1);
        assert_eq!(engine.stats().node_count, 2);

        let _ = fs::remove_file(path);
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
        append_commit(&path, session.engine(), Some(uuid)).unwrap();
        compact(&path, session.engine(), uuid).unwrap();

        let report = check(&path).unwrap();
        assert_eq!(report.wal_records, 0);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn check_migrates_legacy_header_versions_in_place() {
        let path = temp_path("cupld_storage_migrate");
        let mut session = Session::new_in_memory();
        session
            .execute_script("CREATE (n:Person {name: 'Ada'})", &BTreeMap::new())
            .unwrap();
        save_compacted(&path, session.engine()).unwrap();

        let mut bytes = fs::read(&path).unwrap();
        bytes[8..12].copy_from_slice(&0u32.to_le_bytes());
        bytes[12..16].copy_from_slice(&0u32.to_le_bytes());
        fs::write(&path, &bytes).unwrap();

        let report = check(&path).unwrap();
        assert_eq!(report.wal_records, 0);

        let bytes = fs::read(&path).unwrap();
        assert_eq!(
            u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            FORMAT_VERSION
        );
        assert_eq!(
            u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            COMPAT_VERSION
        );

        let _ = fs::remove_file(path);
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

        let _ = fs::remove_file(path);
    }
}
