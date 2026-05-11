use std::path::{Path, PathBuf};

use cupld::json::{self, JsonValue};

use super::McpInstallSpec;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct McpInstallRecord {
    pub target: String,
    pub scope: String,
    pub server_name: String,
    pub config_path: PathBuf,
    pub command: String,
    pub args: Vec<String>,
    pub db_path: PathBuf,
    pub root: PathBuf,
    pub managed_block_signature: String,
    pub installed_at: String,
    pub cupld_version: String,
}

#[derive(Default)]
pub(crate) struct McpInstallRecordBuilder {
    target: Option<String>,
    scope: Option<String>,
    server_name: Option<String>,
    config_path: Option<PathBuf>,
    command: Option<String>,
    args: Option<Vec<String>>,
    db_path: Option<PathBuf>,
    root: Option<PathBuf>,
    managed_block_signature: Option<String>,
    installed_at: Option<String>,
    cupld_version: Option<String>,
}

impl McpInstallRecordBuilder {
    pub(crate) fn finish(self, context: &str) -> Result<McpInstallRecord, String> {
        Ok(McpInstallRecord {
            target: self
                .target
                .ok_or(format!("missing `target` in {context}"))?,
            scope: self.scope.ok_or(format!("missing `scope` in {context}"))?,
            server_name: self
                .server_name
                .ok_or(format!("missing `server_name` in {context}"))?,
            config_path: self
                .config_path
                .ok_or(format!("missing `config_path` in {context}"))?,
            command: self
                .command
                .ok_or(format!("missing `command` in {context}"))?,
            args: self.args.ok_or(format!("missing `args` in {context}"))?,
            db_path: self
                .db_path
                .ok_or(format!("missing `db_path` in {context}"))?,
            root: self.root.ok_or(format!("missing `root` in {context}"))?,
            managed_block_signature: self
                .managed_block_signature
                .ok_or(format!("missing `managed_block_signature` in {context}"))?,
            installed_at: self
                .installed_at
                .ok_or(format!("missing `installed_at` in {context}"))?,
            cupld_version: self
                .cupld_version
                .ok_or(format!("missing `cupld_version` in {context}"))?,
        })
    }
}

impl McpInstallRecord {
    pub(crate) fn from_spec(spec: &McpInstallSpec, rendered_config: &str) -> Option<Self> {
        Some(Self {
            target: spec.target.as_str().to_owned(),
            scope: spec.scope.as_str().to_owned(),
            server_name: spec.server_name.clone(),
            config_path: spec.config_path.clone()?,
            command: spec.command.clone(),
            args: spec.args.clone(),
            db_path: spec.db_path.clone(),
            root: spec.root.clone(),
            managed_block_signature: signature(rendered_config),
            installed_at: install_timestamp(),
            cupld_version: env!("CARGO_PKG_VERSION").to_owned(),
        })
    }

    pub(crate) fn same_key(&self, other: &Self) -> bool {
        self.target == other.target
            && self.scope == other.scope
            && self.server_name == other.server_name
            && self.config_path == other.config_path
    }
}

pub(crate) fn parse_mcp_install_record_key(
    builder: &mut McpInstallRecordBuilder,
    key: &str,
    parsed: &JsonValue,
    line_number: usize,
) -> Result<(), String> {
    match key {
        "target" => set_field(
            &mut builder.target,
            parse_string_value(parsed, "target", line_number)?,
            "target",
            line_number,
        ),
        "scope" => set_field(
            &mut builder.scope,
            parse_string_value(parsed, "scope", line_number)?,
            "scope",
            line_number,
        ),
        "server_name" => set_field(
            &mut builder.server_name,
            parse_string_value(parsed, "server_name", line_number)?,
            "server_name",
            line_number,
        ),
        "config_path" => set_field(
            &mut builder.config_path,
            parse_path_value(parsed, "config_path", line_number)?,
            "config_path",
            line_number,
        ),
        "command" => set_field(
            &mut builder.command,
            parse_string_value(parsed, "command", line_number)?,
            "command",
            line_number,
        ),
        "args" => set_field(
            &mut builder.args,
            parse_string_array(parsed, "args", line_number)?,
            "args",
            line_number,
        ),
        "db_path" => set_field(
            &mut builder.db_path,
            parse_path_value(parsed, "db_path", line_number)?,
            "db_path",
            line_number,
        ),
        "root" => set_field(
            &mut builder.root,
            parse_path_value(parsed, "root", line_number)?,
            "root",
            line_number,
        ),
        "managed_block_signature" => set_field(
            &mut builder.managed_block_signature,
            parse_string_value(parsed, "managed_block_signature", line_number)?,
            "managed_block_signature",
            line_number,
        ),
        "installed_at" => set_field(
            &mut builder.installed_at,
            parse_string_value(parsed, "installed_at", line_number)?,
            "installed_at",
            line_number,
        ),
        "cupld_version" => set_field(
            &mut builder.cupld_version,
            parse_string_value(parsed, "cupld_version", line_number)?,
            "cupld_version",
            line_number,
        ),
        other => Err(format!(
            "line {line_number}: unknown mcp_install key `{other}`"
        )),
    }
}

pub(crate) fn render_mcp_install_record(output: &mut String, record: &McpInstallRecord) {
    output.push('\n');
    output.push_str("[[mcp_install]]\n");
    render_string(output, "target", &record.target);
    render_string(output, "scope", &record.scope);
    render_string(output, "server_name", &record.server_name);
    render_path(output, "config_path", &record.config_path);
    render_string(output, "command", &record.command);
    output.push_str("args = [");
    for (index, arg) in record.args.iter().enumerate() {
        if index > 0 {
            output.push_str(", ");
        }
        json::write_quoted_string(output, arg);
    }
    output.push_str("]\n");
    render_path(output, "db_path", &record.db_path);
    render_path(output, "root", &record.root);
    render_string(
        output,
        "managed_block_signature",
        &record.managed_block_signature,
    );
    render_string(output, "installed_at", &record.installed_at);
    render_string(output, "cupld_version", &record.cupld_version);
}

fn render_string(output: &mut String, key: &str, value: &str) {
    output.push_str(key);
    output.push_str(" = ");
    json::write_quoted_string(output, value);
    output.push('\n');
}

fn render_path(output: &mut String, key: &str, path: &Path) {
    render_string(output, key, &path.display().to_string());
}

fn parse_path_value(value: &JsonValue, key: &str, line_number: usize) -> Result<PathBuf, String> {
    match value {
        JsonValue::String(path) => Ok(PathBuf::from(path)),
        _ => Err(format!(
            "line {line_number}: invalid `{key}`: expected quoted string"
        )),
    }
}

fn parse_string_value(value: &JsonValue, key: &str, line_number: usize) -> Result<String, String> {
    match value {
        JsonValue::String(string) => Ok(string.clone()),
        _ => Err(format!(
            "line {line_number}: invalid `{key}`: expected quoted string"
        )),
    }
}

fn parse_string_array(
    value: &JsonValue,
    key: &str,
    line_number: usize,
) -> Result<Vec<String>, String> {
    match value {
        JsonValue::Array(values) => values
            .iter()
            .map(|value| parse_string_value(value, key, line_number))
            .collect(),
        _ => Err(format!(
            "line {line_number}: invalid `{key}`: expected string array"
        )),
    }
}

fn set_field<T>(
    slot: &mut Option<T>,
    value: T,
    key: &str,
    line_number: usize,
) -> Result<(), String> {
    if slot.is_some() {
        Err(format!("line {line_number}: duplicate `{key}`"))
    } else {
        *slot = Some(value);
        Ok(())
    }
}

fn signature(contents: &str) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in contents.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

fn install_timestamp() -> String {
    let seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{seconds}")
}
