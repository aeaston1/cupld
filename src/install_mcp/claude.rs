use std::fs;
use std::path::{Path, PathBuf};

use cupld::json::{self, JsonValue};

use super::{
    ConfigAction, ConfigEditPlan, ExistingConfig, MAX_CONFIG_BYTES, McpConfigWriteOutcome,
    McpInstallSpec,
    backup::{self, BackupFailures},
};

pub(crate) struct ClaudeMcpConfigWriter;

impl ClaudeMcpConfigWriter {
    pub(crate) fn load(path: &Path) -> Result<ExistingConfig, String> {
        load_existing_config(path)
    }

    pub(crate) fn render_managed_entry(spec: &McpInstallSpec) -> JsonValue {
        JsonValue::object([
            ("command", JsonValue::from(spec.command.clone())),
            (
                "args",
                JsonValue::array(spec.args.iter().cloned().map(JsonValue::from)),
            ),
            (
                "env",
                JsonValue::object(std::iter::empty::<(&str, JsonValue)>()),
            ),
            ("cupldManaged", JsonValue::from(true)),
        ])
    }

    pub(crate) fn render_document(spec: &McpInstallSpec) -> String {
        let doc = JsonValue::object([(
            "mcpServers",
            JsonValue::object([(spec.server_name.clone(), Self::render_managed_entry(spec))]),
        )]);
        pretty_json(&doc)
    }

    pub(crate) fn plan(
        path: PathBuf,
        existing: ExistingConfig,
        spec: &McpInstallSpec,
    ) -> Result<ConfigEditPlan, String> {
        let managed_entry = Self::render_managed_entry(spec);
        let before = existing.contents.clone();
        let mut parsed_before = None;
        let document = match before.as_deref() {
            None => JsonValue::object([(
                "mcpServers",
                JsonValue::object([(spec.server_name.clone(), managed_entry)]),
            )]),
            Some(contents) if contents.trim().is_empty() => JsonValue::object([(
                "mcpServers",
                JsonValue::object([(spec.server_name.clone(), managed_entry)]),
            )]),
            Some(contents) => {
                let parsed = match json::parse(contents) {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        return Ok(ConfigEditPlan::blocked(
                            path,
                            existing.exists,
                            format!("invalid Claude MCP JSON: {error}"),
                        ));
                    }
                };
                parsed_before = Some(parsed.clone());
                merge_document(
                    parsed,
                    &spec.server_name,
                    managed_entry,
                    &path,
                    existing.exists,
                )?
            }
        };
        if let Some(blocked) = blocked_from_document(
            path.clone(),
            existing.exists,
            before.clone(),
            document.clone(),
        ) {
            return Ok(blocked);
        }
        if parsed_before.as_ref() == Some(&document)
            && let Some(before_contents) = before.clone()
        {
            return Ok(ConfigEditPlan {
                path,
                file_exists: existing.exists,
                before,
                after: before_contents,
                action: ConfigAction::Unchanged,
                blocked_reason: None,
            });
        }
        let after = pretty_json(&document);
        if before.as_deref() == Some(after.as_str()) {
            Ok(ConfigEditPlan {
                path,
                file_exists: existing.exists,
                before,
                after,
                action: ConfigAction::Unchanged,
                blocked_reason: None,
            })
        } else {
            Ok(ConfigEditPlan {
                path,
                file_exists: existing.exists,
                before,
                after,
                action: if existing.exists {
                    ConfigAction::Update
                } else {
                    ConfigAction::Create
                },
                blocked_reason: None,
            })
        }
    }

    pub(crate) fn write(plan: &ConfigEditPlan) -> Result<McpConfigWriteOutcome, String> {
        write_with_failures(plan, &BackupFailures::default())
    }
}

fn merge_document(
    parsed: JsonValue,
    server_name: &str,
    managed_entry: JsonValue,
    path: &Path,
    exists: bool,
) -> Result<JsonValue, String> {
    let JsonValue::Object(mut top_entries) = parsed else {
        return Ok(JsonValue::object([(
            "mcpServers",
            JsonValue::object([(server_name.to_owned(), managed_entry)]),
        )]));
    };

    let mcp_index = top_entries.iter().position(|(key, _)| key == "mcpServers");
    match mcp_index {
        Some(index) => {
            let JsonValue::Object(mut servers) = top_entries.remove(index).1 else {
                return Ok(JsonValue::object([(
                    "__cupldBlocked",
                    JsonValue::String("mcpServers must be an object".to_owned()),
                )]));
            };

            if let Some(server_index) = servers.iter().position(|(key, _)| key == server_name) {
                let existing = servers[server_index].1.clone();
                if existing == managed_entry {
                    servers[server_index].1 = managed_entry;
                } else if is_cupld_managed(&existing) {
                    servers[server_index].1 = managed_entry;
                } else {
                    return Ok(blocked_document(
                        path,
                        exists,
                        format!("unmanaged Claude MCP server `{server_name}` already exists"),
                    ));
                }
            } else {
                servers.push((server_name.to_owned(), managed_entry));
            }
            top_entries.insert(index, ("mcpServers".to_owned(), JsonValue::Object(servers)));
            Ok(JsonValue::Object(top_entries))
        }
        None => {
            top_entries.push((
                "mcpServers".to_owned(),
                JsonValue::Object(vec![(server_name.to_owned(), managed_entry)]),
            ));
            Ok(JsonValue::Object(top_entries))
        }
    }
}

fn blocked_document(path: &Path, exists: bool, reason: String) -> JsonValue {
    JsonValue::object([
        ("__cupldBlocked", JsonValue::String(reason)),
        ("__cupldPath", JsonValue::String(path.display().to_string())),
        ("__cupldExists", JsonValue::Bool(exists)),
    ])
}

pub(crate) fn blocked_from_document(
    path: PathBuf,
    exists: bool,
    before: Option<String>,
    document: JsonValue,
) -> Option<ConfigEditPlan> {
    let reason = document
        .get("__cupldBlocked")
        .and_then(JsonValue::as_str)?
        .to_owned();
    Some(ConfigEditPlan::blocked_with_before(
        path, exists, before, reason,
    ))
}

fn is_cupld_managed(value: &JsonValue) -> bool {
    value
        .get("cupldManaged")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false)
}

fn write_with_failures(
    plan: &ConfigEditPlan,
    failures: &BackupFailures,
) -> Result<McpConfigWriteOutcome, String> {
    match plan.action {
        ConfigAction::Blocked => Err(plan
            .blocked_reason
            .clone()
            .unwrap_or_else(|| "blocked config write".to_owned())),
        ConfigAction::Unchanged => Ok(McpConfigWriteOutcome {
            action: ConfigAction::Unchanged,
            path: Some(plan.path.clone()),
            backup_path: None,
        }),
        ConfigAction::Create | ConfigAction::Update => {
            let outcome = if cfg!(test) {
                backup::write_atomic_with_backup_for_test(&plan.path, &plan.after, failures)?
            } else {
                backup::write_atomic_with_backup(&plan.path, &plan.after)?
            };
            Ok(McpConfigWriteOutcome {
                action: plan.action,
                path: Some(plan.path.clone()),
                backup_path: outcome.backup_path,
            })
        }
    }
}

fn load_existing_config(path: &Path) -> Result<ExistingConfig, String> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ExistingConfig {
                exists: false,
                contents: None,
            });
        }
        Err(error) => return Err(error.to_string()),
    };
    if metadata.len() > MAX_CONFIG_BYTES {
        return Err(format!(
            "config {} is too large; limit is {} bytes",
            path.display(),
            MAX_CONFIG_BYTES
        ));
    }
    Ok(ExistingConfig {
        exists: true,
        contents: Some(fs::read_to_string(path).map_err(|error| error.to_string())?),
    })
}

pub(crate) fn pretty_json(value: &JsonValue) -> String {
    let mut output = String::new();
    write_pretty(&mut output, value, 0);
    output.push('\n');
    output
}

fn write_pretty(output: &mut String, value: &JsonValue, indent: usize) {
    match value {
        JsonValue::Object(entries) => {
            output.push('{');
            if !entries.is_empty() {
                output.push('\n');
                for (index, (key, value)) in entries.iter().enumerate() {
                    output.push_str(&" ".repeat(indent + 2));
                    json::write_quoted_string(output, key);
                    output.push_str(": ");
                    write_pretty(output, value, indent + 2);
                    if index + 1 < entries.len() {
                        output.push(',');
                    }
                    output.push('\n');
                }
                output.push_str(&" ".repeat(indent));
            }
            output.push('}');
        }
        JsonValue::Array(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push_str(", ");
                }
                write_pretty(output, value, indent);
            }
            output.push(']');
        }
        other => json::write_to(output, other),
    }
}

#[cfg(test)]
mod tests {
    use super::ClaudeMcpConfigWriter;
    use crate::install_mcp::{ConfigAction, ExistingConfig, McpInstallSpec};
    use crate::skill_install::{InstallScope, SkillInstallTarget};
    use std::path::PathBuf;

    fn spec() -> McpInstallSpec {
        McpInstallSpec {
            target: SkillInstallTarget::Claude,
            scope: InstallScope::Cwd,
            server_name: "cupld-memory".to_owned(),
            command: "cupld".to_owned(),
            args: vec![
                "mcp".to_owned(),
                "serve".to_owned(),
                "--db".to_owned(),
                "default".to_owned(),
            ],
            db_path: PathBuf::from("/tmp/default.cupld"),
            root: PathBuf::from("/tmp/data"),
            config_path: Some(PathBuf::from(".mcp.json")),
        }
    }

    #[test]
    fn empty_file_renders_mcp_servers_document() {
        let plan = ClaudeMcpConfigWriter::plan(
            PathBuf::from(".mcp.json"),
            ExistingConfig {
                exists: false,
                contents: None,
            },
            &spec(),
        )
        .unwrap();

        assert_eq!(plan.action, ConfigAction::Create);
        assert!(plan.after.contains("\"mcpServers\""));
        assert!(plan.after.contains("\"cupld-memory\""));
    }

    #[test]
    fn unrelated_top_level_keys_are_preserved() {
        let before = "{ \"other\": true }";
        let plan = ClaudeMcpConfigWriter::plan(
            PathBuf::from(".mcp.json"),
            ExistingConfig {
                exists: true,
                contents: Some(before.to_owned()),
            },
            &spec(),
        )
        .unwrap();

        assert!(plan.after.contains("\"other\": true"));
        assert!(plan.after.contains("\"mcpServers\""));
    }

    #[test]
    fn unrelated_mcp_servers_are_preserved() {
        let before = "{ \"mcpServers\": { \"other\": { \"command\": \"x\" } } }";
        let plan = ClaudeMcpConfigWriter::plan(
            PathBuf::from(".mcp.json"),
            ExistingConfig {
                exists: true,
                contents: Some(before.to_owned()),
            },
            &spec(),
        )
        .unwrap();

        assert!(plan.after.contains("\"other\""));
        assert!(plan.after.contains("\"cupld-memory\""));
    }

    #[test]
    fn existing_matching_entry_is_unchanged_when_pretty_format_matches() {
        let rendered = ClaudeMcpConfigWriter::render_document(&spec());
        let plan = ClaudeMcpConfigWriter::plan(
            PathBuf::from(".mcp.json"),
            ExistingConfig {
                exists: true,
                contents: Some(rendered),
            },
            &spec(),
        )
        .unwrap();

        assert_eq!(plan.action, ConfigAction::Unchanged);
    }

    #[test]
    fn existing_matching_compact_entry_is_unchanged() {
        let before = "{\"mcpServers\":{\"cupld-memory\":{\"command\":\"cupld\",\"args\":[\"mcp\",\"serve\",\"--db\",\"default\"],\"env\":{},\"cupldManaged\":true}}}";
        let plan = ClaudeMcpConfigWriter::plan(
            PathBuf::from(".mcp.json"),
            ExistingConfig {
                exists: true,
                contents: Some(before.to_owned()),
            },
            &spec(),
        )
        .unwrap();

        assert_eq!(plan.action, ConfigAction::Unchanged);
        assert_eq!(plan.after, before);
    }

    #[test]
    fn stale_managed_entry_is_replaced() {
        let before = "{ \"mcpServers\": { \"cupld-memory\": { \"command\": \"old\", \"cupldManaged\": true } } }";
        let plan = ClaudeMcpConfigWriter::plan(
            PathBuf::from(".mcp.json"),
            ExistingConfig {
                exists: true,
                contents: Some(before.to_owned()),
            },
            &spec(),
        )
        .unwrap();

        assert_eq!(plan.action, ConfigAction::Update);
        assert!(plan.after.contains("\"command\": \"cupld\""));
    }

    #[test]
    fn unmarked_same_name_entry_conflicts() {
        let before = "{ \"mcpServers\": { \"cupld-memory\": { \"command\": \"old\" } } }";
        let plan = ClaudeMcpConfigWriter::plan(
            PathBuf::from(".mcp.json"),
            ExistingConfig {
                exists: true,
                contents: Some(before.to_owned()),
            },
            &spec(),
        )
        .unwrap();

        assert_eq!(plan.action, ConfigAction::Blocked);
    }

    #[test]
    fn mcp_servers_wrong_type_is_rejected() {
        let before = "{ \"mcpServers\": [] }";
        let plan = ClaudeMcpConfigWriter::plan(
            PathBuf::from(".mcp.json"),
            ExistingConfig {
                exists: true,
                contents: Some(before.to_owned()),
            },
            &spec(),
        )
        .unwrap();

        assert_eq!(plan.action, ConfigAction::Blocked);
    }

    #[test]
    fn invalid_json_is_rejected() {
        let plan = ClaudeMcpConfigWriter::plan(
            PathBuf::from(".mcp.json"),
            ExistingConfig {
                exists: true,
                contents: Some("{".to_owned()),
            },
            &spec(),
        )
        .unwrap();

        assert_eq!(plan.action, ConfigAction::Blocked);
    }
}
