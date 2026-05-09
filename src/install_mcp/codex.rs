use std::fs;
use std::path::{Path, PathBuf};

use cupld::json;

use super::{
    ConfigAction, ConfigEditPlan, ExistingConfig, MAX_CONFIG_BYTES, McpConfigWriteOutcome,
    McpInstallSpec,
    backup::{self, BackupFailures},
};

pub(crate) struct CodexMcpConfigWriter;

impl CodexMcpConfigWriter {
    pub(crate) fn load(path: &Path) -> Result<ExistingConfig, String> {
        load_existing_config(path)
    }

    pub(crate) fn render_managed_entry(spec: &McpInstallSpec) -> String {
        let mut output = String::new();
        output.push_str("# BEGIN cupld managed mcp server: ");
        output.push_str(&spec.server_name);
        output.push('\n');
        output.push_str("[mcp_servers.");
        output.push_str(&toml_key(&spec.server_name));
        output.push_str("]\ncommand = ");
        json::write_quoted_string(&mut output, &spec.command);
        output.push_str("\nargs = [");
        for (index, arg) in spec.args.iter().enumerate() {
            if index > 0 {
                output.push_str(", ");
            }
            json::write_quoted_string(&mut output, arg);
        }
        output.push_str("]\nenabled = true\n# END cupld managed mcp server: ");
        output.push_str(&spec.server_name);
        output.push('\n');
        output
    }

    pub(crate) fn plan(
        path: PathBuf,
        existing: ExistingConfig,
        spec: &McpInstallSpec,
    ) -> Result<ConfigEditPlan, String> {
        let rendered = Self::render_managed_entry(spec);
        let current = existing.contents.clone().unwrap_or_default();
        let begin = marker_begin(&spec.server_name);
        let end = marker_end(&spec.server_name);
        let begin_count = current.matches(&begin).count();
        let end_count = current.matches(&end).count();
        if begin_count > 1 || end_count > 1 {
            return Ok(ConfigEditPlan::blocked(
                path,
                existing.exists,
                format!(
                    "duplicate managed Codex MCP blocks for {}",
                    spec.server_name
                ),
            ));
        }
        if begin_count != end_count {
            return Ok(ConfigEditPlan::blocked(
                path,
                existing.exists,
                format!("missing managed block marker for {}", spec.server_name),
            ));
        }

        let planned = if begin_count == 1 {
            let start = current.find(&begin).expect("checked begin marker");
            let end_start = current.find(&end).expect("checked end marker");
            if end_start < start {
                return Ok(ConfigEditPlan::blocked(
                    path,
                    existing.exists,
                    format!(
                        "managed block end appears before begin for {}",
                        spec.server_name
                    ),
                ));
            }
            let end_after = end_start + end.len();
            let end_after = if current[end_after..].starts_with('\n') {
                end_after + 1
            } else {
                end_after
            };
            let mut next = String::new();
            next.push_str(&current[..start]);
            next.push_str(&rendered);
            next.push_str(&current[end_after..]);
            next
        } else {
            let unmanaged_header = format!("[mcp_servers.{}]", toml_key(&spec.server_name));
            if current.lines().any(|line| line.trim() == unmanaged_header) {
                return Ok(ConfigEditPlan::blocked(
                    path,
                    existing.exists,
                    format!(
                        "unmanaged Codex MCP server `{}` already exists; refusing to overwrite",
                        spec.server_name
                    ),
                ));
            }
            let mut next = current;
            if !next.is_empty() && !next.ends_with('\n') {
                next.push('\n');
            }
            if !next.is_empty() {
                next.push('\n');
            }
            next.push_str(&rendered);
            next
        };

        if existing.contents.as_deref() == Some(planned.as_str()) {
            Ok(ConfigEditPlan {
                path,
                file_exists: existing.exists,
                before: existing.contents,
                after: planned,
                action: ConfigAction::Unchanged,
                blocked_reason: None,
            })
        } else {
            Ok(ConfigEditPlan {
                path,
                file_exists: existing.exists,
                before: existing.contents,
                after: planned,
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

fn marker_begin(server_name: &str) -> String {
    format!("# BEGIN cupld managed mcp server: {server_name}")
}

fn marker_end(server_name: &str) -> String {
    format!("# END cupld managed mcp server: {server_name}")
}

fn toml_key(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
    {
        value.to_owned()
    } else {
        let mut output = String::new();
        json::write_quoted_string(&mut output, value);
        output
    }
}

#[cfg(test)]
mod tests {
    use super::CodexMcpConfigWriter;
    use crate::install_mcp::{ConfigAction, McpInstallSpec};
    use crate::skill_install::{InstallScope, SkillInstallTarget};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static NEXT_TEMP_DIR_ID: AtomicUsize = AtomicUsize::new(1);

    fn spec() -> McpInstallSpec {
        McpInstallSpec {
            target: SkillInstallTarget::Codex,
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
            config_path: Some(PathBuf::from("config.toml")),
        }
    }

    fn temp_path(prefix: &str) -> PathBuf {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let suffix = NEXT_TEMP_DIR_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "cupld_mcp_codex_{prefix}_{}_{}_{}",
            std::process::id(),
            timestamp,
            suffix
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn empty_file_gets_managed_block() {
        let existing = crate::install_mcp::ExistingConfig {
            exists: true,
            contents: Some(String::new()),
        };
        let plan =
            CodexMcpConfigWriter::plan(PathBuf::from("config.toml"), existing, &spec()).unwrap();

        assert_eq!(plan.action, ConfigAction::Update);
        assert_eq!(
            plan.after,
            "# BEGIN cupld managed mcp server: cupld-memory\n[mcp_servers.cupld-memory]\ncommand = \"cupld\"\nargs = [\"mcp\", \"serve\", \"--db\", \"default\"]\nenabled = true\n# END cupld managed mcp server: cupld-memory\n"
        );
    }

    #[test]
    fn unrelated_toml_and_comments_are_preserved() {
        let before = "# keep\n[other]\nvalue = true\n";
        let existing = crate::install_mcp::ExistingConfig {
            exists: true,
            contents: Some(before.to_owned()),
        };
        let plan =
            CodexMcpConfigWriter::plan(PathBuf::from("config.toml"), existing, &spec()).unwrap();

        assert!(plan.after.starts_with(before));
        assert!(plan.after.contains("[mcp_servers.cupld-memory]"));
    }

    #[test]
    fn matching_managed_block_is_unchanged() {
        let rendered = CodexMcpConfigWriter::render_managed_entry(&spec());
        let existing = crate::install_mcp::ExistingConfig {
            exists: true,
            contents: Some(rendered),
        };
        let plan =
            CodexMcpConfigWriter::plan(PathBuf::from("config.toml"), existing, &spec()).unwrap();

        assert_eq!(plan.action, ConfigAction::Unchanged);
    }

    #[test]
    fn stale_managed_block_is_replaced() {
        let before = "# BEGIN cupld managed mcp server: cupld-memory\nold\n# END cupld managed mcp server: cupld-memory\n";
        let existing = crate::install_mcp::ExistingConfig {
            exists: true,
            contents: Some(before.to_owned()),
        };
        let plan =
            CodexMcpConfigWriter::plan(PathBuf::from("config.toml"), existing, &spec()).unwrap();

        assert_eq!(plan.action, ConfigAction::Update);
        assert!(!plan.after.contains("\nold\n"));
    }

    #[test]
    fn missing_end_marker_is_rejected() {
        let existing = crate::install_mcp::ExistingConfig {
            exists: true,
            contents: Some("# BEGIN cupld managed mcp server: cupld-memory\nold\n".to_owned()),
        };
        let plan =
            CodexMcpConfigWriter::plan(PathBuf::from("config.toml"), existing, &spec()).unwrap();

        assert_eq!(plan.action, ConfigAction::Blocked);
    }

    #[test]
    fn duplicate_managed_blocks_are_rejected() {
        let rendered = CodexMcpConfigWriter::render_managed_entry(&spec());
        let existing = crate::install_mcp::ExistingConfig {
            exists: true,
            contents: Some(format!("{rendered}\n{rendered}")),
        };
        let plan =
            CodexMcpConfigWriter::plan(PathBuf::from("config.toml"), existing, &spec()).unwrap();

        assert_eq!(plan.action, ConfigAction::Blocked);
    }

    #[test]
    fn unmanaged_same_name_table_conflicts() {
        let existing = crate::install_mcp::ExistingConfig {
            exists: true,
            contents: Some("[mcp_servers.cupld-memory]\ncommand = \"other\"\n".to_owned()),
        };
        let plan =
            CodexMcpConfigWriter::plan(PathBuf::from("config.toml"), existing, &spec()).unwrap();

        assert_eq!(plan.action, ConfigAction::Blocked);
    }

    #[test]
    fn oversized_config_is_rejected_before_planning() {
        let root = temp_path("oversized");
        let path = root.join("config.toml");
        fs::write(
            &path,
            vec![b'a'; (crate::install_mcp::MAX_CONFIG_BYTES + 1) as usize],
        )
        .unwrap();

        let error = CodexMcpConfigWriter::load(&path).unwrap_err();

        assert!(error.contains("too large"));
        fs::remove_dir_all(root).unwrap();
    }
}
