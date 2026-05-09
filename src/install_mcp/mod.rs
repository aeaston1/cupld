pub(crate) mod backup;
pub(crate) mod claude;
pub(crate) mod codex;
pub(crate) mod prompt;
pub(crate) mod state;

use std::env;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::skill_install::{InstallScope, SkillInstallTarget};
use claude::ClaudeMcpConfigWriter;
use codex::CodexMcpConfigWriter;
use state::McpInstallRecord;

pub(crate) const MAX_CONFIG_BYTES: u64 = 1024 * 1024;
pub(crate) const DEFAULT_SERVER_NAME: &str = "cupld-memory";

#[cfg(test)]
thread_local! {
    static TEST_HOME: std::cell::RefCell<Option<PathBuf>> = const { std::cell::RefCell::new(None) };
    static TEST_CWD: std::cell::RefCell<Option<PathBuf>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(crate) fn with_test_paths<T>(home: &Path, cwd: &Path, run: impl FnOnce() -> T) -> T {
    TEST_HOME.with(|slot| *slot.borrow_mut() = Some(home.to_path_buf()));
    TEST_CWD.with(|slot| *slot.borrow_mut() = Some(cwd.to_path_buf()));
    let output = run();
    TEST_CWD.with(|slot| *slot.borrow_mut() = None);
    TEST_HOME.with(|slot| *slot.borrow_mut() = None);
    output
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ExistingConfig {
    pub exists: bool,
    pub contents: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConfigAction {
    Create,
    Update,
    Unchanged,
    Blocked,
}

impl ConfigAction {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Create => "created",
            Self::Update => "updated",
            Self::Unchanged => "already_installed",
            Self::Blocked => "blocked",
        }
    }

    fn dry_run_label(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Update => "update",
            Self::Unchanged => "unchanged",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ConfigEditPlan {
    pub path: PathBuf,
    pub file_exists: bool,
    pub before: Option<String>,
    pub after: String,
    pub action: ConfigAction,
    pub blocked_reason: Option<String>,
}

impl ConfigEditPlan {
    pub(crate) fn blocked(path: PathBuf, file_exists: bool, reason: String) -> Self {
        Self::blocked_with_before(path, file_exists, None, reason)
    }

    pub(crate) fn blocked_with_before(
        path: PathBuf,
        file_exists: bool,
        before: Option<String>,
        reason: String,
    ) -> Self {
        Self {
            path,
            file_exists,
            before,
            after: String::new(),
            action: ConfigAction::Blocked,
            blocked_reason: Some(reason),
        }
    }

    pub(crate) fn would_backup(&self) -> bool {
        self.file_exists && matches!(self.action, ConfigAction::Update)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct McpConfigWriteOutcome {
    pub action: ConfigAction,
    pub path: Option<PathBuf>,
    pub backup_path: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct McpInstallSpec {
    pub target: SkillInstallTarget,
    pub scope: InstallScope,
    pub server_name: String,
    pub command: String,
    pub args: Vec<String>,
    pub db_path: PathBuf,
    pub root: PathBuf,
    pub config_path: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct McpPrintSpec {
    pub target: SkillInstallTarget,
    pub scope: InstallScope,
    pub server_name: String,
    pub command: String,
    pub args: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum McpConfigPlan {
    Write(ConfigEditPlan),
    PrintManual {
        target: SkillInstallTarget,
        scope: InstallScope,
        rendered: String,
    },
    Unsupported {
        target: SkillInstallTarget,
        reason: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct McpInstallPlan {
    pub spec: McpInstallSpec,
    pub config: McpConfigPlan,
    pub skill_path: PathBuf,
    pub skill_action: SkillPreviewAction,
    pub install_state_action: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SkillPreviewAction {
    Install,
    Overwrite,
    AlreadyInstalled,
}

impl SkillPreviewAction {
    fn label(self) -> &'static str {
        match self {
            Self::Install => "install",
            Self::Overwrite => "overwrite",
            Self::AlreadyInstalled => "already_installed",
        }
    }
}

impl SkillInstallTarget {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::Opencode => "opencode",
        }
    }
}

impl InstallScope {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Cwd => "cwd",
            Self::Home => "home",
        }
    }
}

pub(crate) fn build_spec(
    target: SkillInstallTarget,
    scope: InstallScope,
    server_name: String,
    command: String,
    db_arg: String,
    db_path: PathBuf,
    root: PathBuf,
) -> Result<McpInstallSpec, String> {
    let args = vec![
        "mcp".to_owned(),
        "serve".to_owned(),
        "--db".to_owned(),
        db_arg,
    ];
    validate_command_shape(&command, &args)?;
    let config_path = config_path(target, scope)?;
    Ok(McpInstallSpec {
        target,
        scope,
        server_name,
        command,
        args,
        db_path,
        root,
        config_path,
    })
}

pub(crate) fn build_print_spec(
    target: SkillInstallTarget,
    scope: InstallScope,
    server_name: String,
    command: String,
    db_arg: String,
) -> Result<McpPrintSpec, String> {
    let args = vec![
        "mcp".to_owned(),
        "serve".to_owned(),
        "--db".to_owned(),
        db_arg,
    ];
    validate_command_shape(&command, &args)?;
    Ok(McpPrintSpec {
        target,
        scope,
        server_name,
        command,
        args,
    })
}

pub(crate) fn build_plan(
    spec: McpInstallSpec,
    skill_path: PathBuf,
    skill_action: SkillPreviewAction,
) -> Result<McpInstallPlan, String> {
    let config = match (spec.target, spec.scope) {
        (SkillInstallTarget::Codex, InstallScope::Cwd | InstallScope::Home) => {
            let path = spec
                .config_path
                .clone()
                .ok_or("missing Codex MCP config path".to_owned())?;
            let existing = CodexMcpConfigWriter::load(&path)?;
            McpConfigPlan::Write(CodexMcpConfigWriter::plan(path, existing, &spec)?)
        }
        (SkillInstallTarget::Claude, InstallScope::Cwd) => {
            let path = spec
                .config_path
                .clone()
                .ok_or("missing Claude MCP config path".to_owned())?;
            let existing = ClaudeMcpConfigWriter::load(&path)?;
            McpConfigPlan::Write(ClaudeMcpConfigWriter::plan(path, existing, &spec)?)
        }
        (SkillInstallTarget::Claude, InstallScope::Home) => McpConfigPlan::PrintManual {
            target: spec.target,
            scope: spec.scope,
            rendered: render_claude_user_command(&spec),
        },
        (SkillInstallTarget::Opencode, _) => McpConfigPlan::Unsupported {
            target: spec.target,
            reason:
                "OpenCode MCP config writing is not supported yet; use --print-only for a manual snippet"
                    .to_owned(),
        },
    };
    Ok(McpInstallPlan {
        spec,
        config,
        skill_path,
        skill_action,
        install_state_action: "update",
    })
}

pub(crate) fn execute_config_plan(
    plan: &McpConfigPlan,
) -> Result<Option<McpConfigWriteOutcome>, String> {
    match plan {
        McpConfigPlan::Write(edit) => match edit.action {
            ConfigAction::Blocked => Err(edit
                .blocked_reason
                .clone()
                .unwrap_or_else(|| "blocked MCP config write".to_owned())),
            _ => match edit
                .path
                .extension()
                .and_then(|extension| extension.to_str())
            {
                Some("json") => ClaudeMcpConfigWriter::write(edit).map(Some),
                _ => CodexMcpConfigWriter::write(edit).map(Some),
            },
        },
        McpConfigPlan::PrintManual { rendered, .. } => {
            print!("{rendered}");
            Ok(None)
        }
        McpConfigPlan::Unsupported { reason, .. } => Err(reason.clone()),
    }
}

pub(crate) fn render_print_spec(spec: &McpPrintSpec) -> String {
    let install_spec = McpInstallSpec {
        target: spec.target,
        scope: spec.scope,
        server_name: spec.server_name.clone(),
        command: spec.command.clone(),
        args: spec.args.clone(),
        db_path: PathBuf::new(),
        root: PathBuf::new(),
        config_path: None,
    };
    match (spec.target, spec.scope) {
        (SkillInstallTarget::Codex, _) => CodexMcpConfigWriter::render_managed_entry(&install_spec),
        (SkillInstallTarget::Claude, InstallScope::Cwd) => {
            ClaudeMcpConfigWriter::render_document(&install_spec)
        }
        (SkillInstallTarget::Claude, InstallScope::Home) => {
            render_claude_user_command(&install_spec)
        }
        (SkillInstallTarget::Opencode, _) => render_generic_stdio_json(&install_spec),
    }
}

pub(crate) fn render_dry_run<W: Write>(
    output: &mut W,
    plan: &McpInstallPlan,
) -> Result<(), String> {
    writeln!(output, "mcp_target {}", plan.spec.target.as_str()).map_err(io_error)?;
    writeln!(output, "mcp_scope {}", plan.spec.scope.as_str()).map_err(io_error)?;
    match &plan.config {
        McpConfigPlan::Write(edit) => {
            writeln!(output, "mcp_config_path {}", edit.path.display()).map_err(io_error)?;
            writeln!(output, "mcp_config_exists {}", edit.file_exists).map_err(io_error)?;
            writeln!(output, "mcp_config_backup {}", edit.would_backup()).map_err(io_error)?;
            writeln!(output, "mcp_config_action {}", edit.action.dry_run_label())
                .map_err(io_error)?;
            if let Some(reason) = edit.blocked_reason.as_deref() {
                writeln!(output, "mcp_config_blocked_reason {reason}").map_err(io_error)?;
            }
        }
        McpConfigPlan::PrintManual { .. } => {
            writeln!(output, "mcp_config_path manual").map_err(io_error)?;
            writeln!(output, "mcp_config_exists false").map_err(io_error)?;
            writeln!(output, "mcp_config_backup false").map_err(io_error)?;
            writeln!(output, "mcp_config_action print").map_err(io_error)?;
        }
        McpConfigPlan::Unsupported { reason, .. } => {
            writeln!(output, "mcp_config_path unsupported").map_err(io_error)?;
            writeln!(output, "mcp_config_exists false").map_err(io_error)?;
            writeln!(output, "mcp_config_backup false").map_err(io_error)?;
            writeln!(output, "mcp_config_action blocked").map_err(io_error)?;
            writeln!(output, "mcp_config_blocked_reason {reason}").map_err(io_error)?;
        }
    }
    writeln!(
        output,
        "skill_path_action {} {}",
        plan.skill_action.label(),
        plan.skill_path.display()
    )
    .map_err(io_error)?;
    writeln!(output, "bootstrap_action ensure_db_root").map_err(io_error)?;
    writeln!(output, "install_state_action {}", plan.install_state_action).map_err(io_error)?;
    Ok(())
}

pub(crate) fn mcp_state_record_from_plan(plan: &McpInstallPlan) -> Option<McpInstallRecord> {
    let rendered = match &plan.config {
        McpConfigPlan::Write(edit) if edit.action != ConfigAction::Blocked => edit.after.as_str(),
        _ => return None,
    };
    McpInstallRecord::from_spec(&plan.spec, rendered)
}

pub(crate) fn config_path(
    target: SkillInstallTarget,
    scope: InstallScope,
) -> Result<Option<PathBuf>, String> {
    let home = home_dir().ok_or("could not resolve home directory".to_owned())?;
    let cwd = current_dir()?;
    Ok(match (target, scope) {
        (SkillInstallTarget::Codex, InstallScope::Home) => {
            Some(home.join(".codex").join("config.toml"))
        }
        (SkillInstallTarget::Codex, InstallScope::Cwd) => {
            Some(cwd.join(".codex").join("config.toml"))
        }
        (SkillInstallTarget::Claude, InstallScope::Cwd) => Some(cwd.join(".mcp.json")),
        (SkillInstallTarget::Claude, InstallScope::Home) => None,
        (SkillInstallTarget::Opencode, _) => None,
    })
}

pub(crate) fn default_mcp_command() -> String {
    match env::current_exe() {
        Ok(path)
            if path.is_absolute()
                && path.exists()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("cupld")) =>
        {
            path.display().to_string()
        }
        _ => "cupld".to_owned(),
    }
}

pub(crate) fn db_arg_for_config(db_path: &Path) -> String {
    if let Ok(package) = cupld::package::WorkspacePackage::discover_current()
        && package.default_db_path() == db_path
    {
        return "default".to_owned();
    }
    db_path.display().to_string()
}

pub(crate) fn validate_command_shape(command: &str, args: &[String]) -> Result<(), String> {
    if command.trim().is_empty() {
        return Err("MCP command cannot be empty".to_owned());
    }
    if args.len() < 4
        || args.first().map(String::as_str) != Some("mcp")
        || args.get(1).map(String::as_str) != Some("serve")
        || !args.iter().any(|arg| arg == "--db")
    {
        return Err("MCP args must include `mcp serve --db <db>`".to_owned());
    }
    Ok(())
}

fn render_claude_user_command(spec: &McpInstallSpec) -> String {
    let mut output = String::new();
    output.push_str("claude mcp add cupld-memory --scope user -- ");
    output.push_str(&shell_word(&spec.command));
    for arg in &spec.args {
        output.push(' ');
        output.push_str(&shell_word(arg));
    }
    output.push('\n');
    output
}

fn render_generic_stdio_json(spec: &McpInstallSpec) -> String {
    let doc = cupld::json::JsonValue::object([(
        "mcpServers",
        cupld::json::JsonValue::object([(
            spec.server_name.clone(),
            cupld::json::JsonValue::object([
                (
                    "command",
                    cupld::json::JsonValue::from(spec.command.clone()),
                ),
                (
                    "args",
                    cupld::json::JsonValue::array(
                        spec.args.iter().cloned().map(cupld::json::JsonValue::from),
                    ),
                ),
            ]),
        )]),
    )]);
    claude::pretty_json(&doc)
}

fn shell_word(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || "/._-".contains(ch))
    {
        value.to_owned()
    } else {
        let escaped = value.replace('\'', "'\\''");
        format!("'{escaped}'")
    }
}

fn home_dir() -> Option<PathBuf> {
    #[cfg(test)]
    if let Some(path) = TEST_HOME.with(|slot| slot.borrow().clone()) {
        return Some(path);
    }
    env_path("HOME")
        .or_else(|| env_path("USERPROFILE"))
        .or_else(|| match (env_path("HOMEDRIVE"), env_path("HOMEPATH")) {
            (Some(drive), Some(path)) => Some(PathBuf::from(format!(
                "{}{}",
                drive.display(),
                path.display()
            ))),
            _ => None,
        })
}

fn current_dir() -> Result<PathBuf, String> {
    #[cfg(test)]
    if let Some(path) = TEST_CWD.with(|slot| slot.borrow().clone()) {
        return Ok(path);
    }
    env::current_dir().map_err(io_error)
}

fn env_path(name: &str) -> Option<PathBuf> {
    env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn io_error(error: std::io::Error) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use super::{build_print_spec, build_spec, render_print_spec, validate_command_shape};
    use crate::skill_install::{InstallScope, SkillInstallTarget};
    use std::path::PathBuf;

    #[test]
    fn generated_command_args_pass_smoke_validation() {
        let spec = build_spec(
            SkillInstallTarget::Codex,
            InstallScope::Cwd,
            "cupld-memory".to_owned(),
            "cupld".to_owned(),
            "default".to_owned(),
            PathBuf::from(".cupld/default.cupld"),
            PathBuf::from(".cupld/data"),
        )
        .unwrap();

        validate_command_shape(&spec.command, &spec.args).unwrap();
    }

    #[test]
    fn print_only_renders_snippet_without_paths() {
        let spec = build_print_spec(
            SkillInstallTarget::Codex,
            InstallScope::Cwd,
            "cupld-memory".to_owned(),
            "cupld".to_owned(),
            "default".to_owned(),
        )
        .unwrap();

        let rendered = render_print_spec(&spec);

        assert!(rendered.contains("[mcp_servers.cupld-memory]"));
        assert!(rendered.contains("\"default\""));
    }
}
