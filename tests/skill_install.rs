use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use cupld::{Session, configured_markdown_root};

static NEXT_TEMP_DIR_ID: AtomicUsize = AtomicUsize::new(1);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(prefix: &str) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let suffix = NEXT_TEMP_DIR_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "cupld_skill_install_{prefix}_{}_{}_{}",
            std::process::id(),
            timestamp,
            suffix
        ));
        fs::create_dir_all(&path).unwrap();
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn quoted_path(path: &Path) -> String {
    let mut output = String::new();
    cupld::json::write_quoted_string(&mut output, &path.display().to_string());
    output
}

#[test]
fn cli_install_skill_writes_bundled_skill_and_records_state() {
    let home = TempDir::new("home");
    let config = TempDir::new("config");
    let skills_root = TempDir::new("skills_root");
    let notes_root = TempDir::new("notes_root");
    let db_path = skills_root.path().join(".cupld").join("default.cupld");

    let output = Command::new(env!("CARGO_BIN_EXE_cupld"))
        .args([
            "install",
            "--path",
            skills_root.path().to_str().unwrap(),
            "--db",
            db_path.to_str().unwrap(),
            "--root",
            notes_root.path().to_str().unwrap(),
        ])
        .env("HOME", home.path())
        .env(
            if cfg!(windows) {
                "APPDATA"
            } else {
                "XDG_CONFIG_HOME"
            },
            config.path(),
        )
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("installed_skill"));

    let skill_path = skills_root.path().join("cupld-md-memory").join("SKILL.md");
    assert!(skill_path.exists());
    let contents = fs::read_to_string(&skill_path).unwrap();
    assert!(contents.contains("name: cupld-md-memory"));
    assert!(contents.contains("cupld query --db default --with-md"));
    assert!(contents.contains("cupld sync markdown --db default"));
    assert!(contents.contains("./.cupld/default.cupld"));
    assert!(contents.contains("./.cupld/data/"));
    assert!(db_path.exists());
    let session = Session::open(&db_path).unwrap();
    assert_eq!(
        configured_markdown_root(session.engine()),
        Some(notes_root.path().canonicalize().unwrap())
    );

    let state_path = config
        .path()
        .join(".cupld")
        .join("installed-skill-path.txt");
    assert!(state_path.exists());
    let install_state_path = config.path().join(".cupld").join("install-state.toml");
    assert!(install_state_path.exists());
    let install_state = fs::read_to_string(install_state_path).unwrap();
    assert!(install_state.contains("bundle_revision = 1"));
    assert!(install_state.contains("skill_signature = "));
    assert!(install_state.contains("skill_path = "));
    assert!(install_state.contains("db_path = "));
    assert!(install_state.contains("root = "));
    assert!(
        !config
            .path()
            .join("cupld")
            .join("installed-skill-path.txt")
            .exists()
    );
}

#[test]
fn cli_install_skill_supports_provider_home_and_cwd_scopes() {
    let home = TempDir::new("home_scope");
    let config = TempDir::new("config_scope");
    let workspace = TempDir::new("workspace_scope");
    let home_db = workspace.path().join(".cupld").join("home.cupld");
    let home_root = workspace.path().join(".cupld").join("home-data");
    let cwd_db = workspace.path().join(".cupld").join("cwd.cupld");
    let cwd_root = workspace.path().join(".cupld").join("cwd-data");

    let home_output = Command::new(env!("CARGO_BIN_EXE_cupld"))
        .args([
            "install",
            "--target",
            "codex",
            "--scope",
            "home",
            "--db",
            home_db.to_str().unwrap(),
            "--root",
            home_root.to_str().unwrap(),
        ])
        .env("HOME", home.path())
        .env(
            if cfg!(windows) {
                "APPDATA"
            } else {
                "XDG_CONFIG_HOME"
            },
            config.path(),
        )
        .current_dir(workspace.path())
        .output()
        .unwrap();
    assert!(home_output.status.success());
    assert!(
        home.path()
            .join(".agents/skills/cupld-md-memory/SKILL.md")
            .exists()
    );

    let cwd_output = Command::new(env!("CARGO_BIN_EXE_cupld"))
        .args([
            "install",
            "--target",
            "claude",
            "--scope",
            "cwd",
            "--db",
            cwd_db.to_str().unwrap(),
            "--root",
            cwd_root.to_str().unwrap(),
        ])
        .env("HOME", home.path())
        .env(
            if cfg!(windows) {
                "APPDATA"
            } else {
                "XDG_CONFIG_HOME"
            },
            config.path(),
        )
        .current_dir(workspace.path())
        .output()
        .unwrap();
    assert!(cwd_output.status.success());
    assert!(
        workspace
            .path()
            .join(".claude/skills/cupld-md-memory/SKILL.md")
            .exists()
    );
}

#[test]
fn cli_install_skill_records_multiple_provider_scope_installs() {
    let home = TempDir::new("home_multi");
    let config = TempDir::new("config_multi");
    let workspace = TempDir::new("workspace_multi");
    let codex_db = workspace.path().join(".cupld").join("codex.cupld");
    let codex_root = workspace.path().join(".cupld").join("codex-data");
    let claude_db = workspace.path().join(".cupld").join("claude.cupld");
    let claude_root = workspace.path().join(".cupld").join("claude-data");

    let codex_output = Command::new(env!("CARGO_BIN_EXE_cupld"))
        .args([
            "install",
            "--target",
            "codex",
            "--scope",
            "home",
            "--db",
            codex_db.to_str().unwrap(),
            "--root",
            codex_root.to_str().unwrap(),
        ])
        .env("HOME", home.path())
        .env(
            if cfg!(windows) {
                "APPDATA"
            } else {
                "XDG_CONFIG_HOME"
            },
            config.path(),
        )
        .current_dir(workspace.path())
        .output()
        .unwrap();
    assert!(codex_output.status.success());

    let claude_output = Command::new(env!("CARGO_BIN_EXE_cupld"))
        .args([
            "install",
            "--target",
            "claude",
            "--scope",
            "cwd",
            "--db",
            claude_db.to_str().unwrap(),
            "--root",
            claude_root.to_str().unwrap(),
        ])
        .env("HOME", home.path())
        .env(
            if cfg!(windows) {
                "APPDATA"
            } else {
                "XDG_CONFIG_HOME"
            },
            config.path(),
        )
        .current_dir(workspace.path())
        .output()
        .unwrap();
    assert!(claude_output.status.success());

    let install_state =
        fs::read_to_string(config.path().join(".cupld").join("install-state.toml")).unwrap();
    assert!(install_state.contains("version = 4"));
    assert_eq!(install_state.matches("[[install]]").count(), 2);

    let codex_skill = home
        .path()
        .join(".agents/skills/cupld-md-memory/SKILL.md")
        .canonicalize()
        .unwrap();
    let claude_skill = workspace
        .path()
        .join(".claude/skills/cupld-md-memory/SKILL.md")
        .canonicalize()
        .unwrap();
    assert!(install_state.contains(&quoted_path(&codex_skill)));
    assert!(install_state.contains(&quoted_path(&codex_db.canonicalize().unwrap())));
    assert!(install_state.contains(&quoted_path(&codex_root.canonicalize().unwrap())));
    assert!(install_state.contains(&quoted_path(&claude_skill)));
    assert!(install_state.contains(&quoted_path(&claude_db.canonicalize().unwrap())));
    assert!(install_state.contains(&quoted_path(&claude_root.canonicalize().unwrap())));
}

#[test]
fn cli_install_uses_package_defaults_and_writes_package_config() {
    let home = TempDir::new("home_defaults");
    let config = TempDir::new("config_defaults");
    let workspace = TempDir::new("workspace_defaults");

    let output = Command::new(env!("CARGO_BIN_EXE_cupld"))
        .args(["install", "--target", "codex", "--scope", "cwd"])
        .env("HOME", home.path())
        .env(
            if cfg!(windows) {
                "APPDATA"
            } else {
                "XDG_CONFIG_HOME"
            },
            config.path(),
        )
        .current_dir(workspace.path())
        .output()
        .unwrap();

    assert!(output.status.success());

    let db_path = workspace.path().join(".cupld").join("default.cupld");
    let notes_root = workspace.path().join(".cupld").join("data");
    let config_path = workspace.path().join(".cupld").join("config.toml");

    assert!(db_path.exists());
    assert!(notes_root.exists());
    assert!(config_path.exists());

    let contents = fs::read_to_string(config_path).unwrap();
    assert!(contents.contains("db_path"));
    assert!(contents.contains("default.cupld"));
    assert!(contents.contains("markdown_root"));
    assert!(contents.contains(".cupld/data"));

    let session = Session::open(&db_path).unwrap();
    assert_eq!(
        configured_markdown_root(session.engine()),
        Some(notes_root.canonicalize().unwrap())
    );
}

#[test]
fn cli_install_mcp_codex_cwd_writes_idempotent_config_and_state() {
    let home = TempDir::new("home_mcp_codex");
    let config = TempDir::new("config_mcp_codex");
    let workspace = TempDir::new("workspace_mcp_codex");

    let output = Command::new(env!("CARGO_BIN_EXE_cupld"))
        .args([
            "install",
            "--mcp",
            "--target",
            "codex",
            "--scope",
            "cwd",
            "--db",
            "default",
            "--mcp-command",
            "cupld",
        ])
        .env("HOME", home.path())
        .env(
            if cfg!(windows) {
                "APPDATA"
            } else {
                "XDG_CONFIG_HOME"
            },
            config.path(),
        )
        .current_dir(workspace.path())
        .output()
        .unwrap();
    assert!(output.status.success());

    let codex_config = workspace.path().join(".codex").join("config.toml");
    let contents = fs::read_to_string(&codex_config).unwrap();
    assert!(contents.contains("# BEGIN cupld managed mcp server: cupld-memory"));
    assert!(contents.contains("[mcp_servers.cupld-memory]"));
    assert!(contents.contains("command = \"cupld\""));
    assert!(contents.contains("args = [\"mcp\", \"serve\", \"--db\", \"default\"]"));
    assert!(
        workspace
            .path()
            .join(".agents/skills/cupld-md-memory/SKILL.md")
            .exists()
    );
    assert!(workspace.path().join(".cupld/default.cupld").exists());

    let state = fs::read_to_string(config.path().join(".cupld/install-state.toml")).unwrap();
    assert!(state.contains("version = 4"));
    assert!(state.contains("[[install]]"));
    assert!(state.contains("[[mcp_install]]"));
    assert!(state.contains("target = \"codex\""));
    assert!(state.contains("scope = \"cwd\""));

    let second = Command::new(env!("CARGO_BIN_EXE_cupld"))
        .args([
            "install",
            "--mcp",
            "--target",
            "codex",
            "--scope",
            "cwd",
            "--db",
            "default",
            "--mcp-command",
            "cupld",
        ])
        .env("HOME", home.path())
        .env(
            if cfg!(windows) {
                "APPDATA"
            } else {
                "XDG_CONFIG_HOME"
            },
            config.path(),
        )
        .current_dir(workspace.path())
        .output()
        .unwrap();
    assert!(second.status.success());
    let stdout = String::from_utf8(second.stdout).unwrap();
    assert!(stdout.contains("mcp_config already_installed"));
    let backups = fs::read_dir(workspace.path().join(".codex"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().contains("cupld-backup"))
        .count();
    assert_eq!(backups, 0);
}

#[test]
fn cli_install_mcp_codex_update_creates_backup_with_original_content() {
    let home = TempDir::new("home_mcp_backup");
    let config = TempDir::new("config_mcp_backup");
    let workspace = TempDir::new("workspace_mcp_backup");
    let codex_dir = workspace.path().join(".codex");
    fs::create_dir_all(&codex_dir).unwrap();
    let codex_config = codex_dir.join("config.toml");
    fs::write(&codex_config, "# keep\n[other]\nvalue = true\n").unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_cupld"))
        .args([
            "install",
            "--mcp",
            "--target",
            "codex",
            "--scope",
            "cwd",
            "--mcp-command",
            "cupld",
        ])
        .env("HOME", home.path())
        .env(
            if cfg!(windows) {
                "APPDATA"
            } else {
                "XDG_CONFIG_HOME"
            },
            config.path(),
        )
        .current_dir(workspace.path())
        .output()
        .unwrap();
    assert!(output.status.success());

    let backups: Vec<_> = fs::read_dir(&codex_dir)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().contains("cupld-backup"))
        .collect();
    assert_eq!(backups.len(), 1);
    assert_eq!(
        fs::read_to_string(backups[0].path()).unwrap(),
        "# keep\n[other]\nvalue = true\n"
    );
    let updated = fs::read_to_string(codex_config).unwrap();
    assert!(updated.contains("# keep\n[other]\nvalue = true\n"));
    assert!(updated.contains("[mcp_servers.cupld-memory]"));
}

#[test]
fn cli_install_mcp_claude_cwd_preserves_other_servers() {
    let home = TempDir::new("home_mcp_claude");
    let config = TempDir::new("config_mcp_claude");
    let workspace = TempDir::new("workspace_mcp_claude");
    fs::write(
        workspace.path().join(".mcp.json"),
        "{ \"mcpServers\": { \"other\": { \"command\": \"x\" } }, \"keep\": true }",
    )
    .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_cupld"))
        .args([
            "install",
            "--mcp",
            "--target",
            "claude",
            "--scope",
            "cwd",
            "--mcp-command",
            "cupld",
        ])
        .env("HOME", home.path())
        .env(
            if cfg!(windows) {
                "APPDATA"
            } else {
                "XDG_CONFIG_HOME"
            },
            config.path(),
        )
        .current_dir(workspace.path())
        .output()
        .unwrap();
    assert!(output.status.success());

    let contents = fs::read_to_string(workspace.path().join(".mcp.json")).unwrap();
    assert!(contents.contains("\"other\""));
    assert!(contents.contains("\"keep\": true"));
    assert!(contents.contains("\"cupld-memory\""));
    assert!(contents.contains("\"cupldManaged\": true"));
    assert!(
        workspace
            .path()
            .join(".claude/skills/cupld-md-memory/SKILL.md")
            .exists()
    );
}

#[test]
fn cli_install_mcp_dry_run_and_print_only_write_nothing() {
    let home = TempDir::new("home_mcp_preview");
    let config = TempDir::new("config_mcp_preview");
    let workspace = TempDir::new("workspace_mcp_preview");

    let dry_run = Command::new(env!("CARGO_BIN_EXE_cupld"))
        .args([
            "install",
            "--mcp",
            "--dry-run",
            "--target",
            "codex",
            "--scope",
            "cwd",
            "--mcp-command",
            "cupld",
        ])
        .env("HOME", home.path())
        .env(
            if cfg!(windows) {
                "APPDATA"
            } else {
                "XDG_CONFIG_HOME"
            },
            config.path(),
        )
        .current_dir(workspace.path())
        .output()
        .unwrap();
    assert!(dry_run.status.success());
    let stdout = String::from_utf8(dry_run.stdout).unwrap();
    assert!(stdout.contains("mcp_config_action create"));
    assert!(stdout.contains("skill_path_action install"));
    assert!(!workspace.path().join(".codex").exists());
    assert!(!workspace.path().join(".agents").exists());
    assert!(!workspace.path().join(".cupld/default.cupld").exists());
    assert!(!config.path().join(".cupld/install-state.toml").exists());

    let print_only = Command::new(env!("CARGO_BIN_EXE_cupld"))
        .args([
            "install",
            "--mcp",
            "--print-only",
            "--target",
            "codex",
            "--scope",
            "cwd",
            "--mcp-command",
            "cupld",
        ])
        .env("HOME", home.path())
        .env(
            if cfg!(windows) {
                "APPDATA"
            } else {
                "XDG_CONFIG_HOME"
            },
            config.path(),
        )
        .current_dir(workspace.path())
        .output()
        .unwrap();
    assert!(print_only.status.success());
    let stdout = String::from_utf8(print_only.stdout).unwrap();
    assert!(stdout.starts_with("# BEGIN cupld managed mcp server"));
    assert!(!stdout.contains("installed_skill"));
    assert!(!workspace.path().join(".codex").exists());
    assert!(!workspace.path().join(".agents").exists());
    assert!(!config.path().join(".cupld/install-state.toml").exists());
}

#[test]
fn cli_install_mcp_opencode_is_unsupported_but_print_only_renders_manual_snippet() {
    let home = TempDir::new("home_mcp_opencode");
    let config = TempDir::new("config_mcp_opencode");
    let workspace = TempDir::new("workspace_mcp_opencode");

    let output = Command::new(env!("CARGO_BIN_EXE_cupld"))
        .args([
            "install",
            "--mcp",
            "--target",
            "opencode",
            "--scope",
            "cwd",
            "--mcp-command",
            "cupld",
        ])
        .env("HOME", home.path())
        .env(
            if cfg!(windows) {
                "APPDATA"
            } else {
                "XDG_CONFIG_HOME"
            },
            config.path(),
        )
        .current_dir(workspace.path())
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("OpenCode MCP config writing is not supported yet"));
    assert!(!workspace.path().join(".opencode").exists());
    assert!(!workspace.path().join(".cupld/default.cupld").exists());

    let print_only = Command::new(env!("CARGO_BIN_EXE_cupld"))
        .args([
            "install",
            "--mcp",
            "--print-only",
            "--target",
            "opencode",
            "--scope",
            "cwd",
            "--mcp-command",
            "cupld",
        ])
        .env("HOME", home.path())
        .env(
            if cfg!(windows) {
                "APPDATA"
            } else {
                "XDG_CONFIG_HOME"
            },
            config.path(),
        )
        .current_dir(workspace.path())
        .output()
        .unwrap();
    assert!(print_only.status.success());
    let stdout = String::from_utf8(print_only.stdout).unwrap();
    assert!(stdout.contains("\"mcpServers\""));
    assert!(stdout.contains("\"cupld-memory\""));
    assert!(!workspace.path().join(".opencode").exists());
    assert!(!workspace.path().join(".cupld/default.cupld").exists());
}

#[test]
fn cli_install_mcp_config_write_failure_stops_before_skill_db_and_state() {
    let home = TempDir::new("home_mcp_config_fail");
    let config = TempDir::new("config_mcp_config_fail");
    let workspace = TempDir::new("workspace_mcp_config_fail");
    fs::write(workspace.path().join(".codex"), "not a directory").unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_cupld"))
        .args([
            "install",
            "--mcp",
            "--target",
            "codex",
            "--scope",
            "cwd",
            "--mcp-command",
            "cupld",
        ])
        .env("HOME", home.path())
        .env(
            if cfg!(windows) {
                "APPDATA"
            } else {
                "XDG_CONFIG_HOME"
            },
            config.path(),
        )
        .current_dir(workspace.path())
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(!workspace.path().join(".agents").exists());
    assert!(!workspace.path().join(".cupld/default.cupld").exists());
    assert!(!config.path().join(".cupld/install-state.toml").exists());
}

#[test]
fn cli_install_mcp_skill_failure_reports_written_config_without_state() {
    let home = TempDir::new("home_mcp_skill_fail");
    let config = TempDir::new("config_mcp_skill_fail");
    let workspace = TempDir::new("workspace_mcp_skill_fail");
    fs::create_dir_all(workspace.path().join(".agents/skills")).unwrap();
    fs::write(
        workspace.path().join(".agents/skills/cupld-md-memory"),
        "not a directory",
    )
    .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_cupld"))
        .args([
            "install",
            "--mcp",
            "--target",
            "codex",
            "--scope",
            "cwd",
            "--mcp-command",
            "cupld",
        ])
        .env("HOME", home.path())
        .env(
            if cfg!(windows) {
                "APPDATA"
            } else {
                "XDG_CONFIG_HOME"
            },
            config.path(),
        )
        .current_dir(workspace.path())
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(workspace.path().join(".codex/config.toml").exists());
    assert!(!workspace.path().join(".cupld/default.cupld").exists());
    assert!(!config.path().join(".cupld/install-state.toml").exists());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("MCP config was already written"));
    assert!(stderr.contains(".codex/config.toml"));
}

#[test]
fn cli_install_mcp_db_bootstrap_failure_reports_written_config_without_state() {
    let home = TempDir::new("home_mcp_db_fail");
    let config = TempDir::new("config_mcp_db_fail");
    let workspace = TempDir::new("workspace_mcp_db_fail");
    fs::create_dir_all(workspace.path().join(".cupld")).unwrap();
    let root_file = workspace.path().join(".cupld/data");
    fs::write(&root_file, "not a directory").unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_cupld"))
        .args([
            "install",
            "--mcp",
            "--target",
            "codex",
            "--scope",
            "cwd",
            "--root",
            root_file.to_str().unwrap(),
            "--mcp-command",
            "cupld",
        ])
        .env("HOME", home.path())
        .env(
            if cfg!(windows) {
                "APPDATA"
            } else {
                "XDG_CONFIG_HOME"
            },
            config.path(),
        )
        .current_dir(workspace.path())
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(workspace.path().join(".codex/config.toml").exists());
    assert!(
        workspace
            .path()
            .join(".agents/skills/cupld-md-memory/SKILL.md")
            .exists()
    );
    assert!(!config.path().join(".cupld/install-state.toml").exists());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("MCP config was already written"));
}

#[test]
fn cli_install_mcp_state_failure_warns_and_keeps_install() {
    let home = TempDir::new("home_mcp_state_fail");
    let config_parent_file = TempDir::new("config_mcp_state_fail");
    let config_file = config_parent_file.path().join("not-a-dir");
    fs::write(&config_file, "file").unwrap();
    let workspace = TempDir::new("workspace_mcp_state_fail");

    let output = Command::new(env!("CARGO_BIN_EXE_cupld"))
        .args([
            "install",
            "--mcp",
            "--target",
            "codex",
            "--scope",
            "cwd",
            "--mcp-command",
            "cupld",
        ])
        .env("HOME", home.path())
        .env(
            if cfg!(windows) {
                "APPDATA"
            } else {
                "XDG_CONFIG_HOME"
            },
            &config_file,
        )
        .current_dir(workspace.path())
        .output()
        .unwrap();

    assert!(output.status.success());
    assert!(workspace.path().join(".codex/config.toml").exists());
    assert!(
        workspace
            .path()
            .join(".agents/skills/cupld-md-memory/SKILL.md")
            .exists()
    );
    assert!(workspace.path().join(".cupld/default.cupld").exists());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("warning:"));
}
