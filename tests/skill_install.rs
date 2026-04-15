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
    assert!(contents.contains("cupld query --with-markdown"));
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
