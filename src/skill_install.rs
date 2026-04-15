use std::env;
use std::fs;
use std::io::{self, BufRead, ErrorKind, IsTerminal, Write};
use std::path::{Path, PathBuf};

use cupld::{Session, set_markdown_root};

pub const BUNDLED_SKILL_NAME: &str = "cupld-md-memory";
pub const BUNDLED_SKILL_FILENAME: &str = "SKILL.md";

const BUNDLED_SKILL_CONTENTS: &str = include_str!("../skills/cupld-md-memory/SKILL.md");
const CONFIG_DIR_NAME: &str = ".cupld";
const INSTALLED_SKILL_PATH_FILE: &str = "installed-skill-path.txt";
const PROMPT_DISABLED_FILE: &str = "skill-install-prompt.disabled";
const DEFAULT_DB_PATH: &str = ".cupld/default.cupld";
const DEFAULT_ROOT_PATH: &str = ".cupld/data";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SkillInstallTarget {
    Codex,
    Claude,
    Opencode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InstallScope {
    Cwd,
    Home,
}

impl InstallScope {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "cwd" => Some(Self::Cwd),
            "home" => Some(Self::Home),
            _ => None,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Cwd => "$CWD",
            Self::Home => "$HOME",
        }
    }
}

impl SkillInstallTarget {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "codex" => Some(Self::Codex),
            "claude" => Some(Self::Claude),
            "opencode" => Some(Self::Opencode),
            _ => None,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Codex => "Codex",
            Self::Claude => "Claude Code",
            Self::Opencode => "OpenCode",
        }
    }

    #[cfg(test)]
    fn relative_project_skills_root(self) -> &'static str {
        match self {
            Self::Codex => ".agents/skills",
            Self::Claude => ".claude/skills",
            Self::Opencode => ".opencode/skills",
        }
    }

    fn skills_root(self, scope: InstallScope, home: &Path, cwd: &Path) -> PathBuf {
        match (self, scope) {
            (Self::Codex, InstallScope::Cwd) => cwd.join(".agents").join("skills"),
            (Self::Codex, InstallScope::Home) => home.join(".agents").join("skills"),
            (Self::Claude, InstallScope::Cwd) => cwd.join(".claude").join("skills"),
            (Self::Claude, InstallScope::Home) => home.join(".claude").join("skills"),
            (Self::Opencode, InstallScope::Cwd) => cwd.join(".opencode").join("skills"),
            (Self::Opencode, InstallScope::Home) => {
                home.join(".config").join("opencode").join("skills")
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstallCommand {
    pub target: Option<SkillInstallTarget>,
    pub scope: Option<InstallScope>,
    pub path: Option<PathBuf>,
    pub db_path: Option<PathBuf>,
    pub root: Option<PathBuf>,
    pub force: bool,
    pub yes: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct PromptState {
    installed_skill_path: Option<PathBuf>,
    prompt_disabled: bool,
}

#[derive(Debug)]
enum InstallChoice {
    Request(InstallRequest),
    Skip,
    NeverAskAgain,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct InstallRequest {
    skills_root: PathBuf,
    db_path: PathBuf,
    root: PathBuf,
    force: bool,
    yes: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum InstallStatus {
    Installed,
    Overwritten,
    AlreadyInstalled,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct InstallOutcome {
    skill_path: PathBuf,
    db_path: PathBuf,
    root: PathBuf,
    status: InstallStatus,
}

pub fn maybe_prompt_for_repl() -> Result<(), String> {
    let interactive = io::stdin().is_terminal() && io::stdout().is_terminal();
    let state = load_prompt_state()?;
    if !should_prompt_for_repl(
        interactive,
        state.installed_skill_path.as_deref(),
        state.prompt_disabled,
    ) {
        return Ok(());
    }

    let choice = prompt_for_repl_choice(&mut io::stdin().lock(), &mut io::stdout(), false, false)?;
    handle_choice(choice)
}

pub fn install(command: InstallCommand) -> Result<(), String> {
    let interactive = io::stdin().is_terminal() && io::stdout().is_terminal();
    let request = resolve_install_request(command, interactive)?;
    handle_request(request, interactive)
}

pub fn should_prompt_for_repl(
    interactive: bool,
    installed_skill_path: Option<&Path>,
    prompt_disabled: bool,
) -> bool {
    interactive && !prompt_disabled && installed_skill_path.is_none()
}

fn handle_choice(choice: InstallChoice) -> Result<(), String> {
    match choice {
        InstallChoice::Request(request) => {
            let interactive = io::stdin().is_terminal() && io::stdout().is_terminal();
            handle_request(request, interactive)
        }
        InstallChoice::Skip => {
            println!("skipped cupld install");
            Ok(())
        }
        InstallChoice::NeverAskAgain => {
            create_prompt_disabled_sentinel()?;
            println!("install_prompt disabled");
            Ok(())
        }
    }
}

fn handle_request(request: InstallRequest, interactive: bool) -> Result<(), String> {
    let outcome = install_request(&request, interactive)?;
    println!("installed_skill {}", outcome.skill_path.display());
    println!("installed_db {}", outcome.db_path.display());
    println!("markdown_root {}", outcome.root.display());
    if let Err(error) = persist_installed_skill_path(&outcome.skill_path) {
        eprintln!("warning: {error}");
    }
    if let Err(error) = clear_prompt_disabled_sentinel() {
        eprintln!("warning: {error}");
    }
    Ok(())
}

fn resolve_install_request(
    command: InstallCommand,
    interactive: bool,
) -> Result<InstallRequest, String> {
    if command.target.is_some() && command.path.is_some() {
        return Err("`install` accepts either --target or --path, not both".to_owned());
    }
    if command.path.is_some() && command.scope.is_some() {
        return Err("`install` accepts --scope only with --target".to_owned());
    }

    let skills_root = if let Some(path) = command.path.as_deref() {
        Some(resolve_input_path(path)?)
    } else if let Some(target) = command.target {
        Some(skills_root_for_target(
            target,
            command.scope.unwrap_or(InstallScope::Home),
        )?)
    } else {
        None
    };

    let db_path = match command.db_path.as_deref() {
        Some(path) => Some(resolve_input_path(path)?),
        None => None,
    };
    let root = match command.root.as_deref() {
        Some(path) => Some(resolve_input_path(path)?),
        None => None,
    };

    if !interactive {
        let Some(skills_root) = skills_root else {
            return Err(
                "non-interactive `install` requires --target <codex|claude|opencode> [--scope <cwd|home>] or --path <skills-root>"
                    .to_owned(),
            );
        };
        let Some(db_path) = db_path else {
            return Err("non-interactive `install` requires --db <path.cupld>".to_owned());
        };
        return Ok(InstallRequest {
            skills_root,
            db_path,
            root: root.unwrap_or(default_root_path()?),
            force: command.force,
            yes: command.yes,
        });
    }

    prompt_for_install_request(
        &mut io::stdin().lock(),
        &mut io::stdout(),
        skills_root,
        db_path,
        root,
        command.force,
        command.yes,
    )
}

fn prompt_for_repl_choice<R: BufRead, W: Write>(
    input: &mut R,
    output: &mut W,
    force: bool,
    yes: bool,
) -> Result<InstallChoice, String> {
    let presets = install_presets()?;

    writeln!(output, "Install cupld agent memory bootstrap?").map_err(io_error)?;
    for (index, target, scope, path) in &presets {
        writeln!(
            output,
            "  {}) {} {} ({})",
            index,
            target.label(),
            scope.label(),
            path.display()
        )
        .map_err(io_error)?;
    }
    let custom_choice = presets.len() + 1;
    let skip_choice = presets.len() + 2;
    let never_choice = presets.len() + 3;
    writeln!(output, "  {}) Custom path", custom_choice).map_err(io_error)?;
    writeln!(output, "  {}) Skip for now", skip_choice).map_err(io_error)?;
    writeln!(output, "  {}) Never ask again", never_choice).map_err(io_error)?;
    write!(output, "Choice [1-{}]: ", never_choice).map_err(io_error)?;
    output.flush().map_err(io_error)?;

    let choice = read_prompt_line(input)?.unwrap_or_default();
    let trimmed = choice.trim();
    let skills_root = if let Some((_, _, _, path)) = presets
        .iter()
        .find(|(index, _, _, _)| trimmed == index.to_string())
    {
        Some(path.clone())
    } else {
        match trimmed {
            value if value == custom_choice.to_string() => {
                write!(output, "Skills root path: ").map_err(io_error)?;
                output.flush().map_err(io_error)?;
                let Some(path) = read_prompt_line(input)? else {
                    return Ok(InstallChoice::Skip);
                };
                let path = path.trim();
                if path.is_empty() {
                    return Ok(InstallChoice::Skip);
                }
                Some(resolve_input_path(Path::new(path))?)
            }
            value if value == skip_choice.to_string() || value.is_empty() => {
                return Ok(InstallChoice::Skip);
            }
            value if value == never_choice.to_string() => return Ok(InstallChoice::NeverAskAgain),
            other => {
                return Err(format!(
                    "expected a choice from 1-{never_choice}, got `{other}`"
                ));
            }
        }
    };

    let request = prompt_for_install_request(input, output, skills_root, None, None, force, yes)?;
    Ok(InstallChoice::Request(request))
}

fn prompt_for_install_request<R: BufRead, W: Write>(
    input: &mut R,
    output: &mut W,
    skills_root: Option<PathBuf>,
    db_path: Option<PathBuf>,
    root: Option<PathBuf>,
    force: bool,
    yes: bool,
) -> Result<InstallRequest, String> {
    let skills_root = match skills_root {
        Some(path) => path,
        None => prompt_for_skills_root(input, output)?,
    };
    let db_path = match db_path {
        Some(path) => path,
        None => prompt_for_path(input, output, "DB path", DEFAULT_DB_PATH)?,
    };
    let root = match root {
        Some(path) => path,
        None => prompt_for_path(input, output, "Markdown root", DEFAULT_ROOT_PATH)?,
    };

    Ok(InstallRequest {
        skills_root,
        db_path,
        root,
        force,
        yes,
    })
}

fn prompt_for_skills_root<R: BufRead, W: Write>(
    input: &mut R,
    output: &mut W,
) -> Result<PathBuf, String> {
    let presets = install_presets()?;
    writeln!(output, "Select skills install location:").map_err(io_error)?;
    for (index, target, scope, path) in &presets {
        writeln!(
            output,
            "  {}) {} {} ({})",
            index,
            target.label(),
            scope.label(),
            path.display()
        )
        .map_err(io_error)?;
    }
    let custom_choice = presets.len() + 1;
    writeln!(output, "  {}) Custom path", custom_choice).map_err(io_error)?;
    write!(output, "Choice [1-{}, default 1]: ", custom_choice).map_err(io_error)?;
    output.flush().map_err(io_error)?;

    let choice = read_prompt_line(input)?.unwrap_or_default();
    let trimmed = choice.trim();
    if trimmed.is_empty() || trimmed == "1" {
        return Ok(presets[0].3.clone());
    }
    if let Some((_, _, _, path)) = presets
        .iter()
        .find(|(index, _, _, _)| trimmed == index.to_string())
    {
        return Ok(path.clone());
    }
    if trimmed == custom_choice.to_string() {
        write!(output, "Skills root path: ").map_err(io_error)?;
        output.flush().map_err(io_error)?;
        let path = read_prompt_line(input)?.unwrap_or_default();
        let path = path.trim();
        if path.is_empty() {
            return Err("expected a skills root path".to_owned());
        }
        return resolve_input_path(Path::new(path));
    }
    Err(format!(
        "expected a choice from 1-{custom_choice}, got `{trimmed}`"
    ))
}

fn prompt_for_path<R: BufRead, W: Write>(
    input: &mut R,
    output: &mut W,
    label: &str,
    default: &str,
) -> Result<PathBuf, String> {
    write!(output, "{label} [{default}]: ").map_err(io_error)?;
    output.flush().map_err(io_error)?;
    let value = read_prompt_line(input)?.unwrap_or_default();
    let value = value.trim();
    let path = if value.is_empty() {
        PathBuf::from(default)
    } else {
        PathBuf::from(value)
    };
    resolve_input_path(&path)
}

fn read_prompt_line<R: BufRead>(input: &mut R) -> Result<Option<String>, String> {
    let mut line = String::new();
    match input.read_line(&mut line).map_err(io_error)? {
        0 => Ok(None),
        _ => Ok(Some(line)),
    }
}

fn install_request(request: &InstallRequest, interactive: bool) -> Result<InstallOutcome, String> {
    let skill_dir = request.skills_root.join(BUNDLED_SKILL_NAME);
    let skill_path = skill_dir.join(BUNDLED_SKILL_FILENAME);
    fs::create_dir_all(&skill_dir).map_err(io_error)?;

    let status = if skill_path.exists() {
        if !request.force && !request.yes && !confirm_overwrite(&skill_path, interactive)? {
            return Err(format!(
                "refusing to overwrite existing skill at {} without --force or --yes",
                skill_path.display()
            ));
        }
        let existing = fs::read_to_string(&skill_path).map_err(io_error)?;
        if existing == BUNDLED_SKILL_CONTENTS {
            InstallStatus::AlreadyInstalled
        } else {
            fs::write(&skill_path, BUNDLED_SKILL_CONTENTS).map_err(io_error)?;
            InstallStatus::Overwritten
        }
    } else {
        fs::write(&skill_path, BUNDLED_SKILL_CONTENTS).map_err(io_error)?;
        InstallStatus::Installed
    };

    let db_path = bootstrap_memory(&request.db_path, &request.root)?;
    Ok(InstallOutcome {
        skill_path: canonicalize_existing_path(&skill_path)?,
        db_path,
        root: canonicalize_existing_path(&request.root)?,
        status,
    })
}

fn bootstrap_memory(db_path: &Path, root: &Path) -> Result<PathBuf, String> {
    if let Some(parent) = db_path.parent() {
        fs::create_dir_all(parent).map_err(io_error)?;
    }
    fs::create_dir_all(root).map_err(io_error)?;

    let canonical_root = canonicalize_existing_path(root)?;
    let mut session = if db_path.exists() {
        Session::open(db_path).map_err(|error| error.to_string())?
    } else {
        let mut session = Session::new_in_memory();
        session
            .save_as(db_path)
            .map_err(|error| error.to_string())?;
        session
    };
    let mut engine = session.engine().clone();
    set_markdown_root(&mut engine, &canonical_root).map_err(|error| error.to_string())?;
    engine.commit().map_err(|error| error.to_string())?;
    session
        .replace_engine(engine)
        .map_err(|error| error.to_string())?;
    session.save().map_err(|error| error.to_string())?;
    canonicalize_existing_path(db_path)
}

fn confirm_overwrite(skill_path: &Path, interactive: bool) -> Result<bool, String> {
    if !interactive {
        return Ok(false);
    }

    print!(
        "overwrite existing skill at {}? [Y/n]: ",
        skill_path.display()
    );
    io::stdout().flush().map_err(io_error)?;
    let mut answer = String::new();
    io::stdin().read_line(&mut answer).map_err(io_error)?;
    Ok(!matches!(answer.trim(), "n" | "N" | "no" | "NO"))
}

fn load_prompt_state() -> Result<PromptState, String> {
    let config_dir = config_dir()?;
    prompt_state_from_config_dir(&config_dir)
}

fn load_installed_skill_path(state_path: &Path) -> Result<Option<PathBuf>, String> {
    let contents = match fs::read_to_string(state_path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error(error)),
    };
    let value = contents.trim();
    if value.is_empty() {
        return Ok(None);
    }
    let path = PathBuf::from(value);
    if path.exists() {
        Ok(Some(canonicalize_existing_path(&path)?))
    } else {
        Ok(None)
    }
}

fn prompt_state_from_config_dir(config_dir: &Path) -> Result<PromptState, String> {
    let prompt_disabled = config_dir.join(PROMPT_DISABLED_FILE).exists();
    let state_path = config_dir.join(INSTALLED_SKILL_PATH_FILE);
    let installed_skill_path = load_installed_skill_path(&state_path)?;
    Ok(PromptState {
        installed_skill_path,
        prompt_disabled,
    })
}

fn persist_installed_skill_path(skill_path: &Path) -> Result<(), String> {
    let config_dir = config_dir()?;
    fs::create_dir_all(&config_dir).map_err(io_error)?;
    let state_path = config_dir.join(INSTALLED_SKILL_PATH_FILE);
    let path = canonicalize_existing_path(skill_path)?;
    fs::write(state_path, format!("{}\n", path.display())).map_err(io_error)
}

fn create_prompt_disabled_sentinel() -> Result<(), String> {
    let config_dir = config_dir()?;
    fs::create_dir_all(&config_dir).map_err(io_error)?;
    fs::write(config_dir.join(PROMPT_DISABLED_FILE), b"disabled\n").map_err(io_error)
}

fn clear_prompt_disabled_sentinel() -> Result<(), String> {
    let config_dir = config_dir()?;
    let sentinel = config_dir.join(PROMPT_DISABLED_FILE);
    if sentinel.exists() {
        fs::remove_file(sentinel).map_err(io_error)?;
    }
    Ok(())
}

fn install_presets() -> Result<Vec<(usize, SkillInstallTarget, InstallScope, PathBuf)>, String> {
    let home = home_dir().ok_or("could not resolve home directory".to_owned())?;
    let cwd = env::current_dir().map_err(io_error)?;
    Ok(vec![
        (
            1,
            SkillInstallTarget::Codex,
            InstallScope::Cwd,
            SkillInstallTarget::Codex.skills_root(InstallScope::Cwd, &home, &cwd),
        ),
        (
            2,
            SkillInstallTarget::Claude,
            InstallScope::Cwd,
            SkillInstallTarget::Claude.skills_root(InstallScope::Cwd, &home, &cwd),
        ),
        (
            3,
            SkillInstallTarget::Opencode,
            InstallScope::Cwd,
            SkillInstallTarget::Opencode.skills_root(InstallScope::Cwd, &home, &cwd),
        ),
        (
            4,
            SkillInstallTarget::Codex,
            InstallScope::Home,
            SkillInstallTarget::Codex.skills_root(InstallScope::Home, &home, &cwd),
        ),
        (
            5,
            SkillInstallTarget::Claude,
            InstallScope::Home,
            SkillInstallTarget::Claude.skills_root(InstallScope::Home, &home, &cwd),
        ),
        (
            6,
            SkillInstallTarget::Opencode,
            InstallScope::Home,
            SkillInstallTarget::Opencode.skills_root(InstallScope::Home, &home, &cwd),
        ),
    ])
}

fn skills_root_for_target(
    target: SkillInstallTarget,
    scope: InstallScope,
) -> Result<PathBuf, String> {
    let home = home_dir().ok_or("could not resolve home directory".to_owned())?;
    let cwd = env::current_dir().map_err(io_error)?;
    Ok(target.skills_root(scope, &home, &cwd))
}

fn default_root_path() -> Result<PathBuf, String> {
    resolve_input_path(Path::new(DEFAULT_ROOT_PATH))
}

fn resolve_input_path(path: &Path) -> Result<PathBuf, String> {
    let expanded = expand_tilde_with_home(path, home_dir().as_deref())?;
    if expanded.is_absolute() {
        Ok(expanded)
    } else {
        Ok(env::current_dir().map_err(io_error)?.join(expanded))
    }
}

fn expand_tilde_with_home(path: &Path, home: Option<&Path>) -> Result<PathBuf, String> {
    let Some(raw) = path.to_str() else {
        return Ok(path.to_path_buf());
    };
    if raw == "~" {
        return home
            .map(Path::to_path_buf)
            .ok_or("could not resolve home directory".to_owned());
    }
    if let Some(rest) = raw.strip_prefix("~/") {
        return Ok(home
            .ok_or("could not resolve home directory".to_owned())?
            .join(rest));
    }
    if let Some(rest) = raw.strip_prefix("~\\") {
        return Ok(home
            .ok_or("could not resolve home directory".to_owned())?
            .join(rest));
    }
    Ok(path.to_path_buf())
}

fn config_dir() -> Result<PathBuf, String> {
    if cfg!(windows)
        && let Some(path) = env_path("APPDATA")
    {
        return Ok(path.join(CONFIG_DIR_NAME));
    }
    if let Some(path) = env_path("XDG_CONFIG_HOME") {
        return Ok(path.join(CONFIG_DIR_NAME));
    }
    let home = home_dir().ok_or("could not resolve home directory".to_owned())?;
    Ok(home.join(".config").join(CONFIG_DIR_NAME))
}

fn home_dir() -> Option<PathBuf> {
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

fn env_path(name: &str) -> Option<PathBuf> {
    env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn canonicalize_existing_path(path: &Path) -> Result<PathBuf, String> {
    path.canonicalize().map_err(io_error)
}

fn io_error(error: io::Error) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use super::{
        BUNDLED_SKILL_CONTENTS, BUNDLED_SKILL_FILENAME, BUNDLED_SKILL_NAME, CONFIG_DIR_NAME,
        INSTALLED_SKILL_PATH_FILE, InstallChoice, InstallCommand, InstallRequest, InstallScope,
        InstallStatus, PROMPT_DISABLED_FILE, PromptState, SkillInstallTarget, bootstrap_memory,
        canonicalize_existing_path, default_root_path, expand_tilde_with_home, install_request,
        load_installed_skill_path, prompt_for_install_request, prompt_for_repl_choice,
        prompt_state_from_config_dir, resolve_install_request, should_prompt_for_repl,
    };
    use cupld::{Session, configured_markdown_root};
    use std::fs;
    use std::io::Cursor;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static NEXT_TEMP_DIR_ID: AtomicUsize = AtomicUsize::new(1);

    #[test]
    fn prompt_only_runs_for_interactive_repl_without_state() {
        assert!(should_prompt_for_repl(true, None, false));
        assert!(!should_prompt_for_repl(false, None, false));
        assert!(!should_prompt_for_repl(
            true,
            Some(Path::new("/tmp/skill")),
            false
        ));
        assert!(!should_prompt_for_repl(true, None, true));
    }

    #[test]
    fn repl_prompt_supports_presets_skip_never_and_custom() {
        let root = temp_dir("repl_prompt_choice");

        let mut codex = Cursor::new(b"1\n\n\n");
        let choice = prompt_for_repl_choice(&mut codex, &mut Vec::new(), false, false).unwrap();
        let request = choice_request(choice);
        assert_eq!(
            request,
            InstallRequest {
                skills_root: std::env::current_dir()
                    .unwrap()
                    .join(".agents")
                    .join("skills"),
                db_path: std::env::current_dir()
                    .unwrap()
                    .join(".cupld")
                    .join("default.cupld"),
                root: std::env::current_dir().unwrap().join(".cupld").join("data"),
                force: false,
                yes: false,
            }
        );

        let mut custom = Cursor::new(
            format!(
                "7\n{}\n{}\n{}\n",
                root.display(),
                root.join("db.cupld").display(),
                root.join("notes").display()
            )
            .into_bytes(),
        );
        let choice = prompt_for_repl_choice(&mut custom, &mut Vec::new(), true, true).unwrap();
        assert_eq!(
            choice_request(choice),
            InstallRequest {
                skills_root: root.clone(),
                db_path: root.join("db.cupld"),
                root: root.join("notes"),
                force: true,
                yes: true,
            }
        );

        let mut skip = Cursor::new(b"8\n");
        assert!(matches!(
            prompt_for_repl_choice(&mut skip, &mut Vec::new(), false, false).unwrap(),
            InstallChoice::Skip
        ));

        let mut never = Cursor::new(b"9\n");
        assert!(matches!(
            prompt_for_repl_choice(&mut never, &mut Vec::new(), false, false).unwrap(),
            InstallChoice::NeverAskAgain
        ));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn explicit_install_prompt_fills_missing_fields() {
        let root = temp_dir("explicit_prompt_choice");
        let mut input = Cursor::new(format!("7\n{}\n\n\n", root.display()).into_bytes());
        let request =
            prompt_for_install_request(&mut input, &mut Vec::new(), None, None, None, false, false)
                .unwrap();
        assert_eq!(request.skills_root, root);
        assert_eq!(
            request.db_path,
            std::env::current_dir()
                .unwrap()
                .join(".cupld")
                .join("default.cupld")
        );
        assert_eq!(
            request.root,
            std::env::current_dir().unwrap().join(".cupld").join("data")
        );
    }

    #[test]
    fn resolve_install_request_requires_flags_when_non_interactive() {
        let command = InstallCommand {
            target: None,
            scope: None,
            path: None,
            db_path: None,
            root: None,
            force: false,
            yes: false,
        };
        let error = resolve_install_request(command, false).unwrap_err();
        assert!(error.contains("non-interactive"));
    }

    #[test]
    fn install_request_is_idempotent_and_bootstraps_memory() {
        let root = temp_dir("install_request");
        let request = InstallRequest {
            skills_root: root.join("skills"),
            db_path: root.join("state").join("default.cupld"),
            root: root.join("notes"),
            force: false,
            yes: false,
        };

        let first = install_request(&request, false).unwrap();
        assert_eq!(first.status, InstallStatus::Installed);
        assert!(first.db_path.exists());
        assert!(first.root.exists());

        let session = Session::open(&first.db_path).unwrap();
        assert_eq!(
            configured_markdown_root(session.engine()),
            Some(first.root.clone())
        );

        let second = install_request(
            &InstallRequest {
                yes: true,
                ..request.clone()
            },
            false,
        )
        .unwrap();
        assert_eq!(second.status, InstallStatus::AlreadyInstalled);

        let skill_path = root
            .join("skills")
            .join(BUNDLED_SKILL_NAME)
            .join(BUNDLED_SKILL_FILENAME);
        fs::write(&skill_path, "different").unwrap();
        let error = install_request(&request, false).unwrap_err();
        assert!(error.contains("without --force or --yes"));

        let forced = install_request(
            &InstallRequest {
                skills_root: root.join("skills"),
                db_path: root.join("state").join("default.cupld"),
                root: root.join("notes"),
                force: true,
                yes: false,
            },
            false,
        )
        .unwrap();
        assert_eq!(forced.status, InstallStatus::Overwritten);
        assert_eq!(
            fs::read_to_string(&skill_path).unwrap(),
            BUNDLED_SKILL_CONTENTS
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn bootstrap_memory_creates_db_and_sets_root() {
        let root = temp_dir("bootstrap_memory");
        let db_path = root.join("state").join("default.cupld");
        let notes_root = root.join("notes");
        let canonical_db = bootstrap_memory(&db_path, &notes_root).unwrap();
        assert_eq!(canonical_db, canonicalize_existing_path(&db_path).unwrap());
        let session = Session::open(&db_path).unwrap();
        assert_eq!(
            configured_markdown_root(session.engine()),
            Some(canonicalize_existing_path(&notes_root).unwrap())
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn installed_path_state_ignores_missing_files() {
        let root = temp_dir("state");
        let state_path = root.join(INSTALLED_SKILL_PATH_FILE);
        fs::write(&state_path, root.join("missing").display().to_string()).unwrap();
        assert_eq!(load_installed_skill_path(&state_path).unwrap(), None);

        let skill_path = root.join(BUNDLED_SKILL_NAME).join(BUNDLED_SKILL_FILENAME);
        fs::create_dir_all(skill_path.parent().unwrap()).unwrap();
        fs::write(&skill_path, BUNDLED_SKILL_CONTENTS).unwrap();
        fs::write(&state_path, skill_path.display().to_string()).unwrap();
        assert_eq!(
            load_installed_skill_path(&state_path).unwrap(),
            Some(canonicalize_existing_path(&skill_path).unwrap())
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn expand_tilde_uses_supplied_home() {
        let home = temp_dir("home");
        let expanded = expand_tilde_with_home(Path::new("~/skills"), Some(&home)).unwrap();
        assert_eq!(expanded, home.join("skills"));
        fs::remove_dir_all(home).unwrap();
    }

    #[test]
    fn default_root_path_uses_repo_convention() {
        assert_eq!(
            default_root_path().unwrap(),
            std::env::current_dir().unwrap().join(".cupld").join("data")
        );
    }

    #[test]
    fn resolve_install_request_supports_target_scope_and_path_rules() {
        let home = std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .or_else(
                || match (std::env::var_os("HOMEDRIVE"), std::env::var_os("HOMEPATH")) {
                    (Some(drive), Some(path)) => Some(std::ffi::OsString::from(format!(
                        "{}{}",
                        drive.to_string_lossy(),
                        path.to_string_lossy()
                    ))),
                    _ => None,
                },
            )
            .map(PathBuf::from)
            .unwrap();
        let cwd = std::env::current_dir().unwrap();
        let command = InstallCommand {
            target: Some(SkillInstallTarget::Codex),
            scope: Some(InstallScope::Cwd),
            path: None,
            db_path: Some(PathBuf::from("db.cupld")),
            root: None,
            force: false,
            yes: false,
        };
        let request = resolve_install_request(command, false).unwrap();
        assert_eq!(
            request,
            InstallRequest {
                skills_root: cwd.join(".agents").join("skills"),
                db_path: cwd.join("db.cupld"),
                root: cwd.join(".cupld").join("data"),
                force: false,
                yes: false,
            }
        );

        let command = InstallCommand {
            target: Some(SkillInstallTarget::Claude),
            scope: Some(InstallScope::Home),
            path: None,
            db_path: Some(PathBuf::from("db.cupld")),
            root: Some(PathBuf::from("notes")),
            force: true,
            yes: true,
        };
        let request = resolve_install_request(command, false).unwrap();
        assert_eq!(
            request,
            InstallRequest {
                skills_root: home.join(".claude").join("skills"),
                db_path: cwd.join("db.cupld"),
                root: cwd.join("notes"),
                force: true,
                yes: true,
            }
        );

        let command = InstallCommand {
            target: None,
            scope: Some(InstallScope::Home),
            path: Some(PathBuf::from("custom")),
            db_path: Some(PathBuf::from("db.cupld")),
            root: None,
            force: false,
            yes: false,
        };
        let error = resolve_install_request(command, false).unwrap_err();
        assert!(error.contains("--scope only with --target"));
    }

    #[test]
    fn target_relative_roots_match_provider_conventions() {
        assert_eq!(
            SkillInstallTarget::Codex.relative_project_skills_root(),
            ".agents/skills"
        );
        assert_eq!(
            SkillInstallTarget::Claude.relative_project_skills_root(),
            ".claude/skills"
        );
        assert_eq!(
            SkillInstallTarget::Opencode.relative_project_skills_root(),
            ".opencode/skills"
        );
    }

    #[test]
    fn prompt_state_reads_installed_skill_and_disable_sentinel() {
        let root = temp_dir("prompt_state");
        let config_dir = root.join(CONFIG_DIR_NAME);
        fs::create_dir_all(&config_dir).unwrap();

        let skill_path = root.join(BUNDLED_SKILL_NAME).join(BUNDLED_SKILL_FILENAME);
        fs::create_dir_all(skill_path.parent().unwrap()).unwrap();
        fs::write(&skill_path, BUNDLED_SKILL_CONTENTS).unwrap();
        fs::write(
            config_dir.join(INSTALLED_SKILL_PATH_FILE),
            format!("{}\n", skill_path.display()),
        )
        .unwrap();
        fs::write(config_dir.join(PROMPT_DISABLED_FILE), b"disabled\n").unwrap();

        let state = prompt_state_from_config_dir(&config_dir).unwrap();
        assert_eq!(
            state,
            PromptState {
                installed_skill_path: Some(canonicalize_existing_path(&skill_path).unwrap()),
                prompt_disabled: true,
            }
        );

        fs::remove_dir_all(root).unwrap();
    }

    fn choice_request(choice: InstallChoice) -> InstallRequest {
        match choice {
            InstallChoice::Request(request) => request,
            InstallChoice::Skip | InstallChoice::NeverAskAgain => panic!("expected install"),
        }
    }

    fn temp_dir(prefix: &str) -> PathBuf {
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
        path
    }
}
