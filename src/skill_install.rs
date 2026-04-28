use std::env;
use std::ffi::OsStr;
use std::fs;
use std::io::{self, BufRead, ErrorKind, IsTerminal, Write};
use std::path::{Path, PathBuf};

use cupld::{Session, json, package::WorkspacePackage, set_markdown_root};

pub const BUNDLED_SKILL_NAME: &str = "cupld-md-memory";
pub const BUNDLED_SKILL_FILENAME: &str = "SKILL.md";

const BUNDLED_SKILL_CONTENTS: &str = include_str!("../skills/cupld-md-memory/SKILL.md");
const CONFIG_DIR_NAME: &str = ".cupld";
const INSTALLED_SKILL_PATH_FILE: &str = "installed-skill-path.txt";
const INSTALL_STATE_FILE: &str = "install-state.toml";
const INSTALL_STATE_VERSION: u32 = 3;
const INSTALL_BUNDLE_REVISION: u32 = 1;
const PROMPT_DISABLED_FILE: &str = "skill-install-prompt.disabled";

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
    install_state: Option<InstallState>,
    installed_skill_path: Option<PathBuf>,
    prompt_disabled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct InstallState {
    version: u32,
    installs: Vec<InstallRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct InstallRecord {
    bundle_revision: u32,
    skill_signature: Option<String>,
    skill_path: PathBuf,
    db_path: PathBuf,
    root: PathBuf,
}

#[derive(Default)]
struct InstallRecordBuilder {
    bundle_revision: Option<u32>,
    skill_signature: Option<String>,
    skill_path: Option<PathBuf>,
    db_path: Option<PathBuf>,
    root: Option<PathBuf>,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RefreshReason {
    SkillContents,
    BundleRevision,
    SkillContentsAndBundleRevision,
}

impl RefreshReason {
    fn description(self) -> &'static str {
        match self {
            Self::SkillContents => "bundled SKILL.md changed",
            Self::BundleRevision => "bootstrap bundle changed",
            Self::SkillContentsAndBundleRevision => "bundled SKILL.md and bootstrap bundle changed",
        }
    }
}

impl InstallState {
    fn empty() -> Self {
        Self {
            version: INSTALL_STATE_VERSION,
            installs: Vec::new(),
        }
    }

    fn first_install(&self) -> Option<&InstallRecord> {
        self.installs.first()
    }

    fn upsert(&mut self, record: InstallRecord) {
        self.version = INSTALL_STATE_VERSION;
        if let Some(existing) = self
            .installs
            .iter_mut()
            .find(|install| install.skill_path == record.skill_path)
        {
            *existing = record;
        } else {
            self.installs.push(record);
        }
    }
}

impl InstallRecord {
    fn from_outcome(outcome: &InstallOutcome) -> Self {
        Self {
            bundle_revision: INSTALL_BUNDLE_REVISION,
            skill_signature: Some(bundled_skill_signature()),
            skill_path: outcome.skill_path.clone(),
            db_path: outcome.db_path.clone(),
            root: outcome.root.clone(),
        }
    }
}

impl InstallRecordBuilder {
    fn has_values(&self) -> bool {
        self.bundle_revision.is_some()
            || self.skill_signature.is_some()
            || self.skill_path.is_some()
            || self.db_path.is_some()
            || self.root.is_some()
    }

    fn finish(self, version: u32, context: &str) -> Result<InstallRecord, String> {
        Ok(InstallRecord {
            bundle_revision: self
                .bundle_revision
                .ok_or(format!("missing `bundle_revision` in {context}"))?,
            skill_signature: if version >= 2 {
                Some(
                    self.skill_signature
                        .ok_or(format!("missing `skill_signature` in {context}"))?,
                )
            } else {
                None
            },
            skill_path: self
                .skill_path
                .ok_or(format!("missing `skill_path` in {context}"))?,
            db_path: self
                .db_path
                .ok_or(format!("missing `db_path` in {context}"))?,
            root: self.root.ok_or(format!("missing `root` in {context}"))?,
        })
    }
}

fn bundled_skill_signature() -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in BUNDLED_SKILL_CONTENTS.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

pub fn maybe_prompt_for_repl() -> Result<(), String> {
    let interactive = io::stdin().is_terminal() && io::stdout().is_terminal();
    let state = load_prompt_state()?;
    if !interactive || state.prompt_disabled {
        return Ok(());
    }

    if let Some(reason) = refresh_reason(&state)? {
        let request = refresh_install_request(&state)?;
        let mut input = io::stdin().lock();
        let mut output = io::stdout();
        let choice = prompt_for_repl_refresh(&mut input, &mut output, &request, reason)?;
        return match choice {
            InstallChoice::Request(request) => {
                let request = prompt_for_repl_refresh_request(&mut input, &mut output, &request)?;
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
        };
    }

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
    if let Err(error) = persist_install_state(&outcome) {
        eprintln!("warning: {error}");
    }
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
    let package = WorkspacePackage::discover_current().map_err(|error| error.to_string())?;
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
        return Ok(InstallRequest {
            skills_root,
            db_path: db_path.unwrap_or_else(|| package.resolve_db_path(None)),
            root: root.unwrap_or_else(|| package.resolve_markdown_root(None)),
            force: command.force,
            yes: command.yes,
        });
    }

    prompt_for_install_request(
        &mut io::stdin().lock(),
        &mut io::stdout(),
        &package,
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
    let package = WorkspacePackage::discover_current().map_err(|error| error.to_string())?;
    let presets = install_presets()?;

    writeln!(output, "No cupld install detected for agent memory.").map_err(io_error)?;
    writeln!(output, "Run `cupld install` now?").map_err(io_error)?;
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

    let request =
        prompt_for_install_request(input, output, &package, skills_root, None, None, force, yes)?;
    Ok(InstallChoice::Request(request))
}

fn prompt_for_repl_refresh<R: BufRead, W: Write>(
    input: &mut R,
    output: &mut W,
    request: &InstallRequest,
    reason: RefreshReason,
) -> Result<InstallChoice, String> {
    writeln!(
        output,
        "cupld agent memory bootstrap needs refresh: {}.",
        reason.description()
    )
    .map_err(io_error)?;
    writeln!(output, "  skill root: {}", request.skills_root.display()).map_err(io_error)?;
    writeln!(output, "  db path: {}", request.db_path.display()).map_err(io_error)?;
    writeln!(output, "  markdown root: {}", request.root.display()).map_err(io_error)?;
    writeln!(output, "  1) Refresh existing install").map_err(io_error)?;
    writeln!(output, "  2) Skip for now").map_err(io_error)?;
    writeln!(output, "  3) Never ask again").map_err(io_error)?;
    write!(output, "Choice [1-3, default 1]: ").map_err(io_error)?;
    output.flush().map_err(io_error)?;

    let choice = read_prompt_line(input)?.unwrap_or_default();
    match choice.trim() {
        "" | "1" => Ok(InstallChoice::Request(request.clone())),
        "2" => Ok(InstallChoice::Skip),
        "3" => Ok(InstallChoice::NeverAskAgain),
        other => Err(format!("expected a choice from 1-3, got `{other}`")),
    }
}

fn prompt_for_repl_refresh_request<R: BufRead, W: Write>(
    input: &mut R,
    output: &mut W,
    request: &InstallRequest,
) -> Result<InstallRequest, String> {
    writeln!(output, "Press Enter to keep the current values.").map_err(io_error)?;
    if request.db_path.starts_with(env::temp_dir()) || request.root.starts_with(env::temp_dir()) {
        writeln!(
            output,
            "warning: current DB/root point into the system temp directory."
        )
        .map_err(io_error)?;
    }
    let skills_root = prompt_for_path(input, output, "Skills root", &request.skills_root)?;
    let db_path = prompt_for_path(input, output, "DB path", &request.db_path)?;
    let root = prompt_for_path(input, output, "Markdown root", &request.root)?;
    Ok(InstallRequest {
        skills_root,
        db_path,
        root,
        force: request.force,
        yes: request.yes,
    })
}

fn refresh_reason(state: &PromptState) -> Result<Option<RefreshReason>, String> {
    if let Some((_, reason)) = refresh_candidate(state)? {
        return Ok(Some(reason));
    }
    if state.install_state.is_some() {
        return Ok(None);
    }
    let Some(skill_path) = state.installed_skill_path.as_deref() else {
        return Ok(None);
    };
    refresh_reason_for_legacy_path(skill_path)
}

fn refresh_candidate(
    state: &PromptState,
) -> Result<Option<(&InstallRecord, RefreshReason)>, String> {
    if let Some(install_state) = state.install_state.as_ref() {
        for install in &install_state.installs {
            if let Some(reason) = refresh_reason_for_install(install)? {
                return Ok(Some((install, reason)));
            }
        }
        return Ok(None);
    }

    Ok(None)
}

fn refresh_reason_for_install(install: &InstallRecord) -> Result<Option<RefreshReason>, String> {
    if !install.skill_path.exists() {
        return Ok(None);
    }
    let bundled_signature = bundled_skill_signature();
    let skill_contents_changed = match install.skill_signature.as_deref() {
        Some(signature) => signature != bundled_signature.as_str(),
        None => {
            fs::read_to_string(&install.skill_path).map_err(io_error)? != BUNDLED_SKILL_CONTENTS
        }
    };
    refresh_reason_from_flags(
        skill_contents_changed,
        install.bundle_revision != INSTALL_BUNDLE_REVISION,
    )
}

fn refresh_reason_for_legacy_path(skill_path: &Path) -> Result<Option<RefreshReason>, String> {
    if !skill_path.exists() {
        return Ok(None);
    }
    refresh_reason_from_flags(
        fs::read_to_string(skill_path).map_err(io_error)? != BUNDLED_SKILL_CONTENTS,
        false,
    )
}

fn refresh_reason_from_flags(
    skill_contents_changed: bool,
    bundle_revision_changed: bool,
) -> Result<Option<RefreshReason>, String> {
    Ok(match (skill_contents_changed, bundle_revision_changed) {
        (false, false) => None,
        (true, false) => Some(RefreshReason::SkillContents),
        (false, true) => Some(RefreshReason::BundleRevision),
        (true, true) => Some(RefreshReason::SkillContentsAndBundleRevision),
    })
}

fn refresh_install_request(state: &PromptState) -> Result<InstallRequest, String> {
    let package = WorkspacePackage::discover_current().map_err(|error| error.to_string())?;
    if let Some((install, _)) = refresh_candidate(state)? {
        return Ok(InstallRequest {
            skills_root: skills_root_from_skill_path(&install.skill_path)?,
            db_path: install.db_path.clone(),
            root: install.root.clone(),
            force: true,
            yes: true,
        });
    }
    if state.install_state.is_some() {
        return Err("missing stale install state record".to_owned());
    }
    let skill_path = state
        .installed_skill_path
        .as_deref()
        .ok_or("missing installed skill path".to_owned())?;
    Ok(InstallRequest {
        skills_root: skills_root_from_skill_path(skill_path)?,
        db_path: package.resolve_db_path(None),
        root: package.resolve_markdown_root(None),
        force: true,
        yes: true,
    })
}

fn prompt_for_install_request<R: BufRead, W: Write>(
    input: &mut R,
    output: &mut W,
    package: &WorkspacePackage,
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
        None => prompt_for_path(input, output, "DB path", &package.resolve_db_path(None))?,
    };
    let root = match root {
        Some(path) => path,
        None => prompt_for_path(
            input,
            output,
            "Markdown root",
            &package.resolve_markdown_root(None),
        )?,
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
    default: &Path,
) -> Result<PathBuf, String> {
    write!(output, "{label} [{}]: ", default.display()).map_err(io_error)?;
    output.flush().map_err(io_error)?;
    let value = read_prompt_line(input)?.unwrap_or_default();
    let value = value.trim();
    let path = if value.is_empty() {
        default.to_path_buf()
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
    persist_workspace_package_state(&db_path, &request.root)?;
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

fn persist_workspace_package_state(db_path: &Path, root: &Path) -> Result<(), String> {
    let mut package = WorkspacePackage::discover_current().map_err(|error| error.to_string())?;
    if !package.owns_path(db_path) || !package.owns_path(root) {
        return Ok(());
    }
    package
        .persist_package_config(Some(db_path), Some(root))
        .map_err(|error| error.to_string())
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
    let install_state = load_install_state(&config_dir.join(INSTALL_STATE_FILE))?;
    let state_path = config_dir.join(INSTALLED_SKILL_PATH_FILE);
    let installed_skill_path = install_state
        .as_ref()
        .and_then(|state| {
            state
                .first_install()
                .map(|install| install.skill_path.clone())
        })
        .or(load_installed_skill_path(&state_path)?);
    Ok(PromptState {
        install_state,
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

fn load_install_state(path: &Path) -> Result<Option<InstallState>, String> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            eprintln!(
                "warning: ignoring unreadable install state at {}: {}",
                path.display(),
                error
            );
            return Ok(None);
        }
    };
    let state = match parse_install_state(&contents) {
        Ok(state) => state,
        Err(error) => {
            eprintln!(
                "warning: ignoring invalid install state at {}: {}",
                path.display(),
                error
            );
            return Ok(None);
        }
    };
    let mut installs = Vec::new();
    for mut install in state.installs {
        if !install.skill_path.exists() {
            continue;
        }
        install.skill_path = match canonicalize_existing_path(&install.skill_path) {
            Ok(path) => path,
            Err(error) => {
                eprintln!(
                    "warning: ignoring install state record for {}: {}",
                    install.skill_path.display(),
                    error
                );
                continue;
            }
        };
        installs.push(install);
    }
    if installs.is_empty() {
        return Ok(None);
    }
    Ok(Some(InstallState {
        version: INSTALL_STATE_VERSION,
        installs,
    }))
}

fn persist_install_state(outcome: &InstallOutcome) -> Result<(), String> {
    let config_dir = config_dir()?;
    fs::create_dir_all(&config_dir).map_err(io_error)?;
    let state_path = config_dir.join(INSTALL_STATE_FILE);
    let mut state = load_install_state(&state_path)?.unwrap_or_else(InstallState::empty);
    state.upsert(InstallRecord::from_outcome(outcome));
    fs::write(state_path, render_install_state(&state)).map_err(io_error)
}

fn parse_install_state(input: &str) -> Result<InstallState, String> {
    let mut version = None;
    let mut legacy = InstallRecordBuilder::default();
    let mut current = None::<InstallRecordBuilder>;
    let mut installs = Vec::new();
    let mut saw_install_section = false;

    for (index, raw_line) in input.lines().enumerate() {
        let line_number = index + 1;
        let line = strip_trailing_comment(raw_line).trim();
        if line.is_empty() {
            continue;
        }

        if line == "[[install]]" {
            saw_install_section = true;
            if legacy.has_values() {
                return Err(format!(
                    "line {line_number}: legacy install keys cannot be mixed with install sections"
                ));
            }
            if let Some(builder) = current.take() {
                let parsed_version =
                    version.ok_or("missing `version` in install state".to_owned())?;
                installs.push(builder.finish(parsed_version, "install record")?);
            }
            current = Some(InstallRecordBuilder::default());
            continue;
        }

        let Some((key, value)) = split_key_value(line) else {
            return Err(format!("line {line_number}: expected `key = value`"));
        };
        let parsed = json::parse(value)
            .map_err(|error| format!("line {line_number}: invalid value for `{key}`: {error}"))?;

        if saw_install_section {
            let builder = current.as_mut().ok_or(format!(
                "line {line_number}: install key appears before install section"
            ))?;
            parse_install_record_key(builder, key, &parsed, line_number)?;
            continue;
        }

        match key {
            "version" => {
                if version.is_some() {
                    return Err(format!("line {line_number}: duplicate `version`"));
                }
                version = Some(parse_u32_value(&parsed, "version", line_number)?);
            }
            "bundle_revision" | "skill_signature" | "skill_path" | "db_path" | "root" => {
                parse_install_record_key(&mut legacy, key, &parsed, line_number)?;
            }
            other => {
                return Err(format!("line {line_number}: unknown key `{other}`"));
            }
        }
    }

    let version = version.ok_or("missing `version` in install state".to_owned())?;
    if version > INSTALL_STATE_VERSION {
        return Err(format!(
            "unsupported install state version {version}; expected <= {INSTALL_STATE_VERSION}"
        ));
    }

    if saw_install_section {
        if let Some(builder) = current.take() {
            installs.push(builder.finish(version, "install record")?);
        }
        if installs.is_empty() {
            return Err("missing install records in install state".to_owned());
        }
    } else if legacy.has_values() {
        installs.push(legacy.finish(version, "install state")?);
    } else {
        return Err("missing install records in install state".to_owned());
    }

    Ok(InstallState {
        version: INSTALL_STATE_VERSION,
        installs,
    })
}

fn parse_install_record_key(
    builder: &mut InstallRecordBuilder,
    key: &str,
    parsed: &json::JsonValue,
    line_number: usize,
) -> Result<(), String> {
    match key {
        "bundle_revision" => set_install_field(
            &mut builder.bundle_revision,
            parse_u32_value(parsed, "bundle_revision", line_number)?,
            "bundle_revision",
            line_number,
        ),
        "skill_signature" => set_install_field(
            &mut builder.skill_signature,
            parse_string_value(parsed, "skill_signature", line_number)?,
            "skill_signature",
            line_number,
        ),
        "skill_path" => set_install_field(
            &mut builder.skill_path,
            parse_path_value(parsed, "skill_path", line_number)?,
            "skill_path",
            line_number,
        ),
        "db_path" => set_install_field(
            &mut builder.db_path,
            parse_path_value(parsed, "db_path", line_number)?,
            "db_path",
            line_number,
        ),
        "root" => set_install_field(
            &mut builder.root,
            parse_path_value(parsed, "root", line_number)?,
            "root",
            line_number,
        ),
        other => Err(format!("line {line_number}: unknown install key `{other}`")),
    }
}

fn set_install_field<T>(
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

fn render_install_state(state: &InstallState) -> String {
    let mut output = String::new();
    output.push_str("version = ");
    output.push_str(&state.version.to_string());
    output.push('\n');
    for install in &state.installs {
        output.push('\n');
        output.push_str("[[install]]\n");
        output.push_str("bundle_revision = ");
        output.push_str(&install.bundle_revision.to_string());
        output.push('\n');
        if let Some(signature) = install.skill_signature.as_deref() {
            output.push_str("skill_signature = ");
            json::write_quoted_string(&mut output, signature);
            output.push('\n');
        }
        render_install_state_path(&mut output, "skill_path", &install.skill_path);
        render_install_state_path(&mut output, "db_path", &install.db_path);
        render_install_state_path(&mut output, "root", &install.root);
    }
    output
}

fn render_install_state_path(output: &mut String, key: &str, path: &Path) {
    output.push_str(key);
    output.push_str(" = ");
    json::write_quoted_string(output, &path.display().to_string());
    output.push('\n');
}

fn parse_u32_value(value: &json::JsonValue, key: &str, line_number: usize) -> Result<u32, String> {
    match value {
        json::JsonValue::Number(json::JsonNumber::Int(number)) if *number >= 0 => {
            u32::try_from(*number)
                .map_err(|_| format!("line {line_number}: `{key}` is outside the supported range"))
        }
        json::JsonValue::Number(json::JsonNumber::Unsigned(number)) => u32::try_from(*number)
            .map_err(|_| format!("line {line_number}: `{key}` is outside the supported range")),
        _ => Err(format!(
            "line {line_number}: invalid `{key}`: expected non-negative integer"
        )),
    }
}

fn parse_path_value(
    value: &json::JsonValue,
    key: &str,
    line_number: usize,
) -> Result<PathBuf, String> {
    match value {
        json::JsonValue::String(path) => Ok(PathBuf::from(path)),
        _ => Err(format!(
            "line {line_number}: invalid `{key}`: expected quoted string"
        )),
    }
}

fn parse_string_value(
    value: &json::JsonValue,
    key: &str,
    line_number: usize,
) -> Result<String, String> {
    match value {
        json::JsonValue::String(string) => Ok(string.clone()),
        _ => Err(format!(
            "line {line_number}: invalid `{key}`: expected quoted string"
        )),
    }
}

fn split_key_value(line: &str) -> Option<(&str, &str)> {
    let index = line.find('=')?;
    let key = line[..index].trim();
    let value = line[index + 1..].trim();
    (!key.is_empty() && !value.is_empty()).then_some((key, value))
}

fn strip_trailing_comment(line: &str) -> &str {
    let mut in_string = false;
    let mut escaped = false;
    for (index, ch) in line.char_indices() {
        if in_string {
            if escaped {
                escaped = false;
                continue;
            }
            match ch {
                '\\' => escaped = true,
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }
        match ch {
            '"' => in_string = true,
            '#' => return &line[..index],
            _ => {}
        }
    }
    line
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

fn skills_root_from_skill_path(skill_path: &Path) -> Result<PathBuf, String> {
    if skill_path.file_name() != Some(OsStr::new(BUNDLED_SKILL_FILENAME)) {
        return Err(format!(
            "installed skill path {} does not end with {}",
            skill_path.display(),
            BUNDLED_SKILL_FILENAME
        ));
    }
    let skill_dir = skill_path.parent().ok_or(format!(
        "could not resolve skill directory for {}",
        skill_path.display()
    ))?;
    if skill_dir.file_name() != Some(OsStr::new(BUNDLED_SKILL_NAME)) {
        return Err(format!(
            "installed skill path {} does not include {}",
            skill_path.display(),
            BUNDLED_SKILL_NAME
        ));
    }
    skill_dir.parent().map(Path::to_path_buf).ok_or(format!(
        "could not resolve skills root for {}",
        skill_path.display()
    ))
}

#[cfg(test)]
fn default_root_path() -> Result<PathBuf, String> {
    Ok(WorkspacePackage::discover_current()
        .map_err(|error| error.to_string())?
        .resolve_markdown_root(None))
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
        INSTALL_BUNDLE_REVISION, INSTALL_STATE_FILE, INSTALL_STATE_VERSION,
        INSTALLED_SKILL_PATH_FILE, InstallChoice, InstallCommand, InstallRecord, InstallRequest,
        InstallScope, InstallState, InstallStatus, PROMPT_DISABLED_FILE, PromptState,
        RefreshReason, SkillInstallTarget, WorkspacePackage, bootstrap_memory,
        bundled_skill_signature, canonicalize_existing_path, default_root_path,
        expand_tilde_with_home, install_request, load_installed_skill_path, parse_install_state,
        prompt_for_install_request, prompt_for_repl_choice, prompt_for_repl_refresh,
        prompt_for_repl_refresh_request, prompt_state_from_config_dir, refresh_install_request,
        refresh_reason, render_install_state, resolve_install_request, should_prompt_for_repl,
        skills_root_from_skill_path,
    };
    use cupld::{Session, configured_markdown_root};
    use std::fs;
    use std::io::Cursor;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static NEXT_TEMP_DIR_ID: AtomicUsize = AtomicUsize::new(1);

    fn test_install_state(record: InstallRecord) -> InstallState {
        InstallState {
            version: INSTALL_STATE_VERSION,
            installs: vec![record],
        }
    }

    fn test_install_record(skill_path: PathBuf, db_path: PathBuf, root: PathBuf) -> InstallRecord {
        InstallRecord {
            bundle_revision: INSTALL_BUNDLE_REVISION,
            skill_signature: Some(bundled_skill_signature()),
            skill_path,
            db_path,
            root,
        }
    }

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
    fn refresh_reason_detects_skill_changes_and_bundle_revision_changes() {
        let root = temp_dir("refresh_reason");
        let skill_path = root.join(BUNDLED_SKILL_NAME).join(BUNDLED_SKILL_FILENAME);
        fs::create_dir_all(skill_path.parent().unwrap()).unwrap();
        fs::write(&skill_path, BUNDLED_SKILL_CONTENTS).unwrap();

        let install_record = test_install_record(
            canonicalize_existing_path(&skill_path).unwrap(),
            root.join("db.cupld"),
            root.join("notes"),
        );
        let state = PromptState {
            install_state: Some(test_install_state(install_record.clone())),
            installed_skill_path: Some(canonicalize_existing_path(&skill_path).unwrap()),
            prompt_disabled: false,
        };
        assert_eq!(refresh_reason(&state).unwrap(), None);

        fs::write(&skill_path, "different").unwrap();
        assert_eq!(refresh_reason(&state).unwrap(), None);

        let stale_revision = PromptState {
            install_state: Some(test_install_state(InstallRecord {
                bundle_revision: INSTALL_BUNDLE_REVISION - 1,
                ..install_record.clone()
            })),
            ..state.clone()
        };
        assert_eq!(
            refresh_reason(&stale_revision).unwrap(),
            Some(RefreshReason::BundleRevision)
        );

        let stale_signature = PromptState {
            install_state: Some(test_install_state(InstallRecord {
                skill_signature: Some("stale".to_owned()),
                ..install_record
            })),
            ..state.clone()
        };
        assert_eq!(
            refresh_reason(&stale_signature).unwrap(),
            Some(RefreshReason::SkillContents)
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn refresh_reason_detects_loaded_install_state_record_changes() {
        let root = temp_dir("refresh_reason_loaded");
        let config_dir = root.join(CONFIG_DIR_NAME);
        fs::create_dir_all(&config_dir).unwrap();
        let skill_path = root
            .join("skills")
            .join(BUNDLED_SKILL_NAME)
            .join(BUNDLED_SKILL_FILENAME);
        fs::create_dir_all(skill_path.parent().unwrap()).unwrap();
        fs::write(&skill_path, BUNDLED_SKILL_CONTENTS).unwrap();

        let install_record = test_install_record(
            canonicalize_existing_path(&skill_path).unwrap(),
            root.join("db.cupld"),
            root.join("notes"),
        );
        fs::write(
            config_dir.join(INSTALL_STATE_FILE),
            render_install_state(&test_install_state(install_record.clone())),
        )
        .unwrap();
        let state = prompt_state_from_config_dir(&config_dir).unwrap();
        assert_eq!(refresh_reason(&state).unwrap(), None);

        fs::write(
            config_dir.join(INSTALL_STATE_FILE),
            render_install_state(&test_install_state(InstallRecord {
                bundle_revision: INSTALL_BUNDLE_REVISION - 1,
                ..install_record.clone()
            })),
        )
        .unwrap();
        let state = prompt_state_from_config_dir(&config_dir).unwrap();
        assert_eq!(
            refresh_reason(&state).unwrap(),
            Some(RefreshReason::BundleRevision)
        );

        fs::write(
            config_dir.join(INSTALL_STATE_FILE),
            render_install_state(&test_install_state(InstallRecord {
                skill_signature: Some("stale".to_owned()),
                ..install_record
            })),
        )
        .unwrap();
        let state = prompt_state_from_config_dir(&config_dir).unwrap();
        assert_eq!(
            refresh_reason(&state).unwrap(),
            Some(RefreshReason::SkillContents)
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn refresh_request_prefers_saved_install_targets() {
        let root = temp_dir("refresh_request");
        let skill_path = root
            .join("skills")
            .join(BUNDLED_SKILL_NAME)
            .join(BUNDLED_SKILL_FILENAME);
        fs::create_dir_all(skill_path.parent().unwrap()).unwrap();
        fs::write(&skill_path, BUNDLED_SKILL_CONTENTS).unwrap();
        let install_record = test_install_record(
            canonicalize_existing_path(&skill_path).unwrap(),
            root.join("db.cupld"),
            root.join("notes"),
        );

        let state = PromptState {
            install_state: Some(test_install_state(InstallRecord {
                skill_signature: Some("stale".to_owned()),
                ..install_record.clone()
            })),
            installed_skill_path: Some(canonicalize_existing_path(&skill_path).unwrap()),
            prompt_disabled: false,
        };

        assert_eq!(
            refresh_install_request(&state).unwrap(),
            InstallRequest {
                skills_root: canonicalize_existing_path(&root.join("skills")).unwrap(),
                db_path: root.join("db.cupld"),
                root: root.join("notes"),
                force: true,
                yes: true,
            }
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn refresh_prompt_supports_refresh_skip_and_never() {
        let request = InstallRequest {
            skills_root: PathBuf::from("/tmp/skills"),
            db_path: PathBuf::from("/tmp/default.cupld"),
            root: PathBuf::from("/tmp/notes"),
            force: true,
            yes: true,
        };

        let mut refresh = Cursor::new(b"\n");
        assert_eq!(
            choice_request(
                prompt_for_repl_refresh(
                    &mut refresh,
                    &mut Vec::new(),
                    &request,
                    RefreshReason::SkillContents,
                )
                .unwrap()
            ),
            request
        );

        let mut skip = Cursor::new(b"2\n");
        assert!(matches!(
            prompt_for_repl_refresh(
                &mut skip,
                &mut Vec::new(),
                &request,
                RefreshReason::BundleRevision,
            )
            .unwrap(),
            InstallChoice::Skip
        ));

        let mut never = Cursor::new(b"3\n");
        assert!(matches!(
            prompt_for_repl_refresh(
                &mut never,
                &mut Vec::new(),
                &request,
                RefreshReason::BundleRevision,
            )
            .unwrap(),
            InstallChoice::NeverAskAgain
        ));
    }

    #[test]
    fn refresh_prompt_request_allows_editing_defaults() {
        let request = InstallRequest {
            skills_root: PathBuf::from("/tmp/skills"),
            db_path: PathBuf::from("/tmp/default.cupld"),
            root: PathBuf::from("/tmp/notes"),
            force: true,
            yes: true,
        };

        let mut keep = Cursor::new(b"\n\n\n");
        assert_eq!(
            prompt_for_repl_refresh_request(&mut keep, &mut Vec::new(), &request).unwrap(),
            request
        );

        let mut custom = Cursor::new(b"/tmp/other-skills\n/tmp/other.cupld\n/tmp/other-notes\n");
        assert_eq!(
            prompt_for_repl_refresh_request(&mut custom, &mut Vec::new(), &request).unwrap(),
            InstallRequest {
                skills_root: PathBuf::from("/tmp/other-skills"),
                db_path: PathBuf::from("/tmp/other.cupld"),
                root: PathBuf::from("/tmp/other-notes"),
                force: true,
                yes: true,
            }
        );
    }

    #[test]
    fn repl_prompt_supports_presets_skip_never_and_custom() {
        let root = temp_dir("repl_prompt_choice");
        let package = WorkspacePackage::discover_current().unwrap();

        let mut codex = Cursor::new(b"1\n\n\n");
        let mut output = Vec::new();
        let choice = prompt_for_repl_choice(&mut codex, &mut output, false, false).unwrap();
        let prompt = String::from_utf8(output).unwrap();
        assert!(prompt.contains("No cupld install detected for agent memory."));
        assert!(prompt.contains("Run `cupld install` now?"));
        let request = choice_request(choice);
        assert_eq!(
            request,
            InstallRequest {
                skills_root: std::env::current_dir()
                    .unwrap()
                    .join(".agents")
                    .join("skills"),
                db_path: package.resolve_db_path(None),
                root: package.resolve_markdown_root(None),
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
        let package = WorkspacePackage::discover_current().unwrap();
        let request = prompt_for_install_request(
            &mut input,
            &mut Vec::new(),
            &package,
            None,
            None,
            None,
            false,
            false,
        )
        .unwrap();
        assert_eq!(request.skills_root, root);
        assert_eq!(request.db_path, package.resolve_db_path(None));
        assert_eq!(request.root, package.resolve_markdown_root(None));
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
    fn install_state_round_trips() {
        let state = test_install_state(test_install_record(
            PathBuf::from("/tmp/skill"),
            PathBuf::from("/tmp/default.cupld"),
            PathBuf::from("/tmp/notes"),
        ));
        assert_eq!(
            parse_install_state(&render_install_state(&state)).unwrap(),
            state
        );
    }

    #[test]
    fn legacy_install_state_migrates_to_install_record() {
        let legacy = format!(
            "version = 2\nbundle_revision = 1\nskill_signature = \"{}\"\nskill_path = \"/tmp/skill\"\ndb_path = \"/tmp/default.cupld\"\nroot = \"/tmp/notes\"\n",
            bundled_skill_signature()
        );
        let state = parse_install_state(&legacy).unwrap();
        assert_eq!(
            state,
            test_install_state(test_install_record(
                PathBuf::from("/tmp/skill"),
                PathBuf::from("/tmp/default.cupld"),
                PathBuf::from("/tmp/notes"),
            ))
        );
    }

    #[test]
    fn skills_root_is_derived_from_installed_skill_path() {
        assert_eq!(
            skills_root_from_skill_path(Path::new("/tmp/skills/cupld-md-memory/SKILL.md")).unwrap(),
            PathBuf::from("/tmp/skills")
        );
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
            WorkspacePackage::discover_current()
                .unwrap()
                .resolve_markdown_root(None)
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
                root: WorkspacePackage::discover_current()
                    .unwrap()
                    .resolve_markdown_root(None),
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
                install_state: None,
                installed_skill_path: Some(canonicalize_existing_path(&skill_path).unwrap()),
                prompt_disabled: true,
            }
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn prompt_state_falls_back_to_installed_skill_path_when_install_state_is_corrupt() {
        let root = temp_dir("prompt_state_corrupt");
        let config_dir = root.join(CONFIG_DIR_NAME);
        fs::create_dir_all(&config_dir).unwrap();

        let skill_path = root.join(BUNDLED_SKILL_NAME).join(BUNDLED_SKILL_FILENAME);
        fs::create_dir_all(skill_path.parent().unwrap()).unwrap();
        fs::write(&skill_path, BUNDLED_SKILL_CONTENTS).unwrap();
        fs::write(config_dir.join(INSTALL_STATE_FILE), b"not valid state\n").unwrap();
        fs::write(
            config_dir.join(INSTALLED_SKILL_PATH_FILE),
            format!("{}\n", skill_path.display()),
        )
        .unwrap();

        let state = prompt_state_from_config_dir(&config_dir).unwrap();
        assert_eq!(
            state,
            PromptState {
                install_state: None,
                installed_skill_path: Some(canonicalize_existing_path(&skill_path).unwrap()),
                prompt_disabled: false,
            }
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn prompt_state_prefers_saved_install_state() {
        let root = temp_dir("prompt_state_install");
        let config_dir = root.join(CONFIG_DIR_NAME);
        fs::create_dir_all(&config_dir).unwrap();

        let skill_path = root.join(BUNDLED_SKILL_NAME).join(BUNDLED_SKILL_FILENAME);
        fs::create_dir_all(skill_path.parent().unwrap()).unwrap();
        fs::write(&skill_path, BUNDLED_SKILL_CONTENTS).unwrap();
        let install_record = test_install_record(
            canonicalize_existing_path(&skill_path).unwrap(),
            root.join("db.cupld"),
            root.join("notes"),
        );
        let install_state = test_install_state(install_record.clone());
        fs::write(
            config_dir.join(INSTALL_STATE_FILE),
            render_install_state(&install_state),
        )
        .unwrap();

        let state = prompt_state_from_config_dir(&config_dir).unwrap();
        assert_eq!(
            state,
            PromptState {
                install_state: Some(install_state.clone()),
                installed_skill_path: Some(install_record.skill_path),
                prompt_disabled: false,
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
