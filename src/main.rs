use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::env;
use std::fs;
use std::io::{self, BufRead, IsTerminal, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{self, Command, ExitCode, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cupld::{
    MAX_TRAVERSAL_DEPTH, MarkdownSyncOptions, MarkdownSyncReport, MarkdownWatchOptions,
    MemoryMaintenanceCheck, MemoryMaintenanceReport, MemoryMaintenanceStatus, QueryResult,
    RuntimeValue, Session, Value,
    automation::{
        AutomationError, AutomationPolicy, format_error_json as machine_error_json,
        parse_params_json as parse_params_json_impl, query_as_json, query_as_ndjson,
    },
    configured_markdown_root,
    context::{
        ContextDirection, ContextRequest, ContextSeedRequest, context_as_json, context_as_ndjson,
        context_as_query_result,
    },
    json, markdown_alias_diagnostics,
    mcp::{self, McpConfig},
    memory_eval,
    package::WorkspacePackage,
    set_markdown_root, sync_markdown_root, sync_markdown_root_with_options,
    watch_markdown_root_with_sync_options,
};
use skill_install::{InstallCommand, InstallScope, SkillInstallTarget};

mod install_mcp;
mod skill_install;
mod visualise;

const MARKDOWN_DOCUMENT_LABEL: &str = "MarkdownDocument";
const MD_LINKS_TO: &str = "MD_LINKS_TO";
const MD_IN_DIRECTORY: &str = "MD_IN_DIRECTORY";
const MD_PARENT_DIRECTORY: &str = "MD_PARENT_DIRECTORY";
const REQUIRED_MARKDOWN_SOURCE_METADATA: [&str; 6] = [
    "src.connector",
    "src.kind",
    "src.root",
    "src.path",
    "src.hash",
    "src.status",
];
const RELEASE_CHECK_CACHE_FILE: &str = "release-check-cache.json";
const RELEASE_CHECK_INTERVAL_SECS: u64 = 24 * 60 * 60;
const LATEST_RELEASE_URL: &str = "https://api.github.com/repos/aeaston1/cupld/releases/latest";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OutputFormat {
    Table,
    Json,
    Ndjson,
}

#[derive(Debug, PartialEq, Eq)]
enum InputEvent {
    Line(String),
    Eof,
    Error(String),
}

struct ReplInput {
    interactive: bool,
    rx: Receiver<InputEvent>,
    pending: VecDeque<InputEvent>,
}

impl ReplInput {
    fn new() -> Self {
        let interactive = io::stdin().is_terminal() && io::stdout().is_terminal();
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            let stdin = io::stdin();
            let mut stdin = stdin.lock();

            loop {
                let mut line = String::new();
                match stdin.read_line(&mut line) {
                    Ok(0) => {
                        if tx.send(InputEvent::Eof).is_err() || !interactive {
                            break;
                        }
                    }
                    Ok(_) => {
                        if tx.send(InputEvent::Line(line)).is_err() {
                            break;
                        }
                    }
                    Err(error) => {
                        let _ = tx.send(InputEvent::Error(error.to_string()));
                        break;
                    }
                }
            }
        });

        Self {
            interactive,
            rx,
            pending: VecDeque::new(),
        }
    }

    fn interactive(&self) -> bool {
        self.interactive
    }

    fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    fn next_line(&mut self) -> Result<Option<String>, String> {
        let event = match self.pending.pop_front() {
            Some(event) => event,
            None => self.rx.recv().map_err(|_| "stdin closed".to_owned())?,
        };
        self.drain_ready();

        match event {
            InputEvent::Line(line) => Ok(Some(line)),
            InputEvent::Eof => Ok(None),
            InputEvent::Error(error) => Err(error),
        }
    }

    fn drain_ready(&mut self) {
        while let Ok(event) = self.rx.try_recv() {
            self.pending.push_back(event);
        }
    }

    #[cfg(test)]
    fn from_events(interactive: bool, events: impl IntoIterator<Item = InputEvent>) -> Self {
        let (tx, rx) = mpsc::channel();
        for event in events {
            tx.send(event).unwrap();
        }
        drop(tx);

        Self {
            interactive,
            rx,
            pending: VecDeque::new(),
        }
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::from(1)
        }
    }
}

fn run() -> Result<(), String> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    let command = parse_cli_command(&args).map_err(|error| format_parse_error(&args, error))?;
    maybe_suggest_release_upgrade(&command);
    if should_offer_skill_install_prompt(&command)
        && let Err(error) = skill_install::maybe_prompt_for_repl()
    {
        eprintln!("warning: {error}");
    }
    match command {
        CliCommand::Help => {
            print_help();
            Ok(())
        }
        CliCommand::Version => {
            print_version();
            Ok(())
        }
        CliCommand::ReplMemory => run_repl(None),
        CliCommand::ReplWithDb(path) => run_repl(Some(path)),
        CliCommand::Visualise { db_path, query } => run_visualise(db_path, query),
        CliCommand::Query {
            db_path,
            with_markdown,
            root_override,
            output,
            params_json,
            params_file,
            max_rows,
            query_args,
        } => run_query(QueryRunConfig {
            db_path,
            with_markdown,
            root_override,
            output,
            params_json: params_json.as_deref(),
            params_file: params_file.as_deref(),
            max_rows,
            query_args: &query_args,
        }),
        CliCommand::Context { output, request } => run_context(output, request),
        CliCommand::EvalMemory(config) => run_eval_memory(config),
        CliCommand::Schema { db_path } => run_schema(&db_path),
        CliCommand::Compact { db_path } => run_compact(db_path),
        CliCommand::Check { db_path } => run_check(db_path),
        CliCommand::Upgrade { db_path } => run_upgrade(db_path),
        CliCommand::Memory(command) => run_memory(command),
        CliCommand::SyncMarkdown {
            db_path,
            root_override,
            watch,
            poll_interval,
            debounce,
            batch_window,
            idle_timeout,
            max_runs,
            include_fs_graph,
        } => run_sync_markdown(
            db_path,
            root_override,
            watch,
            poll_interval,
            debounce,
            batch_window,
            idle_timeout,
            max_runs,
            include_fs_graph,
        ),
        CliCommand::SourceSetRoot { db_path, root } => run_source_set_root(db_path, root),
        CliCommand::McpServe {
            db_path,
            root_override,
            read_only,
        } => run_mcp_serve(db_path, root_override, read_only),
        CliCommand::Install(command) => skill_install::install(command),
    }
}

fn format_parse_error(args: &[String], error: String) -> String {
    if matches!(args.first().map(String::as_str), Some("context")) {
        let code = error
            .split_once(':')
            .map(|(code, _)| code)
            .unwrap_or(error.as_str());
        if code.starts_with("context_") {
            return format_error_json(code, &error);
        }
    }
    error
}

#[derive(Debug, PartialEq, Eq)]
enum CliCommand {
    Help,
    Version,
    ReplMemory,
    ReplWithDb(PathBuf),
    Visualise {
        db_path: PathBuf,
        query: Option<String>,
    },
    Query {
        db_path: PathBuf,
        with_markdown: bool,
        root_override: Option<PathBuf>,
        output: OutputFormat,
        params_json: Option<String>,
        params_file: Option<PathBuf>,
        max_rows: usize,
        query_args: Vec<String>,
    },
    Context {
        output: OutputFormat,
        request: ContextRequest,
    },
    EvalMemory(MemoryEvalConfig),
    Schema {
        db_path: PathBuf,
    },
    Compact {
        db_path: PathBuf,
    },
    Check {
        db_path: PathBuf,
    },
    Upgrade {
        db_path: PathBuf,
    },
    Memory(MemoryCommand),
    SyncMarkdown {
        db_path: PathBuf,
        root_override: Option<PathBuf>,
        watch: bool,
        poll_interval: Duration,
        debounce: Duration,
        batch_window: Duration,
        idle_timeout: Option<Duration>,
        max_runs: Option<usize>,
        include_fs_graph: bool,
    },
    SourceSetRoot {
        db_path: PathBuf,
        root: PathBuf,
    },
    McpServe {
        db_path: PathBuf,
        root_override: Option<PathBuf>,
        read_only: bool,
    },
    Install(InstallCommand),
}

#[derive(Debug, PartialEq, Eq)]
enum MemoryCommand {
    Check {
        db_path: PathBuf,
        root_override: Option<PathBuf>,
        output: OutputFormat,
        strict: bool,
    },
    FindStale {
        db_path: PathBuf,
        root_override: Option<PathBuf>,
        output: OutputFormat,
    },
    FindOrphans {
        db_path: PathBuf,
        output: OutputFormat,
    },
    Reindex {
        db_path: PathBuf,
        output: OutputFormat,
    },
    Deferred {
        subcommand: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MemoryAction {
    Check,
    FindStale,
    FindOrphans,
    Reindex,
}

#[derive(Clone, Copy, Debug)]
struct MemoryOptionSpec {
    allow_root: bool,
    allow_strict: bool,
}

#[derive(Debug, PartialEq, Eq)]
struct MemoryEvalConfig {
    fixtures: PathBuf,
    case: Option<String>,
    output: OutputFormat,
    update_snapshots: bool,
    ci: bool,
}

fn parse_cli_command(args: &[String]) -> Result<CliCommand, String> {
    match args.first().map(String::as_str) {
        Some("help") | Some("-h") | Some("--help") => {
            if args.len() == 1 {
                Ok(CliCommand::Help)
            } else {
                Err(format!(
                    "error: `{}` does not accept additional arguments\n\n{}",
                    args[0],
                    cli_usage_text()
                ))
            }
        }
        Some("-v") | Some("--version") => {
            if args.len() == 1 {
                Ok(CliCommand::Version)
            } else {
                Err(format!(
                    "error: `{}` does not accept additional arguments\n\n{}",
                    args[0],
                    cli_usage_text()
                ))
            }
        }
        Some("query") => parse_query_command(&args[1..]),
        Some("context") => parse_context_command(&args[1..]),
        Some("eval") => parse_eval_command(&args[1..]),
        Some("schema") => {
            ensure_subcommand_has_no_option(&args[1..], "schema", "--visualise")?;
            ensure_subcommand_has_no_option(&args[1..], "schema", "--query")?;
            Ok(CliCommand::Schema {
                db_path: parse_db_path(&args[1..], "schema", false)?,
            })
        }
        Some("compact") => {
            ensure_subcommand_has_no_option(&args[1..], "compact", "--visualise")?;
            ensure_subcommand_has_no_option(&args[1..], "compact", "--query")?;
            Ok(CliCommand::Compact {
                db_path: parse_db_path(&args[1..], "compact", false)?,
            })
        }
        Some("check") => {
            ensure_subcommand_has_no_option(&args[1..], "check", "--visualise")?;
            ensure_subcommand_has_no_option(&args[1..], "check", "--query")?;
            Ok(CliCommand::Check {
                db_path: parse_db_path(&args[1..], "check", false)?,
            })
        }
        Some("upgrade") => {
            ensure_subcommand_has_no_option(&args[1..], "upgrade", "--visualise")?;
            ensure_subcommand_has_no_option(&args[1..], "upgrade", "--query")?;
            Ok(CliCommand::Upgrade {
                db_path: parse_optional_db_path(&args[1..], "upgrade")?,
            })
        }
        Some("memory") => parse_memory_command(&args[1..]),
        Some("sync") => parse_sync_command(&args[1..]),
        Some("source") => parse_source_command(&args[1..]),
        Some("mcp") => parse_mcp_command(&args[1..]),
        Some("install") => parse_install_command(&args[1..]),
        Some(path) if path.starts_with('-') => parse_top_level_command(args),
        Some(path) => {
            if args.len() == 1 {
                return Ok(CliCommand::ReplWithDb(PathBuf::from(path)));
            }
            if args[1].starts_with('-') {
                return parse_top_level_command(args);
            }
            Err(format!(
                "error: unknown command `{}`\n\n{}",
                path,
                cli_usage_text()
            ))
        }
        None => Ok(CliCommand::ReplMemory),
    }
}

fn parse_memory_command(args: &[String]) -> Result<CliCommand, String> {
    ensure_subcommand_has_no_option(args, "memory", "--visualise")?;
    ensure_subcommand_has_no_option(args, "memory", "--query")?;

    let Some(subcommand) = args.first() else {
        return Err(format!(
            "error: expected `memory <check|find-stale|find-orphans|reindex|repair|citation-audit>`\n\n{}",
            cli_usage_text()
        ));
    };

    match subcommand.as_str() {
        "check" => parse_memory_included_command(
            &args[1..],
            MemoryAction::Check,
            MemoryOptionSpec {
                allow_root: true,
                allow_strict: true,
            },
        ),
        "find-stale" => parse_memory_included_command(
            &args[1..],
            MemoryAction::FindStale,
            MemoryOptionSpec {
                allow_root: true,
                allow_strict: false,
            },
        ),
        "find-orphans" => parse_memory_included_command(
            &args[1..],
            MemoryAction::FindOrphans,
            MemoryOptionSpec {
                allow_root: false,
                allow_strict: false,
            },
        ),
        "reindex" => parse_memory_included_command(
            &args[1..],
            MemoryAction::Reindex,
            MemoryOptionSpec {
                allow_root: false,
                allow_strict: false,
            },
        ),
        "repair" | "citation-audit" => Ok(CliCommand::Memory(MemoryCommand::Deferred {
            subcommand: subcommand.clone(),
        })),
        value if value.starts_with('-') => Err(format!(
            "error: expected memory subcommand before option `{value}`\n\n{}",
            cli_usage_text()
        )),
        value => Err(format!(
            "error: unknown memory subcommand `{value}`; expected one of check, find-stale, find-orphans, reindex, repair, citation-audit\n\n{}",
            cli_usage_text()
        )),
    }
}

fn parse_memory_included_command(
    args: &[String],
    action: MemoryAction,
    spec: MemoryOptionSpec,
) -> Result<CliCommand, String> {
    let mut db_path = None;
    let mut root_override = None;
    let mut output = OutputFormat::Table;
    let mut strict = false;
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--db" => {
                let Some(path) = args.get(index + 1) else {
                    return Err(format!(
                        "expected --db <path.cupld|default> for `memory {}` command",
                        action.as_str()
                    ));
                };
                if db_path.is_some() {
                    return Err(format!(
                        "expected exactly one --db <path.cupld|default> for `memory {}` command",
                        action.as_str()
                    ));
                }
                db_path = Some(parse_db_flag_value(path)?);
                index += 2;
            }
            "--root" if spec.allow_root => {
                let Some(path) = args.get(index + 1) else {
                    return Err("expected a path after `--root`".to_owned());
                };
                if root_override.is_some() {
                    return Err("duplicate option `--root`".to_owned());
                }
                root_override = Some(PathBuf::from(path));
                index += 2;
            }
            "--root" => {
                return Err(format!(
                    "error: `memory {}` does not accept `--root`\n\n{}",
                    action.as_str(),
                    cli_usage_text()
                ));
            }
            "--output" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(format!(
                        "expected --output <table|json|ndjson> for `memory {}` command",
                        action.as_str()
                    ));
                };
                output = parse_output_format(value)?;
                index += 2;
            }
            "--strict" if spec.allow_strict => {
                if strict {
                    return Err("duplicate option `--strict`".to_owned());
                }
                strict = true;
                index += 1;
            }
            "--strict" => {
                return Err(format!(
                    "error: `memory {}` does not accept `--strict`\n\n{}",
                    action.as_str(),
                    cli_usage_text()
                ));
            }
            value if value.starts_with('-') => {
                return Err(format!(
                    "error: unknown option `{value}`\n\n{}",
                    cli_usage_text()
                ));
            }
            value => {
                return Err(format!(
                    "error: unexpected argument `{value}`\n\n{}",
                    cli_usage_text()
                ));
            }
        }
    }

    let Some(db_path) = db_path else {
        return Err(format!(
            "expected --db <path.cupld|default> for `memory {}` command",
            action.as_str()
        ));
    };

    Ok(CliCommand::Memory(match action {
        MemoryAction::Check => MemoryCommand::Check {
            db_path,
            root_override,
            output,
            strict,
        },
        MemoryAction::FindStale => MemoryCommand::FindStale {
            db_path,
            root_override,
            output,
        },
        MemoryAction::FindOrphans => MemoryCommand::FindOrphans { db_path, output },
        MemoryAction::Reindex => MemoryCommand::Reindex { db_path, output },
    }))
}

impl MemoryAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Check => "check",
            Self::FindStale => "find-stale",
            Self::FindOrphans => "find-orphans",
            Self::Reindex => "reindex",
        }
    }
}

fn parse_eval_command(args: &[String]) -> Result<CliCommand, String> {
    ensure_subcommand_has_no_option(args, "eval", "--visualise")?;
    ensure_subcommand_has_no_option(args, "eval", "--query")?;

    match args.first().map(String::as_str) {
        Some("memory") => parse_eval_memory_command(&args[1..]),
        Some(value) if value.starts_with('-') => Err(format!(
            "error: expected eval subcommand before option `{value}`\n\n{}",
            cli_usage_text()
        )),
        Some(value) => Err(format!(
            "error: unknown eval subcommand `{value}`; expected memory\n\n{}",
            cli_usage_text()
        )),
        None => Err(format!(
            "error: expected `eval memory [--fixtures <path>] [--case <name>] [--output <json|ndjson|table>] [--ci] [--update-snapshots>`\n\n{}",
            cli_usage_text()
        )),
    }
}

fn parse_eval_memory_command(args: &[String]) -> Result<CliCommand, String> {
    let mut fixtures = None;
    let mut case = None;
    let mut output = OutputFormat::Table;
    let mut update_snapshots = false;
    let mut ci = false;
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--fixtures" => {
                let Some(path) = args.get(index + 1) else {
                    return Err("expected --fixtures <path> for `eval memory` command".to_owned());
                };
                if fixtures.is_some() {
                    return Err("duplicate option `--fixtures`".to_owned());
                }
                fixtures = Some(PathBuf::from(path));
                index += 2;
            }
            "--case" => {
                let Some(name) = args.get(index + 1) else {
                    return Err("expected --case <name> for `eval memory` command".to_owned());
                };
                if case.is_some() {
                    return Err("duplicate option `--case`".to_owned());
                }
                case = Some(name.clone());
                index += 2;
            }
            "--output" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(
                        "expected --output <table|json|ndjson> for `eval memory` command"
                            .to_owned(),
                    );
                };
                output = parse_output_format(value)?;
                index += 2;
            }
            "--update-snapshots" => {
                if update_snapshots {
                    return Err("duplicate option `--update-snapshots`".to_owned());
                }
                update_snapshots = true;
                index += 1;
            }
            "--ci" => {
                if ci {
                    return Err("duplicate option `--ci`".to_owned());
                }
                ci = true;
                index += 1;
            }
            value if value.starts_with('-') => {
                return Err(format!(
                    "error: unknown option `{value}`\n\n{}",
                    cli_usage_text()
                ));
            }
            value => {
                return Err(format!(
                    "error: unexpected argument `{value}`\n\n{}",
                    cli_usage_text()
                ));
            }
        }
    }

    if ci && update_snapshots {
        return Err("`eval memory --ci` must not be combined with `--update-snapshots`".to_owned());
    }

    Ok(CliCommand::EvalMemory(MemoryEvalConfig {
        fixtures: fixtures.unwrap_or_else(|| PathBuf::from("tests/fixtures/memory")),
        case,
        output,
        update_snapshots,
        ci,
    }))
}

fn resolve_default_db_alias() -> Result<PathBuf, String> {
    WorkspacePackage::discover_current()
        .map(|package| package.default_db_path())
        .map_err(|error| error.to_string())
}

fn parse_db_flag_value(value: &str) -> Result<PathBuf, String> {
    match value {
        "default" => resolve_default_db_alias(),
        _ => Ok(PathBuf::from(value)),
    }
}

fn parse_query_command(args: &[String]) -> Result<CliCommand, String> {
    ensure_subcommand_has_no_option(args, "query", "--visualise")?;
    ensure_subcommand_has_no_option(args, "query", "--query")?;

    let mut db_path = None;
    let mut with_markdown = false;
    let mut root_override = None;
    let mut output = OutputFormat::Table;
    let mut params_json = None;
    let mut params_file = None;
    let mut max_rows = query_max_rows_default();
    let mut query_args = Vec::new();
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--db" => {
                let Some(path) = args.get(index + 1) else {
                    return Err("expected --db <path.cupld|default> for `query` command".to_owned());
                };
                if db_path.is_some() {
                    return Err(
                        "expected exactly one --db <path.cupld|default> for `query` command"
                            .to_owned(),
                    );
                }
                db_path = Some(parse_db_flag_value(path)?);
                index += 2;
            }
            "--with-md" => {
                if with_markdown {
                    return Err("duplicate option `--with-md`".to_owned());
                }
                with_markdown = true;
                index += 1;
            }
            "--root" => {
                let Some(path) = args.get(index + 1) else {
                    return Err("expected a path after `--root`".to_owned());
                };
                if root_override.is_some() {
                    return Err("duplicate option `--root`".to_owned());
                }
                root_override = Some(PathBuf::from(path));
                index += 2;
            }
            "--output" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(
                        "expected --output <table|json|ndjson> for `query` command".to_owned()
                    );
                };
                output = parse_output_format(value)?;
                index += 2;
            }
            "--params-json" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("expected --params-json <json> for `query` command".to_owned());
                };
                if params_json.is_some() {
                    return Err("duplicate option `--params-json`".to_owned());
                }
                params_json = Some(value.clone());
                index += 2;
            }
            "--params-file" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("expected --params-file <path> for `query` command".to_owned());
                };
                if params_file.is_some() {
                    return Err("duplicate option `--params-file`".to_owned());
                }
                params_file = Some(PathBuf::from(value));
                index += 2;
            }
            "--max-rows" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("expected --max-rows <n> for `query` command".to_owned());
                };
                max_rows = value
                    .parse::<usize>()
                    .map_err(|_| "expected --max-rows <n> for `query` command".to_owned())?;
                index += 2;
            }
            value if value.starts_with('-') => {
                return Err(format!(
                    "error: unknown option `{value}`\n\n{}",
                    cli_usage_text()
                ));
            }
            _ => {
                query_args.extend(args[index..].iter().cloned());
                break;
            }
        }
    }

    let Some(db_path) = db_path else {
        return Err("expected --db <path.cupld|default> for `query` command".to_owned());
    };

    Ok(CliCommand::Query {
        db_path,
        with_markdown,
        root_override,
        output,
        params_json,
        params_file,
        max_rows,
        query_args,
    })
}

fn parse_context_command(args: &[String]) -> Result<CliCommand, String> {
    let mut db_path = None;
    let mut output = OutputFormat::Json;
    let mut nodes = Vec::new();
    let mut paths = Vec::new();
    let mut seeds = Vec::new();
    let mut depth = 1u8;
    let mut direction = ContextDirection::Both;
    let mut edge_types = Vec::new();
    let mut labels = Vec::new();
    let mut max_nodes = 20usize;
    let mut max_edges = 100usize;
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--db" => {
                let Some(path) = args.get(index + 1) else {
                    return Err(
                        "expected --db <path.cupld|default> for `context` command".to_owned()
                    );
                };
                db_path = Some(parse_db_flag_value(path)?);
                index += 2;
            }
            "--output" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(
                        "expected --output <table|json|ndjson> for `context` command".to_owned(),
                    );
                };
                output = parse_output_format(value)?;
                index += 2;
            }
            "--top-k" => {
                return Err("context_legacy_top_k_removed".to_owned());
            }
            "--node" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("expected --node <id> for `context` command".to_owned());
                };
                nodes.push(
                    value
                        .parse::<usize>()
                        .map_err(|_| "expected --node <id> for `context` command".to_owned())?,
                );
                seeds.push(ContextSeedRequest::Node(*nodes.last().unwrap()));
                index += 2;
            }
            "--path" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("expected --path <src.path> for `context` command".to_owned());
                };
                paths.push(value.to_owned());
                seeds.push(ContextSeedRequest::Path(value.to_owned()));
                index += 2;
            }
            "--depth" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("expected --depth <n> for `context` command".to_owned());
                };
                depth = value
                    .parse::<u8>()
                    .map_err(|_| "context_invalid_depth: expected --depth <n>".to_owned())?;
                if depth > MAX_TRAVERSAL_DEPTH {
                    return Err(format!(
                        "context_depth_above_max: --depth must be <= {MAX_TRAVERSAL_DEPTH}"
                    ));
                }
                index += 2;
            }
            "--direction" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(
                        "expected --direction <in|out|both> for `context` command".to_owned()
                    );
                };
                direction = parse_context_direction(value)?;
                index += 2;
            }
            "--edge-type" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("expected --edge-type <type> for `context` command".to_owned());
                };
                edge_types.push(value.to_owned());
                index += 2;
            }
            "--label" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("expected --label <label> for `context` command".to_owned());
                };
                labels.push(value.to_owned());
                index += 2;
            }
            "--max-nodes" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("expected --max-nodes <n> for `context` command".to_owned());
                };
                max_nodes = value
                    .parse::<usize>()
                    .map_err(|_| "expected --max-nodes <n> for `context` command".to_owned())?;
                index += 2;
            }
            "--max-edges" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("expected --max-edges <n> for `context` command".to_owned());
                };
                max_edges = value
                    .parse::<usize>()
                    .map_err(|_| "expected --max-edges <n> for `context` command".to_owned())?;
                index += 2;
            }
            value => {
                return Err(format!(
                    "error: unexpected argument `{value}`\n\n{}",
                    cli_usage_text()
                ));
            }
        }
    }

    let Some(db_path) = db_path else {
        return Err("expected --db <path.cupld|default> for `context` command".to_owned());
    };
    if nodes.is_empty() && paths.is_empty() {
        return Err("context_seed_required".to_owned());
    }
    Ok(CliCommand::Context {
        output,
        request: ContextRequest {
            db_path,
            nodes,
            paths,
            seeds,
            depth,
            direction,
            edge_types,
            labels,
            max_nodes,
            max_edges,
        },
    })
}

fn parse_context_direction(input: &str) -> Result<ContextDirection, String> {
    match input {
        "in" => Ok(ContextDirection::In),
        "out" => Ok(ContextDirection::Out),
        "both" => Ok(ContextDirection::Both),
        _ => Err("context_invalid_direction: expected --direction <in|out|both>".to_owned()),
    }
}

fn parse_output_format(input: &str) -> Result<OutputFormat, String> {
    match input {
        "table" => Ok(OutputFormat::Table),
        "json" => Ok(OutputFormat::Json),
        "ndjson" => Ok(OutputFormat::Ndjson),
        _ => Err("expected --output <table|json|ndjson>".to_owned()),
    }
}

fn parse_sync_command(args: &[String]) -> Result<CliCommand, String> {
    ensure_subcommand_has_no_option(args, "sync", "--visualise")?;
    ensure_subcommand_has_no_option(args, "sync", "--query")?;

    match args.first().map(String::as_str) {
        Some("markdown") => {}
        _ => {
            return Err(format!(
                "error: expected `sync markdown --db <path.cupld|default> [--root <path>] [--watch] [--include-fs-graph]`\n\n{}",
                cli_usage_text()
            ));
        }
    }

    let mut db_path = None;
    let mut root_override = None;
    let mut watch = false;
    let mut poll_interval = Duration::from_millis(100);
    let mut debounce = Duration::from_millis(200);
    let mut batch_window = Duration::from_secs(2);
    let mut idle_timeout = None;
    let mut max_runs = None;
    let mut include_fs_graph = false;
    let mut index = 1;

    while index < args.len() {
        match args[index].as_str() {
            "--db" => {
                let Some(path) = args.get(index + 1) else {
                    return Err(
                        "expected --db <path.cupld|default> for `sync markdown` command".to_owned(),
                    );
                };
                if db_path.is_some() {
                    return Err(
                        "expected exactly one --db <path.cupld|default> for `sync markdown` command"
                            .to_owned(),
                    );
                }
                db_path = Some(parse_db_flag_value(path)?);
                index += 2;
            }
            "--root" => {
                let Some(path) = args.get(index + 1) else {
                    return Err("expected a path after `--root`".to_owned());
                };
                if root_override.is_some() {
                    return Err("duplicate option `--root`".to_owned());
                }
                root_override = Some(PathBuf::from(path));
                index += 2;
            }
            "--watch" => {
                watch = true;
                index += 1;
            }
            "--include-fs-graph" | "--filesystem-graph" => {
                if include_fs_graph {
                    return Err("duplicate option `--include-fs-graph`".to_owned());
                }
                include_fs_graph = true;
                index += 1;
            }
            "--poll-ms" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("expected --poll-ms <n> for `sync markdown` command".to_owned());
                };
                poll_interval = Duration::from_millis(value.parse::<u64>().map_err(|_| {
                    "expected --poll-ms <n> for `sync markdown` command".to_owned()
                })?);
                index += 2;
            }
            "--debounce-ms" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("expected --debounce-ms <n> for `sync markdown` command".to_owned());
                };
                debounce = Duration::from_millis(value.parse::<u64>().map_err(|_| {
                    "expected --debounce-ms <n> for `sync markdown` command".to_owned()
                })?);
                index += 2;
            }
            "--batch-ms" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("expected --batch-ms <n> for `sync markdown` command".to_owned());
                };
                batch_window = Duration::from_millis(value.parse::<u64>().map_err(|_| {
                    "expected --batch-ms <n> for `sync markdown` command".to_owned()
                })?);
                index += 2;
            }
            "--idle-ms" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("expected --idle-ms <n> for `sync markdown` command".to_owned());
                };
                idle_timeout = Some(Duration::from_millis(value.parse::<u64>().map_err(
                    |_| "expected --idle-ms <n> for `sync markdown` command".to_owned(),
                )?));
                index += 2;
            }
            "--max-runs" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("expected --max-runs <n> for `sync markdown` command".to_owned());
                };
                max_runs = Some(value.parse::<usize>().map_err(|_| {
                    "expected --max-runs <n> for `sync markdown` command".to_owned()
                })?);
                index += 2;
            }
            value => {
                return Err(format!(
                    "error: unexpected argument `{value}`\n\n{}",
                    cli_usage_text()
                ));
            }
        }
    }

    let Some(db_path) = db_path else {
        return Err("expected --db <path.cupld|default> for `sync markdown` command".to_owned());
    };

    Ok(CliCommand::SyncMarkdown {
        db_path,
        root_override,
        watch,
        poll_interval,
        debounce,
        batch_window,
        idle_timeout,
        max_runs,
        include_fs_graph,
    })
}

fn parse_source_command(args: &[String]) -> Result<CliCommand, String> {
    ensure_subcommand_has_no_option(args, "source", "--visualise")?;
    ensure_subcommand_has_no_option(args, "source", "--query")?;

    match args.first().map(String::as_str) {
        Some("set-root") => {}
        _ => {
            return Err(format!(
                "error: expected `source set-root --db <path.cupld|default> <path>`\n\n{}",
                cli_usage_text()
            ));
        }
    }

    let mut db_path = None;
    let mut root = None;
    let mut index = 1;

    while index < args.len() {
        match args[index].as_str() {
            "--db" => {
                let Some(path) = args.get(index + 1) else {
                    return Err(
                        "expected --db <path.cupld|default> for `source set-root` command"
                            .to_owned(),
                    );
                };
                if db_path.is_some() {
                    return Err(
                        "expected exactly one --db <path.cupld|default> for `source set-root` command"
                            .to_owned()
                    );
                }
                db_path = Some(parse_db_flag_value(path)?);
                index += 2;
            }
            value if value.starts_with('-') => {
                return Err(format!(
                    "error: unknown option `{value}`\n\n{}",
                    cli_usage_text()
                ));
            }
            value => {
                if root.is_some() {
                    return Err(format!(
                        "error: unexpected argument `{value}`\n\n{}",
                        cli_usage_text()
                    ));
                }
                root = Some(PathBuf::from(value));
                index += 1;
            }
        }
    }

    let Some(db_path) = db_path else {
        return Err("expected --db <path.cupld|default> for `source set-root` command".to_owned());
    };
    let Some(root) = root else {
        return Err("expected a root path for `source set-root`".to_owned());
    };

    Ok(CliCommand::SourceSetRoot { db_path, root })
}

fn parse_mcp_command(args: &[String]) -> Result<CliCommand, String> {
    if args.first().map(String::as_str) != Some("serve") {
        return Err(format!(
            "error: expected `mcp serve --db <path.cupld|default> [--root <path>] [--read-only]`\n\n{}",
            cli_usage_text()
        ));
    }
    let mut db_path = None;
    let mut root_override = None;
    let mut read_only = false;
    let mut index = 1;
    while index < args.len() {
        match args[index].as_str() {
            "--db" => {
                let Some(path) = args.get(index + 1) else {
                    return Err("expected --db <path.cupld|default> for `mcp serve`".to_owned());
                };
                db_path = Some(parse_db_flag_value(path)?);
                index += 2;
            }
            "--root" => {
                let Some(path) = args.get(index + 1) else {
                    return Err("expected --root <path> for `mcp serve`".to_owned());
                };
                root_override = Some(PathBuf::from(path));
                index += 2;
            }
            "--read-only" => {
                read_only = true;
                index += 1;
            }
            value => {
                return Err(format!(
                    "error: unexpected argument `{value}`\n\n{}",
                    cli_usage_text()
                ));
            }
        }
    }
    let Some(db_path) = db_path else {
        return Err("expected --db <path.cupld|default> for `mcp serve`".to_owned());
    };
    Ok(CliCommand::McpServe {
        db_path,
        root_override,
        read_only,
    })
}

fn parse_install_command(args: &[String]) -> Result<CliCommand, String> {
    ensure_subcommand_has_no_option(args, "install", "--visualise")?;
    ensure_subcommand_has_no_option(args, "install", "--query")?;

    let mut target = None;
    let mut scope = None;
    let mut path = None;
    let mut db_path = None;
    let mut root = None;
    let mut force = false;
    let mut yes = false;
    let mut mcp = false;
    let mut dry_run = false;
    let mut print_only = false;
    let mut mcp_server_name = None;
    let mut mcp_command = None;
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--target" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(
                        "expected --target <codex|claude|opencode> for `install`".to_owned()
                    );
                };
                if target.is_some() {
                    return Err("duplicate option `--target`".to_owned());
                }
                let parsed = SkillInstallTarget::parse(value).ok_or_else(|| {
                    "expected --target <codex|claude|opencode> for `install`".to_owned()
                })?;
                target = Some(parsed);
                index += 2;
            }
            "--scope" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("expected --scope <cwd|home> for `install`".to_owned());
                };
                if scope.is_some() {
                    return Err("duplicate option `--scope`".to_owned());
                }
                let parsed = InstallScope::parse(value)
                    .ok_or_else(|| "expected --scope <cwd|home> for `install`".to_owned())?;
                scope = Some(parsed);
                index += 2;
            }
            "--path" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("expected a path after `--path`".to_owned());
                };
                if path.is_some() {
                    return Err("duplicate option `--path`".to_owned());
                }
                path = Some(PathBuf::from(value));
                index += 2;
            }
            "--db" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("expected --db <path.cupld|default> for `install`".to_owned());
                };
                if db_path.is_some() {
                    return Err("duplicate option `--db`".to_owned());
                }
                db_path = Some(parse_db_flag_value(value)?);
                index += 2;
            }
            "--root" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("expected a path after `--root`".to_owned());
                };
                if root.is_some() {
                    return Err("duplicate option `--root`".to_owned());
                }
                root = Some(PathBuf::from(value));
                index += 2;
            }
            "--force" => {
                if force {
                    return Err("duplicate option `--force`".to_owned());
                }
                force = true;
                index += 1;
            }
            "--yes" => {
                if yes {
                    return Err("duplicate option `--yes`".to_owned());
                }
                yes = true;
                index += 1;
            }
            "--mcp" => {
                if mcp {
                    return Err("duplicate option `--mcp`".to_owned());
                }
                mcp = true;
                index += 1;
            }
            "--dry-run" => {
                if dry_run {
                    return Err("duplicate option `--dry-run`".to_owned());
                }
                dry_run = true;
                index += 1;
            }
            "--print-only" => {
                if print_only {
                    return Err("duplicate option `--print-only`".to_owned());
                }
                print_only = true;
                index += 1;
            }
            "--mcp-server-name" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("expected --mcp-server-name <name> for `install`".to_owned());
                };
                if mcp_server_name.is_some() {
                    return Err("duplicate option `--mcp-server-name`".to_owned());
                }
                if value.trim().is_empty() {
                    return Err("expected non-empty --mcp-server-name for `install`".to_owned());
                }
                mcp_server_name = Some(value.clone());
                index += 2;
            }
            "--mcp-command" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("expected --mcp-command <path|command> for `install`".to_owned());
                };
                if mcp_command.is_some() {
                    return Err("duplicate option `--mcp-command`".to_owned());
                }
                if value.trim().is_empty() {
                    return Err("expected non-empty --mcp-command for `install`".to_owned());
                }
                mcp_command = Some(value.clone());
                index += 2;
            }
            value => {
                return Err(format!(
                    "error: unexpected argument `{value}`\n\n{}",
                    cli_usage_text()
                ));
            }
        }
    }

    Ok(CliCommand::Install(InstallCommand {
        target,
        scope,
        path,
        db_path,
        root,
        force,
        yes,
        mcp,
        dry_run,
        print_only,
        mcp_server_name,
        mcp_command,
    }))
}

fn parse_top_level_command(args: &[String]) -> Result<CliCommand, String> {
    let mut db_path = None;
    let mut positional_db_path = None;
    let mut visualise = false;
    let mut visualise_query = None;
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--visualise" => {
                if visualise {
                    return Err(duplicate_top_level_option("--visualise"));
                }
                visualise = true;
                index += 1;
            }
            "--query" => {
                let Some(query) = args.get(index + 1) else {
                    return Err(missing_top_level_query("--query"));
                };
                if visualise_query.is_some() {
                    return Err(duplicate_top_level_option("--query"));
                }
                visualise_query = Some(query.clone());
                index += 2;
            }
            "--db" => {
                let Some(path) = args.get(index + 1) else {
                    return Err(missing_top_level_db_path("--db"));
                };
                if db_path.is_some() || positional_db_path.is_some() {
                    return Err(duplicate_top_level_db_path());
                }
                db_path = Some(parse_db_flag_value(path)?);
                index += 2;
            }
            "-h" | "--help" | "help" => return Ok(CliCommand::Help),
            "-v" | "--version" => return Ok(CliCommand::Version),
            other if other.starts_with('-') => {
                return Err(format!(
                    "error: unknown option `{}`\n\n{}",
                    other,
                    cli_usage_text()
                ));
            }
            other if is_registered_command(other) => {
                return Err(format!(
                    "error: top-level options cannot be combined with `{}`\n\n{}",
                    other,
                    cli_usage_text()
                ));
            }
            other => {
                if db_path.is_some() || positional_db_path.is_some() {
                    return Err(duplicate_top_level_db_path());
                }
                positional_db_path = Some(PathBuf::from(other));
                index += 1;
            }
        }
    }

    let db_path = db_path.or(positional_db_path);
    match (visualise, db_path, visualise_query) {
        (true, Some(db_path), query) => Ok(CliCommand::Visualise { db_path, query }),
        (true, None, _) => Err(format!(
            "error: `--visualise` requires a database path\n\n{}",
            cli_usage_text()
        )),
        (false, _, Some(_)) => Err(format!(
            "error: `--query` requires `--visualise`\n\n{}",
            cli_usage_text()
        )),
        (false, Some(db_path), None) => Ok(CliCommand::ReplWithDb(db_path)),
        (false, None, None) => Ok(CliCommand::ReplMemory),
    }
}

fn print_help() {
    println!("{}", cli_usage_text());
}

fn print_version() {
    println!("{}", version_text());
}

fn version_text() -> &'static str {
    concat!("cupld ", env!("CARGO_PKG_VERSION"))
}

fn cli_usage_text() -> &'static str {
    "cupld is a local graph database CLI and REPL.

Usage:
  cupld [<path.cupld>]
  cupld --db <path.cupld|default> [--visualise [--query <query>]]
  cupld <command> [options]
  cupld -v|--version
  cupld -h|--help|help

Commands:
  cupld                   Start an in-memory REPL session.
  cupld <path.cupld>      Open or create a file-backed REPL session.
  cupld --db <path|default> Open a file-backed REPL session; `default` maps to `./.cupld/default.cupld`.
  --visualise             Open the interactive scene viewer for --db.
  --query                 Seed the scene with one read-only RETURN query.
  query                   Run a query against --db using inline text or stdin.
  context                 Build compact seeded context rows for agent prompts.
  eval memory             Run deterministic markdown memory eval fixtures.
  --with-md               Overlay markdown documents into `query` before execution.
  --root                  Override the markdown root for `query` or `sync markdown`.
  --include-fs-graph      Persist markdown directory nodes and filesystem structural edges during `sync markdown`.
  --watch                 Keep polling markdown for changes after the initial sync.
  --poll-ms               Poll interval for `sync markdown --watch`.
  --debounce-ms           Stable-change debounce window for `sync markdown --watch`.
  --batch-ms              Max coalescing window before a forced watched sync.
  --idle-ms               Exit watched sync after this long with no pending changes.
  --max-runs              Stop watched sync after this many sync runs, including the initial run.
  --node                  Seed `context` from a graph node id; repeatable.
  --path                  Seed `context` from a markdown src.path; repeatable.
  --depth                 Traversal depth for `context` seeds.
  --direction             Traversal direction for `context`: in, out, both.
  --edge-type             Restrict `context` traversal to an edge type; repeatable.
  --label                 Restrict `context` nodes to a label; repeatable.
  --max-nodes             Maximum context nodes to return.
  --max-edges             Maximum context edges to traverse.
  --fixtures              Fixture directory for `eval memory`; defaults to tests/fixtures/memory.
  --case                  Restrict `eval memory` to one fixture case.
  --ci                    Print concise PR-friendly failures for `eval memory`.
  --update-snapshots      Update `eval memory` snapshots.
  --output                Select output mode for query/context/memory/eval memory: table, json, ndjson.
  --params-json           Provide named query parameters as a JSON object.
  --params-file           Read named query parameters from a JSON file.
  --max-rows              Hard cap result rows in non-interactive query mode.
  schema                  Print SHOW SCHEMA for --db.
  compact                 Rewrite --db and reset its WAL.
  check                   Validate --db and print recovery metadata.
  upgrade                 Back up --db, validate it, and run memory check; defaults to --db default.
  memory check            Validate memory DB health and markdown maintenance status.
  memory find-stale       List indexed markdown documents that differ from the filesystem.
  memory find-orphans     List tombstoned markdown documents and directories retained in memory.
  memory reindex          Verify existing schema indexes in the database.
  sync markdown           Materialize markdown documents into --db and optionally watch for changes.
  source set-root         Persist the default markdown root in --db.
  mcp serve               Run the stdio MCP memory server for --db.
  install                 Install the bundled cupld-md-memory SKILL.md and bootstrap local cupld memory.
  install --mcp           Also write supported harness MCP config; use --dry-run or --print-only for previews.
  -v, --version           Print the cupld version.
  -h, --help, help        Show this help text.

REPL:
  Run .help inside the REPL for interactive commands."
}

struct QueryRunConfig<'a> {
    db_path: PathBuf,
    with_markdown: bool,
    root_override: Option<PathBuf>,
    output: OutputFormat,
    params_json: Option<&'a str>,
    params_file: Option<&'a Path>,
    max_rows: usize,
    query_args: &'a [String],
}

fn run_query(config: QueryRunConfig<'_>) -> Result<(), String> {
    let QueryRunConfig {
        db_path,
        with_markdown,
        root_override,
        output,
        params_json,
        params_file,
        max_rows,
        query_args,
    } = config;
    let (db_path, query) = parse_query(db_path, query_args)?;
    let params = load_params(params_json, params_file)
        .map_err(|error| format_command_error(output, &error))?;
    let mut session = if with_markdown {
        open_query_session_with_markdown(&db_path, root_override.as_deref())
            .map_err(|error| format_command_error(output, &error))?
    } else {
        Session::open(&db_path)
            .map_err(AutomationError::from)
            .map_err(|error| format_command_error(output, &error))?
    };
    let results = session
        .execute_script(&query, &params)
        .map_err(AutomationError::from)
        .map_err(|error| format_command_error(output, &error))?;
    match output {
        OutputFormat::Table => {
            let limited = cap_results(&results, max_rows);
            print_results(&limited, output);
        }
        OutputFormat::Json => {
            println!(
                "{}",
                query_as_json(&results, AutomationPolicy::query(max_rows))
            );
        }
        OutputFormat::Ndjson => {
            for line in query_as_ndjson(&results, AutomationPolicy::query(max_rows)) {
                println!("{line}");
            }
        }
    }
    Ok(())
}

fn run_context(output: OutputFormat, request: ContextRequest) -> Result<(), String> {
    let envelope = request
        .run()
        .map_err(|error| format_command_error(output, &error))?;
    match output {
        OutputFormat::Table => print_results(&[context_as_query_result(&envelope)], output),
        OutputFormat::Json => println!("{}", context_as_json(&envelope)),
        OutputFormat::Ndjson => {
            for line in context_as_ndjson(&envelope) {
                println!("{line}");
            }
        }
    }
    Ok(())
}

fn run_eval_memory(config: MemoryEvalConfig) -> Result<(), String> {
    let report = memory_eval::run(memory_eval::MemoryEvalConfig {
        fixtures: config.fixtures,
        case: config.case,
        update_snapshots: config.update_snapshots,
    })?;
    if config.ci {
        print!("{}", memory_eval::report_as_ci(&report));
    } else {
        match config.output {
            OutputFormat::Table => {
                print!("{}", memory_eval::report_as_table(&report));
            }
            OutputFormat::Json => println!("{}", memory_eval::report_as_json(&report)),
            OutputFormat::Ndjson => {
                for line in memory_eval::report_as_ndjson(&report) {
                    println!("{line}");
                }
            }
        }
    }
    if report.ok {
        Ok(())
    } else {
        Err("memory eval failed".to_owned())
    }
}

fn cap_results(results: &[QueryResult], max_rows: usize) -> Vec<QueryResult> {
    results
        .iter()
        .map(|result| QueryResult {
            columns: result.columns.clone(),
            rows: result.rows.iter().take(max_rows).cloned().collect(),
        })
        .collect()
}

fn format_error_json(code: &str, message: &str) -> String {
    machine_error_json(code, message)
}

fn format_command_error(output: OutputFormat, error: &AutomationError) -> String {
    match output {
        OutputFormat::Table => error.to_string(),
        OutputFormat::Json | OutputFormat::Ndjson => {
            format_error_json(error.code(), error.message())
        }
    }
}

fn load_params(
    params_json: Option<&str>,
    params_file: Option<&Path>,
) -> Result<BTreeMap<String, Value>, AutomationError> {
    if params_json.is_some() && params_file.is_some() {
        return Err(AutomationError::new(
            "params_json_conflict",
            "`query` accepts either --params-json or --params-file, not both",
        ));
    }
    if let Some(json) = params_json {
        return parse_params_json(json);
    }
    if let Some(path) = params_file {
        let input = std::fs::read_to_string(path).map_err(|error| {
            AutomationError::new(
                "params_file_read",
                format!("failed to read params file {}: {error}", path.display()),
            )
        })?;
        return parse_params_json(&input);
    }
    Ok(BTreeMap::new())
}

fn parse_params_json(input: &str) -> Result<BTreeMap<String, Value>, AutomationError> {
    parse_params_json_impl(input)
}

fn parse_non_negative_usize(input: &str) -> Option<usize> {
    input.parse::<usize>().ok()
}

fn query_max_rows_default() -> usize {
    match env::var("CUPLD_QUERY_MAX_ROWS") {
        Ok(value) => parse_non_negative_usize(&value).unwrap_or(1_000),
        Err(_) => 1_000,
    }
}

fn install_prompt_disabled() -> bool {
    matches!(
        env::var("CUPLD_NO_INSTALL_PROMPT").ok().as_deref(),
        Some("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

fn release_upgrade_check_disabled() -> bool {
    matches!(
        env::var("CUPLD_NO_UPGRADE_CHECK").ok().as_deref(),
        Some("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

fn should_offer_skill_install_prompt(command: &CliCommand) -> bool {
    !install_prompt_disabled()
        && matches!(command, CliCommand::ReplMemory | CliCommand::ReplWithDb(_))
}

fn maybe_suggest_release_upgrade(command: &CliCommand) {
    if release_upgrade_check_disabled() {
        return;
    }
    let Some(db_path) = command.db_path_for_upgrade_hint() else {
        return;
    };
    let Some(latest) = latest_release_for_startup_hint() else {
        return;
    };
    if !is_newer_semver(&latest.version, env!("CARGO_PKG_VERSION")) {
        return;
    }
    eprintln!(
        "A newer cupld release is available: {} (current {}). Update cupld, then run `cupld upgrade --db {}` before normal DB use. See {}",
        latest.version,
        env!("CARGO_PKG_VERSION"),
        db_path.display(),
        latest.url
    );
}

impl CliCommand {
    fn db_path_for_upgrade_hint(&self) -> Option<&Path> {
        match self {
            Self::ReplWithDb(path)
            | Self::Visualise { db_path: path, .. }
            | Self::Query { db_path: path, .. }
            | Self::Schema { db_path: path }
            | Self::Compact { db_path: path }
            | Self::Check { db_path: path }
            | Self::Upgrade { db_path: path }
            | Self::SyncMarkdown { db_path: path, .. }
            | Self::SourceSetRoot { db_path: path, .. }
            | Self::McpServe { db_path: path, .. } => Some(path),
            Self::Context { request, .. } => Some(&request.db_path),
            Self::Memory(command) => command.db_path_for_upgrade_hint(),
            Self::Help
            | Self::Version
            | Self::ReplMemory
            | Self::EvalMemory(_)
            | Self::Install(_) => None,
        }
    }
}

impl MemoryCommand {
    fn db_path_for_upgrade_hint(&self) -> Option<&Path> {
        match self {
            Self::Check { db_path, .. }
            | Self::FindStale { db_path, .. }
            | Self::FindOrphans { db_path, .. }
            | Self::Reindex { db_path, .. } => Some(db_path),
            Self::Deferred { .. } => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LatestReleaseInfo {
    version: String,
    url: String,
}

fn latest_release_for_startup_hint() -> Option<LatestReleaseInfo> {
    let cache_path = release_check_cache_path()?;
    let now = unix_timestamp_secs()?;
    if let Some(cached) = read_release_check_cache(&cache_path, now) {
        return cached.latest;
    }

    let fetched = fetch_latest_release();
    let _ = write_release_check_cache(&cache_path, now, fetched.as_ref());
    fetched
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ReleaseCheckCache {
    checked_at_unix: u64,
    latest: Option<LatestReleaseInfo>,
}

fn read_release_check_cache(path: &Path, now: u64) -> Option<ReleaseCheckCache> {
    let contents = fs::read_to_string(path).ok()?;
    let parsed = json::parse(&contents).ok()?;
    let checked_at_unix = json_u64(parsed.get("checked_at_unix")?)?;
    if now.saturating_sub(checked_at_unix) >= RELEASE_CHECK_INTERVAL_SECS {
        return None;
    }
    let latest_version = parsed
        .get("latest_version")
        .and_then(json::JsonValue::as_str)
        .map(str::to_owned);
    let latest_url = parsed
        .get("latest_url")
        .and_then(json::JsonValue::as_str)
        .map(str::to_owned);
    let latest = match (latest_version, latest_url) {
        (Some(version), Some(url)) => Some(LatestReleaseInfo { version, url }),
        _ => None,
    };
    Some(ReleaseCheckCache {
        checked_at_unix,
        latest,
    })
}

fn json_u64(value: &json::JsonValue) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|value| u64::try_from(value).ok()))
}

fn write_release_check_cache(
    path: &Path,
    checked_at_unix: u64,
    latest: Option<&LatestReleaseInfo>,
) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let (latest_version, latest_url) = match latest {
        Some(latest) => (
            json::JsonValue::from(latest.version.clone()),
            json::JsonValue::from(latest.url.clone()),
        ),
        None => (json::JsonValue::Null, json::JsonValue::Null),
    };
    let rendered = json::stringify(&json::JsonValue::object([
        ("checked_at_unix", json::JsonValue::from(checked_at_unix)),
        ("latest_version", latest_version),
        ("latest_url", latest_url),
    ]));
    fs::write(path, rendered).map_err(|error| error.to_string())
}

fn fetch_latest_release() -> Option<LatestReleaseInfo> {
    let output = Command::new("curl")
        .args([
            "--fail",
            "--silent",
            "--show-error",
            "--location",
            "--max-time",
            "2",
            "--header",
            concat!("User-Agent: cupld/", env!("CARGO_PKG_VERSION")),
            LATEST_RELEASE_URL,
        ])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let body = String::from_utf8(output.stdout).ok()?;
    parse_latest_release_response(&body)
}

fn parse_latest_release_response(input: &str) -> Option<LatestReleaseInfo> {
    let parsed = json::parse(input).ok()?;
    let version = parsed.get("tag_name")?.as_str()?.to_owned();
    let url = parsed
        .get("html_url")
        .and_then(json::JsonValue::as_str)
        .unwrap_or("https://github.com/aeaston1/cupld/releases/latest")
        .to_owned();
    Some(LatestReleaseInfo { version, url })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Semver {
    major: u64,
    minor: u64,
    patch: u64,
}

fn parse_semver(input: &str) -> Option<Semver> {
    let version = input
        .trim()
        .strip_prefix('v')
        .unwrap_or_else(|| input.trim());
    let mut parts = version.split('.');
    let major = parts.next()?.parse::<u64>().ok()?;
    let minor = parts.next()?.parse::<u64>().ok()?;
    let patch = parts.next()?.parse::<u64>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some(Semver {
        major,
        minor,
        patch,
    })
}

fn is_newer_semver(candidate: &str, current: &str) -> bool {
    match (parse_semver(candidate), parse_semver(current)) {
        (Some(candidate), Some(current)) => candidate > current,
        _ => false,
    }
}

fn unix_timestamp_secs() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
}

fn release_check_cache_path() -> Option<PathBuf> {
    if cfg!(windows)
        && let Some(path) = env_path("APPDATA")
    {
        return Some(path.join(".cupld").join(RELEASE_CHECK_CACHE_FILE));
    }
    if let Some(path) = env_path("XDG_CONFIG_HOME") {
        return Some(path.join(".cupld").join(RELEASE_CHECK_CACHE_FILE));
    }
    home_dir().map(|home| {
        home.join(".config")
            .join(".cupld")
            .join(RELEASE_CHECK_CACHE_FILE)
    })
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

fn env_path(key: &str) -> Option<PathBuf> {
    env::var_os(key)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn run_visualise(db_path: PathBuf, query: Option<String>) -> Result<(), String> {
    visualise::run(db_path, query)
}

fn run_schema(db_path: &Path) -> Result<(), String> {
    let mut session = Session::open(db_path).map_err(|error| error.to_string())?;
    let results = session
        .execute_script("SHOW SCHEMA", &BTreeMap::new())
        .map_err(|error| error.to_string())?;
    print_results(&results, OutputFormat::Table);
    Ok(())
}

fn run_compact(db_path: PathBuf) -> Result<(), String> {
    let mut session = Session::open(&db_path).map_err(|error| error.to_string())?;
    session.compact().map_err(|error| error.to_string())?;
    println!("compacted {}", db_path.display());
    Ok(())
}

fn run_check(db_path: PathBuf) -> Result<(), String> {
    let check = check_database(&db_path)?;
    println!(
        "ok db={} last_tx_id={} wal_records={} recovered_tail={} ambiguous_markdown_aliases={}",
        db_path.display(),
        check.last_tx_id,
        check.wal_records,
        check.recovered_tail,
        check.ambiguous_markdown_aliases
    );
    Ok(())
}

struct DatabaseCheckSummary {
    last_tx_id: u64,
    wal_records: usize,
    recovered_tail: bool,
    ambiguous_markdown_aliases: usize,
}

fn check_database(db_path: &Path) -> Result<DatabaseCheckSummary, String> {
    let report = Session::check(db_path).map_err(|error| error.to_string())?;
    let session = Session::open(db_path).map_err(|error| error.to_string())?;
    let alias_diagnostics = markdown_alias_diagnostics(session.engine());
    Ok(DatabaseCheckSummary {
        last_tx_id: report.last_tx_id,
        wal_records: report.wal_records,
        recovered_tail: report.recovered_tail,
        ambiguous_markdown_aliases: alias_diagnostics.ambiguous_alias_count(),
    })
}

fn run_upgrade(db_path: PathBuf) -> Result<(), String> {
    let backup_path = backup_database(&db_path)?;
    let check = check_database(&db_path)?;
    let memory_report = build_memory_check_report(&db_path, None, OutputFormat::Table, false)?;

    println!("backup={}", backup_path.display());
    println!(
        "check=pass db={} last_tx_id={} wal_records={} recovered_tail={} ambiguous_markdown_aliases={}",
        db_path.display(),
        check.last_tx_id,
        check.wal_records,
        check.recovered_tail,
        check.ambiguous_markdown_aliases
    );
    println!(
        "memory_check={} db={} root={}",
        memory_report.status.as_str(),
        memory_report.db_path.display(),
        memory_report
            .root
            .as_ref()
            .map(|root| root.display().to_string())
            .unwrap_or_else(|| "null".to_owned())
    );
    Ok(())
}

fn backup_database(db_path: &Path) -> Result<PathBuf, String> {
    if !db_path.exists() {
        return Err(format!("database does not exist: {}", db_path.display()));
    }
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_nanos();
    let file_name = db_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            format!(
                "database path has no valid file name: {}",
                db_path.display()
            )
        })?;
    let backup_path = db_path.with_file_name(format!("{file_name}.backup.{timestamp}"));
    fs::copy(db_path, &backup_path).map_err(|error| {
        format!(
            "failed to back up {} to {}: {error}",
            db_path.display(),
            backup_path.display()
        )
    })?;
    Ok(backup_path)
}

fn resolved_report_db_path(db_path: &Path, output: OutputFormat) -> Result<PathBuf, String> {
    if db_path.is_absolute() {
        return Ok(db_path.to_path_buf());
    }
    env::current_dir()
        .map(|cwd| cwd.join(db_path))
        .map_err(|error| {
            format_command_error(
                output,
                &AutomationError::new(
                    "memory_db_path",
                    format!("failed to resolve database path: {error}"),
                ),
            )
        })
}

fn maintenance_status_for_problem(problem: bool, strict: bool) -> MemoryMaintenanceStatus {
    match (problem, strict) {
        (true, true) => MemoryMaintenanceStatus::Fail,
        (true, false) => MemoryMaintenanceStatus::Warn,
        (false, _) => MemoryMaintenanceStatus::Pass,
    }
}

fn maintenance_report_status(checks: &[MemoryMaintenanceCheck]) -> MemoryMaintenanceStatus {
    if checks
        .iter()
        .any(|check| check.status == MemoryMaintenanceStatus::Fail)
    {
        MemoryMaintenanceStatus::Fail
    } else if checks
        .iter()
        .any(|check| check.status == MemoryMaintenanceStatus::Warn)
    {
        MemoryMaintenanceStatus::Warn
    } else {
        MemoryMaintenanceStatus::Pass
    }
}

fn run_memory(command: MemoryCommand) -> Result<(), String> {
    match command {
        MemoryCommand::Check {
            db_path,
            root_override,
            output,
            strict,
        } => run_memory_check(db_path, root_override, output, strict),
        MemoryCommand::FindStale {
            db_path,
            root_override,
            output,
        } => run_memory_find_stale(db_path, root_override, output),
        MemoryCommand::FindOrphans { db_path, output } => run_memory_find_orphans(db_path, output),
        MemoryCommand::Reindex { db_path, output } => run_memory_reindex(db_path, output),
        MemoryCommand::Deferred { subcommand } => Err(format!(
            "unsupported: `cupld memory {subcommand}` is intentionally out of scope for this round"
        )),
    }
}

fn run_memory_check(
    db_path: PathBuf,
    root_override: Option<PathBuf>,
    output: OutputFormat,
    strict: bool,
) -> Result<(), String> {
    let report = build_memory_check_report(&db_path, root_override.as_deref(), output, strict)?;
    let status = report.status;
    print_memory_report(&report, output);
    if strict && status == MemoryMaintenanceStatus::Warn {
        process::exit(2);
    }
    Ok(())
}

fn build_memory_check_report(
    db_path: &Path,
    root_override: Option<&Path>,
    output: OutputFormat,
    strict: bool,
) -> Result<MemoryMaintenanceReport, String> {
    let report_db_path = resolved_report_db_path(db_path, output)?;
    let integrity = Session::check(db_path)
        .map_err(AutomationError::from)
        .map_err(|error| format_command_error(output, &error))?;
    let mut session = Session::open(db_path)
        .map_err(AutomationError::from)
        .map_err(|error| format_command_error(output, &error))?;
    let root =
        resolve_markdown_root(root_override.as_deref(), Some(&session)).map_err(|message| {
            format_command_error(output, &AutomationError::new("memory_root", message))
        })?;
    let stale = memory_stale_items(&mut session, &root)
        .map_err(|error| format_command_error(output, &error))?;
    let orphans =
        memory_orphan_items(&mut session).map_err(|error| format_command_error(output, &error))?;
    let alias_diagnostics = markdown_alias_diagnostics(session.engine());
    let stale_summary = memory_stale_summary(&stale);
    let metadata_summary = markdown_metadata_summary(&session);
    let duplicate_path_count = duplicate_current_markdown_path_count(&session);
    let duplicate_link_edge_count = duplicate_connector_owned_markdown_link_edge_count(&session);
    let schema_index_summary = schema_index_summary(&session);
    let has_warning = integrity.recovered_tail
        || stale_summary.missing_or_tombstoned_documents > 0
        || stale_summary.stale_current_documents > 0
        || metadata_summary.missing_required_metadata > 0
        || duplicate_path_count > 0
        || duplicate_link_edge_count > 0
        || alias_diagnostics.ambiguous_alias_count() > 0
        || schema_index_summary.non_ready_indexes > 0
        || !orphans.rows.is_empty();
    let aggregate_status = if has_warning {
        MemoryMaintenanceStatus::Warn
    } else {
        MemoryMaintenanceStatus::Pass
    };
    let checks = vec![
        MemoryMaintenanceCheck::new(
            "status",
            aggregate_status,
            RuntimeValue::String(aggregate_status.as_str().to_owned()),
        ),
        MemoryMaintenanceCheck::new(
            "last_tx_id",
            MemoryMaintenanceStatus::Pass,
            RuntimeValue::Int(integrity.last_tx_id as i64),
        ),
        MemoryMaintenanceCheck::new(
            "wal_records",
            MemoryMaintenanceStatus::Pass,
            RuntimeValue::Int(integrity.wal_records as i64),
        ),
        MemoryMaintenanceCheck::new(
            "recovered_tail",
            maintenance_status_for_problem(integrity.recovered_tail, false),
            RuntimeValue::Bool(integrity.recovered_tail),
        ),
        MemoryMaintenanceCheck::new(
            "missing_tombstoned_markdown_documents",
            maintenance_status_for_problem(
                stale_summary.missing_or_tombstoned_documents > 0,
                false,
            ),
            RuntimeValue::Int(stale_summary.missing_or_tombstoned_documents as i64),
        ),
        MemoryMaintenanceCheck::new(
            "stale_current_markdown_documents",
            maintenance_status_for_problem(stale_summary.stale_current_documents > 0, false),
            RuntimeValue::Int(stale_summary.stale_current_documents as i64),
        ),
        MemoryMaintenanceCheck::new(
            "markdown_documents_missing_source_metadata",
            maintenance_status_for_problem(metadata_summary.missing_required_metadata > 0, false),
            RuntimeValue::Int(metadata_summary.missing_required_metadata as i64),
        )
        .with_message(format!(
            "required_metadata={}",
            REQUIRED_MARKDOWN_SOURCE_METADATA.join(",")
        )),
        MemoryMaintenanceCheck::new(
            "duplicate_current_markdown_document_paths",
            maintenance_status_for_problem(duplicate_path_count > 0, false),
            RuntimeValue::Int(duplicate_path_count as i64),
        ),
        MemoryMaintenanceCheck::new(
            "duplicate_connector_owned_md_links_to_edges",
            maintenance_status_for_problem(duplicate_link_edge_count > 0, false),
            RuntimeValue::Int(duplicate_link_edge_count as i64),
        ),
        MemoryMaintenanceCheck::new(
            "schema_indexes",
            maintenance_status_for_problem(schema_index_summary.non_ready_indexes > 0, false),
            RuntimeValue::Int(schema_index_summary.total_indexes as i64),
        )
        .with_message(format!(
            "ready={} non_ready={}",
            schema_index_summary.ready_indexes, schema_index_summary.non_ready_indexes
        )),
        MemoryMaintenanceCheck::new(
            "stale_items",
            maintenance_status_for_problem(!stale.rows.is_empty(), false),
            RuntimeValue::Int(stale.rows.len() as i64),
        ),
        MemoryMaintenanceCheck::new(
            "orphan_items",
            maintenance_status_for_problem(!orphans.rows.is_empty(), false),
            RuntimeValue::Int(orphans.rows.len() as i64),
        ),
        MemoryMaintenanceCheck::new(
            "ambiguous_markdown_aliases",
            maintenance_status_for_problem(alias_diagnostics.ambiguous_alias_count() > 0, false),
            RuntimeValue::Int(alias_diagnostics.ambiguous_alias_count() as i64),
        ),
    ];
    let status = maintenance_report_status(&checks);
    let report = MemoryMaintenanceReport {
        command: "memory.check",
        db_path: report_db_path,
        root: Some(root),
        strict: Some(strict),
        status,
        checks,
        markdown_alias_diagnostics: Some(alias_diagnostics),
        items: QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
        },
    };
    Ok(report)
}

fn run_memory_find_stale(
    db_path: PathBuf,
    root_override: Option<PathBuf>,
    output: OutputFormat,
) -> Result<(), String> {
    let report_db_path = resolved_report_db_path(&db_path, output)?;
    let mut session = Session::open(&db_path)
        .map_err(AutomationError::from)
        .map_err(|error| format_command_error(output, &error))?;
    let root =
        resolve_markdown_root(root_override.as_deref(), Some(&session)).map_err(|message| {
            format_command_error(output, &AutomationError::new("memory_root", message))
        })?;
    let items = memory_stale_items(&mut session, &root)
        .map_err(|error| format_command_error(output, &error))?;
    let checks = vec![MemoryMaintenanceCheck::new(
        "stale_items",
        maintenance_status_for_problem(!items.rows.is_empty(), false),
        RuntimeValue::Int(items.rows.len() as i64),
    )];
    let report = MemoryMaintenanceReport {
        command: "memory.find-stale",
        db_path: report_db_path,
        root: Some(root),
        strict: None,
        status: maintenance_report_status(&checks),
        checks,
        markdown_alias_diagnostics: None,
        items,
    };
    print_memory_report(&report, output);
    Ok(())
}

fn run_memory_find_orphans(db_path: PathBuf, output: OutputFormat) -> Result<(), String> {
    let report_db_path = resolved_report_db_path(&db_path, output)?;
    let mut session = Session::open(&db_path)
        .map_err(AutomationError::from)
        .map_err(|error| format_command_error(output, &error))?;
    let items =
        memory_orphan_items(&mut session).map_err(|error| format_command_error(output, &error))?;
    let checks = vec![MemoryMaintenanceCheck::new(
        "orphan_items",
        maintenance_status_for_problem(!items.rows.is_empty(), false),
        RuntimeValue::Int(items.rows.len() as i64),
    )];
    let report = MemoryMaintenanceReport {
        command: "memory.find-orphans",
        db_path: report_db_path,
        root: None,
        strict: None,
        status: maintenance_report_status(&checks),
        checks,
        markdown_alias_diagnostics: None,
        items,
    };
    print_memory_report(&report, output);
    Ok(())
}

fn run_memory_reindex(db_path: PathBuf, output: OutputFormat) -> Result<(), String> {
    let report_db_path = resolved_report_db_path(&db_path, output)?;
    let session = open_initial_session(Some(db_path.clone())).map_err(|message| {
        format_command_error(output, &AutomationError::new("memory_db", message))
    })?;
    let indexes = session.engine().show_indexes(None);
    let index_count = indexes.len();
    let checks = vec![
        MemoryMaintenanceCheck::new(
            "index_count",
            MemoryMaintenanceStatus::Pass,
            RuntimeValue::Int(index_count as i64),
        ),
        MemoryMaintenanceCheck::new(
            "schema_indexes",
            MemoryMaintenanceStatus::Pass,
            RuntimeValue::String(if index_count == 0 { "none" } else { "verified" }.to_owned()),
        )
        .with_message(
            "existing schema index definitions were inspected; no new indexes were created",
        ),
    ];
    let items = QueryResult {
        columns: vec![
            "name".to_owned(),
            "target_kind".to_owned(),
            "target_name".to_owned(),
            "property".to_owned(),
            "kind".to_owned(),
            "unique".to_owned(),
            "status".to_owned(),
            "outcome".to_owned(),
        ],
        rows: indexes
            .into_iter()
            .map(|index| {
                let outcome = if index.status == "ready" {
                    "verified"
                } else {
                    "status_preserved"
                };
                vec![
                    RuntimeValue::String(index.name),
                    RuntimeValue::String(index.target_kind),
                    RuntimeValue::String(index.target_name),
                    RuntimeValue::String(index.property),
                    RuntimeValue::String(index.kind),
                    RuntimeValue::Bool(index.unique),
                    RuntimeValue::String(index.status),
                    RuntimeValue::String(outcome.to_owned()),
                ]
            })
            .collect(),
    };
    let report = MemoryMaintenanceReport {
        command: "memory.reindex",
        db_path: report_db_path,
        root: None,
        strict: None,
        status: maintenance_report_status(&checks),
        checks,
        markdown_alias_diagnostics: None,
        items,
    };
    print_memory_report(&report, output);
    Ok(())
}

fn memory_stale_items(session: &mut Session, root: &Path) -> Result<QueryResult, AutomationError> {
    let result = session
        .execute_script(
            "MATCH (d:MarkdownDocument)
             RETURN d.`src.path` AS path,
                    d.`md.title` AS title,
                    d.`src.hash` AS source_hash,
                    d.`src.root` AS source_root,
                    d.`src.status` AS status
             ORDER BY d.`src.path`",
            &BTreeMap::new(),
        )
        .map_err(AutomationError::from)?
        .into_iter()
        .next()
        .unwrap_or_else(|| QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
        });
    let mut rows = Vec::new();
    for row in result.rows {
        let path = optional_string_column(&result.columns, &row, "path")?;
        let title = optional_string_column(&result.columns, &row, "title")?;
        let source_hash = optional_string_column(&result.columns, &row, "source_hash")?;
        let source_root = optional_string_column(&result.columns, &row, "source_root")?;
        let status = optional_string_column(&result.columns, &row, "status")?;
        let metadata_incomplete =
            path.is_none() || source_hash.is_none() || source_root.is_none() || status.is_none();
        let path_for_report = path.clone().unwrap_or_default();
        let status_for_report = status.clone().unwrap_or_else(|| "unknown".to_owned());
        let root_for_report = root.display().to_string();
        if metadata_incomplete {
            push_stale_item(
                &mut rows,
                "metadata_incomplete",
                &path_for_report,
                title.as_deref(),
                &status_for_report,
                source_hash.as_deref(),
                None,
                source_root.as_deref(),
                &root_for_report,
            );
            continue;
        }

        let Some(path) = path else {
            continue;
        };
        if source_root.as_deref() != Some(root_for_report.as_str()) {
            push_stale_item(
                &mut rows,
                "root_mismatch",
                &path,
                title.as_deref(),
                &status_for_report,
                source_hash.as_deref(),
                None,
                source_root.as_deref(),
                &root_for_report,
            );
        }

        let disk_path = root.join(&path);
        match fs::read(&disk_path) {
            Ok(bytes) => {
                let disk_hash = stable_hash_hex(&bytes);
                if status.as_deref() == Some("missing") {
                    push_stale_item(
                        &mut rows,
                        "tombstoned_document",
                        &path,
                        title.as_deref(),
                        &status_for_report,
                        source_hash.as_deref(),
                        Some(&disk_hash),
                        source_root.as_deref(),
                        &root_for_report,
                    );
                } else if source_hash.as_deref() != Some(disk_hash.as_str()) {
                    push_stale_item(
                        &mut rows,
                        "hash_mismatch",
                        &path,
                        title.as_deref(),
                        &status_for_report,
                        source_hash.as_deref(),
                        Some(&disk_hash),
                        source_root.as_deref(),
                        &root_for_report,
                    );
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if status.as_deref() == Some("missing") {
                    push_stale_item(
                        &mut rows,
                        "tombstoned_document",
                        &path,
                        title.as_deref(),
                        &status_for_report,
                        source_hash.as_deref(),
                        None,
                        source_root.as_deref(),
                        &root_for_report,
                    );
                } else if status.as_deref() == Some("current") {
                    push_stale_item(
                        &mut rows,
                        "missing_file",
                        &path,
                        title.as_deref(),
                        &status_for_report,
                        source_hash.as_deref(),
                        None,
                        source_root.as_deref(),
                        &root_for_report,
                    );
                }
            }
            Err(error) => {
                return Err(AutomationError::new(
                    "memory_file_read",
                    format!("failed to read {}: {error}", disk_path.display()),
                ));
            }
        }
    }
    Ok(QueryResult {
        columns: vec![
            "kind".to_owned(),
            "path".to_owned(),
            "title".to_owned(),
            "status".to_owned(),
            "stored_hash".to_owned(),
            "current_hash".to_owned(),
            "stored_root".to_owned(),
            "resolved_root".to_owned(),
            "suggestion".to_owned(),
        ],
        rows,
    })
}

fn push_stale_item(
    rows: &mut Vec<Vec<RuntimeValue>>,
    kind: &str,
    path: &str,
    title: Option<&str>,
    status: &str,
    stored_hash: Option<&str>,
    current_hash: Option<&str>,
    stored_root: Option<&str>,
    resolved_root: &str,
) {
    rows.push(vec![
        RuntimeValue::String(kind.to_owned()),
        string_or_null(path),
        option_string(title),
        RuntimeValue::String(status.to_owned()),
        option_string(stored_hash),
        option_string(current_hash),
        option_string(stored_root),
        RuntimeValue::String(resolved_root.to_owned()),
        RuntimeValue::String(stale_item_suggestion(kind, resolved_root)),
    ]);
}

fn stale_item_suggestion(kind: &str, root: &str) -> String {
    match kind {
        "missing_file" => format!(
            "restore the file or run `cupld sync markdown --db ... --root {root}` to refresh persisted markdown state"
        ),
        "hash_mismatch" => format!(
            "run `cupld sync markdown --db ... --root {root}` to refresh persisted markdown state"
        ),
        "tombstoned_document" => format!(
            "restore the file and run `cupld sync markdown --db ... --root {root}` if the document should be current"
        ),
        "metadata_incomplete" => format!(
            "run `cupld sync markdown --db ... --root {root}` to restore required source metadata"
        ),
        "root_mismatch" => format!(
            "run `cupld sync markdown --db ... --root {root}` if this is the intended markdown root"
        ),
        _ => format!("run `cupld sync markdown --db ... --root {root}`"),
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct MemoryStaleSummary {
    missing_or_tombstoned_documents: usize,
    stale_current_documents: usize,
}

fn memory_stale_summary(items: &QueryResult) -> MemoryStaleSummary {
    let Some(kind_index) = items.columns.iter().position(|column| column == "kind") else {
        return MemoryStaleSummary::default();
    };
    let mut summary = MemoryStaleSummary::default();
    for row in &items.rows {
        let Some(RuntimeValue::String(kind)) = row.get(kind_index) else {
            continue;
        };
        match kind.as_str() {
            "missing_file" | "tombstoned_document" => {
                summary.missing_or_tombstoned_documents += 1;
            }
            "hash_mismatch" => {
                summary.stale_current_documents += 1;
            }
            _ => {}
        }
    }
    summary
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct MarkdownMetadataSummary {
    missing_required_metadata: usize,
}

fn markdown_metadata_summary(session: &Session) -> MarkdownMetadataSummary {
    let mut missing_required_metadata = 0;
    for node in session
        .engine()
        .nodes()
        .filter(|node| node.labels().contains(MARKDOWN_DOCUMENT_LABEL))
    {
        if REQUIRED_MARKDOWN_SOURCE_METADATA.iter().any(
            |key| !matches!(node.property(key), Some(Value::String(value)) if !value.is_empty()),
        ) {
            missing_required_metadata += 1;
        }
    }
    MarkdownMetadataSummary {
        missing_required_metadata,
    }
}

fn duplicate_current_markdown_path_count(session: &Session) -> usize {
    let mut seen = BTreeSet::new();
    let mut duplicates = BTreeSet::new();
    for node in session
        .engine()
        .nodes()
        .filter(|node| node.labels().contains(MARKDOWN_DOCUMENT_LABEL))
    {
        if string_property(node.property("src.status")) != Some("current") {
            continue;
        }
        let Some(path) = string_property(node.property("src.path")) else {
            continue;
        };
        if !seen.insert(path.to_owned()) {
            duplicates.insert(path.to_owned());
        }
    }
    duplicates.len()
}

fn duplicate_connector_owned_markdown_link_edge_count(session: &Session) -> usize {
    let mut seen = BTreeSet::new();
    let mut duplicates = 0;
    for edge in session
        .engine()
        .edges()
        .filter(|edge| edge.edge_type() == MD_LINKS_TO)
        .filter(|edge| string_property(edge.property("src.connector")) == Some("markdown"))
    {
        let from_path = session
            .engine()
            .node(edge.from())
            .and_then(|node| string_property(node.property("src.path")));
        let to_path = session
            .engine()
            .node(edge.to())
            .and_then(|node| string_property(node.property("src.path")));
        let key = (
            from_path.map(ToOwned::to_owned),
            to_path.map(ToOwned::to_owned),
            edge.edge_type().to_owned(),
        );
        if !seen.insert(key) {
            duplicates += 1;
        }
    }
    duplicates
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SchemaIndexSummary {
    total_indexes: usize,
    ready_indexes: usize,
    non_ready_indexes: usize,
}

fn schema_index_summary(session: &Session) -> SchemaIndexSummary {
    let rows = session.engine().show_indexes(None);
    let ready_indexes = rows.iter().filter(|row| row.status == "ready").count();
    SchemaIndexSummary {
        total_indexes: rows.len(),
        ready_indexes,
        non_ready_indexes: rows.len().saturating_sub(ready_indexes),
    }
}

fn option_string(value: Option<&str>) -> RuntimeValue {
    value
        .map(|value| RuntimeValue::String(value.to_owned()))
        .unwrap_or(RuntimeValue::Null)
}

fn string_or_null(value: &str) -> RuntimeValue {
    if value.is_empty() {
        RuntimeValue::Null
    } else {
        RuntimeValue::String(value.to_owned())
    }
}

fn memory_orphan_items(session: &mut Session) -> Result<QueryResult, AutomationError> {
    let mut rows = Vec::new();
    for node in session
        .engine()
        .nodes()
        .filter(|node| node.labels().contains(MARKDOWN_DOCUMENT_LABEL))
    {
        let status = string_property(node.property("src.status")).unwrap_or("unknown");
        if status != "current" {
            continue;
        }
        let node_id = node.id();
        let mut markdown_inbound_count = 0;
        let mut markdown_outbound_count = 0;
        let mut native_inbound_count = 0;
        let mut native_outbound_count = 0;
        for edge in session.engine().edges() {
            let touches_node = edge.from() == node_id || edge.to() == node_id;
            if !touches_node {
                continue;
            }
            if edge.edge_type() == MD_LINKS_TO {
                if edge.to() == node_id {
                    markdown_inbound_count += 1;
                }
                if edge.from() == node_id {
                    markdown_outbound_count += 1;
                }
            } else if !is_markdown_structural_edge(edge.edge_type()) {
                if edge.to() == node_id {
                    native_inbound_count += 1;
                }
                if edge.from() == node_id {
                    native_outbound_count += 1;
                }
            }
        }
        if markdown_inbound_count == 0
            && markdown_outbound_count == 0
            && native_inbound_count == 0
            && native_outbound_count == 0
        {
            rows.push(vec![
                RuntimeValue::String(
                    string_property(node.property("src.path"))
                        .unwrap_or("unknown")
                        .to_owned(),
                ),
                RuntimeValue::String(
                    string_property(node.property("md.title"))
                        .unwrap_or("")
                        .to_owned(),
                ),
                RuntimeValue::String(status.to_owned()),
                RuntimeValue::Int(markdown_inbound_count),
                RuntimeValue::Int(markdown_outbound_count),
                RuntimeValue::Int(native_inbound_count),
                RuntimeValue::Int(native_outbound_count),
                RuntimeValue::String("no_markdown_or_native_connectivity".to_owned()),
            ]);
        }
    }
    rows.sort_by(|left, right| value_string(&left[0]).cmp(&value_string(&right[0])));
    Ok(QueryResult {
        columns: vec![
            "path".to_owned(),
            "title".to_owned(),
            "status".to_owned(),
            "markdown_inbound_count".to_owned(),
            "markdown_outbound_count".to_owned(),
            "native_inbound_count".to_owned(),
            "native_outbound_count".to_owned(),
            "reason".to_owned(),
        ],
        rows,
    })
}

fn is_markdown_structural_edge(edge_type: &str) -> bool {
    edge_type == MD_IN_DIRECTORY || edge_type == MD_PARENT_DIRECTORY
}

fn string_property(value: Option<&Value>) -> Option<&str> {
    match value {
        Some(Value::String(value)) => Some(value),
        _ => None,
    }
}

fn print_memory_report(report: &MemoryMaintenanceReport, output: OutputFormat) {
    match output {
        OutputFormat::Table => print!("{}", report.as_table()),
        OutputFormat::Json => println!("{}", report.as_json()),
        OutputFormat::Ndjson => {
            for line in report.as_ndjson() {
                println!("{line}");
            }
        }
    }
}
fn string_column(
    columns: &[String],
    row: &[RuntimeValue],
    column: &str,
) -> Result<String, AutomationError> {
    optional_string_column(columns, row, column)?.ok_or_else(|| {
        AutomationError::new(
            "memory_query_contract",
            format!("missing expected `{column}` string in memory query result"),
        )
    })
}

fn optional_string_column(
    columns: &[String],
    row: &[RuntimeValue],
    column: &str,
) -> Result<Option<String>, AutomationError> {
    let Some(index) = columns.iter().position(|name| name == column) else {
        return Err(AutomationError::new(
            "memory_query_contract",
            format!("missing expected `{column}` column in memory query result"),
        ));
    };
    match row.get(index) {
        Some(RuntimeValue::String(value)) => Ok(Some(value.clone())),
        Some(RuntimeValue::Null) => Ok(None),
        Some(other) => Err(AutomationError::new(
            "memory_query_contract",
            format!("expected `{column}` to be a string, found {other:?}"),
        )),
        None => Err(AutomationError::new(
            "memory_query_contract",
            format!("missing value for `{column}` in memory query result row"),
        )),
    }
}

fn stable_hash_hex(bytes: &[u8]) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

fn run_sync_markdown(
    db_path: PathBuf,
    root_override: Option<PathBuf>,
    watch: bool,
    poll_interval: Duration,
    debounce: Duration,
    batch_window: Duration,
    idle_timeout: Option<Duration>,
    max_runs: Option<usize>,
    include_fs_graph: bool,
) -> Result<(), String> {
    let mut session = open_initial_session(Some(db_path.clone()))?;
    let root = resolve_markdown_root(root_override.as_deref(), Some(&session))?;
    let package = WorkspacePackage::discover_current().map_err(|error| error.to_string())?;
    let include_fs_graph = include_fs_graph || package.configured_markdown_include_fs_graph();
    let sync_options = MarkdownSyncOptions { include_fs_graph };
    let mut engine = session.engine().clone();
    let report = if watch {
        let options = MarkdownWatchOptions {
            poll_interval,
            debounce,
            max_batch_window: batch_window,
            idle_timeout,
            max_runs,
        };
        let report =
            watch_markdown_root_with_sync_options(&mut engine, &root, &sync_options, &options)
                .map_err(|error| error.to_string())?;
        println!(
            "watch root={} runs={} events={}",
            report.root.display(),
            report.sync_runs,
            report.events_seen
        );
        report.last_report.unwrap_or(MarkdownSyncReport {
            root: root.clone(),
            scanned_documents: 0,
            upserted_documents: 0,
            tombstoned_documents: 0,
            link_edges: 0,
            upserted_directories: 0,
            tombstoned_directories: 0,
            structural_edges: 0,
        })
    } else if include_fs_graph {
        sync_markdown_root_with_options(&mut engine, &root, &sync_options)
            .map_err(|error| error.to_string())?
    } else {
        sync_markdown_root_with_options(&mut engine, &root, &sync_options)
            .map_err(|error| error.to_string())?
    };
    engine.commit().map_err(|error| error.to_string())?;
    session
        .replace_engine(engine)
        .map_err(|error| error.to_string())?;
    session.save().map_err(|error| error.to_string())?;
    println!(
        "synced root={} scanned={} upserted={} tombstoned={} links={}",
        report.root.display(),
        report.scanned_documents,
        report.upserted_documents,
        report.tombstoned_documents,
        report.link_edges
    );
    Ok(())
}

fn run_source_set_root(db_path: PathBuf, root: PathBuf) -> Result<(), String> {
    let mut session = open_initial_session(Some(db_path.clone()))?;
    let root = resolve_markdown_root(Some(&root), None)?;
    let mut engine = session.engine().clone();
    set_markdown_root(&mut engine, &root).map_err(|error| error.to_string())?;
    engine.commit().map_err(|error| error.to_string())?;
    session
        .replace_engine(engine)
        .map_err(|error| error.to_string())?;
    session.save().map_err(|error| error.to_string())?;
    persist_local_package_state(&db_path, &root)?;
    println!("markdown_root {}", root.display());
    Ok(())
}

fn run_mcp_serve(
    db_path: PathBuf,
    root_override: Option<PathBuf>,
    read_only: bool,
) -> Result<(), String> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    mcp::serve_stdio(
        McpConfig {
            db_path,
            root_override,
            read_only,
        },
        stdin.lock(),
        stdout.lock(),
    )
}

fn persist_local_package_state(db_path: &Path, root: &Path) -> Result<(), String> {
    let mut package = WorkspacePackage::discover_current().map_err(|error| error.to_string())?;
    if !package.owns_path(db_path) || !package.owns_path(root) {
        return Ok(());
    }
    package
        .persist_package_config(Some(db_path), Some(root))
        .map_err(|error| error.to_string())
}

fn open_query_session_with_markdown(
    db_path: &Path,
    root_override: Option<&Path>,
) -> Result<Session, AutomationError> {
    let session = Session::open(db_path).map_err(AutomationError::from)?;
    let root = resolve_markdown_root_for_automation(root_override, Some(&session))?;
    let mut engine = session.engine().clone();
    sync_markdown_root(&mut engine, &root).map_err(AutomationError::from)?;
    Ok(Session::from_engine(engine))
}

fn resolve_markdown_root_for_automation(
    root_override: Option<&Path>,
    session: Option<&Session>,
) -> Result<PathBuf, AutomationError> {
    let package = WorkspacePackage::discover_current()
        .map_err(|error| AutomationError::new(error.code(), error.message()))?;
    if let Some(root) = root_override {
        return Ok(package.resolve_markdown_root(Some(root)));
    }
    if let Some(root) = package.configured_markdown_root() {
        return Ok(root);
    }
    if let Some(session) = session
        && let Some(root) = configured_markdown_root(session.engine())
    {
        return Ok(root);
    }
    Ok(package.default_markdown_root())
}

fn resolve_markdown_root(
    root_override: Option<&Path>,
    session: Option<&Session>,
) -> Result<PathBuf, String> {
    let package = WorkspacePackage::discover_current().map_err(|error| error.to_string())?;
    if let Some(root) = root_override {
        return Ok(package.resolve_markdown_root(Some(root)));
    }
    if let Some(root) = package.configured_markdown_root() {
        return Ok(root);
    }
    if let Some(session) = session
        && let Some(root) = configured_markdown_root(session.engine())
    {
        return Ok(root);
    }
    Ok(package.default_markdown_root())
}

fn run_repl(path: Option<PathBuf>) -> Result<(), String> {
    let mut session = open_initial_session(path)?;
    let mut output = OutputFormat::Table;
    let mut repl_input = ReplInput::new();

    loop {
        if repl_input.interactive() && !repl_input.has_pending() {
            print!("{}", prompt(&session));
            io::stdout().flush().map_err(|error| error.to_string())?;
        }

        let Some(line) = repl_input.next_line()? else {
            if !repl_input.interactive() {
                break;
            }
            if confirm_exit(&mut session, &mut repl_input)? {
                break;
            }
            continue;
        };

        let input = line.trim();
        if input.is_empty() {
            continue;
        }

        if input.starts_with('.') {
            if handle_dot_command(input, &mut session, &mut output, &mut repl_input)? {
                break;
            }
            continue;
        }

        match session.execute_script(input, &BTreeMap::new()) {
            Ok(results) => print_results(&results, output),
            Err(error) => eprintln!("{error}"),
        }
    }

    Ok(())
}

fn open_initial_session(path: Option<PathBuf>) -> Result<Session, String> {
    match path {
        Some(path) if path.exists() => Session::open(path).map_err(|error| error.to_string()),
        Some(path) => {
            let mut session = Session::new_in_memory();
            session.save_as(path).map_err(|error| error.to_string())?;
            Ok(session)
        }
        None => Ok(Session::new_in_memory()),
    }
}

fn handle_dot_command(
    input: &str,
    session: &mut Session,
    output: &mut OutputFormat,
    repl_input: &mut ReplInput,
) -> Result<bool, String> {
    let mut parts = input.split_whitespace();
    match parts.next().unwrap_or_default() {
        ".help" => {
            println!(".help");
            println!(".quit");
            println!(".output table|json|ndjson");
            println!(".open <path.cupld>");
            println!(".save");
            println!(".saveas <path.cupld>");
            println!(".schema");
            println!(".indexes");
            println!(".constraints");
            println!(".stats");
            println!(".transactions");
            Ok(false)
        }
        ".quit" => confirm_exit(session, repl_input),
        ".output" => {
            *output = match parts.next() {
                Some("table") => OutputFormat::Table,
                Some("json") => OutputFormat::Json,
                Some("ndjson") => OutputFormat::Ndjson,
                _ => return Err("expected .output table|json|ndjson".to_owned()),
            };
            Ok(false)
        }
        ".open" => {
            let Some(path) = parts.next() else {
                return Err("expected .open <path.cupld>".to_owned());
            };
            if session.is_dirty() && !prompt_save(session, repl_input)? {
                return Ok(false);
            }
            *session = open_initial_session(Some(PathBuf::from(path)))?;
            Ok(false)
        }
        ".save" => {
            session.save().map_err(|error| error.to_string())?;
            Ok(false)
        }
        ".saveas" => {
            let Some(path) = parts.next() else {
                return Err("expected .saveas <path.cupld>".to_owned());
            };
            session.save_as(path).map_err(|error| error.to_string())?;
            Ok(false)
        }
        ".schema" => {
            let results = session
                .execute_script("SHOW SCHEMA", &BTreeMap::new())
                .map_err(|error| error.to_string())?;
            print_results(&results, *output);
            Ok(false)
        }
        ".indexes" => {
            let results = session
                .execute_script("SHOW INDEXES", &BTreeMap::new())
                .map_err(|error| error.to_string())?;
            print_results(&results, *output);
            Ok(false)
        }
        ".constraints" => {
            let results = session
                .execute_script("SHOW CONSTRAINTS", &BTreeMap::new())
                .map_err(|error| error.to_string())?;
            print_results(&results, *output);
            Ok(false)
        }
        ".stats" => {
            let results = session
                .execute_script("SHOW STATS", &BTreeMap::new())
                .map_err(|error| error.to_string())?;
            print_results(&results, *output);
            Ok(false)
        }
        ".transactions" => {
            let results = session
                .execute_script("SHOW TRANSACTIONS", &BTreeMap::new())
                .map_err(|error| error.to_string())?;
            print_results(&results, *output);
            Ok(false)
        }
        other => Err(format!("unknown dot-command {other}")),
    }
}

fn confirm_exit(session: &mut Session, repl_input: &mut ReplInput) -> Result<bool, String> {
    if session.is_dirty() {
        prompt_save(session, repl_input)
    } else {
        Ok(true)
    }
}

fn prompt_save(session: &mut Session, repl_input: &mut ReplInput) -> Result<bool, String> {
    print!("save changes? [y/N]: ");
    io::stdout().flush().map_err(|error| error.to_string())?;
    let Some(answer) = repl_input.next_line()? else {
        return Ok(true);
    };
    if !matches!(answer.trim(), "y" | "Y" | "yes" | "YES") {
        return Ok(true);
    }
    if session.path().is_some() {
        session.save().map_err(|error| error.to_string())?;
        Ok(true)
    } else {
        print!("save as: ");
        io::stdout().flush().map_err(|error| error.to_string())?;
        let Some(path) = repl_input.next_line()? else {
            return Ok(false);
        };
        let path = path.trim();
        if path.is_empty() {
            return Ok(false);
        }
        session.save_as(path).map_err(|error| error.to_string())?;
        Ok(true)
    }
}

fn prompt(session: &Session) -> String {
    let tx = session.transaction_info();
    let location = session
        .path()
        .map(path_label)
        .unwrap_or_else(|| "mem".to_owned());
    let state = if tx.active {
        Some(if tx.failed { "tx-failed" } else { "tx" })
    } else if session.is_dirty() {
        Some("dirty")
    } else {
        None
    };
    match state {
        Some(state) => format!("cupld[{location} {state}]> "),
        None => format!("cupld[{location}]> "),
    }
}

fn path_label(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("db")
        .to_owned()
}

fn parse_db_path(
    args: &[String],
    command: &str,
    allow_additional_args: bool,
) -> Result<PathBuf, String> {
    if args.is_empty() {
        return Err(format!(
            "expected --db <path.cupld|default> for `{command}` command"
        ));
    }
    if args[0] != "--db" {
        return Err(format!(
            "expected --db <path.cupld|default> for `{command}` command"
        ));
    }
    if args.len() < 2 {
        return Err(format!(
            "expected --db <path.cupld|default> for `{command}` command"
        ));
    }
    if !allow_additional_args && args.len() > 2 {
        return Err(format!(
            "`{command}` accepts only --db <path.cupld|default>\n\n{}",
            cli_usage_text()
        ));
    }

    parse_db_flag_value(&args[1])
}

fn parse_optional_db_path(args: &[String], command: &str) -> Result<PathBuf, String> {
    if args.is_empty() {
        return parse_db_flag_value("default");
    }
    if args.len() == 2 && args[0] == "--db" {
        return parse_db_flag_value(&args[1]);
    }
    if args.first().map(String::as_str) == Some("--db") {
        return Err(format!(
            "expected --db <path.cupld|default> for `{command}` command"
        ));
    }
    Err(format!(
        "`{command}` accepts only optional --db <path.cupld|default>\n\n{}",
        cli_usage_text()
    ))
}

fn parse_query(db_path: PathBuf, query_args: &[String]) -> Result<(PathBuf, String), String> {
    if query_args.is_empty() {
        let mut input = String::new();
        io::stdin()
            .read_to_string(&mut input)
            .map_err(|error| error.to_string())?;
        if input.trim().is_empty() {
            return Err(
                "expected query text, e.g. `cupld query --db <path.cupld|default> MATCH ...`"
                    .to_owned(),
            );
        }
        return Ok((db_path, input));
    }

    Ok((db_path, query_args.join(" ")))
}

fn ensure_subcommand_has_no_option(
    args: &[String],
    command: &str,
    option: &str,
) -> Result<(), String> {
    if args.iter().any(|arg| arg == option) {
        Err(invalid_subcommand_option(command, option))
    } else {
        Ok(())
    }
}

fn invalid_subcommand_option(command: &str, option: &str) -> String {
    format!(
        "error: `{}` is a top-level option and cannot be combined with `{}`\n\n{}",
        option,
        command,
        cli_usage_text()
    )
}

fn duplicate_top_level_option(option: &str) -> String {
    format!(
        "error: duplicate option `{}`\n\n{}",
        option,
        cli_usage_text()
    )
}

fn duplicate_top_level_db_path() -> String {
    format!(
        "error: provide exactly one database path via `<path.cupld>` or `--db <path.cupld|default>`\n\n{}",
        cli_usage_text()
    )
}

fn missing_top_level_db_path(option: &str) -> String {
    format!(
        "error: expected a database path after `{}`\n\n{}",
        option,
        cli_usage_text()
    )
}

fn missing_top_level_query(option: &str) -> String {
    format!(
        "error: expected query text after `{}`\n\n{}",
        option,
        cli_usage_text()
    )
}

fn is_registered_command(input: &str) -> bool {
    matches!(
        input,
        "query"
            | "context"
            | "schema"
            | "compact"
            | "check"
            | "upgrade"
            | "memory"
            | "sync"
            | "source"
            | "mcp"
            | "install"
    )
}

fn print_results(results: &[QueryResult], format: OutputFormat) {
    for result in results {
        if result.columns.is_empty() && result.rows.is_empty() {
            continue;
        }
        match format {
            OutputFormat::Table => print_table(result),
            OutputFormat::Json => println!("{}", result_as_json(result)),
            OutputFormat::Ndjson => {
                for line in result_as_ndjson(result) {
                    println!("{line}");
                }
            }
        }
    }
}

fn print_table(result: &QueryResult) {
    let mut widths = result
        .columns
        .iter()
        .map(|column| column.len())
        .collect::<Vec<_>>();
    let rows = result
        .rows
        .iter()
        .map(|row| row.iter().map(table_value).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    for row in &rows {
        for (index, value) in row.iter().enumerate() {
            widths[index] = widths[index].max(value.len());
        }
    }
    let header = result
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| format!("{column:width$}", width = widths[index]))
        .collect::<Vec<_>>()
        .join(" | ");
    println!("{header}");
    println!(
        "{}",
        widths
            .iter()
            .map(|width| "-".repeat(*width))
            .collect::<Vec<_>>()
            .join("-+-")
    );
    for row in rows {
        println!(
            "{}",
            row.iter()
                .enumerate()
                .map(|(index, value)| format!("{value:width$}", width = widths[index]))
                .collect::<Vec<_>>()
                .join(" | ")
        );
    }
}

fn table_value(value: &RuntimeValue) -> String {
    let rendered = value_string(value);
    if rendered.len() > 60 {
        format!("{}...", &rendered[..57])
    } else {
        rendered
    }
}

fn result_as_json(result: &QueryResult) -> String {
    json::stringify(&json::query_result_rows_to_json(result))
}

fn result_as_ndjson(result: &QueryResult) -> Vec<String> {
    result
        .rows
        .iter()
        .map(|row| json::stringify(&json::row_to_json_object(&result.columns, row)))
        .collect()
}

fn value_string(value: &RuntimeValue) -> String {
    match value {
        RuntimeValue::Null => "null".to_owned(),
        RuntimeValue::Bool(value) => value.to_string(),
        RuntimeValue::Int(value) => value.to_string(),
        RuntimeValue::Float(value) => value.to_string(),
        RuntimeValue::String(value) => value.clone(),
        RuntimeValue::Bytes(value) => format!("{value:?}"),
        RuntimeValue::Datetime(value) => format!("{value:?}"),
        RuntimeValue::List(values) => format!(
            "[{}]",
            values
                .iter()
                .map(value_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        RuntimeValue::Map(entries) => format!(
            "{{{}}}",
            entries
                .iter()
                .map(|(key, value)| format!("{key}: {}", value_string(value)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        RuntimeValue::Node(node_id) => format!("n{}", node_id.get()),
        RuntimeValue::Edge(edge_id) => format!("e{}", edge_id.get()),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CliCommand, InputEvent, LatestReleaseInfo, MemoryCommand, MemoryEvalConfig, OutputFormat,
        RELEASE_CHECK_INTERVAL_SECS, ReplInput, cap_results, cli_usage_text, format_error_json,
        is_newer_semver, parse_cli_command, parse_latest_release_response, parse_params_json,
        parse_semver, read_release_check_cache, result_as_json, result_as_ndjson,
        should_offer_skill_install_prompt, table_value, version_text, write_release_check_cache,
    };
    use crate::skill_install::{InstallCommand, InstallScope, SkillInstallTarget};
    use cupld::context::{ContextDirection, ContextRequest, ContextSeedRequest};
    use cupld::{MAX_TRAVERSAL_DEPTH, QueryResult, RuntimeValue, Value, json};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn default_alias_db_path() -> PathBuf {
        std::env::current_dir()
            .unwrap()
            .join(".cupld")
            .join("default.cupld")
    }

    fn temp_cache_path(prefix: &str) -> PathBuf {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "cupld_{prefix}_{}_{}.json",
            std::process::id(),
            timestamp
        ))
    }

    #[test]
    fn compares_release_semver_versions() {
        assert_eq!(
            parse_semver("v1.2.3"),
            Some(super::Semver {
                major: 1,
                minor: 2,
                patch: 3,
            })
        );
        assert!(is_newer_semver("v0.3.1", "0.3.0"));
        assert!(is_newer_semver("v0.4.0", "0.3.9"));
        assert!(is_newer_semver("1.0.0", "0.99.99"));
        assert!(!is_newer_semver("0.3.0", "0.3.0"));
        assert!(!is_newer_semver("0.2.9", "0.3.0"));
        assert!(!is_newer_semver("not-a-version", "0.3.0"));
    }

    #[test]
    fn parses_latest_release_response() {
        let release = parse_latest_release_response(
            r#"{"tag_name":"v0.4.0","html_url":"https://github.com/aeaston1/cupld/releases/tag/v0.4.0"}"#,
        )
        .unwrap();
        assert_eq!(release.version, "v0.4.0");
        assert_eq!(
            release.url,
            "https://github.com/aeaston1/cupld/releases/tag/v0.4.0"
        );
    }

    #[test]
    fn release_check_cache_reads_fresh_entries() {
        let path = temp_cache_path("release_cache_fresh");
        let latest = LatestReleaseInfo {
            version: "v9.0.0".to_owned(),
            url: "https://example.com/v9".to_owned(),
        };
        write_release_check_cache(&path, 1_000, Some(&latest)).unwrap();

        let cache =
            read_release_check_cache(&path, 1_000 + RELEASE_CHECK_INTERVAL_SECS - 1).unwrap();

        assert_eq!(cache.checked_at_unix, 1_000);
        assert_eq!(cache.latest, Some(latest));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn release_check_cache_ignores_stale_and_malformed_entries() {
        let stale = temp_cache_path("release_cache_stale");
        let latest = LatestReleaseInfo {
            version: "v9.0.0".to_owned(),
            url: "https://example.com/v9".to_owned(),
        };
        write_release_check_cache(&stale, 1_000, Some(&latest)).unwrap();
        assert!(read_release_check_cache(&stale, 1_000 + RELEASE_CHECK_INTERVAL_SECS).is_none());
        let _ = fs::remove_file(stale);

        let malformed = temp_cache_path("release_cache_malformed");
        fs::write(&malformed, "not json").unwrap();
        assert!(read_release_check_cache(&malformed, 1_000).is_none());
        let _ = fs::remove_file(malformed);
    }

    #[test]
    fn release_check_cache_can_record_failed_fetch() {
        let path = temp_cache_path("release_cache_failed_fetch");
        write_release_check_cache(&path, 2_000, None).unwrap();

        let cache = read_release_check_cache(&path, 2_100).unwrap();

        assert_eq!(cache.checked_at_unix, 2_000);
        assert_eq!(cache.latest, None);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn repl_input_drains_pasted_lines_into_pending_queue() {
        let mut input = ReplInput::from_events(
            true,
            [
                InputEvent::Line("CREATE LABEL Person;\n".to_owned()),
                InputEvent::Line("CREATE EDGE TYPE KNOWS;\n".to_owned()),
            ],
        );

        assert!(!input.has_pending());
        assert_eq!(
            input.next_line().unwrap(),
            Some("CREATE LABEL Person;\n".to_owned())
        );
        assert!(input.has_pending());
        assert_eq!(
            input.next_line().unwrap(),
            Some("CREATE EDGE TYPE KNOWS;\n".to_owned())
        );
    }

    #[test]
    fn renders_json_and_ndjson_rows() {
        let result = QueryResult {
            columns: vec!["name".to_owned(), "age".to_owned()],
            rows: vec![vec![
                RuntimeValue::String("Ada".to_owned()),
                RuntimeValue::Int(36),
            ]],
        };

        assert_eq!(result_as_json(&result), r#"[{"name":"Ada","age":36}]"#);
        assert_eq!(
            result_as_ndjson(&result),
            vec![r#"{"name":"Ada","age":36}"#]
        );
        let parsed = json::parse(&result_as_json(&result)).unwrap();
        assert_eq!(
            parsed.as_array().unwrap()[0]
                .get("name")
                .and_then(json::JsonValue::as_str),
            Some("Ada")
        );
    }

    #[test]
    fn truncates_long_table_values() {
        let value = RuntimeValue::String("x".repeat(120));
        assert!(table_value(&value).ends_with("..."));
    }

    #[test]
    fn parses_in_memory_repl() {
        let args = Vec::<String>::new();
        assert_eq!(parse_cli_command(&args), Ok(CliCommand::ReplMemory));
    }

    #[test]
    fn parses_repl_with_db_path() {
        let args = vec!["db.cupld".to_owned()];
        assert_eq!(
            parse_cli_command(&args),
            Ok(CliCommand::ReplWithDb(PathBuf::from("db.cupld")))
        );
    }

    #[test]
    fn parses_query_with_db_option() {
        let args = vec![
            "query".to_owned(),
            "--db".to_owned(),
            "db.cupld".to_owned(),
            "MATCH".to_owned(),
            "(n)".to_owned(),
        ];
        assert_eq!(
            parse_cli_command(&args),
            Ok(CliCommand::Query {
                db_path: PathBuf::from("db.cupld"),
                with_markdown: false,
                root_override: None,
                output: OutputFormat::Table,
                params_json: None,
                params_file: None,
                max_rows: 1_000,
                query_args: vec!["MATCH".into(), "(n)".into()],
            })
        );
    }

    #[test]
    fn parses_query_with_default_db_alias() {
        let args = vec![
            "query".to_owned(),
            "--db".to_owned(),
            "default".to_owned(),
            "MATCH".to_owned(),
            "(n)".to_owned(),
        ];
        assert_eq!(
            parse_cli_command(&args),
            Ok(CliCommand::Query {
                db_path: default_alias_db_path(),
                with_markdown: false,
                root_override: None,
                output: OutputFormat::Table,
                params_json: None,
                params_file: None,
                max_rows: 1_000,
                query_args: vec!["MATCH".into(), "(n)".into()],
            })
        );
    }

    #[test]
    fn parses_upgrade_with_default_db_alias() {
        let args = vec!["upgrade".to_owned()];
        assert_eq!(
            parse_cli_command(&args),
            Ok(CliCommand::Upgrade {
                db_path: default_alias_db_path(),
            })
        );

        let explicit = vec![
            "upgrade".to_owned(),
            "--db".to_owned(),
            "db.cupld".to_owned(),
        ];
        assert_eq!(
            parse_cli_command(&explicit),
            Ok(CliCommand::Upgrade {
                db_path: PathBuf::from("db.cupld"),
            })
        );
    }

    #[test]
    fn parses_query_with_machine_options() {
        let args = vec![
            "query".to_owned(),
            "--db".to_owned(),
            "db.cupld".to_owned(),
            "--output".to_owned(),
            "json".to_owned(),
            "--params-json".to_owned(),
            "{\"name\":\"Ada\"}".to_owned(),
            "--max-rows".to_owned(),
            "25".to_owned(),
            "MATCH".to_owned(),
            "(n)".to_owned(),
            "RETURN".to_owned(),
            "n".to_owned(),
        ];
        assert_eq!(
            parse_cli_command(&args),
            Ok(CliCommand::Query {
                db_path: PathBuf::from("db.cupld"),
                with_markdown: false,
                root_override: None,
                output: OutputFormat::Json,
                params_json: Some("{\"name\":\"Ada\"}".to_owned()),
                params_file: None,
                max_rows: 25,
                query_args: vec!["MATCH".into(), "(n)".into(), "RETURN".into(), "n".into()],
            })
        );
    }

    #[test]
    fn parses_context_command_defaults_and_repeated_flags() {
        assert_eq!(
            parse_cli_command(&[
                "context".to_owned(),
                "--db".to_owned(),
                "db.cupld".to_owned(),
                "--node".to_owned(),
                "42".to_owned(),
            ]),
            Ok(CliCommand::Context {
                output: OutputFormat::Json,
                request: ContextRequest {
                    db_path: PathBuf::from("db.cupld"),
                    nodes: vec![42],
                    paths: Vec::new(),
                    seeds: vec![ContextSeedRequest::Node(42)],
                    depth: 1,
                    direction: ContextDirection::Both,
                    edge_types: Vec::new(),
                    labels: Vec::new(),
                    max_nodes: 20,
                    max_edges: 100,
                },
            })
        );

        assert_eq!(
            parse_cli_command(&[
                "context".to_owned(),
                "--db".to_owned(),
                "db.cupld".to_owned(),
                "--output".to_owned(),
                "ndjson".to_owned(),
                "--node".to_owned(),
                "7".to_owned(),
                "--node".to_owned(),
                "8".to_owned(),
                "--path".to_owned(),
                "notes/a.md".to_owned(),
                "--path".to_owned(),
                "notes/b.md".to_owned(),
                "--depth".to_owned(),
                "3".to_owned(),
                "--direction".to_owned(),
                "out".to_owned(),
                "--edge-type".to_owned(),
                "LINKS_TO".to_owned(),
                "--edge-type".to_owned(),
                "MENTIONS".to_owned(),
                "--label".to_owned(),
                "MarkdownDocument".to_owned(),
                "--label".to_owned(),
                "Concept".to_owned(),
                "--max-nodes".to_owned(),
                "50".to_owned(),
                "--max-edges".to_owned(),
                "150".to_owned(),
            ]),
            Ok(CliCommand::Context {
                output: OutputFormat::Ndjson,
                request: ContextRequest {
                    db_path: PathBuf::from("db.cupld"),
                    nodes: vec![7, 8],
                    paths: vec!["notes/a.md".to_owned(), "notes/b.md".to_owned()],
                    seeds: vec![
                        ContextSeedRequest::Node(7),
                        ContextSeedRequest::Node(8),
                        ContextSeedRequest::Path("notes/a.md".to_owned()),
                        ContextSeedRequest::Path("notes/b.md".to_owned()),
                    ],
                    depth: 3,
                    direction: ContextDirection::Out,
                    edge_types: vec!["LINKS_TO".to_owned(), "MENTIONS".to_owned()],
                    labels: vec!["MarkdownDocument".to_owned(), "Concept".to_owned()],
                    max_nodes: 50,
                    max_edges: 150,
                },
            })
        );
    }

    #[test]
    fn parses_context_command_with_default_db_alias() {
        assert_eq!(
            parse_cli_command(&[
                "context".to_owned(),
                "--db".to_owned(),
                "default".to_owned(),
                "--path".to_owned(),
                "notes/a.md".to_owned(),
            ]),
            Ok(CliCommand::Context {
                output: OutputFormat::Json,
                request: ContextRequest {
                    db_path: default_alias_db_path(),
                    nodes: Vec::new(),
                    paths: vec!["notes/a.md".to_owned()],
                    seeds: vec![ContextSeedRequest::Path("notes/a.md".to_owned())],
                    depth: 1,
                    direction: ContextDirection::Both,
                    edge_types: Vec::new(),
                    labels: Vec::new(),
                    max_nodes: 20,
                    max_edges: 100,
                },
            })
        );
    }

    #[test]
    fn rejects_context_legacy_top_k_and_missing_seed() {
        assert_eq!(
            parse_cli_command(&[
                "context".to_owned(),
                "--db".to_owned(),
                "default".to_owned(),
                "--top-k".to_owned(),
                "20".to_owned(),
            ]),
            Err("context_legacy_top_k_removed".to_owned())
        );
        assert_eq!(
            parse_cli_command(&[
                "context".to_owned(),
                "--db".to_owned(),
                "default".to_owned(),
            ]),
            Err("context_seed_required".to_owned())
        );
    }

    #[test]
    fn rejects_context_invalid_values_and_depth_above_max() {
        assert_eq!(
            parse_cli_command(&[
                "context".to_owned(),
                "--db".to_owned(),
                "default".to_owned(),
                "--node".to_owned(),
                "abc".to_owned(),
            ]),
            Err("expected --node <id> for `context` command".to_owned())
        );
        assert_eq!(
            parse_cli_command(&[
                "context".to_owned(),
                "--db".to_owned(),
                "default".to_owned(),
                "--node".to_owned(),
                "1".to_owned(),
                "--direction".to_owned(),
                "sideways".to_owned(),
            ]),
            Err("context_invalid_direction: expected --direction <in|out|both>".to_owned())
        );
        assert_eq!(
            parse_cli_command(&[
                "context".to_owned(),
                "--db".to_owned(),
                "default".to_owned(),
                "--node".to_owned(),
                "1".to_owned(),
                "--depth".to_owned(),
                "wide".to_owned(),
            ]),
            Err("context_invalid_depth: expected --depth <n>".to_owned())
        );
        assert_eq!(
            parse_cli_command(&[
                "context".to_owned(),
                "--db".to_owned(),
                "default".to_owned(),
                "--node".to_owned(),
                "1".to_owned(),
                "--depth".to_owned(),
                (MAX_TRAVERSAL_DEPTH + 1).to_string(),
            ]),
            Err(format!(
                "context_depth_above_max: --depth must be <= {MAX_TRAVERSAL_DEPTH}"
            ))
        );
    }

    #[test]
    fn parses_eval_memory_command_forms() {
        assert_eq!(
            parse_cli_command(&[
                "eval".to_owned(),
                "memory".to_owned(),
                "--fixtures".to_owned(),
                "tests/fixtures/memory".to_owned(),
                "--output".to_owned(),
                "json".to_owned(),
            ]),
            Ok(CliCommand::EvalMemory(MemoryEvalConfig {
                fixtures: PathBuf::from("tests/fixtures/memory"),
                case: None,
                output: OutputFormat::Json,
                update_snapshots: false,
                ci: false,
            }))
        );

        assert_eq!(
            parse_cli_command(&[
                "eval".to_owned(),
                "memory".to_owned(),
                "--case".to_owned(),
                "aliases".to_owned(),
                "--output".to_owned(),
                "ndjson".to_owned(),
            ]),
            Ok(CliCommand::EvalMemory(MemoryEvalConfig {
                fixtures: PathBuf::from("tests/fixtures/memory"),
                case: Some("aliases".to_owned()),
                output: OutputFormat::Ndjson,
                update_snapshots: false,
                ci: false,
            }))
        );

        assert_eq!(
            parse_cli_command(&[
                "eval".to_owned(),
                "memory".to_owned(),
                "--update-snapshots".to_owned(),
            ]),
            Ok(CliCommand::EvalMemory(MemoryEvalConfig {
                fixtures: PathBuf::from("tests/fixtures/memory"),
                case: None,
                output: OutputFormat::Table,
                update_snapshots: true,
                ci: false,
            }))
        );

        assert_eq!(
            parse_cli_command(&["eval".to_owned(), "memory".to_owned(), "--ci".to_owned(),]),
            Ok(CliCommand::EvalMemory(MemoryEvalConfig {
                fixtures: PathBuf::from("tests/fixtures/memory"),
                case: None,
                output: OutputFormat::Table,
                update_snapshots: false,
                ci: true,
            }))
        );
    }

    #[test]
    fn rejects_invalid_eval_memory_command_forms() {
        assert!(matches!(
            parse_cli_command(&[
                "eval".to_owned(),
                "memory".to_owned(),
                "--unknown".to_owned(),
            ]),
            Err(error) if error.contains("unknown option `--unknown`")
        ));
        assert_eq!(
            parse_cli_command(&[
                "eval".to_owned(),
                "memory".to_owned(),
                "--fixtures".to_owned(),
            ]),
            Err("expected --fixtures <path> for `eval memory` command".to_owned())
        );
        assert_eq!(
            parse_cli_command(&[
                "eval".to_owned(),
                "memory".to_owned(),
                "--ci".to_owned(),
                "--update-snapshots".to_owned(),
            ]),
            Err("`eval memory --ci` must not be combined with `--update-snapshots`".to_owned())
        );
        assert!(matches!(
            parse_cli_command(&["eval".to_owned(), "context".to_owned()]),
            Err(error) if error.contains("unknown eval subcommand `context`")
        ));
        assert!(matches!(
            parse_cli_command(&[
                "eval".to_owned(),
                "memory".to_owned(),
                "--query".to_owned(),
                "MATCH (n) RETURN n".to_owned(),
            ]),
            Err(error) if error.contains("top-level option")
        ));
    }

    #[test]
    fn parses_memory_check_options() {
        let args = vec![
            "memory".to_owned(),
            "check".to_owned(),
            "--db".to_owned(),
            "default".to_owned(),
            "--root".to_owned(),
            "notes".to_owned(),
            "--output".to_owned(),
            "json".to_owned(),
            "--strict".to_owned(),
        ];
        assert_eq!(
            parse_cli_command(&args),
            Ok(CliCommand::Memory(MemoryCommand::Check {
                db_path: default_alias_db_path(),
                root_override: Some(PathBuf::from("notes")),
                output: OutputFormat::Json,
                strict: true,
            }))
        );
    }

    #[test]
    fn parses_memory_maintenance_commands() {
        assert_eq!(
            parse_cli_command(&[
                "memory".to_owned(),
                "find-stale".to_owned(),
                "--db".to_owned(),
                "db.cupld".to_owned(),
                "--root".to_owned(),
                "notes".to_owned(),
                "--output".to_owned(),
                "ndjson".to_owned(),
            ]),
            Ok(CliCommand::Memory(MemoryCommand::FindStale {
                db_path: PathBuf::from("db.cupld"),
                root_override: Some(PathBuf::from("notes")),
                output: OutputFormat::Ndjson,
            }))
        );
        assert_eq!(
            parse_cli_command(&[
                "memory".to_owned(),
                "find-orphans".to_owned(),
                "--db".to_owned(),
                "db.cupld".to_owned(),
                "--output".to_owned(),
                "json".to_owned(),
            ]),
            Ok(CliCommand::Memory(MemoryCommand::FindOrphans {
                db_path: PathBuf::from("db.cupld"),
                output: OutputFormat::Json,
            }))
        );
        assert_eq!(
            parse_cli_command(&[
                "memory".to_owned(),
                "reindex".to_owned(),
                "--db".to_owned(),
                "db.cupld".to_owned(),
            ]),
            Ok(CliCommand::Memory(MemoryCommand::Reindex {
                db_path: PathBuf::from("db.cupld"),
                output: OutputFormat::Table,
            }))
        );
    }

    #[test]
    fn parses_deferred_memory_commands() {
        assert_eq!(
            parse_cli_command(&["memory".to_owned(), "repair".to_owned()]),
            Ok(CliCommand::Memory(MemoryCommand::Deferred {
                subcommand: "repair".to_owned(),
            }))
        );
        assert_eq!(
            parse_cli_command(&["memory".to_owned(), "citation-audit".to_owned()]),
            Ok(CliCommand::Memory(MemoryCommand::Deferred {
                subcommand: "citation-audit".to_owned(),
            }))
        );
    }

    #[test]
    fn errors_for_unknown_memory_subcommands_and_unsupported_options() {
        assert!(matches!(
            parse_cli_command(&["memory".to_owned(), "wat".to_owned()]),
            Err(error) if error.contains("unknown memory subcommand `wat`")
        ));
        assert!(matches!(
            parse_cli_command(&[
                "memory".to_owned(),
                "find-orphans".to_owned(),
                "--db".to_owned(),
                "default".to_owned(),
                "--root".to_owned(),
                "notes".to_owned(),
            ]),
            Err(error) if error.contains("does not accept `--root`")
        ));
        assert!(matches!(
            parse_cli_command(&[
                "memory".to_owned(),
                "find-stale".to_owned(),
                "--db".to_owned(),
                "default".to_owned(),
                "--strict".to_owned(),
            ]),
            Err(error) if error.contains("does not accept `--strict`")
        ));
    }

    #[test]
    fn parses_params_json_into_runtime_values() {
        let params = parse_params_json(
            "{\"name\":\"Ada\",\"age\":36,\"active\":true,\"tags\":[\"a\",\"b\"],\"meta\":{\"team\":\"graph\"}}",
        )
        .unwrap();

        assert_eq!(params.get("name"), Some(&Value::String("Ada".to_owned())));
        assert_eq!(params.get("age"), Some(&Value::Int(36)));
        assert_eq!(params.get("active"), Some(&Value::Bool(true)));
        assert_eq!(
            params.get("tags"),
            Some(&Value::List(vec![
                Value::String("a".to_owned()),
                Value::String("b".to_owned())
            ]))
        );
        assert_eq!(
            params.get("meta"),
            Some(&Value::from(cupld::PropertyMap::from_pairs([(
                "team",
                Value::String("graph".to_owned())
            )])))
        );
    }

    #[test]
    fn caps_results_to_max_rows() {
        let input = vec![QueryResult {
            columns: vec!["value".to_owned()],
            rows: vec![
                vec![RuntimeValue::Int(1)],
                vec![RuntimeValue::Int(2)],
                vec![RuntimeValue::Int(3)],
            ],
        }];
        let capped = cap_results(&input, 2);
        assert_eq!(capped[0].rows.len(), 2);
    }

    #[test]
    fn formats_machine_error_json() {
        assert_eq!(
            format_error_json("constraint_unique_violation", "duplicate \"email\""),
            "{\"ok\":false,\"error\":{\"code\":\"constraint_unique_violation\",\"message\":\"duplicate \\\"email\\\"\"}}"
        );
    }

    #[test]
    fn errors_for_query_without_db_flag() {
        let args = vec!["query".to_owned(), "db.cupld".to_owned()];
        assert_eq!(
            parse_cli_command(&args),
            Err("expected --db <path.cupld|default> for `query` command".to_owned())
        );
    }

    #[test]
    fn query_does_not_accept_include_fs_graph() {
        let args = vec![
            "query".to_owned(),
            "--db".to_owned(),
            "default".to_owned(),
            "--include-fs-graph".to_owned(),
        ];
        assert!(matches!(
            parse_cli_command(&args),
            Err(error) if error.contains("unknown option `--include-fs-graph`")
        ));
    }

    #[test]
    fn parses_schema_with_default_db_alias() {
        let args = vec!["schema".to_owned(), "--db".to_owned(), "default".to_owned()];
        assert_eq!(
            parse_cli_command(&args),
            Ok(CliCommand::Schema {
                db_path: default_alias_db_path(),
            })
        );
    }

    #[test]
    fn parses_install_with_target_and_db() {
        let args = vec![
            "install".to_owned(),
            "--target".to_owned(),
            "codex".to_owned(),
            "--scope".to_owned(),
            "cwd".to_owned(),
            "--db".to_owned(),
            "db.cupld".to_owned(),
        ];
        assert_eq!(
            parse_cli_command(&args),
            Ok(CliCommand::Install(InstallCommand {
                target: Some(SkillInstallTarget::Codex),
                scope: Some(InstallScope::Cwd),
                path: None,
                db_path: Some(PathBuf::from("db.cupld")),
                root: None,
                force: false,
                yes: false,
                mcp: false,
                dry_run: false,
                print_only: false,
                mcp_server_name: None,
                mcp_command: None,
            }))
        );
    }

    #[test]
    fn parses_install_with_default_db_alias() {
        let args = vec![
            "install".to_owned(),
            "--target".to_owned(),
            "codex".to_owned(),
            "--scope".to_owned(),
            "cwd".to_owned(),
            "--db".to_owned(),
            "default".to_owned(),
        ];
        assert_eq!(
            parse_cli_command(&args),
            Ok(CliCommand::Install(InstallCommand {
                target: Some(SkillInstallTarget::Codex),
                scope: Some(InstallScope::Cwd),
                path: None,
                db_path: Some(default_alias_db_path()),
                root: None,
                force: false,
                yes: false,
                mcp: false,
                dry_run: false,
                print_only: false,
                mcp_server_name: None,
                mcp_command: None,
            }))
        );
    }

    #[test]
    fn parses_install_with_path_root_force_and_yes() {
        let args = vec![
            "install".to_owned(),
            "--path".to_owned(),
            "skills-root".to_owned(),
            "--db".to_owned(),
            "db.cupld".to_owned(),
            "--root".to_owned(),
            "notes".to_owned(),
            "--force".to_owned(),
            "--yes".to_owned(),
        ];
        assert_eq!(
            parse_cli_command(&args),
            Ok(CliCommand::Install(InstallCommand {
                target: None,
                scope: None,
                path: Some(PathBuf::from("skills-root")),
                db_path: Some(PathBuf::from("db.cupld")),
                root: Some(PathBuf::from("notes")),
                force: true,
                yes: true,
                mcp: false,
                dry_run: false,
                print_only: false,
                mcp_server_name: None,
                mcp_command: None,
            }))
        );
    }

    #[test]
    fn parses_install_mcp_flags() {
        let args = vec![
            "install".to_owned(),
            "--mcp".to_owned(),
            "--dry-run".to_owned(),
            "--print-only".to_owned(),
            "--target".to_owned(),
            "codex".to_owned(),
            "--scope".to_owned(),
            "cwd".to_owned(),
            "--mcp-server-name".to_owned(),
            "memory".to_owned(),
            "--mcp-command".to_owned(),
            "cupld".to_owned(),
        ];
        assert_eq!(
            parse_cli_command(&args),
            Ok(CliCommand::Install(InstallCommand {
                target: Some(SkillInstallTarget::Codex),
                scope: Some(InstallScope::Cwd),
                path: None,
                db_path: None,
                root: None,
                force: false,
                yes: false,
                mcp: true,
                dry_run: true,
                print_only: true,
                mcp_server_name: Some("memory".to_owned()),
                mcp_command: Some("cupld".to_owned()),
            }))
        );
    }

    #[test]
    fn errors_for_duplicate_install_mcp_flags() {
        assert_eq!(
            parse_cli_command(&["install".to_owned(), "--mcp".to_owned(), "--mcp".to_owned(),]),
            Err("duplicate option `--mcp`".to_owned())
        );
        assert_eq!(
            parse_cli_command(&[
                "install".to_owned(),
                "--mcp-server-name".to_owned(),
                "one".to_owned(),
                "--mcp-server-name".to_owned(),
                "two".to_owned(),
            ]),
            Err("duplicate option `--mcp-server-name`".to_owned())
        );
    }

    #[test]
    fn parses_top_level_db_option() {
        let args = vec!["--db".to_owned(), "db.cupld".to_owned()];
        assert_eq!(
            parse_cli_command(&args),
            Ok(CliCommand::ReplWithDb(PathBuf::from("db.cupld")))
        );
    }

    #[test]
    fn parses_top_level_default_db_alias() {
        let args = vec!["--db".to_owned(), "default".to_owned()];
        assert_eq!(
            parse_cli_command(&args),
            Ok(CliCommand::ReplWithDb(default_alias_db_path()))
        );
    }

    #[test]
    fn parses_top_level_visualise_with_positional_path() {
        let args = vec!["--visualise".to_owned(), "db.cupld".to_owned()];
        assert_eq!(
            parse_cli_command(&args),
            Ok(CliCommand::Visualise {
                db_path: PathBuf::from("db.cupld"),
                query: None,
            })
        );
    }

    #[test]
    fn parses_top_level_visualise_with_positional_path_before_option() {
        let args = vec!["db.cupld".to_owned(), "--visualise".to_owned()];
        assert_eq!(
            parse_cli_command(&args),
            Ok(CliCommand::Visualise {
                db_path: PathBuf::from("db.cupld"),
                query: None,
            })
        );
    }

    #[test]
    fn parses_top_level_visualise_with_db_flag_before_or_after() {
        let before = vec![
            "--visualise".to_owned(),
            "--db".to_owned(),
            "db.cupld".to_owned(),
        ];
        let after = vec![
            "--db".to_owned(),
            "db.cupld".to_owned(),
            "--visualise".to_owned(),
        ];

        assert_eq!(
            parse_cli_command(&before),
            Ok(CliCommand::Visualise {
                db_path: PathBuf::from("db.cupld"),
                query: None,
            })
        );
        assert_eq!(
            parse_cli_command(&after),
            Ok(CliCommand::Visualise {
                db_path: PathBuf::from("db.cupld"),
                query: None,
            })
        );
    }

    #[test]
    fn parses_top_level_visualise_with_query_flag() {
        let args = vec![
            "--visualise".to_owned(),
            "--db".to_owned(),
            "db.cupld".to_owned(),
            "--query".to_owned(),
            "MATCH (n) RETURN n LIMIT 5".to_owned(),
        ];
        assert_eq!(
            parse_cli_command(&args),
            Ok(CliCommand::Visualise {
                db_path: PathBuf::from("db.cupld"),
                query: Some("MATCH (n) RETURN n LIMIT 5".to_owned()),
            })
        );
    }

    #[test]
    fn parses_top_level_visualise_with_default_db_alias() {
        let args = vec![
            "--visualise".to_owned(),
            "--db".to_owned(),
            "default".to_owned(),
        ];
        assert_eq!(
            parse_cli_command(&args),
            Ok(CliCommand::Visualise {
                db_path: default_alias_db_path(),
                query: None,
            })
        );
    }

    #[test]
    fn parses_sync_markdown_with_default_db_alias() {
        let args = vec![
            "sync".to_owned(),
            "markdown".to_owned(),
            "--db".to_owned(),
            "default".to_owned(),
        ];
        assert_eq!(
            parse_cli_command(&args),
            Ok(CliCommand::SyncMarkdown {
                db_path: default_alias_db_path(),
                root_override: None,
                watch: false,
                poll_interval: std::time::Duration::from_millis(100),
                debounce: std::time::Duration::from_millis(200),
                batch_window: std::time::Duration::from_secs(2),
                idle_timeout: None,
                max_runs: None,
                include_fs_graph: false,
            })
        );
    }

    #[test]
    fn parses_sync_markdown_include_fs_graph() {
        let args = vec![
            "sync".to_owned(),
            "markdown".to_owned(),
            "--db".to_owned(),
            "default".to_owned(),
            "--include-fs-graph".to_owned(),
        ];
        assert_eq!(
            parse_cli_command(&args),
            Ok(CliCommand::SyncMarkdown {
                db_path: default_alias_db_path(),
                root_override: None,
                watch: false,
                poll_interval: std::time::Duration::from_millis(100),
                debounce: std::time::Duration::from_millis(200),
                batch_window: std::time::Duration::from_secs(2),
                idle_timeout: None,
                max_runs: None,
                include_fs_graph: true,
            })
        );
    }

    #[test]
    fn parses_sync_markdown_filesystem_graph_alias() {
        let args = vec![
            "sync".to_owned(),
            "markdown".to_owned(),
            "--db".to_owned(),
            "default".to_owned(),
            "--filesystem-graph".to_owned(),
        ];
        assert_eq!(
            parse_cli_command(&args),
            Ok(CliCommand::SyncMarkdown {
                db_path: default_alias_db_path(),
                root_override: None,
                watch: false,
                poll_interval: std::time::Duration::from_millis(100),
                debounce: std::time::Duration::from_millis(200),
                batch_window: std::time::Duration::from_secs(2),
                idle_timeout: None,
                max_runs: None,
                include_fs_graph: true,
            })
        );
    }

    #[test]
    fn errors_for_duplicate_sync_markdown_include_fs_graph() {
        let args = vec![
            "sync".to_owned(),
            "markdown".to_owned(),
            "--db".to_owned(),
            "default".to_owned(),
            "--include-fs-graph".to_owned(),
            "--include-fs-graph".to_owned(),
        ];
        assert_eq!(
            parse_cli_command(&args),
            Err("duplicate option `--include-fs-graph`".to_owned())
        );
    }

    #[test]
    fn parses_sync_markdown_include_fs_graph_with_watch_options() {
        let args = vec![
            "sync".to_owned(),
            "markdown".to_owned(),
            "--db".to_owned(),
            "default".to_owned(),
            "--root".to_owned(),
            "notes".to_owned(),
            "--include-fs-graph".to_owned(),
            "--watch".to_owned(),
            "--poll-ms".to_owned(),
            "10".to_owned(),
            "--debounce-ms".to_owned(),
            "20".to_owned(),
            "--batch-ms".to_owned(),
            "30".to_owned(),
            "--idle-ms".to_owned(),
            "40".to_owned(),
            "--max-runs".to_owned(),
            "2".to_owned(),
        ];
        assert_eq!(
            parse_cli_command(&args),
            Ok(CliCommand::SyncMarkdown {
                db_path: default_alias_db_path(),
                root_override: Some(PathBuf::from("notes")),
                watch: true,
                poll_interval: std::time::Duration::from_millis(10),
                debounce: std::time::Duration::from_millis(20),
                batch_window: std::time::Duration::from_millis(30),
                idle_timeout: Some(std::time::Duration::from_millis(40)),
                max_runs: Some(2),
                include_fs_graph: true,
            })
        );
    }

    #[test]
    fn parses_source_set_root_with_default_db_alias() {
        let args = vec![
            "source".to_owned(),
            "set-root".to_owned(),
            "--db".to_owned(),
            "default".to_owned(),
            "notes".to_owned(),
        ];
        assert_eq!(
            parse_cli_command(&args),
            Ok(CliCommand::SourceSetRoot {
                db_path: default_alias_db_path(),
                root: PathBuf::from("notes"),
            })
        );
    }

    #[test]
    fn errors_for_visualise_without_db_path() {
        let args = vec!["--visualise".to_owned()];
        assert!(matches!(
            parse_cli_command(&args),
            Err(error) if error.contains("`--visualise` requires a database path")
        ));
    }

    #[test]
    fn errors_for_duplicate_top_level_db_paths() {
        let args = vec![
            "--db".to_owned(),
            "first.cupld".to_owned(),
            "second.cupld".to_owned(),
        ];
        assert!(matches!(
            parse_cli_command(&args),
            Err(error) if error.contains("provide exactly one database path")
        ));
    }

    #[test]
    fn errors_for_query_flag_without_visualise() {
        let args = vec!["--query".to_owned(), "MATCH (n) RETURN n".to_owned()];
        assert!(matches!(
            parse_cli_command(&args),
            Err(error) if error.contains("`--query` requires `--visualise`")
        ));
    }

    #[test]
    fn errors_for_visualise_missing_query_text() {
        let args = vec![
            "--visualise".to_owned(),
            "db.cupld".to_owned(),
            "--query".to_owned(),
        ];
        assert!(matches!(
            parse_cli_command(&args),
            Err(error) if error.contains("expected query text after `--query`")
        ));
    }

    #[test]
    fn errors_for_visualise_with_query_subcommand() {
        let args = vec![
            "query".to_owned(),
            "--db".to_owned(),
            "db.cupld".to_owned(),
            "--query".to_owned(),
        ];
        assert!(matches!(
            parse_cli_command(&args),
            Err(error) if error.contains("top-level option")
        ));
    }

    #[test]
    fn parses_help_flags() {
        assert_eq!(
            parse_cli_command(&["help".to_owned()]),
            Ok(CliCommand::Help)
        );
        assert_eq!(parse_cli_command(&["-h".to_owned()]), Ok(CliCommand::Help));
        assert_eq!(
            parse_cli_command(&["--help".to_owned()]),
            Ok(CliCommand::Help)
        );
    }

    #[test]
    fn parses_version_flags() {
        assert_eq!(
            parse_cli_command(&["-v".to_owned()]),
            Ok(CliCommand::Version)
        );
        assert_eq!(
            parse_cli_command(&["--version".to_owned()]),
            Ok(CliCommand::Version)
        );
        assert_eq!(version_text(), concat!("cupld ", env!("CARGO_PKG_VERSION")));
    }

    #[test]
    fn help_text_includes_core_sections_and_flags() {
        let help = cli_usage_text();

        assert!(help.contains("Usage:"));
        assert!(help.contains("Commands:"));
        assert!(help.contains("REPL:"));
        assert!(help.contains("cupld --db <path.cupld|default>"));
        assert!(help.contains("[--visualise [--query <query>]]"));
        assert!(help.contains("cupld -v|--version"));
        assert!(help.contains("-v, --version"));
        assert!(help.contains("`default` maps to `./.cupld/default.cupld`."));
        assert!(help.contains(
            "Install the bundled cupld-md-memory SKILL.md and bootstrap local cupld memory."
        ));
        assert!(help.contains("Seed the scene with one read-only RETURN query."));
        assert!(help.contains("Run a query against --db using inline text or stdin."));
        assert!(help.contains("--include-fs-graph"));
        assert!(help.contains("Run .help inside the REPL for interactive commands."));
        assert_eq!(help.matches("Usage:").count(), 1);
        assert_eq!(help.matches("Commands:").count(), 1);
    }

    #[test]
    fn errors_for_help_with_extra_args() {
        let args = vec!["help".to_owned(), "extra".to_owned()];
        assert!(matches!(
            parse_cli_command(&args),
            Err(error) if error.contains("does not accept additional arguments")
        ));
    }

    #[test]
    fn errors_for_unknown_option() {
        let args = vec!["--wat".to_owned()];
        assert!(matches!(
            parse_cli_command(&args),
            Err(error) if error.contains("unknown option")
        ));
    }

    #[test]
    fn errors_for_version_with_extra_args() {
        let args = vec!["--version".to_owned(), "extra".to_owned()];
        assert!(matches!(
            parse_cli_command(&args),
            Err(error) if error.contains("does not accept additional arguments")
        ));
    }

    #[test]
    fn errors_for_schema_missing_db_flag() {
        let args = vec!["schema".to_owned()];
        assert_eq!(
            parse_cli_command(&args),
            Err("expected --db <path.cupld|default> for `schema` command".to_owned())
        );
    }

    #[test]
    fn errors_for_unknown_multi_token_command() {
        let args = vec!["foo".to_owned(), "bar".to_owned()];
        assert!(matches!(
            parse_cli_command(&args),
            Err(error) if error.contains("unknown command `foo`")
        ));
    }

    #[test]
    fn errors_for_install_with_invalid_target() {
        let args = vec![
            "install".to_owned(),
            "--target".to_owned(),
            "other".to_owned(),
        ];
        assert_eq!(
            parse_cli_command(&args),
            Err("expected --target <codex|claude|opencode> for `install`".to_owned())
        );
    }

    #[test]
    fn errors_for_install_with_invalid_scope() {
        let args = vec![
            "install".to_owned(),
            "--target".to_owned(),
            "codex".to_owned(),
            "--scope".to_owned(),
            "repo".to_owned(),
        ];
        assert_eq!(
            parse_cli_command(&args),
            Err("expected --scope <cwd|home> for `install`".to_owned())
        );
    }

    #[test]
    fn skill_prompt_only_runs_for_repl_commands() {
        assert!(should_offer_skill_install_prompt(&CliCommand::ReplMemory));
        assert!(should_offer_skill_install_prompt(&CliCommand::ReplWithDb(
            PathBuf::from("db.cupld")
        )));
        assert!(!should_offer_skill_install_prompt(&CliCommand::Query {
            db_path: PathBuf::from("db.cupld"),
            with_markdown: false,
            root_override: None,
            output: OutputFormat::Table,
            params_json: None,
            params_file: None,
            max_rows: 1_000,
            query_args: vec!["MATCH (n) RETURN n".into()],
        }));
        assert!(!should_offer_skill_install_prompt(&CliCommand::Install(
            InstallCommand {
                target: Some(SkillInstallTarget::Codex),
                scope: Some(InstallScope::Home),
                path: None,
                db_path: Some(PathBuf::from("db.cupld")),
                root: None,
                force: false,
                yes: false,
                mcp: false,
                dry_run: false,
                print_only: false,
                mcp_server_name: None,
                mcp_command: None,
            }
        )));
    }
}
