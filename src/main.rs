use std::collections::{BTreeMap, VecDeque};
use std::env;
use std::io::{self, BufRead, IsTerminal, Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::mpsc::{self, Receiver};
use std::thread;

use cupld::{
    PropertyMap, QueryResult, RuntimeValue, Session, Value, configured_markdown_root,
    set_markdown_root, sync_markdown_root,
};
use skill_install::{InstallCommand, InstallScope, SkillInstallTarget};

mod skill_install;
mod visualise;

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
    let command = parse_cli_command(&args)?;
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
        CliCommand::Context {
            db_path,
            output,
            top_k,
        } => run_context(db_path, output, top_k),
        CliCommand::Schema { db_path } => run_schema(&db_path),
        CliCommand::Compact { db_path } => run_compact(db_path),
        CliCommand::Check { db_path } => run_check(db_path),
        CliCommand::SyncMarkdown {
            db_path,
            root_override,
        } => run_sync_markdown(db_path, root_override),
        CliCommand::SourceSetRoot { db_path, root } => run_source_set_root(db_path, root),
        CliCommand::Install(command) => skill_install::install(command),
    }
}

#[derive(Debug, PartialEq, Eq)]
enum CliCommand {
    Help,
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
        db_path: PathBuf,
        output: OutputFormat,
        top_k: usize,
    },
    Schema {
        db_path: PathBuf,
    },
    Compact {
        db_path: PathBuf,
    },
    Check {
        db_path: PathBuf,
    },
    SyncMarkdown {
        db_path: PathBuf,
        root_override: Option<PathBuf>,
    },
    SourceSetRoot {
        db_path: PathBuf,
        root: PathBuf,
    },
    Install(InstallCommand),
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
        Some("query") => parse_query_command(&args[1..]),
        Some("context") => parse_context_command(&args[1..]),
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
        Some("sync") => parse_sync_command(&args[1..]),
        Some("source") => parse_source_command(&args[1..]),
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
                    return Err("expected --db <path.cupld> for `query` command".to_owned());
                };
                if db_path.is_some() {
                    return Err(
                        "expected exactly one --db <path.cupld> for `query` command".to_owned()
                    );
                }
                db_path = Some(PathBuf::from(path));
                index += 2;
            }
            "--with-markdown" => {
                if with_markdown {
                    return Err("duplicate option `--with-markdown`".to_owned());
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
        return Err("expected --db <path.cupld> for `query` command".to_owned());
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
    let mut top_k = 20usize;
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--db" => {
                let Some(path) = args.get(index + 1) else {
                    return Err("expected --db <path.cupld> for `context` command".to_owned());
                };
                db_path = Some(PathBuf::from(path));
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
                let Some(value) = args.get(index + 1) else {
                    return Err("expected --top-k <n> for `context` command".to_owned());
                };
                top_k = value
                    .parse::<usize>()
                    .map_err(|_| "expected --top-k <n> for `context` command".to_owned())?;
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
        return Err("expected --db <path.cupld> for `context` command".to_owned());
    };
    Ok(CliCommand::Context {
        db_path,
        output,
        top_k,
    })
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
                "error: expected `sync markdown --db <path.cupld> [--root <path>]`\n\n{}",
                cli_usage_text()
            ));
        }
    }

    let mut db_path = None;
    let mut root_override = None;
    let mut index = 1;

    while index < args.len() {
        match args[index].as_str() {
            "--db" => {
                let Some(path) = args.get(index + 1) else {
                    return Err("expected --db <path.cupld> for `sync markdown` command".to_owned());
                };
                if db_path.is_some() {
                    return Err(
                        "expected exactly one --db <path.cupld> for `sync markdown` command"
                            .to_owned(),
                    );
                }
                db_path = Some(PathBuf::from(path));
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
            value => {
                return Err(format!(
                    "error: unexpected argument `{value}`\n\n{}",
                    cli_usage_text()
                ));
            }
        }
    }

    let Some(db_path) = db_path else {
        return Err("expected --db <path.cupld> for `sync markdown` command".to_owned());
    };

    Ok(CliCommand::SyncMarkdown {
        db_path,
        root_override,
    })
}

fn parse_source_command(args: &[String]) -> Result<CliCommand, String> {
    ensure_subcommand_has_no_option(args, "source", "--visualise")?;
    ensure_subcommand_has_no_option(args, "source", "--query")?;

    match args.first().map(String::as_str) {
        Some("set-root") => {}
        _ => {
            return Err(format!(
                "error: expected `source set-root --db <path.cupld> <path>`\n\n{}",
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
                        "expected --db <path.cupld> for `source set-root` command".to_owned()
                    );
                };
                if db_path.is_some() {
                    return Err(
                        "expected exactly one --db <path.cupld> for `source set-root` command"
                            .to_owned(),
                    );
                }
                db_path = Some(PathBuf::from(path));
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
        return Err("expected --db <path.cupld> for `source set-root` command".to_owned());
    };
    let Some(root) = root else {
        return Err("expected a root path for `source set-root`".to_owned());
    };

    Ok(CliCommand::SourceSetRoot { db_path, root })
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
                    return Err("expected --db <path.cupld> for `install`".to_owned());
                };
                if db_path.is_some() {
                    return Err("duplicate option `--db`".to_owned());
                }
                db_path = Some(PathBuf::from(value));
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
                db_path = Some(PathBuf::from(path));
                index += 2;
            }
            "-h" | "--help" | "help" => return Ok(CliCommand::Help),
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

fn cli_usage_text() -> &'static str {
    "cupld is a local graph database CLI and REPL.

Usage:
  cupld
  cupld <path.cupld>
  cupld --db <path.cupld>
  cupld --visualise <path.cupld>
  cupld <path.cupld> --visualise
  cupld --visualise --db <path.cupld>
  cupld --db <path.cupld> --visualise
  cupld --visualise --db <path.cupld> --query 'MATCH (n) RETURN n LIMIT 10'
  cupld query --db <path.cupld> [--with-markdown] [--root <path>] [--output <table|json|ndjson>] [--params-json <json> | --params-file <path>] [--max-rows <n>] [query]
  cupld context --db <path.cupld> [--top-k <n>] [--output <table|json|ndjson>]
  cupld schema --db <path.cupld>
  cupld compact --db <path.cupld>
  cupld check --db <path.cupld>
  cupld sync markdown --db <path.cupld> [--root <path>]
  cupld source set-root --db <path.cupld> <path>
  cupld install [--target <codex|claude|opencode> [--scope <cwd|home>] | --path <skills-root>] [--db <path.cupld>] [--root <path>] [--force] [--yes]
  cupld -h
  cupld --help
  cupld help

Commands:
  cupld                   Start an in-memory REPL session.
  cupld <path.cupld>      Open or create a file-backed REPL session.
  cupld --db <path>       Open a file-backed REPL session via a global flag.
  --visualise             Open the interactive scene viewer for --db.
  --query                 Seed the scene with one read-only RETURN query.
  query                   Run a query against --db using inline text or stdin.
  context                 Build compact context rows (top-k nodes) for agent prompts.
  --with-markdown         Overlay markdown documents into `query` before execution.
  --root                  Override the markdown root for `query` or `sync markdown`.
  --output                Select output mode for query/context: table, json, ndjson.
  --params-json           Provide named query parameters as a JSON object.
  --params-file           Read named query parameters from a JSON file.
  --max-rows              Hard cap result rows in non-interactive query mode.
  schema                  Print SHOW SCHEMA for --db.
  compact                 Rewrite --db and reset its WAL.
  check                   Validate --db and print recovery metadata.
  sync markdown           Materialize markdown documents into --db and persist them.
  source set-root         Persist the default markdown root in --db.
  install                 Install the bundled cupld-md-memory SKILL.md and bootstrap local cupld memory.
  -h, --help, help        Show this help text.

Examples:
  cupld
  cupld .cupld/default.cupld
  cupld --db .cupld/default.cupld
  cupld --visualise .cupld/default.cupld
  cupld --visualise --db .cupld/default.cupld --query 'MATCH (n:Person) RETURN n LIMIT 10'
  cupld --db .cupld/default.cupld --visualise
  cupld query --db .cupld/default.cupld --output json 'MATCH (n) RETURN n'
  cupld query --db .cupld/default.cupld --params-json '{\"name\":\"Ada\"}' 'MATCH (n:Person {name: $name}) RETURN n'
  cupld query --db .cupld/default.cupld --with-markdown --root notes 'MATCH (n) RETURN n'
  cupld context --db .cupld/default.cupld --top-k 25
  echo 'MATCH (n) RETURN n' | cupld query --db .cupld/default.cupld
  cupld schema --db .cupld/default.cupld
  cupld sync markdown --db .cupld/default.cupld
  cupld source set-root --db .cupld/default.cupld notes
  cupld install
  cupld install --target codex --scope home --db .cupld/default.cupld
  cupld install --target claude --scope cwd --db .cupld/default.cupld --root notes
  cupld install --path ~/.claude/skills --db .cupld/default.cupld --yes

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
    let params = load_params(params_json, params_file)?;
    let mut session = if with_markdown {
        open_query_session_with_markdown(&db_path, root_override.as_deref())?
    } else {
        Session::open(&db_path).map_err(|error| error.to_string())?
    };
    let results = session
        .execute_script(&query, &params)
        .map_err(|error| format_error_json(error.code(), error.message()))?;
    let limited = cap_results(&results, max_rows);
    print_results(&limited, output);
    Ok(())
}

fn run_context(db_path: PathBuf, output: OutputFormat, top_k: usize) -> Result<(), String> {
    let query = format!(
        "MATCH (n) RETURN id(n) AS node_id, labels(n) AS labels, n.name AS name, n.title AS title ORDER BY id(n) LIMIT {top_k}"
    );
    let mut session = Session::open(db_path).map_err(|error| error.to_string())?;
    let results = session
        .execute_script(&query, &BTreeMap::new())
        .map_err(|error| format_error_json(error.code(), error.message()))?;
    print_results(&results, output);
    Ok(())
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
    format!(
        "{{\"error\":{{\"code\":\"{}\",\"message\":\"{}\"}}}}",
        escape_json(code),
        escape_json(message)
    )
}

fn load_params(
    params_json: Option<&str>,
    params_file: Option<&Path>,
) -> Result<BTreeMap<String, Value>, String> {
    if params_json.is_some() && params_file.is_some() {
        return Err("`query` accepts either --params-json or --params-file, not both".to_owned());
    }
    if let Some(json) = params_json {
        return parse_params_json(json);
    }
    if let Some(path) = params_file {
        let input = std::fs::read_to_string(path).map_err(|error| error.to_string())?;
        return parse_params_json(&input);
    }
    Ok(BTreeMap::new())
}

fn parse_params_json(input: &str) -> Result<BTreeMap<String, Value>, String> {
    let mut parser = JsonParamParser::new(input);
    let value = parser.parse_value()?;
    parser.consume_whitespace();
    if !parser.is_done() {
        return Err("invalid params json: trailing characters".to_owned());
    }
    match value {
        Value::Map(map) => Ok(map.into_iter().collect()),
        _ => Err("params json must be an object mapping parameter names to values".to_owned()),
    }
}

struct JsonParamParser<'a> {
    input: &'a [u8],
    index: usize,
}

impl<'a> JsonParamParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input: input.as_bytes(),
            index: 0,
        }
    }

    fn is_done(&self) -> bool {
        self.index >= self.input.len()
    }

    fn consume_whitespace(&mut self) {
        while let Some(ch) = self.peek() {
            if ch.is_ascii_whitespace() {
                self.index += 1;
            } else {
                break;
            }
        }
    }

    fn peek(&self) -> Option<char> {
        self.input.get(self.index).map(|byte| *byte as char)
    }

    fn next(&mut self) -> Option<char> {
        let ch = self.peek()?;
        self.index += 1;
        Some(ch)
    }

    fn expect(&mut self, expected: char) -> Result<(), String> {
        match self.next() {
            Some(ch) if ch == expected => Ok(()),
            Some(ch) => Err(format!(
                "invalid params json: expected `{expected}`, found `{ch}`"
            )),
            None => Err(format!(
                "invalid params json: expected `{expected}`, found EOF"
            )),
        }
    }

    fn parse_value(&mut self) -> Result<Value, String> {
        self.consume_whitespace();
        match self.peek() {
            Some('{') => self.parse_object(),
            Some('[') => self.parse_array(),
            Some('"') => self.parse_string().map(Value::String),
            Some('t') => self.parse_keyword("true", Value::Bool(true)),
            Some('f') => self.parse_keyword("false", Value::Bool(false)),
            Some('n') => self.parse_keyword("null", Value::Null),
            Some('-' | '0'..='9') => self.parse_number(),
            Some(ch) => Err(format!("invalid params json: unexpected `{ch}`")),
            None => Err("invalid params json: unexpected EOF".to_owned()),
        }
    }

    fn parse_keyword(&mut self, keyword: &str, value: Value) -> Result<Value, String> {
        for expected in keyword.chars() {
            self.expect(expected)?;
        }
        Ok(value)
    }

    fn parse_string(&mut self) -> Result<String, String> {
        self.expect('"')?;
        let mut output = String::new();
        loop {
            let Some(ch) = self.next() else {
                return Err("invalid params json: unterminated string".to_owned());
            };
            match ch {
                '"' => return Ok(output),
                '\\' => {
                    let Some(escaped) = self.next() else {
                        return Err("invalid params json: incomplete escape".to_owned());
                    };
                    match escaped {
                        '"' | '\\' | '/' => output.push(escaped),
                        'b' => output.push('\u{0008}'),
                        'f' => output.push('\u{000C}'),
                        'n' => output.push('\n'),
                        'r' => output.push('\r'),
                        't' => output.push('\t'),
                        _ => {
                            return Err(format!(
                                "invalid params json: unsupported escape `\\{escaped}`"
                            ));
                        }
                    }
                }
                other => output.push(other),
            }
        }
    }

    fn parse_array(&mut self) -> Result<Value, String> {
        self.expect('[')?;
        self.consume_whitespace();
        let mut values = Vec::new();
        if matches!(self.peek(), Some(']')) {
            self.next();
            return Ok(Value::List(values));
        }
        loop {
            values.push(self.parse_value()?);
            self.consume_whitespace();
            match self.next() {
                Some(',') => {
                    self.consume_whitespace();
                }
                Some(']') => return Ok(Value::List(values)),
                Some(ch) => {
                    return Err(format!(
                        "invalid params json: expected `,` or `]`, found `{ch}`"
                    ));
                }
                None => return Err("invalid params json: unterminated array".to_owned()),
            }
        }
    }

    fn parse_object(&mut self) -> Result<Value, String> {
        self.expect('{')?;
        self.consume_whitespace();
        let mut map = Vec::new();
        if matches!(self.peek(), Some('}')) {
            self.next();
            return Ok(Value::from(PropertyMap::from_pairs(map)));
        }
        loop {
            self.consume_whitespace();
            let key = self.parse_string()?;
            self.consume_whitespace();
            self.expect(':')?;
            let value = self.parse_value()?;
            map.push((key, value));
            self.consume_whitespace();
            match self.next() {
                Some(',') => {
                    self.consume_whitespace();
                }
                Some('}') => return Ok(Value::from(PropertyMap::from_pairs(map))),
                Some(ch) => {
                    return Err(format!(
                        "invalid params json: expected `,` or `}}`, found `{ch}`"
                    ));
                }
                None => return Err("invalid params json: unterminated object".to_owned()),
            }
        }
    }

    fn parse_number(&mut self) -> Result<Value, String> {
        let start = self.index;
        if matches!(self.peek(), Some('-')) {
            self.index += 1;
        }
        while matches!(self.peek(), Some('0'..='9')) {
            self.index += 1;
        }
        let mut is_float = false;
        if matches!(self.peek(), Some('.')) {
            is_float = true;
            self.index += 1;
            while matches!(self.peek(), Some('0'..='9')) {
                self.index += 1;
            }
        }
        let slice = std::str::from_utf8(&self.input[start..self.index])
            .map_err(|_| "invalid params json: invalid number".to_owned())?;
        if is_float {
            slice
                .parse::<f64>()
                .map(Value::Float)
                .map_err(|_| format!("invalid params json: invalid float `{slice}`"))
        } else {
            slice
                .parse::<i64>()
                .map(Value::Int)
                .map_err(|_| format!("invalid params json: invalid integer `{slice}`"))
        }
    }
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

fn should_offer_skill_install_prompt(command: &CliCommand) -> bool {
    !install_prompt_disabled()
        && matches!(command, CliCommand::ReplMemory | CliCommand::ReplWithDb(_))
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
    let report = Session::check(&db_path).map_err(|error| error.to_string())?;
    println!(
        "ok db={} last_tx_id={} wal_records={} recovered_tail={}",
        db_path.display(),
        report.last_tx_id,
        report.wal_records,
        report.recovered_tail
    );
    Ok(())
}

fn run_sync_markdown(db_path: PathBuf, root_override: Option<PathBuf>) -> Result<(), String> {
    let mut session = open_initial_session(Some(db_path.clone()))?;
    let root = resolve_markdown_root(root_override.as_deref(), Some(&session))?;
    let mut engine = session.engine().clone();
    let report = sync_markdown_root(&mut engine, &root).map_err(|error| error.to_string())?;
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
    println!("markdown_root {}", root.display());
    Ok(())
}

fn open_query_session_with_markdown(
    db_path: &Path,
    root_override: Option<&Path>,
) -> Result<Session, String> {
    let session = Session::open(db_path).map_err(|error| error.to_string())?;
    let root = resolve_markdown_root(root_override, Some(&session))?;
    let mut engine = session.engine().clone();
    sync_markdown_root(&mut engine, &root).map_err(|error| error.to_string())?;
    Ok(Session::from_engine(engine))
}

fn resolve_markdown_root(
    root_override: Option<&Path>,
    session: Option<&Session>,
) -> Result<PathBuf, String> {
    if let Some(root) = root_override {
        return absolutize_path(root);
    }
    if let Some(session) = session
        && let Some(root) = configured_markdown_root(session.engine())
    {
        return Ok(root);
    }
    absolutize_path(Path::new(".cupld/data"))
}

fn absolutize_path(path: &Path) -> Result<PathBuf, String> {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()
            .map_err(|error| error.to_string())?
            .join(path)
    };
    if path.exists() {
        path.canonicalize().map_err(|error| error.to_string())
    } else {
        Ok(path)
    }
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
            "expected --db <path.cupld> for `{command}` command"
        ));
    }
    if args[0] != "--db" {
        return Err(format!(
            "expected --db <path.cupld> for `{command}` command"
        ));
    }
    if args.len() < 2 {
        return Err(format!(
            "expected --db <path.cupld> for `{command}` command"
        ));
    }
    if !allow_additional_args && args.len() > 2 {
        return Err(format!(
            "`{command}` accepts only --db <path.cupld>\n\n{}",
            cli_usage_text()
        ));
    }

    Ok(PathBuf::from(&args[1]))
}

fn parse_query(db_path: PathBuf, query_args: &[String]) -> Result<(PathBuf, String), String> {
    if query_args.is_empty() {
        let mut input = String::new();
        io::stdin()
            .read_to_string(&mut input)
            .map_err(|error| error.to_string())?;
        if input.trim().is_empty() {
            return Err(
                "expected query text, e.g. `cupld query --db <path.cupld> MATCH ...`".to_owned(),
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
        "error: provide exactly one database path via `<path.cupld>` or `--db <path.cupld>`\n\n{}",
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
            | "sync"
            | "source"
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
    let rows = result
        .rows
        .iter()
        .map(|row| row_as_json(&result.columns, row))
        .collect::<Vec<_>>()
        .join(",");
    format!("[{rows}]")
}

fn result_as_ndjson(result: &QueryResult) -> Vec<String> {
    result
        .rows
        .iter()
        .map(|row| row_as_json(&result.columns, row))
        .collect()
}

fn row_as_json(columns: &[String], row: &[RuntimeValue]) -> String {
    let fields = columns
        .iter()
        .zip(row.iter())
        .map(|(column, value)| format!("\"{}\":{}", escape_json(column), value_as_json(value)))
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{fields}}}")
}

fn value_as_json(value: &RuntimeValue) -> String {
    match value {
        RuntimeValue::Null => "null".to_owned(),
        RuntimeValue::Bool(value) => value.to_string(),
        RuntimeValue::Int(value) => value.to_string(),
        RuntimeValue::Float(value) => value.to_string(),
        RuntimeValue::String(value) => format!("\"{}\"", escape_json(value)),
        RuntimeValue::Bytes(value) => format!("\"{}\"", escape_json(&format!("{value:?}"))),
        RuntimeValue::Datetime(value) => format!("\"{}\"", escape_json(&format!("{value:?}"))),
        RuntimeValue::List(values) => format!(
            "[{}]",
            values
                .iter()
                .map(value_as_json)
                .collect::<Vec<_>>()
                .join(",")
        ),
        RuntimeValue::Map(entries) => format!(
            "{{{}}}",
            entries
                .iter()
                .map(|(key, value)| format!("\"{}\":{}", escape_json(key), value_as_json(value)))
                .collect::<Vec<_>>()
                .join(",")
        ),
        RuntimeValue::Node(node_id) => format!("\"n{}\"", node_id.get()),
        RuntimeValue::Edge(edge_id) => format!("\"e{}\"", edge_id.get()),
    }
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

fn escape_json(input: &str) -> String {
    input
        .chars()
        .flat_map(|ch| match ch {
            '"' => ['\\', '"'].into_iter().collect::<Vec<_>>(),
            '\\' => ['\\', '\\'].into_iter().collect(),
            '\n' => ['\\', 'n'].into_iter().collect(),
            '\r' => ['\\', 'r'].into_iter().collect(),
            '\t' => ['\\', 't'].into_iter().collect(),
            other => [other].into_iter().collect(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        CliCommand, InputEvent, OutputFormat, ReplInput, cap_results, cli_usage_text,
        format_error_json, parse_cli_command, parse_params_json, result_as_json, result_as_ndjson,
        should_offer_skill_install_prompt, table_value,
    };
    use crate::skill_install::{InstallCommand, InstallScope, SkillInstallTarget};
    use cupld::{QueryResult, RuntimeValue, Value};
    use std::path::PathBuf;

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
    fn parses_context_command_defaults_and_overrides() {
        assert_eq!(
            parse_cli_command(&[
                "context".to_owned(),
                "--db".to_owned(),
                "db.cupld".to_owned(),
            ]),
            Ok(CliCommand::Context {
                db_path: PathBuf::from("db.cupld"),
                output: OutputFormat::Json,
                top_k: 20,
            })
        );

        assert_eq!(
            parse_cli_command(&[
                "context".to_owned(),
                "--db".to_owned(),
                "db.cupld".to_owned(),
                "--output".to_owned(),
                "ndjson".to_owned(),
                "--top-k".to_owned(),
                "7".to_owned(),
            ]),
            Ok(CliCommand::Context {
                db_path: PathBuf::from("db.cupld"),
                output: OutputFormat::Ndjson,
                top_k: 7,
            })
        );
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
            "{\"error\":{\"code\":\"constraint_unique_violation\",\"message\":\"duplicate \\\"email\\\"\"}}"
        );
    }

    #[test]
    fn errors_for_query_without_db_flag() {
        let args = vec!["query".to_owned(), "db.cupld".to_owned()];
        assert_eq!(
            parse_cli_command(&args),
            Err("expected --db <path.cupld> for `query` command".to_owned())
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
            }))
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
    fn help_text_includes_sections_commands_and_examples() {
        let help = cli_usage_text();

        assert!(help.contains("Usage:"));
        assert!(help.contains("Commands:"));
        assert!(help.contains("Examples:"));
        assert!(help.contains("REPL:"));
        assert!(help.contains("cupld --db <path.cupld>"));
        assert!(help.contains("cupld --visualise <path.cupld>"));
        assert!(help.contains("cupld --db <path.cupld> --visualise"));
        assert!(help.contains("cupld --visualise --db <path.cupld> --query"));
        assert!(help.contains("cupld install [--target <codex|claude|opencode> [--scope <cwd|home>] | --path <skills-root>] [--db <path.cupld>] [--root <path>] [--force] [--yes]"));
        assert!(help.contains("Open the interactive scene viewer for --db."));
        assert!(help.contains(
            "Install the bundled cupld-md-memory SKILL.md and bootstrap local cupld memory."
        ));
        assert!(help.contains("Seed the scene with one read-only RETURN query."));
        assert!(help.contains("Run a query against --db using inline text or stdin."));
        assert!(help.contains("echo 'MATCH (n) RETURN n' | cupld query --db .cupld/default.cupld"));
        assert!(help.contains("Run .help inside the REPL for interactive commands."));
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
        let args = vec!["--version".to_owned()];
        assert!(matches!(
            parse_cli_command(&args),
            Err(error) if error.contains("unknown option")
        ));
    }

    #[test]
    fn errors_for_schema_missing_db_flag() {
        let args = vec!["schema".to_owned()];
        assert_eq!(
            parse_cli_command(&args),
            Err("expected --db <path.cupld> for `schema` command".to_owned())
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
            }
        )));
    }
}
