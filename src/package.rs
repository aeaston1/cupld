use std::fs;
use std::path::{Path, PathBuf};

use crate::json::{self, JsonNumber, JsonValue};

const PACKAGE_DIR: &str = ".cupld";
const CONFIG_FILENAME: &str = "config.toml";
const DEFAULT_DB_FILENAME: &str = "default.cupld";
const DEFAULT_MARKDOWN_DIR: &str = "data";
const PACKAGE_CONFIG_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackageError {
    code: &'static str,
    message: String,
}

impl PackageError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for PackageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for PackageError {}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PackageLayoutConfig {
    pub db_path: Option<PathBuf>,
    pub markdown_root: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackageConfig {
    pub version: u32,
    pub package: PackageLayoutConfig,
}

impl Default for PackageConfig {
    fn default() -> Self {
        Self {
            version: PACKAGE_CONFIG_VERSION,
            package: PackageLayoutConfig::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkspacePackage {
    workspace_root: PathBuf,
    package_dir: PathBuf,
    config_path: PathBuf,
    config: PackageConfig,
}

impl WorkspacePackage {
    pub fn discover_current() -> Result<Self, PackageError> {
        let cwd = std::env::current_dir()
            .map_err(|error| PackageError::new("package_io", error.to_string()))?;
        Self::discover_from(cwd)
    }

    pub fn discover_from(path: impl AsRef<Path>) -> Result<Self, PackageError> {
        let workspace_root = absolutize(path.as_ref())?;
        let package_dir = workspace_root.join(PACKAGE_DIR);
        let config_path = package_dir.join(CONFIG_FILENAME);
        let config = load_config(&config_path)?;
        Ok(Self {
            workspace_root,
            package_dir,
            config_path,
            config,
        })
    }

    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }

    pub fn package_dir(&self) -> &Path {
        &self.package_dir
    }

    pub fn owns_path(&self, path: &Path) -> bool {
        absolutize_from(path, &self.workspace_root)
            .ok()
            .and_then(|path| path.strip_prefix(&self.workspace_root).ok().map(|_| ()))
            .is_some()
    }

    pub fn config_path(&self) -> &Path {
        &self.config_path
    }

    pub fn config(&self) -> &PackageConfig {
        &self.config
    }

    pub fn configured_db_path(&self) -> Option<PathBuf> {
        self.config
            .package
            .db_path
            .as_deref()
            .map(|path| self.resolve_package_path(path))
    }

    pub fn configured_markdown_root(&self) -> Option<PathBuf> {
        self.config
            .package
            .markdown_root
            .as_deref()
            .map(|path| self.resolve_package_path(path))
    }

    pub fn default_db_path(&self) -> PathBuf {
        self.package_dir.join(DEFAULT_DB_FILENAME)
    }

    pub fn default_markdown_root(&self) -> PathBuf {
        self.package_dir.join(DEFAULT_MARKDOWN_DIR)
    }

    pub fn resolve_db_path(&self, override_path: Option<&Path>) -> PathBuf {
        match override_path {
            Some(path) => self.resolve_package_path(path),
            None => self
                .configured_db_path()
                .unwrap_or_else(|| self.default_db_path()),
        }
    }

    pub fn resolve_markdown_root(&self, override_path: Option<&Path>) -> PathBuf {
        match override_path {
            Some(path) => self.resolve_package_path(path),
            None => self
                .configured_markdown_root()
                .unwrap_or_else(|| self.default_markdown_root()),
        }
    }

    pub fn persist_package_config(
        &mut self,
        db_path: Option<&Path>,
        markdown_root: Option<&Path>,
    ) -> Result<(), PackageError> {
        if let Some(db_path) = db_path {
            self.config.package.db_path = Some(self.store_path(db_path)?);
        }
        if let Some(markdown_root) = markdown_root {
            self.config.package.markdown_root = Some(self.store_path(markdown_root)?);
        }
        self.write()
    }

    pub fn persist_markdown_root(&mut self, markdown_root: &Path) -> Result<(), PackageError> {
        self.persist_package_config(None, Some(markdown_root))
    }

    pub fn write(&self) -> Result<(), PackageError> {
        fs::create_dir_all(&self.package_dir)
            .map_err(|error| PackageError::new("package_io", error.to_string()))?;
        let contents = render_config(&self.config);
        fs::write(&self.config_path, contents)
            .map_err(|error| PackageError::new("package_io", error.to_string()))
    }

    fn resolve_package_path(&self, path: &Path) -> PathBuf {
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.workspace_root.join(path)
        }
    }

    fn store_path(&self, path: &Path) -> Result<PathBuf, PackageError> {
        let absolute = absolutize_from(path, &self.workspace_root)?;
        Ok(match absolute.strip_prefix(&self.workspace_root) {
            Ok(relative) => relative.to_path_buf(),
            Err(_) => absolute,
        })
    }
}

fn load_config(path: &Path) -> Result<PackageConfig, PackageError> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(PackageConfig::default());
        }
        Err(error) => return Err(PackageError::new("package_io", error.to_string())),
    };

    parse_config(&contents)
}

fn parse_config(input: &str) -> Result<PackageConfig, PackageError> {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Section {
        Root,
        Package,
    }

    let mut config = PackageConfig::default();
    let mut saw_version = false;
    let mut saw_package_table = false;
    let mut saw_db_path = false;
    let mut saw_markdown_root = false;
    let mut section = Section::Root;

    for (index, raw_line) in input.lines().enumerate() {
        let line_number = index + 1;
        let line = strip_trailing_comment(raw_line).trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with('[') {
            if !line.ends_with(']') {
                return Err(parse_error(line_number, "invalid table header"));
            }
            let table = line[1..line.len() - 1].trim();
            match table {
                "package" => {
                    if saw_package_table {
                        return Err(parse_error(line_number, "duplicate `[package]` table"));
                    }
                    saw_package_table = true;
                    section = Section::Package;
                }
                unknown => {
                    return Err(parse_error(
                        line_number,
                        format!("unknown table `{unknown}`"),
                    ));
                }
            }
            continue;
        }

        let Some((key, value)) = split_key_value(line) else {
            return Err(parse_error(line_number, "expected `key = value`"));
        };
        let parsed_value = json::parse(value).map_err(|error| {
            parse_error(line_number, format!("invalid value for `{key}`: {error}"))
        })?;

        match (section, key) {
            (Section::Root, "version") => {
                if saw_version {
                    return Err(parse_error(line_number, "duplicate `version`"));
                }
                saw_version = true;
                config.version = parse_version(&parsed_value).map_err(|message| {
                    parse_error(line_number, format!("invalid `version`: {message}"))
                })?;
            }
            (Section::Root, unknown) => {
                return Err(parse_error(
                    line_number,
                    format!("unknown top-level key `{unknown}`"),
                ));
            }
            (Section::Package, "db_path") => {
                if saw_db_path {
                    return Err(parse_error(line_number, "duplicate `db_path`"));
                }
                saw_db_path = true;
                config.package.db_path =
                    Some(parse_path_value(&parsed_value).map_err(|message| {
                        parse_error(line_number, format!("invalid `db_path`: {message}"))
                    })?);
            }
            (Section::Package, "markdown_root") => {
                if saw_markdown_root {
                    return Err(parse_error(line_number, "duplicate `markdown_root`"));
                }
                saw_markdown_root = true;
                config.package.markdown_root =
                    Some(parse_path_value(&parsed_value).map_err(|message| {
                        parse_error(line_number, format!("invalid `markdown_root`: {message}"))
                    })?);
            }
            (Section::Package, unknown) => {
                return Err(parse_error(
                    line_number,
                    format!("unknown package key `{unknown}`"),
                ));
            }
        }
    }

    if config.version > PACKAGE_CONFIG_VERSION {
        return Err(PackageError::new(
            "package_config_version",
            format!(
                "unsupported package config version {}; expected <= {}",
                config.version, PACKAGE_CONFIG_VERSION
            ),
        ));
    }
    if config.version < PACKAGE_CONFIG_VERSION {
        config.version = PACKAGE_CONFIG_VERSION;
    }
    Ok(config)
}

fn parse_error(line_number: usize, message: impl Into<String>) -> PackageError {
    PackageError::new(
        "package_config_parse",
        format!("line {line_number}: {}", message.into()),
    )
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

fn split_key_value(line: &str) -> Option<(&str, &str)> {
    let index = line.find('=')?;
    let key = line[..index].trim();
    let value = line[index + 1..].trim();
    (!key.is_empty() && !value.is_empty()).then_some((key, value))
}

fn parse_version(value: &JsonValue) -> Result<u32, &'static str> {
    match value {
        JsonValue::Number(JsonNumber::Int(value)) if *value >= 0 => {
            u32::try_from(*value).map_err(|_| "version is outside the supported range")
        }
        JsonValue::Number(JsonNumber::Unsigned(value)) => {
            u32::try_from(*value).map_err(|_| "version is outside the supported range")
        }
        _ => Err("version must be a non-negative integer"),
    }
}

fn parse_path_value(value: &JsonValue) -> Result<PathBuf, &'static str> {
    match value {
        JsonValue::String(path) => Ok(PathBuf::from(path)),
        _ => Err("path must be a quoted string"),
    }
}

fn render_config(config: &PackageConfig) -> String {
    let mut output = String::new();
    output.push_str("version = ");
    output.push_str(&config.version.to_string());
    output.push('\n');
    if config.package.db_path.is_some() || config.package.markdown_root.is_some() {
        output.push('\n');
        output.push_str("[package]\n");
        if let Some(path) = &config.package.db_path {
            output.push_str("db_path = ");
            json::write_quoted_string(&mut output, &path.display().to_string());
            output.push('\n');
        }
        if let Some(path) = &config.package.markdown_root {
            output.push_str("markdown_root = ");
            json::write_quoted_string(&mut output, &path.display().to_string());
            output.push('\n');
        }
    }
    output
}

fn absolutize(path: &Path) -> Result<PathBuf, PackageError> {
    absolutize_from(
        path,
        &std::env::current_dir()
            .map_err(|error| PackageError::new("package_path_resolution", error.to_string()))?,
    )
}

fn absolutize_from(path: &Path, base: &Path) -> Result<PathBuf, PackageError> {
    Ok(if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    })
}

#[cfg(test)]
mod tests {
    use super::{WorkspacePackage, parse_config, render_config};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static NEXT_TEMP_DIR_ID: AtomicUsize = AtomicUsize::new(1);

    fn temp_dir(prefix: &str) -> PathBuf {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let suffix = NEXT_TEMP_DIR_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "cupld_package_{prefix}_{}_{}_{}",
            std::process::id(),
            timestamp,
            suffix
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn package_defaults_without_config_use_standard_layout() {
        let root = temp_dir("defaults");
        let package = WorkspacePackage::discover_from(&root).unwrap();

        assert_eq!(
            package.default_db_path(),
            root.join(".cupld").join("default.cupld")
        );
        assert_eq!(
            package.default_markdown_root(),
            root.join(".cupld").join("data")
        );
        assert_eq!(package.resolve_db_path(None), package.default_db_path());
        assert_eq!(
            package.resolve_markdown_root(None),
            package.default_markdown_root()
        );
    }

    #[test]
    fn package_config_round_trips_relative_paths() {
        let root = temp_dir("roundtrip");
        let db_path = root.join(".cupld").join("graph.cupld");
        let markdown_root = root.join("notes");

        let mut package = WorkspacePackage::discover_from(&root).unwrap();
        package
            .persist_package_config(Some(&db_path), Some(&markdown_root))
            .unwrap();

        let reloaded = WorkspacePackage::discover_from(&root).unwrap();
        assert_eq!(reloaded.configured_db_path(), Some(db_path));
        assert_eq!(reloaded.configured_markdown_root(), Some(markdown_root));

        let config = fs::read_to_string(root.join(".cupld").join("config.toml")).unwrap();
        assert_eq!(
            config,
            "version = 1\n\n[package]\ndb_path = \".cupld/graph.cupld\"\nmarkdown_root = \"notes\"\n"
        );
    }

    #[test]
    fn package_config_version_is_migrated_forward_when_older() {
        let root = temp_dir("version");
        let package_dir = root.join(".cupld");
        fs::create_dir_all(&package_dir).unwrap();
        fs::write(
            package_dir.join("config.toml"),
            "version = 0\n[package]\ndb_path = \".cupld/legacy.cupld\"\n",
        )
        .unwrap();

        let package = WorkspacePackage::discover_from(&root).unwrap();
        assert_eq!(package.config().version, 1);
        assert_eq!(
            package.configured_db_path(),
            Some(root.join(".cupld").join("legacy.cupld"))
        );
    }

    #[test]
    fn package_parser_rejects_duplicate_and_unknown_keys() {
        let duplicate = parse_config("version = 1\nversion = 1\n").unwrap_err();
        assert_eq!(duplicate.code(), "package_config_parse");
        assert!(duplicate.message().contains("duplicate `version`"));

        let unknown = parse_config("[package]\nextra = \"nope\"\n").unwrap_err();
        assert!(unknown.message().contains("unknown package key `extra`"));
    }

    #[test]
    fn package_parser_accepts_comments_and_inline_comments() {
        let config = parse_config(
            "# header\nversion = 1 # inline\n\n[package]\ndb_path = \"notes.cupld\" # trailing\n",
        )
        .unwrap();
        assert_eq!(config.version, 1);
        assert_eq!(config.package.db_path, Some(PathBuf::from("notes.cupld")));
    }

    #[test]
    fn package_parser_rejects_unknown_tables_and_future_versions() {
        let table = parse_config("[other]\nvalue = 1\n").unwrap_err();
        assert!(table.message().contains("unknown table `other`"));

        let version = parse_config("version = 2\n").unwrap_err();
        assert_eq!(version.code(), "package_config_version");
    }

    #[test]
    fn package_writer_omits_empty_package_table() {
        let rendered = render_config(
            &WorkspacePackage::discover_from(temp_dir("render"))
                .unwrap()
                .config()
                .clone(),
        );
        assert_eq!(rendered, "version = 1\n");
    }

    #[test]
    fn package_resolves_explicit_absolute_and_relative_paths() {
        let root = temp_dir("resolve");
        let package = WorkspacePackage::discover_from(&root).unwrap();

        assert_eq!(
            package.resolve_db_path(Some(Path::new("custom.cupld"))),
            root.join("custom.cupld")
        );
        assert_eq!(
            package.resolve_markdown_root(Some(root.join("notes").as_path())),
            root.join("notes")
        );
    }
}
