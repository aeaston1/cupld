use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AtomicWriteOutcome {
    pub backup_path: Option<PathBuf>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct BackupFailures {
    pub backup: bool,
    pub temp_write: bool,
    pub rename: bool,
}

pub(crate) fn write_atomic_with_backup(
    path: &Path,
    contents: &str,
) -> Result<AtomicWriteOutcome, String> {
    write_atomic_with_backup_inner(path, contents, &BackupFailures::default())
}

pub(crate) fn write_atomic_with_backup_for_test(
    path: &Path,
    contents: &str,
    failures: &BackupFailures,
) -> Result<AtomicWriteOutcome, String> {
    write_atomic_with_backup_inner(path, contents, failures)
}

fn write_atomic_with_backup_inner(
    path: &Path,
    contents: &str,
    failures: &BackupFailures,
) -> Result<AtomicWriteOutcome, String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(io_error)?;
    }

    let backup_path = if path.exists() {
        let backup_path = next_backup_path(path)?;
        if failures.backup {
            return Err(format!(
                "failed to create backup at {}",
                backup_path.display()
            ));
        }
        fs::copy(path, &backup_path).map_err(io_error)?;
        Some(backup_path)
    } else {
        None
    };

    let temp_path = next_temp_path(path)?;
    if failures.temp_write {
        let _ = fs::remove_file(&temp_path);
        return Err(format!(
            "failed to write temporary config at {}",
            temp_path.display()
        ));
    }
    fs::write(&temp_path, contents).map_err(io_error)?;
    if failures.rename {
        let _ = fs::remove_file(&temp_path);
        return Err(format!("failed to replace config at {}", path.display()));
    }
    fs::rename(&temp_path, path).map_err(io_error)?;

    Ok(AtomicWriteOutcome { backup_path })
}

fn next_backup_path(path: &Path) -> Result<PathBuf, String> {
    let mut counter = 0usize;
    loop {
        let suffix = if counter == 0 {
            timestamp_suffix()
        } else {
            format!("{}-{counter}", timestamp_suffix())
        };
        let candidate = PathBuf::from(format!("{}.cupld-backup-{suffix}", path.display()));
        if !candidate.exists() {
            return Ok(candidate);
        }
        counter += 1;
    }
}

fn next_temp_path(path: &Path) -> Result<PathBuf, String> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(format!("invalid config path {}", path.display()))?;
    let mut counter = 0usize;
    loop {
        let candidate = parent.join(format!(
            ".{file_name}.cupld-tmp-{}-{}",
            std::process::id(),
            counter
        ));
        if !candidate.exists() {
            return Ok(candidate);
        }
        counter += 1;
    }
}

fn timestamp_suffix() -> String {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0));
    let seconds = elapsed.as_secs();
    let days = seconds / 86_400;
    let seconds_of_day = seconds % 86_400;
    let (year, month, day) = civil_from_days(days as i64);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    format!("{year:04}{month:02}{day:02}-{hour:02}{minute:02}{second:02}")
}

fn civil_from_days(days_since_unix_epoch: i64) -> (i64, u64, u64) {
    let z = days_since_unix_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if m <= 2 { 1 } else { 0 };
    (year, m as u64, d as u64)
}

fn io_error(error: std::io::Error) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use super::{BackupFailures, write_atomic_with_backup_for_test};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

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
                "cupld_mcp_backup_{prefix}_{}_{}_{}",
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
    fn backup_content_matches_original() {
        let temp = TempDir::new("content");
        let path = temp.path().join("config.toml");
        fs::write(&path, "old").unwrap();

        let outcome =
            write_atomic_with_backup_for_test(&path, "new", &BackupFailures::default()).unwrap();

        assert_eq!(fs::read_to_string(&path).unwrap(), "new");
        assert_eq!(
            fs::read_to_string(outcome.backup_path.unwrap()).unwrap(),
            "old"
        );
    }

    #[test]
    fn rename_failure_leaves_original_and_backup() {
        let temp = TempDir::new("rename");
        let path = temp.path().join("config.toml");
        fs::write(&path, "old").unwrap();
        let failures = BackupFailures {
            rename: true,
            ..BackupFailures::default()
        };

        let error = write_atomic_with_backup_for_test(&path, "new", &failures).unwrap_err();

        assert!(error.contains("failed to replace config"));
        assert_eq!(fs::read_to_string(&path).unwrap(), "old");
        let backups = fs::read_dir(temp.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().contains("cupld-backup"))
            .count();
        assert_eq!(backups, 1);
    }

    #[test]
    fn backup_creation_failure_stops_before_config_write() {
        let temp = TempDir::new("backup_fail");
        let path = temp.path().join("config.toml");
        fs::write(&path, "old").unwrap();
        let failures = BackupFailures {
            backup: true,
            ..BackupFailures::default()
        };

        let error = write_atomic_with_backup_for_test(&path, "new", &failures).unwrap_err();

        assert!(error.contains("failed to create backup"));
        assert_eq!(fs::read_to_string(&path).unwrap(), "old");
    }
}
