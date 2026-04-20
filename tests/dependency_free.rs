use std::fs;
use std::path::Path;

#[test]
fn cargo_manifest_has_no_dependency_sections() {
    let manifest = fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    for forbidden in [
        "[dependencies]",
        "[dev-dependencies]",
        "[build-dependencies]",
    ] {
        assert!(
            !manifest.contains(forbidden),
            "manifest unexpectedly contains {forbidden}"
        );
    }
}

#[test]
fn cargo_lock_contains_only_the_root_package() {
    let lockfile = fs::read_to_string(repo_root().join("Cargo.lock")).unwrap();
    let package_blocks = lockfile.match_indices("[[package]]").count();
    assert_eq!(
        package_blocks, 1,
        "lockfile should contain exactly one package"
    );
    assert!(lockfile.contains("name = \"cupld\""));
    assert!(!lockfile.contains("dependencies = ["));
}

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}
