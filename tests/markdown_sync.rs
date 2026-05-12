mod support;

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use cupld::{
    MarkdownWatchOptions, RuntimeValue, Session, configured_markdown_root, set_markdown_root,
    sync_markdown_root, watch_markdown_root,
};

use support::{TestDb, run};

static NEXT_TEMP_DIR_ID: AtomicUsize = AtomicUsize::new(1);

#[test]
fn synced_markdown_persists_in_cupld_file() {
    let db = TestDb::new("markdown_persist");
    let root = temp_dir("markdown_persist");
    fs::create_dir_all(&root).unwrap();
    fs::write(
        root.join("note.md"),
        "---\n\
title: Synced Title\n\
tags: [rust]\n\
---\n\
Body\n",
    )
    .unwrap();
    fs::write(root.join("plain.md"), "# Plain Title\nPlain body").unwrap();

    sync_root_into_db(db.path(), &root);

    let mut reopened = db.open();
    let result = run(
        &mut reopened,
        "MATCH (d:MarkdownDocument)
         RETURN d.`src.path`, d.`md.title`, d.`md.has_frontmatter`, d.`src.status`
         ORDER BY d.`src.path`",
    );
    assert_eq!(
        result.rows,
        vec![
            vec![
                RuntimeValue::String("note.md".to_owned()),
                RuntimeValue::String("Synced Title".to_owned()),
                RuntimeValue::Bool(true),
                RuntimeValue::String("current".to_owned()),
            ],
            vec![
                RuntimeValue::String("plain.md".to_owned()),
                RuntimeValue::String("Plain Title".to_owned()),
                RuntimeValue::Bool(false),
                RuntimeValue::String("current".to_owned()),
            ],
        ]
    );

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn syncs_frontmatter_relationships_with_edge_metadata() {
    let db = TestDb::new("markdown_frontmatter_edges");
    let root = temp_dir("markdown_frontmatter_edges");
    fs::create_dir_all(&root).unwrap();
    fs::write(
        root.join("note.md"),
        r#"---
related: [[other]]
parent: [[map]]
links:
  - misc
---
Body with [[other]] and [deep](other.md#intro) and [misc](misc.md)
"#,
    )
    .unwrap();
    fs::write(root.join("other.md"), "# Other").unwrap();
    fs::write(root.join("map.md"), "# Map").unwrap();
    fs::write(root.join("misc.md"), "# Misc").unwrap();

    sync_root_into_db(db.path(), &root);

    let mut reopened = db.open();
    let result = run(
        &mut reopened,
        "MATCH (:MarkdownDocument {`src.path`: 'note.md'})-[e:MD_LINKS_TO]->(d:MarkdownDocument)
         RETURN d.`src.path`, e.`md.link_target`, e.`md.link_targets`, e.`md.link_sources`, e.`md.link_rels`
         ORDER BY d.`src.path`",
    );
    assert_eq!(
        result.rows,
        vec![
            vec![
                RuntimeValue::String("map.md".to_owned()),
                RuntimeValue::String("map".to_owned()),
                RuntimeValue::List(vec![RuntimeValue::String("map".to_owned())]),
                RuntimeValue::List(vec![RuntimeValue::String("frontmatter".to_owned())]),
                RuntimeValue::List(vec![RuntimeValue::String("up".to_owned())]),
            ],
            vec![
                RuntimeValue::String("misc.md".to_owned()),
                RuntimeValue::String("misc".to_owned()),
                RuntimeValue::List(vec![
                    RuntimeValue::String("misc".to_owned()),
                    RuntimeValue::String("misc.md".to_owned()),
                ]),
                RuntimeValue::List(vec![
                    RuntimeValue::String("frontmatter".to_owned()),
                    RuntimeValue::String("body".to_owned()),
                ]),
                RuntimeValue::List(vec![RuntimeValue::String("link".to_owned())]),
            ],
            vec![
                RuntimeValue::String("other.md".to_owned()),
                RuntimeValue::String("other".to_owned()),
                RuntimeValue::List(vec![
                    RuntimeValue::String("other".to_owned()),
                    RuntimeValue::String("other.md#intro".to_owned()),
                ]),
                RuntimeValue::List(vec![
                    RuntimeValue::String("frontmatter".to_owned()),
                    RuntimeValue::String("body".to_owned()),
                ]),
                RuntimeValue::List(vec![RuntimeValue::String("related".to_owned())]),
            ],
        ]
    );

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn syncs_document_directory_and_parent_directory_edges_with_metadata() {
    let db = TestDb::new("markdown_structural_edges");
    let root = temp_dir("markdown_structural_edges");
    fs::create_dir_all(root.join("guides/api")).unwrap();
    fs::write(root.join("note.md"), "# Root").unwrap();
    fs::write(root.join("guides/index.md"), "# Guides").unwrap();
    fs::write(root.join("guides/api/ref.md"), "# Reference").unwrap();
    let root_string = root
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .replace('\\', "/");

    sync_root_into_db(db.path(), &root);
    sync_root_into_db(db.path(), &root);

    let mut reopened = db.open();
    let in_directory = run(
        &mut reopened,
        "MATCH (d:MarkdownDocument)-[e:MD_IN_DIRECTORY]->(dir:MarkdownDirectory)
         RETURN d.`src.path`, dir.`src.path`, e.`src.connector`, e.`src.kind`, e.`src.root`, e.`src.status`, e.`md.edge_source`, e.`md.edge_weight`
         ORDER BY d.`src.path`",
    );
    assert_eq!(
        in_directory.rows,
        vec![
            vec![
                RuntimeValue::String("guides/api/ref.md".to_owned()),
                RuntimeValue::String("guides/api".to_owned()),
                RuntimeValue::String("markdown".to_owned()),
                RuntimeValue::String("structural_edge".to_owned()),
                RuntimeValue::String(root_string.clone()),
                RuntimeValue::String("current".to_owned()),
                RuntimeValue::String("filesystem".to_owned()),
                RuntimeValue::Float(0.25),
            ],
            vec![
                RuntimeValue::String("guides/index.md".to_owned()),
                RuntimeValue::String("guides".to_owned()),
                RuntimeValue::String("markdown".to_owned()),
                RuntimeValue::String("structural_edge".to_owned()),
                RuntimeValue::String(root_string.clone()),
                RuntimeValue::String("current".to_owned()),
                RuntimeValue::String("filesystem".to_owned()),
                RuntimeValue::Float(0.25),
            ],
            vec![
                RuntimeValue::String("note.md".to_owned()),
                RuntimeValue::String(String::new()),
                RuntimeValue::String("markdown".to_owned()),
                RuntimeValue::String("structural_edge".to_owned()),
                RuntimeValue::String(root_string),
                RuntimeValue::String("current".to_owned()),
                RuntimeValue::String("filesystem".to_owned()),
                RuntimeValue::Float(0.25),
            ],
        ]
    );

    let parent_edges = run(
        &mut reopened,
        "MATCH (child:MarkdownDirectory)-[e:MD_PARENT_DIRECTORY]->(parent:MarkdownDirectory)
         RETURN child.`src.path`, parent.`src.path`, e.`src.kind`, e.`md.edge_source`, e.`md.edge_weight`
         ORDER BY child.`src.path`",
    );
    assert_eq!(
        parent_edges.rows,
        vec![
            vec![
                RuntimeValue::String("guides".to_owned()),
                RuntimeValue::String(String::new()),
                RuntimeValue::String("structural_edge".to_owned()),
                RuntimeValue::String("filesystem".to_owned()),
                RuntimeValue::Float(0.25),
            ],
            vec![
                RuntimeValue::String("guides/api".to_owned()),
                RuntimeValue::String("guides".to_owned()),
                RuntimeValue::String("structural_edge".to_owned()),
                RuntimeValue::String("filesystem".to_owned()),
                RuntimeValue::Float(0.25),
            ],
        ]
    );

    let root_parent_edges = run(
        &mut reopened,
        "MATCH (root:MarkdownDirectory {`src.path`: ''})-[e:MD_PARENT_DIRECTORY]->(:MarkdownDirectory)
         RETURN count(e)",
    );
    assert_eq!(root_parent_edges.rows, vec![vec![RuntimeValue::Int(0)]]);

    let structural_counts = run(
        &mut reopened,
        "MATCH (d:MarkdownDocument)-[e:MD_IN_DIRECTORY]->(dir:MarkdownDirectory)
         RETURN count(e)",
    );
    assert_eq!(structural_counts.rows, vec![vec![RuntimeValue::Int(3)]]);
    let parent_counts = run(
        &mut reopened,
        "MATCH (child:MarkdownDirectory)-[e:MD_PARENT_DIRECTORY]->(parent:MarkdownDirectory)
         RETURN count(e)",
    );
    assert_eq!(parent_counts.rows, vec![vec![RuntimeValue::Int(2)]]);

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn file_moves_do_not_leave_stale_directory_edges() {
    let db = TestDb::new("markdown_file_move_structural_edges");
    let root = temp_dir("markdown_file_move_structural_edges");
    fs::create_dir_all(root.join("old")).unwrap();
    fs::create_dir_all(root.join("new")).unwrap();
    fs::write(root.join("old/note.md"), "# Note").unwrap();

    sync_root_into_db(db.path(), &root);

    fs::rename(root.join("old/note.md"), root.join("new/note.md")).unwrap();
    sync_root_into_db(db.path(), &root);

    let mut reopened = db.open();
    let moved = run(
        &mut reopened,
        "MATCH (:MarkdownDocument {`src.path`: 'new/note.md'})-[e:MD_IN_DIRECTORY]->(dir:MarkdownDirectory)
         RETURN dir.`src.path`, e.`src.status`",
    );
    assert_eq!(
        moved.rows,
        vec![vec![
            RuntimeValue::String("new".to_owned()),
            RuntimeValue::String("current".to_owned()),
        ]]
    );

    let stale = run(
        &mut reopened,
        "MATCH (:MarkdownDocument {`src.path`: 'old/note.md'})-[e:MD_IN_DIRECTORY]->(:MarkdownDirectory)
         RETURN count(e)",
    );
    assert_eq!(stale.rows, vec![vec![RuntimeValue::Int(0)]]);

    let old_status = run(
        &mut reopened,
        "MATCH (d:MarkdownDocument {`src.path`: 'old/note.md'})
         RETURN d.`src.status`",
    );
    assert_eq!(
        old_status.rows,
        vec![vec![RuntimeValue::String("missing".to_owned())]]
    );

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn folder_deletion_tombstones_directories_and_preserves_manual_edges() {
    let db = TestDb::new("markdown_folder_delete_structural_edges");
    let root = temp_dir("markdown_folder_delete_structural_edges");
    fs::create_dir_all(root.join("docs/child")).unwrap();
    fs::write(root.join("docs/child/note.md"), "# Note").unwrap();

    sync_root_into_db(db.path(), &root);

    let mut session = db.open();
    run(
        &mut session,
        "MATCH (dir:MarkdownDirectory {`src.path`: 'docs/child'})
         CREATE (:Person {name: 'Ada'})-[:MD_PARENT_DIRECTORY {note: 'manual'}]->(dir)",
    );
    drop(session);

    fs::remove_dir_all(root.join("docs/child")).unwrap();
    sync_root_into_db(db.path(), &root);

    let mut reopened = db.open();
    let status = run(
        &mut reopened,
        "MATCH (dir:MarkdownDirectory {`src.path`: 'docs/child'})
         RETURN dir.`src.status`",
    );
    assert_eq!(
        status.rows,
        vec![vec![RuntimeValue::String("missing".to_owned())]]
    );

    let stale_doc_edges = run(
        &mut reopened,
        "MATCH (:MarkdownDocument)-[e:MD_IN_DIRECTORY]->(dir:MarkdownDirectory {`src.path`: 'docs/child'})
         RETURN count(e)",
    );
    assert_eq!(stale_doc_edges.rows, vec![vec![RuntimeValue::Int(0)]]);

    let stale_parent_edges = run(
        &mut reopened,
        "MATCH (dir:MarkdownDirectory {`src.path`: 'docs/child'})-[e:MD_PARENT_DIRECTORY]->(:MarkdownDirectory)
         RETURN count(e)",
    );
    assert_eq!(stale_parent_edges.rows, vec![vec![RuntimeValue::Int(0)]]);

    let manual_edge = run(
        &mut reopened,
        "MATCH (p:Person {name: 'Ada'})-[e:MD_PARENT_DIRECTORY]->(dir:MarkdownDirectory {`src.path`: 'docs/child'})
         RETURN p.name, e.note, dir.`src.status`",
    );
    assert_eq!(
        manual_edge.rows,
        vec![vec![
            RuntimeValue::String("Ada".to_owned()),
            RuntimeValue::String("manual".to_owned()),
            RuntimeValue::String("missing".to_owned()),
        ]]
    );

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn same_folder_membership_does_not_create_or_cleanup_md_links() {
    let db = TestDb::new("markdown_same_folder_not_links");
    let root = temp_dir("markdown_same_folder_not_links");
    fs::create_dir_all(root.join("folder")).unwrap();
    fs::write(root.join("folder/a.md"), "# A").unwrap();
    fs::write(root.join("folder/b.md"), "# B").unwrap();

    sync_root_into_db(db.path(), &root);

    let mut session = db.open();
    let link_count = run(
        &mut session,
        "MATCH (:MarkdownDocument)-[e:MD_LINKS_TO]->(:MarkdownDocument)
         RETURN count(e)",
    );
    assert_eq!(link_count.rows, vec![vec![RuntimeValue::Int(0)]]);
    run(
        &mut session,
        "MATCH (a:MarkdownDocument {`src.path`: 'folder/a.md'})-[:MD_IN_DIRECTORY]->(:MarkdownDirectory {`src.path`: 'folder'})<-[:MD_IN_DIRECTORY]-(b:MarkdownDocument {`src.path`: 'folder/b.md'})
         CREATE (a)-[:MD_LINKS_TO {`src.connector`: 'manual'}]->(b)",
    );
    drop(session);

    sync_root_into_db(db.path(), &root);

    let mut reopened = db.open();
    let manual_link = run(
        &mut reopened,
        "MATCH (:MarkdownDocument {`src.path`: 'folder/a.md'})-[e:MD_LINKS_TO]->(:MarkdownDocument {`src.path`: 'folder/b.md'})
         RETURN e.`src.connector`",
    );
    assert_eq!(
        manual_link.rows,
        vec![vec![RuntimeValue::String("manual".to_owned())]]
    );

    let in_directory = run(
        &mut reopened,
        "MATCH (:MarkdownDocument)-[e:MD_IN_DIRECTORY]->(dir:MarkdownDirectory {`src.path`: 'folder'})
         RETURN count(e)",
    );
    assert_eq!(in_directory.rows, vec![vec![RuntimeValue::Int(2)]]);

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn tombstones_missing_documents_without_breaking_native_edges() {
    let db = TestDb::new("markdown_tombstone");
    let root = temp_dir("markdown_tombstone");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("note.md"), "# Note").unwrap();

    sync_root_into_db(db.path(), &root);

    let mut session = db.open();
    run(
        &mut session,
        "MATCH (d:MarkdownDocument {`src.path`: 'note.md'})
         CREATE (:Person {name: 'Ada'})-[:REFERS_TO]->(d)",
    );
    drop(session);

    fs::remove_file(root.join("note.md")).unwrap();
    sync_root_into_db(db.path(), &root);

    let mut reopened = db.open();
    let result = run(
        &mut reopened,
        "MATCH (p:Person)-[:REFERS_TO]->(d:MarkdownDocument {`src.path`: 'note.md'})
         RETURN p.name, d.`src.status`",
    );
    assert_eq!(
        result.rows,
        vec![vec![
            RuntimeValue::String("Ada".to_owned()),
            RuntimeValue::String("missing".to_owned()),
        ]]
    );

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn alias_resolution_falls_back_after_direct_path_and_stem_matches() {
    let db = TestDb::new("markdown_alias_resolution");
    let root = temp_dir("markdown_alias_resolution");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("Friendly.md"), "# Direct").unwrap();
    fs::write(
        root.join("aliased.md"),
        "---\n\
aliases: [Friendly]\n\
---\n\
# Aliased\n",
    )
    .unwrap();
    fs::write(
        root.join("alias-target.md"),
        "---\n\
aliases: [Alias Only]\n\
---\n\
# Alias Only\n",
    )
    .unwrap();
    fs::write(root.join("source.md"), "[[Friendly]]").unwrap();
    fs::write(root.join("alias-source.md"), "[[Alias Only]]").unwrap();

    sync_root_into_db(db.path(), &root);

    let mut reopened = db.open();
    let direct = run(
        &mut reopened,
        "MATCH (:MarkdownDocument {`src.path`: 'source.md'})-[:MD_LINKS_TO]->(d:MarkdownDocument)
         RETURN d.`src.path`",
    );
    assert_eq!(
        direct.rows,
        vec![vec![RuntimeValue::String("Friendly.md".to_owned())]]
    );
    let alias = run(
        &mut reopened,
        "MATCH (:MarkdownDocument {`src.path`: 'alias-source.md'})-[:MD_LINKS_TO]->(d:MarkdownDocument)
         RETURN d.`src.path`",
    );
    assert_eq!(
        alias.rows,
        vec![vec![RuntimeValue::String("alias-target.md".to_owned())]]
    );

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn ambiguous_aliases_are_skipped_without_failing_sync() {
    let db = TestDb::new("markdown_alias_ambiguous");
    let root = temp_dir("markdown_alias_ambiguous");
    fs::create_dir_all(&root).unwrap();
    fs::write(
        root.join("one.md"),
        "---\n\
aliases: [Shared]\n\
---\n\
# One\n",
    )
    .unwrap();
    fs::write(
        root.join("two.md"),
        "---\n\
aliases: [Shared]\n\
---\n\
# Two\n",
    )
    .unwrap();
    fs::write(root.join("source.md"), "[[Shared]]").unwrap();

    sync_root_into_db(db.path(), &root);

    let mut reopened = db.open();
    let result = run(
        &mut reopened,
        "MATCH (:MarkdownDocument {`src.path`: 'source.md'})-[e:MD_LINKS_TO]->(:MarkdownDocument)
         RETURN count(e)",
    );
    assert_eq!(result.rows, vec![vec![RuntimeValue::Int(0)]]);

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn resolves_links_through_index_slug_case_and_url_candidates() {
    let db = TestDb::new("markdown_enhanced_link_resolution");
    let root = temp_dir("markdown_enhanced_link_resolution");
    fs::create_dir_all(root.join("foo/bar")).unwrap();
    fs::create_dir_all(root.join("Guides")).unwrap();
    fs::create_dir_all(root.join("games/tutorials")).unwrap();
    fs::write(root.join("foo/bar/index.md"), "# Index").unwrap();
    fs::write(root.join("Guides/Topic.md"), "# Case Target").unwrap();
    fs::write(
        root.join("games/tutorials/index.md"),
        "---\nslug: Games/Tutorials\n---\n# Tutorials",
    )
    .unwrap();
    fs::write(
        root.join("source.md"),
        "[index](foo/bar)\n\
         [root-index](/foo/bar)\n\
         [case](/guides/topic)\n\
         [slug](/en-US/docs/Games/Tutorials)\n\
         [url](https://developer.mozilla.org/en-US/docs/Games/Tutorials?x=1#intro)\n\
         [external](https://example.com/not-in-vault)\n",
    )
    .unwrap();

    sync_root_into_db(db.path(), &root);

    let mut reopened = db.open();
    let result = run(
        &mut reopened,
        "MATCH (:MarkdownDocument {`src.path`: 'source.md'})-[e:MD_LINKS_TO]->(d:MarkdownDocument)
         RETURN d.`src.path`, e.`md.link_targets`
         ORDER BY d.`src.path`",
    );
    assert_eq!(
        result.rows,
        vec![
            vec![
                RuntimeValue::String("Guides/Topic.md".to_owned()),
                RuntimeValue::List(vec![RuntimeValue::String("/guides/topic".to_owned())]),
            ],
            vec![
                RuntimeValue::String("foo/bar/index.md".to_owned()),
                RuntimeValue::List(vec![
                    RuntimeValue::String("foo/bar".to_owned()),
                    RuntimeValue::String("/foo/bar".to_owned()),
                ]),
            ],
            vec![
                RuntimeValue::String("games/tutorials/index.md".to_owned()),
                RuntimeValue::List(vec![
                    RuntimeValue::String("/en-US/docs/Games/Tutorials".to_owned()),
                    RuntimeValue::String(
                        "https://developer.mozilla.org/en-US/docs/Games/Tutorials?x=1#intro"
                            .to_owned(),
                    ),
                ]),
            ],
        ]
    );

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn resolves_docs_site_paths_to_lowercase_index_layouts() {
    let db = TestDb::new("markdown_docs_site_path_layout");
    let root = temp_dir("markdown_docs_site_path_layout");
    fs::create_dir_all(root.join("web/javascript/guide")).unwrap();
    fs::write(
        root.join("web/javascript/guide/index.md"),
        "# JavaScript Guide",
    )
    .unwrap();
    fs::write(
        root.join("source.md"),
        "[guide](/en-US/docs/Web/JavaScript/Guide)",
    )
    .unwrap();

    sync_root_into_db(db.path(), &root);

    let mut reopened = db.open();
    let result = run(
        &mut reopened,
        "MATCH (:MarkdownDocument {`src.path`: 'source.md'})-[:MD_LINKS_TO]->(d:MarkdownDocument)
         RETURN d.`src.path`",
    );
    assert_eq!(
        result.rows,
        vec![vec![RuntimeValue::String(
            "web/javascript/guide/index.md".to_owned()
        )]]
    );

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn unresolved_external_urls_create_no_link_edges() {
    let db = TestDb::new("markdown_external_url_unresolved");
    let root = temp_dir("markdown_external_url_unresolved");
    fs::create_dir_all(&root).unwrap();
    fs::write(
        root.join("source.md"),
        "[external](https://example.com/not-in-vault?x=1#section)",
    )
    .unwrap();

    sync_root_into_db(db.path(), &root);

    let mut reopened = db.open();
    let result = run(
        &mut reopened,
        "MATCH (:MarkdownDocument {`src.path`: 'source.md'})-[e:MD_LINKS_TO]->(:MarkdownDocument)
         RETURN count(e)",
    );
    assert_eq!(result.rows, vec![vec![RuntimeValue::Int(0)]]);

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn fragment_only_links_are_ignored_but_document_fragments_still_link_documents() {
    let db = TestDb::new("markdown_fragment_links");
    let root = temp_dir("markdown_fragment_links");
    fs::create_dir_all(&root).unwrap();
    fs::write(
        root.join("note.md"),
        "---\n\
next: other.md#section\n\
---\n\
[local](#section)\n",
    )
    .unwrap();
    fs::write(root.join("other.md"), "# Other").unwrap();

    sync_root_into_db(db.path(), &root);

    let mut reopened = db.open();
    let result = run(
        &mut reopened,
        "MATCH (:MarkdownDocument {`src.path`: 'note.md'})-[e:MD_LINKS_TO]->(d:MarkdownDocument)
         RETURN d.`src.path`, e.`md.link_targets`, e.`md.link_rels`",
    );
    assert_eq!(
        result.rows,
        vec![vec![
            RuntimeValue::String("other.md".to_owned()),
            RuntimeValue::List(vec![RuntimeValue::String("other.md#section".to_owned())]),
            RuntimeValue::List(vec![RuntimeValue::String("next".to_owned())]),
        ]]
    );

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn configured_root_survives_save_open_and_compact() {
    let db = TestDb::new("markdown_config");
    let root = temp_dir("markdown_config");
    fs::create_dir_all(&root).unwrap();
    let expected_root = root.canonicalize().unwrap();

    let mut session = db.open();
    let mut engine = session.engine().clone();
    set_markdown_root(&mut engine, &root).unwrap();
    engine.commit().unwrap();
    session.replace_engine(engine).unwrap();
    session.save().unwrap();
    drop(session);

    let mut reopened = db.open();
    assert_eq!(
        configured_markdown_root(reopened.engine()),
        Some(expected_root.clone())
    );
    reopened.compact().unwrap();
    drop(reopened);

    let reopened = Session::open(db.path()).unwrap();
    assert_eq!(
        configured_markdown_root(reopened.engine()),
        Some(expected_root)
    );

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn watch_mode_coalesces_duplicate_events_and_partial_writes() {
    let db = TestDb::new("markdown_watch_partial");
    let root = temp_dir("markdown_watch_partial");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("note.md"), "# Start\nBody").unwrap();

    let writer_root = root.clone();
    let writer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(40));
        fs::write(writer_root.join("note.md"), "# Start\nBody v2").unwrap();
        thread::sleep(Duration::from_millis(15));
        fs::write(writer_root.join("note.md"), "# Start\nBody final").unwrap();
    });

    let report = watch_root_into_db(
        db.path(),
        &root,
        MarkdownWatchOptions {
            poll_interval: Duration::from_millis(10),
            debounce: Duration::from_millis(40),
            max_batch_window: Duration::from_millis(150),
            idle_timeout: Some(Duration::from_millis(500)),
            max_runs: Some(2),
        },
    );
    writer.join().unwrap();

    assert_eq!(report.sync_runs, 2);
    assert!(report.events_seen >= 1);

    let mut reopened = db.open();
    let result = run(
        &mut reopened,
        "MATCH (d:MarkdownDocument {`src.path`: 'note.md'}) RETURN d.`md.body`",
    );
    assert_eq!(
        result.rows,
        vec![vec![RuntimeValue::String("# Start\nBody final".to_owned())]]
    );

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn watch_mode_handles_rename_save_restart_and_malformed_frontmatter() {
    let db = TestDb::new("markdown_watch_restart");
    let root = temp_dir("markdown_watch_restart");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("note.md"), "# One").unwrap();
    sync_root_into_db(db.path(), &root);

    let temp_path = root.join("note.tmp.md");
    let final_path = root.join("note.md");
    let writer = thread::spawn({
        let temp_path = temp_path.clone();
        let final_path = final_path.clone();
        move || {
            thread::sleep(Duration::from_millis(40));
            fs::write(
                &temp_path,
                "---\nfoo: [unterminated\n# Recovered\nBody after restart",
            )
            .unwrap();
            fs::rename(&temp_path, &final_path).unwrap();
        }
    });

    let report = watch_root_into_db(
        db.path(),
        &root,
        MarkdownWatchOptions {
            poll_interval: Duration::from_millis(10),
            debounce: Duration::from_millis(40),
            max_batch_window: Duration::from_millis(150),
            idle_timeout: Some(Duration::from_millis(500)),
            max_runs: Some(2),
        },
    );
    writer.join().unwrap();

    assert_eq!(report.sync_runs, 2);

    let mut reopened = db.open();
    let result = run(
        &mut reopened,
        "MATCH (d:MarkdownDocument {`src.path`: 'note.md'}) RETURN d.`md.title`, d.`md.has_frontmatter`",
    );
    assert_eq!(
        result.rows,
        vec![vec![
            RuntimeValue::String("Recovered".to_owned()),
            RuntimeValue::Bool(false),
        ]]
    );

    fs::remove_dir_all(root).unwrap();
}

fn sync_root_into_db(db_path: &std::path::Path, root: &std::path::Path) {
    let mut session = Session::open(db_path).unwrap();
    let mut engine = session.engine().clone();
    let report = sync_markdown_root(&mut engine, root).unwrap();
    assert!(report.upserted_documents > 0 || report.tombstoned_documents > 0);
    engine.commit().unwrap();
    session.replace_engine(engine).unwrap();
    session.save().unwrap();
}

fn watch_root_into_db(
    db_path: &std::path::Path,
    root: &std::path::Path,
    options: MarkdownWatchOptions,
) -> cupld::MarkdownWatchReport {
    let mut session = Session::open(db_path).unwrap();
    let mut engine = session.engine().clone();
    let report = watch_markdown_root(&mut engine, root, &options).unwrap();
    engine.commit().unwrap();
    session.replace_engine(engine).unwrap();
    session.save().unwrap();
    report
}

fn temp_dir(prefix: &str) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let suffix = NEXT_TEMP_DIR_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "cupld_markdown_{prefix}_{}_{}_{}",
        std::process::id(),
        timestamp,
        suffix
    ))
}
