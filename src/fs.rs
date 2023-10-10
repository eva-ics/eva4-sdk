use bmart::tools::Sorting;
use eva_common::{EResult, Error};
use glob_match::glob_match;
use serde::{Deserialize, Serialize};
use std::fs::Metadata;
use std::path::{Path, PathBuf};
use tokio::fs;

const MAX_LINK_DEPTH: usize = 5;

#[derive(Debug, Sorting, Serialize, Deserialize)]
#[sorting(id = "path")]
pub struct Entry {
    pub path: PathBuf,
    #[serde(skip)]
    pub meta: Option<Metadata>,
    pub kind: Kind,
}

async fn resolve_symlink_metadata(path: PathBuf) -> Option<Metadata> {
    let mut source = path;
    for _ in 0..MAX_LINK_DEPTH {
        let Ok(target) = fs::read_link(&source).await else {
            return None;
        };
        let Ok(meta) = fs::metadata(&target).await else {
            return None;
        };
        if meta.is_symlink() {
            source = target;
        } else {
            return Some(meta);
        }
    }
    None
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    File,
    Dir,
    #[default]
    Any,
    Unknown,
}

#[async_recursion::async_recursion]
async fn list_entries_by_mask(
    path: &Path,
    masks: &[&str],
    kind: Kind,
    result: &mut Vec<Entry>,
    recursive: bool,
) -> Result<(), std::io::Error> {
    let mut entries = fs::read_dir(path).await?;
    while let Some(entry) = entries.next_entry().await? {
        let mut meta = entry.metadata().await?;
        let entry_kind = if meta.is_file() {
            Kind::File
        } else if meta.is_dir() {
            Kind::Dir
        } else if meta.is_symlink() {
            if let Some(target_meta) = resolve_symlink_metadata(entry.path()).await {
                meta = target_meta;
                if meta.is_file() {
                    Kind::File
                } else if meta.is_dir() {
                    Kind::Dir
                } else {
                    Kind::Unknown
                }
            } else {
                Kind::Unknown
            }
        } else {
            Kind::Unknown
        };
        if entry_kind == kind || (kind == Kind::Any && entry_kind != Kind::Unknown) {
            for mask in masks {
                if glob_match(mask, &entry.file_name().to_string_lossy()) {
                    result.push(Entry {
                        path: entry.path(),
                        meta: Some(meta),
                        kind: entry_kind,
                    });
                    break;
                }
            }
        }
        if entry_kind == Kind::Dir && recursive {
            let mut subdir = path.to_owned();
            subdir.push(entry.file_name());
            list_entries_by_mask(&subdir, masks, kind, result, true).await?;
        }
    }
    Ok(())
}

pub async fn list(
    path: &Path,
    masks: &[&str],
    kind: Kind,
    recursive: bool,
    absolute: bool,
) -> EResult<Vec<Entry>> {
    let mut entries = Vec::new();
    list_entries_by_mask(path, masks, kind, &mut entries, recursive).await?;
    let s = path.to_string_lossy();
    let mut result: Vec<Entry> = if absolute {
        entries
    } else {
        entries
            .into_iter()
            .filter_map(|r| {
                let p = r.path.to_string_lossy();
                if s.len() + 1 < p.len() {
                    Some(Entry {
                        path: Path::new(&p[s.len() + 1..]).to_owned(),
                        meta: r.meta,
                        kind: r.kind,
                    })
                } else {
                    None
                }
            })
            .collect()
    };
    result.sort();
    Ok(result)
}

pub async fn safe_list(
    path: &Path,
    masks: &[&str],
    kind: Kind,
    recursive: bool,
    absolute: bool,
) -> EResult<Vec<Entry>> {
    if path.to_string_lossy().contains("../") {
        Err(Error::invalid_params("path can not contain ../"))
    } else {
        list(path, masks, kind, recursive, absolute).await
    }
}
