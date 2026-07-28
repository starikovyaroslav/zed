use anyhow::Result;
use buffer_diff::BufferDiff;
use collections::HashSet;
use futures::StreamExt;
use git::{
    repository::{CommitDiff, CommitFile, CommitFileStatus, GitComparisonTarget, RepoPath},
    status::{DiffTreeType, FileStatus, StatusCode, TrackedStatus, TreeDiff, TreeDiffStatus},
};
use gpui::{
    App, AppContext as _, AsyncApp, AsyncWindowContext, Context, Entity, EventEmitter,
    SharedString, Subscription, Task, WeakEntity, Window,
};

use language::{Buffer, Capability, DiskState};
use settings::WorktreeId;
use std::{path::PathBuf, sync::Arc};
use text::BufferId;
use util::{ResultExt, paths::PathStyle, rel_path::RelPath};
use ztracing::instrument;

use crate::{
    ConflictSet, Project,
    git_store::{GitStoreEvent, Repository, RepositoryEvent},
};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DiffBase {
    Head,
    Index,
    Staged,
    Merge {
        base_ref: SharedString,
    },
    Comparison {
        base: SharedString,
        target: ComparisonTarget,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ComparisonTarget {
    Revision(SharedString),
    Index,
    Worktree,
}

impl DiffBase {
    pub fn is_merge_base(&self) -> bool {
        matches!(self, DiffBase::Merge { .. })
    }

    pub fn is_comparison(&self) -> bool {
        matches!(self, DiffBase::Comparison { .. })
    }
}

pub struct DiffBufferList {
    diff_base: DiffBase,
    repo: Option<Entity<Repository>>,
    project: Entity<Project>,
    base_commit: Option<SharedString>,
    head_commit: Option<SharedString>,
    tree_diff: Option<TreeDiff>,
    comparison_diff: Option<CommitDiff>,
    load_error: Option<SharedString>,
    tree_diff_update_needed: bool,
    tree_diff_base_task: Option<Task<()>>,
    _subscription: Subscription,
    update_needed: postage::watch::Sender<()>,
    _task: Task<()>,
}

pub enum BranchDiffEvent {
    FileListChanged,
    DiffBaseChanged,
}

enum ReloadDiffTask {
    Tree(futures::channel::oneshot::Receiver<Result<TreeDiff>>),
    Comparison(futures::channel::oneshot::Receiver<Result<CommitDiff>>),
}

enum ReloadedDiff {
    Tree(TreeDiff),
    Comparison(CommitDiff),
}

impl EventEmitter<BranchDiffEvent> for DiffBufferList {}

impl DiffBufferList {
    pub fn new(
        source: DiffBase,
        project: Entity<Project>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let git_store = project.read(cx).git_store().clone();
        let repo = git_store.read(cx).active_repository();
        let git_store_subscription = cx.subscribe_in(
            &git_store,
            window,
            move |this, _git_store, event, _window, cx| {
                let should_update = match event {
                    GitStoreEvent::ActiveRepositoryChanged(new_repo_id) => {
                        this.repo.is_none() && new_repo_id.is_some()
                    }
                    GitStoreEvent::RepositoryUpdated(
                        event_repo_id,
                        RepositoryEvent::StatusesChanged | RepositoryEvent::HeadChanged,
                        _,
                    ) => this
                        .repo
                        .as_ref()
                        .is_some_and(|r| r.read(cx).snapshot().id == *event_repo_id),
                    _ => false,
                };

                if should_update {
                    this.tree_diff_update_needed =
                        this.diff_base.is_merge_base() || this.diff_base.is_comparison();
                    cx.emit(BranchDiffEvent::FileListChanged);
                    *this.update_needed.borrow_mut() = ();
                }
            },
        );

        let (send, recv) = postage::watch::channel::<()>();
        let worker = window.spawn(cx, {
            let this = cx.weak_entity();
            async |cx| Self::handle_status_updates(this, recv, cx).await
        });

        Self {
            diff_base: source,
            repo,
            project,
            tree_diff: None,
            comparison_diff: None,
            load_error: None,
            tree_diff_update_needed: false,
            tree_diff_base_task: None,
            base_commit: None,
            head_commit: None,
            _subscription: git_store_subscription,
            _task: worker,
            update_needed: send,
        }
    }

    pub fn diff_base(&self) -> &DiffBase {
        &self.diff_base
    }

    pub fn set_repo(&mut self, repo: Option<Entity<Repository>>, cx: &mut Context<Self>) {
        let same_repo = match (self.repo.as_ref(), repo.as_ref()) {
            (Some(current), Some(new)) => current.read(cx).id == new.read(cx).id,
            (None, None) => true,
            _ => false,
        };
        if same_repo {
            return;
        }

        self.repo = repo;
        self.tree_diff = None;
        self.comparison_diff = None;
        self.load_error = None;
        self.tree_diff_update_needed =
            self.diff_base.is_merge_base() || self.diff_base.is_comparison();
        self.tree_diff_base_task = None;
        self.base_commit = None;
        self.head_commit = None;
        cx.emit(BranchDiffEvent::FileListChanged);
        *self.update_needed.borrow_mut() = ();
    }

    pub fn set_diff_base(&mut self, diff_base: DiffBase, cx: &mut Context<Self>) {
        if self.diff_base == diff_base {
            return;
        }

        self.tree_diff_update_needed = diff_base.is_merge_base() || diff_base.is_comparison();
        self.tree_diff = None;
        self.comparison_diff = None;
        self.load_error = None;
        self.tree_diff_base_task = None;
        self.diff_base = diff_base;
        self.base_commit = None;
        self.head_commit = None;

        cx.emit(BranchDiffEvent::DiffBaseChanged);
        if self.tree_diff_update_needed {
            *self.update_needed.borrow_mut() = ();
        }
    }

    pub async fn handle_status_updates(
        this: WeakEntity<Self>,
        mut recv: postage::watch::Receiver<()>,
        cx: &mut AsyncWindowContext,
    ) {
        this.update(cx, |this, cx| this.spawn_reload_tree_diff(cx))
            .log_err();
        while recv.next().await.is_some() {
            let Ok(()) = this.update(cx, |this, cx| {
                let mut needs_update = this.tree_diff_update_needed;
                this.tree_diff_update_needed = false;

                if this.repo.is_none() {
                    let active_repo = this
                        .project
                        .read(cx)
                        .git_store()
                        .read(cx)
                        .active_repository();
                    if active_repo.is_some() {
                        this.repo = active_repo;
                        needs_update = true;
                    }
                } else if let Some(repo) = this.repo.as_ref() {
                    repo.update(cx, |repo, _| {
                        if let Some(branch) = &repo.branch
                            && let DiffBase::Merge { base_ref } = &this.diff_base
                            && let Some(commit) = branch.most_recent_commit.as_ref()
                            && &branch.ref_name == base_ref
                            && this.base_commit.as_ref() != Some(&commit.sha)
                        {
                            this.base_commit = Some(commit.sha.clone());
                            needs_update = true;
                        }

                        if repo.head_commit.as_ref().map(|c| &c.sha) != this.head_commit.as_ref() {
                            this.head_commit = repo.head_commit.as_ref().map(|c| c.sha.clone());
                            needs_update = true;
                        }
                    })
                }

                if needs_update {
                    this.spawn_reload_tree_diff(cx);
                }
            }) else {
                return;
            };
        }
    }

    pub fn status_for_buffer_id(&self, buffer_id: BufferId, cx: &App) -> Option<FileStatus> {
        let (repo, path) = self
            .project
            .read(cx)
            .git_store()
            .read(cx)
            .repository_and_path_for_buffer_id(buffer_id, cx)?;
        if self.repo() == Some(&repo) {
            return self.merge_statuses(
                repo.read(cx)
                    .status_for_path(&path)
                    .map(|status| status.status),
                self.tree_diff
                    .as_ref()
                    .and_then(|diff| diff.entries.get(&path)),
            );
        }
        None
    }

    pub fn merge_statuses(
        &self,
        diff_from_head: Option<FileStatus>,
        diff_from_merge_base: Option<&TreeDiffStatus>,
    ) -> Option<FileStatus> {
        match (diff_from_head, diff_from_merge_base) {
            (None, None) => None,
            (Some(diff_from_head), None) => Some(diff_from_head),
            (Some(diff_from_head @ FileStatus::Unmerged(_)), _) => Some(diff_from_head),

            // file does not exist in HEAD
            // but *does* exist in work-tree
            // and *does* exist in merge-base
            (
                Some(FileStatus::Untracked)
                | Some(FileStatus::Tracked(TrackedStatus {
                    index_status: StatusCode::Added,
                    worktree_status: _,
                })),
                Some(_),
            ) => Some(FileStatus::Tracked(TrackedStatus {
                index_status: StatusCode::Modified,
                worktree_status: StatusCode::Modified,
            })),

            // file exists in HEAD
            // but *does not* exist in work-tree
            (Some(diff_from_head), Some(diff_from_merge_base)) if diff_from_head.is_deleted() => {
                match diff_from_merge_base {
                    TreeDiffStatus::Added => None, // unchanged, didn't exist in merge base or worktree
                    _ => Some(diff_from_head),
                }
            }

            // file exists in HEAD
            // and *does* exist in work-tree
            (Some(FileStatus::Tracked(_)), Some(tree_status)) => {
                Some(FileStatus::Tracked(TrackedStatus {
                    index_status: match tree_status {
                        TreeDiffStatus::Added { .. } => StatusCode::Added,
                        _ => StatusCode::Modified,
                    },
                    worktree_status: match tree_status {
                        TreeDiffStatus::Added => StatusCode::Added,
                        _ => StatusCode::Modified,
                    },
                }))
            }

            (_, Some(diff_from_merge_base)) => {
                Some(diff_status_to_file_status(diff_from_merge_base))
            }
        }
    }

    fn spawn_reload_tree_diff(&mut self, cx: &mut Context<Self>) {
        if !self.diff_base.is_merge_base() && !self.diff_base.is_comparison() {
            return;
        }

        let task = cx.spawn(async move |this, cx| {
            if let Err(error) = Self::reload_tree_diff(this.clone(), cx).await {
                log::error!("failed to reload git comparison: {error:#}");
                this.update(cx, |this, cx| {
                    this.load_error = Some(error.to_string().into());
                    cx.emit(BranchDiffEvent::FileListChanged);
                    cx.notify();
                })
                .ok();
            }
        });

        self.tree_diff_base_task = Some(task);
        self.load_error = None;
        cx.notify();
    }

    pub fn is_tree_base_loading(&self) -> bool {
        self.tree_diff_base_task
            .as_ref()
            .is_some_and(|task| !task.is_ready())
    }

    pub fn load_error(&self) -> Option<&SharedString> {
        self.load_error.as_ref()
    }

    pub async fn reload_tree_diff(this: WeakEntity<Self>, cx: &mut AsyncApp) -> Result<()> {
        let task = this.update(cx, |this, cx| {
            let Some(repo) = this.repo.as_ref() else {
                this.tree_diff.take();
                this.comparison_diff.take();
                return None;
            };
            match this.diff_base.clone() {
                DiffBase::Merge { base_ref } => {
                    let task = repo.update(cx, |repo, cx| {
                        repo.diff_tree(
                            DiffTreeType::MergeBase {
                                base: base_ref,
                                head: "HEAD".into(),
                            },
                            cx,
                        )
                    });
                    Some(ReloadDiffTask::Tree(task))
                }
                DiffBase::Comparison { base, target } => {
                    let target = match target {
                        ComparisonTarget::Revision(revision) => {
                            GitComparisonTarget::Revision(revision)
                        }
                        ComparisonTarget::Index => GitComparisonTarget::Index,
                        ComparisonTarget::Worktree => GitComparisonTarget::Worktree,
                    };
                    Some(ReloadDiffTask::Comparison(
                        repo.update(cx, |repo, _| repo.compare(base, target)),
                    ))
                }
                DiffBase::Head | DiffBase::Index | DiffBase::Staged => None,
            }
        })?;
        let Some(task) = task else { return Ok(()) };

        let diff = match task {
            ReloadDiffTask::Tree(task) => ReloadedDiff::Tree(task.await??),
            ReloadDiffTask::Comparison(task) => ReloadedDiff::Comparison(task.await??),
        };
        this.update(cx, |this, cx| {
            match diff {
                ReloadedDiff::Tree(diff) => {
                    this.tree_diff = Some(diff);
                    this.comparison_diff = None;
                }
                ReloadedDiff::Comparison(diff) => {
                    this.comparison_diff = Some(diff);
                    this.tree_diff = None;
                }
            }
            this.load_error = None;
            cx.emit(BranchDiffEvent::FileListChanged);
            cx.notify();
        })
    }

    pub fn repo(&self) -> Option<&Entity<Repository>> {
        self.repo.as_ref()
    }

    #[instrument(skip_all)]
    pub fn load_buffers(&mut self, cx: &mut Context<Self>) -> Vec<DiffBuffer> {
        let mut output = Vec::default();
        let Some(repo) = self.repo.clone() else {
            return output;
        };
        if (self.diff_base.is_merge_base() && self.tree_diff.is_none())
            || (self.diff_base.is_comparison() && self.comparison_diff.is_none())
        {
            return output;
        }

        if let DiffBase::Comparison { target, .. } = self.diff_base.clone() {
            let Some(diff) = self.comparison_diff.as_ref() else {
                return output;
            };
            for file in &diff.files {
                let Some(project_path) = repo.read(cx).repo_path_to_project_path(&file.path, cx)
                else {
                    continue;
                };
                output.push(DiffBuffer {
                    repo_path: file.path.clone(),
                    load: Self::load_comparison_buffer(
                        self.project.clone(),
                        target.clone(),
                        file,
                        project_path,
                        repo.clone(),
                        cx,
                    ),
                    file_status: comparison_file_status(file.status()),
                });
            }
            return output;
        }

        self.project.update(cx, |_project, cx| {
            let mut seen = HashSet::default();

            for item in repo.read(cx).cached_status() {
                seen.insert(item.repo_path.clone());
                let branch_diff = self
                    .tree_diff
                    .as_ref()
                    .and_then(|t| t.entries.get(&item.repo_path))
                    .cloned();
                let Some(status) = (match self.diff_base {
                    DiffBase::Head | DiffBase::Merge { .. } => {
                        self.merge_statuses(Some(item.status), branch_diff.as_ref())
                    }
                    DiffBase::Index => item.status.staging().has_unstaged().then_some(item.status),
                    DiffBase::Staged => item.status.staging().has_staged().then_some(item.status),
                    DiffBase::Comparison { .. } => unreachable!(),
                }) else {
                    continue;
                };
                if !status.has_changes() {
                    continue;
                }

                let Some(project_path) =
                    repo.read(cx).repo_path_to_project_path(&item.repo_path, cx)
                else {
                    continue;
                };
                let task = Self::load_buffer(
                    self.diff_base.clone(),
                    branch_diff,
                    project_path,
                    repo.clone(),
                    cx,
                );

                output.push(DiffBuffer {
                    repo_path: item.repo_path.clone(),
                    load: task,
                    file_status: item.status,
                });
            }
            let Some(tree_diff) = self.tree_diff.as_ref() else {
                return;
            };

            for (path, branch_diff) in tree_diff.entries.iter() {
                if seen.contains(&path) {
                    continue;
                }

                let Some(project_path) = repo.read(cx).repo_path_to_project_path(&path, cx) else {
                    continue;
                };
                let task = Self::load_buffer(
                    self.diff_base.clone(),
                    Some(branch_diff.clone()),
                    project_path,
                    repo.clone(),
                    cx,
                );

                let file_status = diff_status_to_file_status(branch_diff);

                output.push(DiffBuffer {
                    repo_path: path.clone(),
                    load: task,
                    file_status,
                });
            }
        });
        output
    }

    #[instrument(skip_all)]
    fn load_comparison_buffer(
        project: Entity<Project>,
        target: ComparisonTarget,
        file: &CommitFile,
        project_path: crate::ProjectPath,
        repo: Entity<Repository>,
        cx: &Context<'_, Self>,
    ) -> Task<Result<LoadedDiffBuffer>> {
        let old_oid = file.old_oid;
        let new_oid = file.new_oid;
        let file_status = file.status();
        let is_binary = file.is_binary;
        let repo_path = file.path.clone();
        let display_revision = match &target {
            ComparisonTarget::Revision(revision) => revision.clone(),
            ComparisonTarget::Index => "index".into(),
            ComparisonTarget::Worktree => "working tree".into(),
        };

        cx.spawn(async move |_this, cx| {
            if target == ComparisonTarget::Worktree && file_status != CommitFileStatus::Deleted {
                let buffer = project
                    .update(cx, |project, cx| project.open_buffer(project_path, cx))
                    .await?;
                let diff = project
                    .update(cx, |project, cx| {
                        project.git_store().update(cx, |git_store, cx| {
                            git_store.open_diff_since(old_oid, buffer.clone(), repo, cx)
                        })
                    })
                    .await?;
                return Ok(LoadedDiffBuffer {
                    display_buffer: buffer.clone(),
                    main_buffer: buffer,
                    diff,
                    conflict_set: None,
                });
            }

            let (mut old_text, mut new_text) = if is_binary {
                (None, "(binary file not shown)".to_string())
            } else {
                let old_text = match old_oid {
                    Some(oid) => Some(
                        repo.update(cx, |repo, cx| repo.load_blob_content(oid, cx))
                            .await?,
                    ),
                    None => None,
                };
                let new_text = match new_oid {
                    Some(oid) => {
                        repo.update(cx, |repo, cx| repo.load_blob_content(oid, cx))
                            .await?
                    }
                    None => String::new(),
                };
                (old_text, new_text)
            };
            if let Some(old_text) = &mut old_text {
                text::LineEnding::normalize(old_text);
            }
            text::LineEnding::normalize(&mut new_text);

            let full_path = repo.read_with(cx, |repo, _| {
                repo.work_directory_abs_path.join(repo_path.as_std_path())
            });
            let file = Arc::new(ComparisonFile {
                path: project_path.path,
                full_path,
                worktree_id: project_path.worktree_id,
                display_name: format!(
                    "{} - {}",
                    display_revision,
                    repo_path
                        .file_name()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| repo_path.as_unix_str().to_string())
                ),
                was_deleted: new_oid.is_none(),
                is_binary,
            }) as Arc<dyn language::File>;
            let language_registry = project.update(cx, |project, _| project.languages().clone());
            let buffer_language_registry = language_registry.clone();
            let buffer = cx.new(|cx| {
                let mut buffer = Buffer::local(&new_text, cx);
                buffer.file_updated(file, cx);
                buffer.set_language_registry(buffer_language_registry);
                buffer.set_capability(Capability::ReadOnly, cx);
                buffer
            });
            let snapshot = buffer.read_with(cx, |buffer, _| buffer.snapshot());
            let diff = cx.new(|cx| {
                BufferDiff::new(
                    &snapshot,
                    snapshot.language().cloned(),
                    Some(language_registry),
                    cx,
                )
            });
            diff.update(cx, |diff, cx| {
                diff.set_base_text(old_text.map(Into::into), snapshot.text, cx)
            })
            .await;

            Ok(LoadedDiffBuffer {
                display_buffer: buffer.clone(),
                main_buffer: buffer,
                diff,
                conflict_set: None,
            })
        })
    }

    #[instrument(skip_all)]
    fn load_buffer(
        diff_base: DiffBase,
        branch_diff: Option<git::status::TreeDiffStatus>,
        project_path: crate::ProjectPath,
        repo: Entity<Repository>,
        cx: &Context<'_, Project>,
    ) -> Task<Result<LoadedDiffBuffer>> {
        let task = cx.spawn(async move |project, cx| {
            let buffer = project
                .update(cx, |project, cx| project.open_buffer(project_path, cx))?
                .await?;

            let main_buffer = buffer.clone();
            let load_conflict_set = diff_base != DiffBase::Staged;
            let (display_buffer, changes) = match diff_base {
                DiffBase::Head => {
                    let diff = project
                        .update(cx, |project, cx| {
                            project.open_uncommitted_diff(buffer.clone(), cx)
                        })?
                        .await?;
                    (buffer, diff)
                }
                DiffBase::Index => {
                    let diff = project
                        .update(cx, |project, cx| {
                            project.open_unstaged_diff(buffer.clone(), cx)
                        })?
                        .await?;
                    (buffer, diff)
                }
                DiffBase::Staged => {
                    let (diff, index_buffer) = project
                        .update(cx, |project, cx| {
                            project.open_staged_diff(buffer.clone(), cx)
                        })?
                        .await?;
                    (index_buffer, diff)
                }
                DiffBase::Merge { .. } => {
                    let diff = if let Some(entry) = branch_diff {
                        let oid = match entry {
                            git::status::TreeDiffStatus::Added { .. } => None,
                            git::status::TreeDiffStatus::Modified { old, .. }
                            | git::status::TreeDiffStatus::Deleted { old } => Some(old),
                        };
                        project
                            .update(cx, |project, cx| {
                                project.git_store().update(cx, |git_store, cx| {
                                    git_store.open_diff_since(oid, buffer.clone(), repo, cx)
                                })
                            })?
                            .await?
                    } else {
                        project
                            .update(cx, |project, cx| {
                                project.open_uncommitted_diff(buffer.clone(), cx)
                            })?
                            .await?
                    };
                    (buffer, diff)
                }
                DiffBase::Comparison { .. } => unreachable!(),
            };
            let conflict_set = if load_conflict_set {
                Some(
                    project
                        .update(cx, |project, cx| {
                            project.git_store().update(cx, |git_store, cx| {
                                git_store.open_conflict_set(main_buffer.clone(), cx)
                            })
                        })?
                        .await,
                )
            } else {
                None
            };
            Ok(LoadedDiffBuffer {
                display_buffer,
                main_buffer,
                diff: changes,
                conflict_set,
            })
        });
        task
    }
}

struct ComparisonFile {
    path: Arc<RelPath>,
    full_path: PathBuf,
    worktree_id: WorktreeId,
    display_name: String,
    was_deleted: bool,
    is_binary: bool,
}

impl language::File for ComparisonFile {
    fn as_local(&self) -> Option<&dyn language::LocalFile> {
        None
    }

    fn disk_state(&self) -> DiskState {
        DiskState::Historic {
            was_deleted: self.was_deleted,
        }
    }

    fn path(&self) -> &Arc<RelPath> {
        &self.path
    }

    fn full_path(&self, _: &App) -> PathBuf {
        self.full_path.clone()
    }

    fn path_style(&self, _: &App) -> PathStyle {
        PathStyle::local()
    }

    fn file_name<'a>(&'a self, _: &'a App) -> &'a str {
        &self.display_name
    }

    fn worktree_id(&self, _: &App) -> WorktreeId {
        self.worktree_id
    }

    fn to_proto(&self, _: &App) -> language::proto::File {
        language::proto::File {
            worktree_id: self.worktree_id.to_proto(),
            entry_id: None,
            path: self.path.as_unix_str().to_owned(),
            mtime: None,
            is_deleted: self.was_deleted,
            is_historic: true,
        }
    }

    fn is_private(&self) -> bool {
        false
    }

    fn can_open(&self) -> bool {
        !self.is_binary
    }
}

fn comparison_file_status(status: CommitFileStatus) -> FileStatus {
    let status = match status {
        CommitFileStatus::Added => StatusCode::Added,
        CommitFileStatus::Deleted => StatusCode::Deleted,
        CommitFileStatus::Renamed => StatusCode::Renamed,
        CommitFileStatus::Copied => StatusCode::Copied,
        CommitFileStatus::Modified => StatusCode::Modified,
    };
    FileStatus::Tracked(TrackedStatus {
        index_status: status,
        worktree_status: StatusCode::Unmodified,
    })
}

fn diff_status_to_file_status(branch_diff: &git::status::TreeDiffStatus) -> FileStatus {
    let file_status = match branch_diff {
        git::status::TreeDiffStatus::Added { .. } => FileStatus::Tracked(TrackedStatus {
            index_status: StatusCode::Added,
            worktree_status: StatusCode::Added,
        }),
        git::status::TreeDiffStatus::Modified { .. } => FileStatus::Tracked(TrackedStatus {
            index_status: StatusCode::Modified,
            worktree_status: StatusCode::Modified,
        }),
        git::status::TreeDiffStatus::Deleted { .. } => FileStatus::Tracked(TrackedStatus {
            index_status: StatusCode::Deleted,
            worktree_status: StatusCode::Deleted,
        }),
    };
    file_status
}

#[derive(Debug)]
pub struct LoadedDiffBuffer {
    pub display_buffer: Entity<Buffer>,
    pub main_buffer: Entity<Buffer>,
    pub diff: Entity<BufferDiff>,
    pub conflict_set: Option<Entity<ConflictSet>>,
}

#[derive(Debug)]
pub struct DiffBuffer {
    pub repo_path: RepoPath,
    pub file_status: FileStatus,
    pub load: Task<Result<LoadedDiffBuffer>>,
}
