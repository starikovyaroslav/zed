use crate::repository::RepoPath;
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GitOperationKind {
    Merge,
    Rebase,
    CherryPick,
    Revert,
}

impl fmt::Display for GitOperationKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Merge => "merge",
            Self::Rebase => "rebase",
            Self::CherryPick => "cherry-pick",
            Self::Revert => "revert",
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GitOperationAction {
    Continue,
    Skip,
    Abort,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GitOperationInput {
    ResolveConflicts,
    CommitMessage,
    EditRebasePlan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GitOperationProgress {
    pub current: usize,
    pub total: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GitOperationState {
    pub kind: GitOperationKind,
    pub progress: Option<GitOperationProgress>,
    pub conflicts: Vec<RepoPath>,
    pub required_input: Option<GitOperationInput>,
    pub available_actions: Vec<GitOperationAction>,
    pub original_head: Option<String>,
    pub target: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GitOperationPreflight {
    pub dirty_worktree: bool,
    pub staged_changes: bool,
    pub detached_head: bool,
    pub operation: Option<GitOperationState>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GitOperation {
    Merge {
        commit: String,
        no_commit: bool,
    },
    Rebase {
        upstream: String,
    },
    CherryPick {
        commits: Vec<String>,
    },
    Revert {
        commits: Vec<String>,
        no_commit: bool,
    },
}

impl GitOperation {
    pub fn kind(&self) -> GitOperationKind {
        match self {
            Self::Merge { .. } => GitOperationKind::Merge,
            Self::Rebase { .. } => GitOperationKind::Rebase,
            Self::CherryPick { .. } => GitOperationKind::CherryPick,
            Self::Revert { .. } => GitOperationKind::Revert,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GitOperationErrorCode {
    OperationInProgress,
    NoOperationInProgress,
    DirtyWorktree,
    DetachedHead,
    Conflicts,
    InvalidRequest,
    ProcessFailed,
    UnsupportedAction,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GitOperationError {
    pub code: GitOperationErrorCode,
    pub message: String,
    pub stderr: String,
    pub state: Option<Box<GitOperationState>>,
}

impl fmt::Display for GitOperationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for GitOperationError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GitOperationOutcome {
    Completed,
    InProgress(GitOperationState),
}

pub type GitOperationResult = Result<GitOperationOutcome, GitOperationError>;
