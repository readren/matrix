# Git Worktree Workflow (Human + AI)

This document describes the workflow for coordinating work between a human and an AI agent using Git worktrees.

## The Setup

In this repository, we maintain two separate worktrees:

1. **Worktree A (Human):** Where the developer performs manual coding, architectural decisions, and review.
2. **Worktree B (AI):** Where the AI agent performs tasks, refactoring, or feature implementation.

This separation allows the AI to work on a dedicated branch without interfering with the human's active workspace, while still having access to the full repository context.

## Constraints

**Git does not allow the same branch to be checked out in two different worktrees simultaneously.**

If Worktree A is on branch `feature-x`, Worktree B cannot check out `feature-x`. It must work on a different branch, typically one derived from the human's branch.

## Workflow: Starting an AI Task

To have the AI help with a branch currently active in Worktree A, follow these steps in **Worktree B**:

### 1. Create a derived branch

Create a new branch in Worktree B that starts from the human's branch:

```powershell
git checkout -b feature-x-ai feature-x
```

This creates `feature-x-ai` at the same commit where `feature-x` currently is.

### 2. Perform AI work

The AI performs its task on `feature-x-ai`.

### 3. Sync changes back to Human

Once the AI is done, the changes can be merged or rebased back into the main feature branch. From **Worktree A**:

```powershell
git merge feature-x-ai
```

## Tips for Synchronization

* **Commit often:** If you have uncommitted changes in Worktree A that the AI needs to see, you must **commit** them. The AI's worktree sees the state of the repository history, not your uncommitted local changes in another worktree.
* **Rebasing:** If the human branch advances significantly, the AI branch should be rebased to stay up to date:
  ```powershell
  # In Worktree B
  git rebase feature-x
  ```
