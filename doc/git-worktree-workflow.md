# Git Worktree Workflow (Human + AI)

This document describes the workflow for coordinating development between a human engineer and an AI agent using Git worktrees.

## The Setup

In this repository, we maintain two separate worktrees:

1. **Worktree A (Human):** Where the developer performs manual coding, architectural decisions, and code review.
2. **Worktree B (AI):** Where the AI agent implements features, performs refactoring, or runs automated tasks.

This physical separation allows the AI to operate in a dedicated workspace on its own branch without altering the human's uncommitted files, while still retaining access to the full repository history and tooling.

## Constraints

**Git forbids checking out the same branch in two different worktrees simultaneously.**

If Worktree A has `branch-a` checked out, Worktree B cannot checkout `branch-a`. Worktree B must operate on a separate branch—typically a feature or task branch derived from `branch-a`.

---

## Workflow: Step-by-Step

### 1. Create a Derived Branch (Worktree B)

To delegate a task to the AI based on the human's active branch (`branch-a`), create a new branch in **Worktree B**:

```powershell
# In Worktree B
git checkout -b branch-b branch-a
```

This initializes `branch-b` at the exact commit currently checked out by `branch-a`.

### 2. Make Changes Concurrently

Both the human and the AI work independently on their respective tasks:

* The human commits changes to `branch-a` in Worktree A.
* The AI commits changes to `branch-b` in Worktree B.

### 3. Synchronize Changes

#### A. Pulling Human Updates into the AI Branch (Human → AI)

If `branch-a` has advanced with new commits, bring `branch-b` up to date by rebasing it on top of `branch-a`:

1. Commit any in-progress changes in Worktree A.
2. In Worktree B, run:
   ```powershell
   # In Worktree B
   git rebase branch-a
   ```
   This replays the AI's commits on top of the latest commit on `branch-a`, maintaining a clean, linear history.

   > [!TIP]
   > **Prefer temporary WIP commits over `git stash`:** If you have uncommitted or temporary work in `branch-b`, commit it as a WIP commit before rebasing:
   > ```powershell
   > git commit -am "WIP"
   > git rebase branch-a
   > git reset --soft HEAD~1
   > ```
   > This leaves your working tree staged and cleanly positioned on top of the rebased history. This approach avoids the conflict-prone and fragile nature of `git stash pop` across rebased history.

#### B. Merging AI Changes back into the Human Branch (AI → Human)

To incorporate completed work from `branch-b` into `branch-a`:

1. Ensure `branch-b` is rebased onto `branch-a` so that `branch-b` is a direct descendant of `branch-a`.
2. Switch to **Worktree A** and perform a fast-forward merge:
   ```powershell
   # In Worktree A
   git merge --ff-only branch-b
   ```

   > [!IMPORTANT]
   > Always pass `--ff-only`. If the branches have unexpectedly diverged, Git will safely abort instead of creating an unwanted merge commit.

### 4. Iterate or Finish

Once the fast-forward merge succeeds, both `branch-a` and `branch-b` point to the identical commit hash.

* **To continue working:** Resume from **Step 2**.
* **When the task is complete:** You can delete `branch-b` from Worktree A once Worktree B switches to another branch or detaches HEAD:
  ```powershell
  # In Worktree A
  git branch -d branch-b
  ```

---

## Tips & Best Practices

* **Commit often in Worktree A:** Uncommitted working tree changes in Worktree A are invisible to Worktree B. If the AI needs to see your latest code, commit it first to `branch-a`.
* **Prefer temporary WIP commits over `git stash`:** While `git stash` is common, committing in-progress work (`git commit -am "WIP"`) and unwinding it after rebasing (`git reset --soft HEAD~1`) is safer. It anchors your changes in Git's
  commit graph during rebase conflict resolution, avoiding stash pop conflicts, index loss, or untracked file collisions.
* **Always run merge commands in Worktree A:** Git does not permit updating or fast-forwarding an actively checked-out branch from another worktree. Merging into `branch-a` must always be executed inside Worktree A.
* **Keep history linear:** Rebase rather than creating merge commits when syncing between the two worktrees. This keeps diffs clear and simplifies review.
