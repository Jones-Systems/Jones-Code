# Checkpoint safety

Checkpoint snapshots stage working files into hidden Git refs. Capturing the physical primary
checkout could retain unrelated dirty files in the shared repository even when they were never
committed on a visible branch. Capture therefore requires a linked Git worktree, independent of
branch name or working-tree cleanliness.

The boundary belongs in `GitVcsDriver.checkpoints.captureCheckpoint` so all callers are covered.
It compares the per-worktree and common Git directories before creating an index, commit, or ref,
and returns `VcsPrimaryCheckoutCheckpointError` when they identify the primary checkout.
Checkpoint reads and restoration retain their existing behavior.

Tests use disposable repositories with linked worktrees. A fixture's index belongs to its
per-worktree Git directory; checkpoint refs belong to the common Git directory. Resolve those
paths through Git instead of assuming `.git` is a directory in the workspace.
