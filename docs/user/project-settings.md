# Settings and project overrides

T3 Code separates **device preferences** (saved locally per browser/device, like Appearance and Confirmations) from **server settings** (stored on each environment, like Threads, Worktrees, and Source control).

## Scope and inheritance

Use the **"Applying settings for …"** selector at the top of Settings (web/desktop) or the header filter (mobile) to choose which **environment(s)** and **project** you are editing:

- **All environments**: Bulk-edits every currently connected environment (offline environments keep their values). Controls show **Mixed** where environments disagree.
- **Project scope**: Creates a project override for that setting without changing environment defaults. Settings that cannot be overridden appear read-only.
- **Inheritance order**: `Project override` → `Environment setting` → `t3.json` in the repo → `Built-in default`. Leave a setting on **Inherit** to defer to the next tier.
- Click the **layers icon** beside a server setting to inspect its inheritance chain, jump to overriding projects, or **Reset all** back to the environment default.
- **Per-project providers**: With a project selected in **Settings → Providers**, toggling a provider enables or disables it for that project only (in either direction). Running turns finish; no new turns start on a disabled provider.

## Pin and archive projects

- **Pin**: Pinned projects sort first in project pickers, the command palette, the sidebar filter, and mobile (`mod+shift+p` on a highlighted row, or **Settings → Projects**).
- **Archive**: Archive finished projects in **Settings → Projects** (mobile: **Settings → Projects → Organize**). Archived projects leave pickers while keeping their threads, settings, and files; unarchive anytime from Settings.
- Pinning or archiving a project that exists on multiple machines requires all of those machines to be connected.

## Project icons and multi-repo projects

- **Icons**: In **Settings → Projects**, set an icon, emoji, monogram, or image (applies across checkouts). Without one, T3 Code generates a two-character monogram (or single CJK character) from the project name.
- **Multiple repositories**: See [Projects with several repositories](./multi-repo-projects.md).

## Worktree settings

Configure worktrees in **Settings → Worktrees** (environment default or project override):

- **Branch naming**: Choose a static prefix (default `t3/`), model-selected semantic prefix (`feat/`, `fix/`), or custom instructions. Falls back to the temporary branch name if generation fails or collides.
- **Submodules** (under Advanced): Defaults to recursive checkout; set to **Top level only** or **Skip** (or `"worktreeSubmodules"` in `t3.json`) for repos with heavy nested submodules.
- **Sparse checkout**: Set `"worktreeSparsePatternsPath"` in `t3.json` to a repo-relative file of Git sparse-checkout patterns (non-cone syntax; requires Git 2.35+; enables `extensions.worktreeConfig`). Set Submodules to **Skip** if you don't want submodules initialized under excluded paths.
- **Worktree location** (per machine): Set an absolute path (e.g. `D:\worktrees` or `~/worktrees`) to store new worktrees elsewhere. Existing worktrees stay where they are; cleanup covers both folders.
- **Fast worktree checkout (macOS beta)**: Enable **Settings → Labs → Fast worktree checkout** (Git 2.42+) to clone new worktrees via APFS copy-on-write from a local cache. Falls back to normal checkout for cross-drive paths, sparse checkouts, or Git LFS/filter repos.

### Storage cleanup

Enable automatic cleanup in **Settings → Worktrees → Cleanup** (off by default; runs on startup, on change, and hourly). Projects can set **Automatic worktree cleanup** to **Inherit**, **Off**, or **Custom**.

- **Triggers**: Inactive days, days after settlement (`0` = next cleanup), merged PRs (including GitHub squash/rebase merges into the default branch), or settled/archived threads with no commits beyond the default branch.
- **Safety**: Active sessions and shared worktrees are never removed. **Keep worktrees with local changes** defaults to **Uncommitted changes** (protects tracked edits and untracked files; removes ignored files like `.env`/build output). Choose **Any local files** (also protects ignored files except `node_modules`) or **Edited tracked files** (protects tracked edits only).
- **Delete with deleted threads**: Enable **Delete worktrees with deleted threads** to automatically clean up safe worktrees once their last thread is deleted.
- **Scratch folders**: Inactive and settled rules also apply to [no-project scratch folders](./thread-sidebar.md#start-without-a-project) (skipped if they contain a Git repo or enabled scheduled task). Because scratch files aren't in Git, deleted files cannot be recovered.
- **Manual run**: Click **Delete now** to run rules immediately and inspect what was removed, kept, or orphaned (untracked folders are listed but never auto-deleted).
- **Captures and logs**: Browser captures and rotated logs have separate machine-wide retention periods.

The Projects category, under Code, holds the project's name, icon, actions,
checkouts and removal. Actions belong to a project: editing them creates the project's own list
on each selected environment, and reset returns to the environment's shared list. A project's
`t3.json` actions can be imported there. When a project has several checkouts on one environment,
choose one in the scope line at the top to see or change only that checkout's settings.

Settings a repository can also declare in `t3.json`, such as the workspace for new threads,
resolve in one order: a project override, then the environment setting, then `t3.json`, then the
built-in default. Leave a setting on **Inherit** to let the next tier decide.
Browser access changes apply when an agent session next starts.

New worktrees initialize git submodules recursively. If that step is slow because the repository
declares many nested submodules, set **Submodules** in **Settings → Worktrees** under Advanced (with the project
selected to override it there) to **Top level only** to stop at the ones the repository declares
itself, or **Skip** to leave them for a setup script. It resolves in the same order as the
workspace default: a `"worktreeSubmodules"` value in the `t3.json` of the branch being checked out
applies when the project and environment are both on **Inherit**.

## Worktree location

Your project folder is the persistent checkout Jones Code uses to create new
task worktrees. Keep that folder and its repository storage available, even
after the thread that originally created it finishes. A project folder can
itself be a linked worktree; automatic cleanup keeps it while it remains
registered as a project. Apply the same rule when cleaning up folders manually.

New worktrees go in the `worktrees` folder of the T3 home directory. To put them somewhere else,
such as another drive, set **Settings → Worktrees → Worktree location** to an absolute path like
`D:\worktrees` or `~/worktrees`. The setting is per machine. Existing worktrees stay where they
are, and cleanup covers both the default folder and the custom one.

## Fast worktree checkout on macOS

Fast worktree checkout is a beta. Turn it on for each Mac in **Settings → Labs → Fast worktree
checkout**. T3 Code then keeps a few recent checkouts of each repository in the `caches` folder of
the T3 home directory. A new worktree is cloned from one of them with APFS copy-on-write, which is
about twice as fast as writing every file and makes the first `git status` in it fast as well.
Turning it off deletes the cache.

The first worktree of a repository uses Git's normal checkout and fills the cache. It needs Git 2.42
or newer. Git's normal checkout is also used for worktrees on a different drive from the T3 home
directory, sparse checkouts, repositories whose attributes use filters such as Git LFS, and
repositories whose checkout settings changed.

## Storage cleanup

Open **Settings → Worktrees → Cleanup** to enable automatic cleanup on one machine or all connected
environments. Policies are off by default and run on the server at startup, when changed, and
hourly. Offline machines keep their existing policies.

Select a project to set **Automatic worktree cleanup** to **Inherit**, **Off**, or **Custom**.
Inherit follows each machine's rules; Off keeps that project's worktrees until you remove them
manually. Custom applies separate worktree rules to the selected project or checkout. Browser
captures and log retention remain machine-wide.

Worktrees can be removed after a chosen number of inactive days, after a thread settles, after
merging, or when they have no commits beyond the default branch and their thread is settled or
archived. Set settled retention to 0 days to remove a worktree at the next cleanup after
settlement. In repositories without remotes,
unchanged-worktree cleanup compares against the branch checked out in the project workspace;
if that checkout has a detached HEAD, the worktree is kept. Only T3-managed worktrees are eligible.
Active sessions and shared worktrees prevent removal. **Keep worktrees with local changes** defaults
to **Uncommitted changes**, which protects tracked edits and untracked files but deletes ignored
files such as `.env` and build output. **Any local files** also protects ignored files other than
`node_modules`. **Edited tracked files** protects only tracked edits and allows untracked and
ignored files to be deleted. Branches and thread history stay.

Jones Code automatically recreates a managed checkout when it can verify the saved checkout and
branch. When it needs a decision, **Needs input** lets you restore the saved branch if available
or create a fresh worktree from the latest default branch.
Codex and Claude threads can also continue without a checkout and attach one later. If restoring
the saved branch fails, Jones Code shows the reason and offers the remaining choices. Your
original message resumes after the selected workspace is ready.

Merge cleanup requires a merged pull request whose commits are included in the remote default
branch. A squash or rebase merge on GitHub also counts when the pull request targeted the default
branch and the worktree is still at the pull request's last commit.

Enable **Delete worktrees with deleted threads** to remove safe worktrees after their last
thread is deleted, including archived threads and worktrees left by earlier deletions. The
server waits for sessions and terminals to stop and retries skipped worktrees after restart.
Existing prompts for deleting a worktree manually remain available when this policy is off.

Choose **Delete now** to run the enabled rules immediately. The latest results show which
worktrees were removed or kept and why, plus any failures.
Results also list folders in the worktree folder that no thread uses, with their sizes; cleanup never removes them, so remove them manually if you want.

On a server that supports it, **Propose retirements** reviews settled checkouts without deleting
anything and opens **Last run**. Checkouts listed under **Ready to retire** have a **Retire**
button; the confirmation lists the exact paths it authorizes, and the summary reports about how
much space was recovered. If the server stops part-way through a retirement, the next **Retire**
of that checkout first confirms the earlier server process has ended, then retries a checkout that
was left untouched; a partly removed checkout stays held. If that checkout was already fully
removed, **Propose retirements** lists it under **Ready to retire**, and **Retire** records it as
retired once its branch reads back, without deleting anything. If Git already dropped the checkout
but its directory remains (read-only folders, such as a Go module cache, can stop a removal),
**Retire** makes those regenerable folders writable and deletes the leftover, but only when it holds
nothing else: tracked files that match the branch, folders the retirement already covered, and
empty folders. Any other file keeps the checkout held and is named in the result. **Retire** is not
offered for a checkout that covers more than 200 paths, and the result says so. The status line
under **Delete now** shows the last run even when no cleanup rule is on.

The inactive and settled retention periods also apply to the folders of
[threads without a project](./thread-sidebar.md#start-without-a-project). Those folders are not
Git repositories, so their files are deleted with them and cannot be recovered; the thread stays,
and its next turn starts in a new, empty folder. A folder is kept when it has a Git repository
anywhere inside it or when its thread has an enabled scheduled task. Every file in such a folder
counts as untracked, so only **Edited tracked files** removes a folder that holds anything other
than `node_modules`; the other two rules remove only empty folders. Folders that no thread uses
are listed here too.

Browser captures and rotated logs have separate retention periods. Expired capture links stop
working. Current logs, message attachments, and browser profiles are kept.

### How Retire treats content that Git does not have

A checkout can hold work that no branch or remote has: edited tracked files, staged changes,
untracked files that are not ignored, and commits that were never pushed. **Propose retirements**
says what **Retire** will do with each checkout before you press the button:

- **Safe to retire.** Everything is already in a branch you keep or can be regenerated. Nothing is
  lost.
- **Saved to a branch first.** The proposal says how many files, and how many bytes, will be saved
  to a new `preserve/<date>/<machine>/<checkout>` branch, and which remote the branch is pushed to,
  or that it stays local-only when the repository has no remote or the remote refuses the push.
  Pressing **Retire** is your approval. The checkout is removed with `--force` only after the saved
  content is read back and verified to match what is in the checkout; if anything changed since the
  proposal, nothing is removed and the result says why.
- **Held.** The checkout stays, and the proposal names each file that cannot be saved.

After a retirement, the result shows the branch, its short commit, the remote and a
**Remote-verified** or **Local-only** badge, whether `--force` was used, and whether the branch read
back. It also shows a restore command you can copy, which recreates the checkout at its old path
from the branch:

```sh
git -C /path/to/project worktree add /path/to/checkout preserve/20261117/my-mac/feature-one
```

To continue the work in a checkout you already have, run
`git switch -c feature-one preserve/20261117/my-mac/feature-one` there instead. The branch holds the
saved work as one commit on top of the checkout's last commit.

Preserve branches are never deleted automatically. Delete one yourself, with `git branch -D` and
`git push <remote> --delete`, once you no longer need what it holds.
A **Local-only** branch lives in the project's repository on that machine, so it is kept only when
that repository outlasts the checkout; otherwise **Retire** holds the checkout instead of removing it.

Some content is never saved to a branch, and holds the checkout until you move or remove it:

- Files that look like secrets: names such as `.env` (but not `.env.example`), `*.pem`, `*.key`,
  `*.p12`, `*.pfx`, `id_*`, `.netrc`, `.npmrc`, `.pypirc` and anything with `credentials` in the name,
  and text files whose content matches a known secret format such as a private key or access token.
  The result names the file and the kind of match, never the matched text.
- Files over 50 MiB, or more than 500 MiB in total.
- Nested Git repositories.

**Discard regenerable build output** (`worktreeRegenerableDiscard`, in **Settings → Worktrees →
Cleanup**) is on by default. It lets **Retire** delete ignored dependency installs, build output and
caches, such as `node_modules`, `.venv`, `dist`, `build`, `target`, `coverage` and Go module
caches, together with the checkout because they can be regenerated. A nested repository, or a
`.env` or key file inside build output, still holds the checkout. Turn the setting off to keep
those folders protected. Ignored folders of any other name stay held until you name them in
the **Retire** confirmation.

## Project icons

## Scheduled tasks and webhooks

Create and manage scheduled tasks in **Settings → Scheduled tasks** (web, desktop, and mobile).

- **Schedules**: Recurring fixed-time schedules use the target environment's time zone. **Once** uses your local time zone (runs on reconnect if offline).
- **Destination**: Choose **New thread each run**, **One thread (continue conversation)**, or **One thread (fresh session each run)**. Deleting a target thread deletes tasks bound to it; archiving a target thread causes runs to fail (settle it instead).
- **Controls**: Edit, pause, resume, run now (except webhook tasks), or delete from the list.

### Webhook automations

Choose **On webhook** as a task's schedule to trigger runs from external services (e.g., GitHub PR or CI events). A public URL requires a [T3 Connect](./remote-access.md) tunnel; click **Rotate** to invalidate and replace the URL.

- **Prompt placeholders**: Insert `{{body.path}}`, `{{headers.name}}`, `{{query.name}}`, `{{body}}`, or `{{request}}`. Missing values render empty.
- **Untrusted input protection**: Placeholder values are wrapped in `<webhook-data-…>` tags, and `$`/`@`/leading `/` tokens are neutralized so untrusted payloads can't invoke skills or commands.
- **Signatures** (web/desktop setup): Enable **Require signature** to verify HMAC signatures (for GitHub, keep `x-hub-signature-256`, hex, and `sha256=` prefix with the same secret and `application/json`).
- **Filters**: Add conditions (using paths without braces, e.g. `headers.x-github-event` equals `pull_request`, or `body.action` is one of `opened, reopened`). All filters must match; non-matching requests are logged as **Filtered**.
- **Deduplication & coalescing**: Set a **Duplicate request key** (e.g. `{{headers.delivery-id}}`; ignores repeats for 7 days) or a **Coalesce key** with a 10–3600s window (default 120s; runs once with the latest payload when the window closes). Coalesced updates are rate-limited to 60/min (`HTTP 429`).
- **Offline delivery**: By default, offline environments return an error. Enable **Settings → Sharing & access → Hold webhooks while offline** to buffer T3 Connect webhooks for up to 24 hours, and optionally set **Skip requests older than** on the task.
- **Debugging**: Open **Deliveries** from a task's menu (web/desktop) to inspect recent payloads and rendered prompts.
