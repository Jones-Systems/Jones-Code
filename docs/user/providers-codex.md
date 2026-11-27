# Codex

Use your ChatGPT plan or an existing Codex CLI login to run Codex in T3 Code.

## Connect with ChatGPT

Connect during onboarding or in **Settings → Providers** (select your target environment first for remote machines). T3 Code manages the Codex runtime and signs you in via OpenAI.

- Manage shared usage and credits through **Manage usage** in T3 Code.
- **Remote sign-in**: If sign-in doesn't return automatically, paste the full `localhost` redirect URL into the sign-in panel.
- **Speed options**: Fast and Ultrafast speeds are not available through ChatGPT sharing—[use an existing Codex login](#use-an-existing-codex-login) instead.
- Disconnecting stops running threads while preserving history.

## Use an existing Codex login

Run `codex login` on the environment's machine, then add a Codex instance in **Settings → Providers** via **Configure manually**. See [provider setup](./install.md#providers) for installation details.

If provider status checks time out on a slower machine, set `T3CODE_CODEX_PROBE_TIMEOUT_MS=60000` in the server environment and restart. See [Provider check timeouts](./install.md#provider-check-timeouts).

## Use multiple accounts

Add another ChatGPT account in **Settings → Providers**, or configure multiple CLI logins with a **shadow home** so work and personal accounts can continue the same threads:

```bash
mkdir -p ~/.codex_personal
CODEX_HOME=~/.codex_personal codex login
```

| Instance       | CODEX_HOME path | Shadow home path    |
| -------------- | --------------- | ------------------- |
| Codex Work     | `~/.codex`      | _(leave empty)_     |
| Codex Personal | `~/.codex`      | `~/.codex_personal` |

Both instances must use the same **CODEX_HOME path**. T3 Code prepares the shared
state in the shadow directory; do not populate it by copying your whole Codex
home.

The shadow account needs its own `auth.json` file. If Codex uses an OS credential
store, configure file storage for this setup. See
[OpenAI's credential storage guide](https://learn.chatgpt.com/docs/auth#credential-storage).

Use a completely separate **CODEX_HOME path**, with no shadow home, when you want
separate Codex sessions and configuration. That instance cannot continue threads
from the other home.

## Switch accounts in an existing thread

Choose the other account from the thread's model picker. T3 Code offers compatible
Codex instances that share the thread's **CODEX_HOME path**. Changing accounts does
not move the conversation into a separate Codex home.

If the saved conversation cannot be reopened after switching accounts, T3 Code
reports the resume error instead of starting an empty conversation. Check that
both accounts can access the shared Codex sessions, or switch back to the
original account to continue.

If the account is missing from the picker, compare the home paths in provider
settings. If two instances show the same unexpected account or models, check their
reported accounts, refresh provider status, and confirm the second instance has
its own shadow path and login. A shadow-home conflict usually means the directory
contains a copied Codex setup. Use a fresh shadow directory and sign in again.

## Stop a goal-driven turn

Press **Stop** to pause the thread's active Codex goal before interrupting its
live child turns and root turn. The pause stays with the native Codex conversation
when its provider session restarts.

If Codex cannot confirm the goal pause, the thread shows **Goal may still be
active** and still attempts Stop. Check the goal before continuing. If the root
does not acknowledge Stop, the thread shows **Turn may still be running** and
reports the failure; it keeps the turn's last known state until Codex supplies a
completion event.

## Answer questions while Codex works

Codex can ask a question and keep working. Answer it in the thread's question
panel. The answer becomes a new message: it reaches the active turn, or starts
another turn if Codex has finished. Unanswered questions survive reconnects.
If you do not want to answer, dismiss the question from its panel. Dismissing
closes it without sending anything to Codex. This requires a Codex version that
supports async questions.

## Approve app access

Codex tools can request access to another app. Respond to the named app's request
in the thread on web, desktop, or mobile. Some tools offer access for one request,
the current session, or permanently. See [Permission modes](./permission-modes.md)
for command and file approvals.

## Use Daybreak

To switch accounts mid-thread, pick another compatible Codex instance sharing the same **CODEX_HOME path** from the model picker.

## Features and options

- **Image generation**: Supported when your Codex version, model, and account allow it. If disabled in Codex config, add `-c features.image_generation=true` to the instance's **Launch arguments** (applies to new sessions).
- **Async questions**: Codex can ask a question while continuing to work; answer in the question panel (steers the active turn or starts a new one) or dismiss it without sending.
- **App access approvals**: Respond to external app access requests directly in chat (one-time, session, or permanent depending on the tool).
- **MCP OAuth sign-in**: Click **Sign in** on the thread notice, approve in your browser, and paste the final redirect URL into **Finish sign-in** (requires provider management permission).
- **Daybreak**: Models with cyber access programs show a **Cyber Access** option (`Standard` or `Daybreak`, initialized from `daybreak = true` in `config.toml`).
- **Usage limits**: Limit stops display the exhausted window, reset time, and (on workspace plans) whether credits or spend limits need adjustment.
- **Feedback to OpenAI**: Send `/feedback <description>` in an existing Codex thread to upload conversation and Codex logs to OpenAI.

## OpenAI Chrome extension

Using your signed-in Chrome profile via [OpenAI's Chrome extension](https://learn.chatgpt.com/docs/chrome-extension) requires a browser connection for the Codex thread. For remote environments, forward the remote page's port or use a reachable URL—T3 Code's built-in preview browser uses its own separate session.
