# Messages and context

Use the composer to send prompts, attach files, reference threads or pull requests, invoke skills, and steer or queue follow-ups.

## Limits and large pastes

- **Message length**: Up to 120,000 characters.
- **Large pastes**: Pasting 32 KiB+ of text (or any paste that would exceed the message limit) turns it into a text-file attachment so it doesn't bloat the prompt. Use `Cmd+Shift+V` (Apple) or `Ctrl+Shift+V` (Windows/Linux) to paste inline instead.

## Formatting

The composer writes Markdown and shows it styled as you type. When your cursor
is on styled text, its Markdown shows instead, such as `**bold**`, and the
markers edit like any other text: delete them to remove the style, or change
`**` to `*` for italic. The style updates when your cursor leaves. To write
plain Markdown with no styling, turn off **Settings → Chat → Rich text
composer**. Lines starting with `- `, `1. `, `- [ ] `, `> `, `# ` or `---`
become lists, task lists, quotes, headings and rules; ` ``` ` followed by Enter
opens a code block. What you typed is what the agent receives, markers and
numbering included, and `#1234` without a space still looks up a pull request.

Some styled text containing or touching literal Markdown characters, such as a
bold `path\` or `2*3`, keeps its styling without showing markers. Use formatting
shortcuts or turn off rich text to edit the formatting of those ranges.

Enter sends. In a list or quote, **Shift+Enter** continues it, and Shift+Enter
on an empty line leaves it; **Tab** nests a list item. In a code block, Enter
starts a new line at the current indentation, **Tab** and **Shift+Tab** indent
the selected lines, and a closing ` ``` ` followed by Enter, or two blank lines
at the end, leave the block. **Backspace** at the start of a code block turns it
back into plain lines. Choose the language in a code block's corner to change
it. Very large code blocks are shown without syntax highlighting.
You can send the first message with **New worktree** selected while branches are still loading.
T3 Code uses the repository's default base unless you choose a branch.

## Attach files

Attach up to 100 files per message. Each image can be up to 10 MiB, with at most
80 MiB of images in one message. Other files, including videos, can be up to
50 MiB each, subject to the environment's upload support and limit. The agent
receives them on the environment's machine. Provider and model limits still
apply, including images already in the conversation. A video attachment gives
the agent a file path; it does not enable native video input.

Uploads begin when you add an attachment. All uploads must finish before the
message can send. Retry or remove a failed upload. On web and desktop, reloading
before an upload finishes requires you to attach that file again.

You can drag or paste images into the web or desktop composer. HEIC and HEIF
photos are converted to JPEG there and when selected from the mobile photo
library; photos over the image limit are also resized to fit. On mobile, you can
also send files to T3 Code through another app's system share sheet.

See [images and videos](#images-and-videos-in-messages) for previewing and saving media.

## Send while the agent is working

On web and desktop, choose **Settings → General → Follow-up behavior** to queue
new messages for the next safe tool boundary or steer the running turn immediately.
The setting applies to this client; already queued messages keep their place. Queued messages
are saved on the server and can be edited, reordered, or removed above the composer.
When Enter sends, `Cmd+Enter` on macOS or `Ctrl+Enter` on Windows and Linux uses
the opposite action: it steers when your default is Queue and queues when your
default is Steer.
If your **Send shortcut** requires `Cmd+Enter` or `Ctrl+Enter`, that shortcut uses
your default follow-up behavior; add **Shift** to use the opposite action. This
applies only to multiline messages when you choose the multiline send option.

Use `Cmd+Shift+Enter` on macOS or `Ctrl+Shift+Enter` on Windows and Linux to send
the oldest queued message as a steer. This leaves the current draft intact and
requires an active turn that supports steering. If that shortcut sends the opposite
action for your draft, move focus outside the composer to steer the queued message. Change
`thread.steerQueuedMessage` in **Settings → Keybindings** to use another shortcut.

Press `Option+Up` on macOS or `Alt+Up` on Windows and Linux with the cursor at the
start of the composer to edit the most recently queued message. Change
`thread.editQueuedMessage` to use another shortcut.

Mobile has the same choice under **Settings → Follow-ups**. While a turn is
running the send button shows which action it will take. Long-press it to use the
other action for a single message. From a hardware keyboard, `Cmd+Return`
(`Ctrl+Return` on Android) uses the other action, or `Cmd+Shift+Return`
(`Ctrl+Shift+Return`) when Return inserts a new line. The button only offers
Steer when the running agent supports it.

- **Queue**: Holds the message on the server for the next turn.
- **Steer**: Injects the message into the running turn (when supported by the agent).

Shortcuts while a turn is running:

- **Opposite action**: `Cmd+Enter` (macOS) or `Ctrl+Enter` (Windows/Linux) uses the non-default action. If your send shortcut already requires `Cmd/Ctrl+Enter`, add `Shift`. On mobile, long-press the send button or use `Cmd/Ctrl+Return` on a hardware keyboard.
- **Steer oldest queued message**: `Cmd+Shift+Enter` (macOS) or `Ctrl+Shift+Enter` (Windows/Linux) sends the first queued message as a steer without touching your current draft. Remap `thread.steerQueuedMessage` in **Settings → Keybindings**, or add a rule in your [keybindings file](./keybindings.md#edit-the-configuration-file) to trigger it on `mod+enter` when the composer is empty:

```json
{
  "key": "mod+enter",
  "command": "thread.steerQueuedMessage",
  "when": "composerFocus && turnRunning && composerEmpty"
}
```

- **Edit most recently queued message**: `Option+Up` (macOS) or `Alt+Up` (Windows/Linux) at the start of the composer (`thread.editQueuedMessage`).

Slash commands (such as `/compact` or `/context`) wait for the current turn to finish rather than steering into it. [Goal](#goals) commands are the exception.

## Queue messages offline on mobile

Mobile keeps local copies of draft attachments so you can preview them and queue messages offline. Uploads resume on reconnect, and drafts/queued messages survive app restarts and T3 Connect sign-out (restored when signing back into the same account).

## Custom models and defaults

- **Remembered defaults**: T3 Code remembers your last provider, model, and options for new threads. A project's configured model overrides this. Leaving reasoning level or service tier unset uses the provider's default.
- **Custom models**: Add unlisted models in **Settings → Providers → Models** (web and desktop; not supported for Antigravity, which uses its account catalog).
- **Rename a built-in model across all clients**: Add an entry in the server's `userdata/settings.json`:

```json
{
  "providerInstances": {
    "codex": {
      "driver": "codex",
      "config": { "customModels": [{ "slug": "gpt-5.5", "name": "Team GPT" }] }
    }
  }
}
```

## Model defaults

On web and desktop, open the composer's provider and model control to choose an
account, search models, and set model options. Favorites appear first, even when
you have not added any yet. Star models you want to keep there. The account badge
distinguishes accounts that offer the same model.

Account, model, and reasoning shortcuts remain available below the composer,
including in narrow windows. Choices that cannot be used are disabled. For
supported models, **M**, **H**, and **E** beside the model control select reasoning
effort, and the lightning control toggles Fast mode. Double-click the lightning
control within 250 ms to select Ultrafast, then click once to return to Standard.
Double-click **M** for Low or **E** for Max. These gestures are available when the
model offers the corresponding option. Permissions are always available at the
bottom of the model menu. A nondefault permission selection also appears beside
the composer controls.

T3 Code remembers your provider, model, and model options for new threads. A
project's configured model takes precedence; resetting that project setting
returns to the remembered selection.

Leaving reasoning level or service tier unset uses the provider's own configuration.

Changing an existing thread's provider, model, or options supersedes earlier settings
saved by its agent, including work still waiting to start. Your choice stays saved
until a later change; the agent can still save a new selection for future turns.
An already admitted turn keeps its selection until a supported transition or its
next turn. **Steer all** explicitly targets the currently running agent.

## Answer questions

On web and desktop, answer a question in its card above the composer. Your
ordinary message draft stays separate and editable while the question is open.
In a short window, scroll the notices and questions to reach the answer field;
the message editor and its shortcuts stay in place.

## Quote an assistant response

- **Web and desktop**: Select text in an assistant message and click **Cite in composer**. Click the pencil on the chip to add or edit a comment; click the chip to jump back to the source.
- **Mobile**: Long-press an assistant response, select text, and tap **Cite**. Tap a quote chip to edit its comment, remove it, or jump to the source message.

Quoted text and comments count toward the message length limit and are preserved across copy/paste and [stashed prompts](#prompt-stash).

## Recall a sent prompt

Press `ArrowUp` in an empty composer to cycle backward through earlier prompts in the thread, and `ArrowDown` to move forward. Only prompt text is restored (not attachments or context chips). Editing a recalled prompt turns it into a normal draft.

## Suggested next prompt

Enable **Settings → Labs → Suggest next prompt** (web and desktop) to show a predicted next message in the empty composer after a Claude or Codex turn completes.

- Press `Tab` to accept or `Escape` to dismiss.
- Each suggestion uses one cached request against your plan's usage. Claude skips suggestions on the first turn and near usage limits.
- Codex requires CLI version 0.165+ (update in **Settings → Providers**). Open Claude threads pick up the setting after restarting.

## Edit an earlier prompt

On web and desktop, select **Edit from here** under a sent message to rewind the conversation to before that message and return it to the composer:

- **Revert and keep changes**: Rewinds the conversation only.
- **Revert files too**: Restores workspace files to the checkpoint before that message (including your own edits). Only available for worktree threads when no other thread or active session shares that directory.

Rewinding removes later messages from the active thread and provider history (requires provider rewind support), but cannot undo external actions or separate provider memory.

## Prompt stash

On web and desktop, the composer shows **Interrupt** while the agent is working and the draft is
empty. Adding text or attachments replaces it with a send button that shows your
[Follow-up behavior](#send-while-the-agent-is-working): a queue icon for Queue or a steer arrow for
Steer. Click it or press your send shortcut to use that action. Hold `Cmd` on macOS or `Ctrl` on
Windows and Linux to switch the button to the opposite action, then click, or use the opposite-action
shortcut described above.

Older servers keep queued messages for the next turn.

On supporting servers, Queue sends compatible messages after the current foreground tool calls finish,
even when you switch threads or disconnect the client. It waits while the agent needs an
approval or answer. Providers that cannot accept input without restarting, commands such as
`/compact`, and messages that change the provider, model, or modes wait for a later turn instead.
At a tool boundary, messages follow their selected queue order. Later messages wait for earlier
delivery to succeed, including any retries.
On servers that support batch steering, compatible queued messages are delivered together in one
provider input, within the server's message-size limits. Queued messages from other agents can use
the same safe tool-boundary delivery. An eligible peer message with no explicit model choice can
join the running Codex or Claude turn it was queued against after a same-account setting change.
Messages that wait for the next turn use their queued settings. Agents that need a separate turn
can explicitly request `separate_turn`. Scheduled wakes still wait for a separate turn. Repeated
fires of the same task keep at most one pending wake in that thread.
Failed or interrupted tools do not release the queue on their own; it waits for the next
qualifying tool completion or the end of the turn. Stopping the thread keeps its queue held.

Queued messages appear above the composer. Rows show a thumbnail of any attached image alongside
the text. Drag a row by its handle to reorder it, use the handle's arrow keys, promote the message
to a steer, or remove it.

On supporting servers, **Steer all** in the queue header sends compatible queued messages together
to the active agent. Its count shows how many messages will be sent. If the queue exceeds the
server's text or attachment limits, the action sends the fitting prefix and leaves the remaining
messages queued. It preserves each message's sender and timeline entry. Editing, pending questions
or approvals, and providers that require a restart can make the batch action unavailable. Older
servers retain the individual row controls.

Choosing **Steer all** sends one combined input to the currently running agent, including messages
queued before you changed accounts or models. The separate timeline cards preserve the original
messages; they do not mean each message started a separate turn. If the active turn finishes before
delivery, the messages are preserved for follow-up delivery. A running goal accepts **Steer all**
and compatible automatic follow-ups. A queued `/goal` control waits for separate handling.

Saving a different model or option for the same account keeps **Steer all** available on the
running turn. For Codex, **Steer** also uses the running turn's settings. The saved settings
apply to a subsequent turn.

If you stop the agent or the server restarts, queued messages keep their order and are held.
Press **Resume** in an empty composer on web and desktop, or **Resume queue** on mobile, to
continue. Sending a new message does not release messages held by Stop. A new
message can start independently when it is otherwise eligible. You can edit,
reorder, or remove held messages without starting them.

The pencil on a queued row opens that message in the composer for editing. The original message
stays in the queue until you save, and its row is highlighted while you edit. The message's
attachments appear above the text with a remove control, and new images can be added the usual way.
The checkmark saves the queued message in place; **Cancel** on its row leaves it unchanged. Whatever
you had typed in the composer before starting the edit is restored afterwards. If the queued
message starts or is removed while you are editing, the edit ends: changed content moves into the
composer when it is empty, and is discarded otherwise.

## Review external voice prompts

On web and desktop, open **Prompts** from the sidebar. Prompts collects work from
your voice source and from work queues on the servers you are connected to. It
does not record audio. Choose a source from the header when more than one server
has prompts. Pending and Queued show counts once their sources report. A `+`
means the sample may hold more, and a `?` means a source could not be read.

- **Pending** holds voice prompts before release. Select a prompt to see where it
  is going and what you can do. Held prompts release after their countdown.
  Pause a prompt when you need more time, and double-click its text or choose
  **Edit** to change it. Editing holds release until you save or cancel; both
  leave the prompt paused. Resume when you are ready, or use **Save and send**.
- **Queued** lists released prompts and queue requests that have not been handed
  to a thread yet. It is read-only. The queue sample says when it was read; use
  **Refresh queue** for a newer one.
- **Sent** lists prompts whose handoff to a thread is confirmed. Sent does not
  mean the work is finished. This sample does not provide a full history.

Search within the loaded sample or click a Workstream badge to filter. Open a
row’s prompt link to share that prompt and stage. Workstream labels show whether
the association came from the thread, a voice suggestion, or the queue.

The progress list in each prompt shows only what was observed. Closing the page
does not resume an edit hold. If a result is uncertain, check the current state
before acting again; Prompts never retries it for you.

## Commands and skills

## Voice input

The slash menu also includes skills unless you turn off **Settings → General →
Show skills in slash menu**. Only skills enabled for the provider are listed.

## Queued messages

Queued messages appear at the end of the thread (web/desktop) or above the composer (mobile). From a queued message you can:

- **Send now**: Injects it immediately into a running turn (or moves a slash command to the front of the queue).
- **Edit**: Opens the message and its attachments in the composer without losing your existing draft.
- **Remove**: Cancels the queued message.

If you stop the agent or the server restarts, queued messages pause in order. Click **Resume** in an empty composer (or send a new message) to continue. When the provider supports native continuation, **Resume** continues without sending a new prompt.

## Commands and skills

Type `/` for commands or `$` for skills from the selected environment and provider.

- Skills also appear in the `/` menu unless you disable **Settings → Chat → Show skills in slash menu** (under Advanced).
- Provider commands must start the message; T3 Code commands (like `/model`) and `$` skills work on any line.
- `/compact` reduces context usage when supported by the provider.
- `/clear` starts a fresh provider session in the same thread (earlier messages and diffs stay visible above a **New session** divider).
- After adding or changing skills, plugins, or MCP servers, run **Restart agent session** from the command palette so your next message picks up the new config.
- Turn on **Settings → Appearance → Thread details → Context window** to view reported context usage. For automatic idle compaction on Claude, see [compact long conversations](./providers-claude.md#compact-long-conversations).

## Goals

With Codex and Claude, send `/goal` followed by what "done" means, for example
`/goal all tests in packages/api pass`. The agent keeps working across turns
until it judges the goal met. The thread shows **Goal** while it works, and a
row above the composer shows the goal and its progress.

- `/goal` alone shows the current goal. `/goal clear` removes it.
- Codex also supports `/goal pause` and `/goal resume`. Stopping a Codex goal
  pauses it.
- Stopping Claude ends the current turn, but the goal stays set. Claude checks it
  again at the end of your next message.

Agents can use `t3_thread_goal` to read a goal or control Codex without queuing
behind its active run. Read first and reuse the returned control token with a
stable request ID. Pause and clear let the current turn finish normally;
replace edits only the objective, preserving status and usage without starting
a turn. Resume on an idle thread starts an owned run; accepted dispatch still
needs native readback. Request receipts are kept for the current server and
session binding. Restart or a binding change expires old control tokens: read
again and reconcile state before issuing another operation. An unknown result
blocks further mutations until readback resolves it. Claude reads are transcript observations;
its SDK does not expose these controls. Thread read/list goal fields are cached
provider projections; use the goal tool for a fresh Codex observation.

## Context in your message

Context you attach lands where your cursor is, as a chip inside your text: a terminal excerpt,
a review comment from a diff or file, a preview annotation, or a file. You can type before and
after a chip, move it by cutting and pasting, and delete it like a character. Hover a chip for
its brief details. Select a terminal excerpt to open its captured output, or select a review
comment, picked element, or preview annotation to open its full details. Chips read as "Terminal
excerpt, Terminal 1 lines 3-4" and similar to screen readers.

A pull request appears as its icon and number. Its color reflects whether it was open, draft,
merged, or closed when it was attached. Select it to inspect the captured title and branches,
then choose **Open pull request** to visit the pull request. On web and desktop, type `#` to browse the newest
pull requests in the current project's repository. Pull requests linked to the thread are included
from any repository and listed first, after any exact number match. Continue typing digits to filter the recent list
by any part of its pull request numbers. A complete number is also resolved directly, even when that
pull request is older than the recent list. Type a single word after `#` to search pull requests in
the repository by text. Choose a result to insert it as a chip.

Another thread can be context too. On web and desktop, type `@` followed by part of its title
to search your connected environments, or drag threads from the sidebar onto the composer.
You can also paste a copied thread reference. The chip shows the thread's current title and
opens it when selected, including threads from another connected environment. Your prompt
carries the title, thread ID, and source environment. On web and desktop, adding a thread also
captures a bounded excerpt of its recent conversation: up to three recent exchanges with tool
activity, plus up to six earlier exchanges containing only the incoming message and last
recorded assistant reply, presented chronologically. Historical replies are labeled as recorded
replies because the source does not retain final-channel metadata. Incoming messages can come
from you, another agent, or a scheduled task.

The excerpt is extracted automatically, without a model-generated summary or a transcript file.
Long messages and tool results are shortened, with omissions marked. Each thread excerpt is
limited to 16,000 characters; excerpts across a message share a 32,000-character budget and may
be omitted when the rest of the prompt leaves too little room. Reasoning and attached file
contents are excluded. Selected text and tool output are copied into the receiving conversation.

The excerpt reflects the source when you attach it. Adding the same reference again keeps the
original capture. The client can capture recent context for a draft on another machine even
when the receiving agent has no link back to the source. On mobile, pasting a foreign thread
reference can also capture an excerpt when the client is connected to the source.
If neither an excerpt nor a live read route is available, reconnect to the source and reattach
the thread, or remove the reference to send.

For live follow-up from another machine, set up a directional [read-only link](./remote-access.md#link-another-environment).
The agent can use that link to read additional history. Attaching a thread shares only the
reference and captured excerpt, and leaves the source thread unchanged.

Dropping or pasting a thread reference preserves its source environment. Existing transcript
files you attached previously remain ordinary file attachments; this does not remove them.

Type `@` and part of a model or provider name, such as `@opus`, to name that model for the
agent, for example when delegating. It does not change the thread's model. Join words with
`:`, `/` or `-`, as in `@opus:opencode`.

Images keep their thumbnail shelf above the text and also get a chip at your cursor, so you can
say exactly which image you mean. Deleting an image chip leaves the image on the shelf; removing
the thumbnail asks first when the image is still mentioned in your text, then removes both. Files
exist only as chips: deleting a file's last chip removes the file from the message.

Copy text that holds chips and paste it into another draft, in the same thread or another one,
and the chips come along with what they point to. Images and files are fetched again from the
environment they came from; while that happens the chip shows a dashed outline, and if it cannot
complete T3 Code tells you and leaves the chip for you to remove or replace. A chip whose
context is no longer available shows the same dashed outline; hover it for what to do.

Copying a message with the copy button, or copying text out of it, gives other apps readable
Markdown with a link in place of each chip. Older messages that were sent before chips still
show their context. Stashing a prompt keeps its chips and what they point to; restoring brings
them back.

On mobile, tap a chip to inspect its content. File references open the current file; attached
files show the copy that was attached to the message.

- **Pull requests (`#`)**: On web and desktop, type `#` to browse recent PRs in the current repository (linked PRs appear first), type digits to match by number, or type a word to search by text.
- **Threads (`@`)**: Type `@` and part of a thread title to reference a thread on the same server (or on a linked machine when Linked environments is enabled). You can also drag threads from the sidebar onto the composer, or paste a copied thread link. The prompt carries only a reference—the agent reads the thread on demand. Dropping a thread from an unlinked environment attaches a snapshot transcript file instead.
- **Models (`@`)**: Type `@` and part of a model or provider name (e.g. `@opus` or `@opus:opencode`) to reference a model in your prompt (such as for delegation) without changing the thread's model.
- **Images vs. files**: Images keep a thumbnail shelf above the composer plus optional inline chips; files live only as inline chips (deleting the last chip removes the file).

## Attached files

On web and desktop, files open beside the conversation with the same controls as a workspace
file: a header row with the view toggle, **Copy contents** and **Save file**. On mobile, documents
open in the same file screen as workspace files; its menu holds **Copy contents**, **Save or
share** and **Open in file viewer**. Pictures, videos and PDFs keep their native viewers, and
other document formats such as Word or Pages open in the device's own viewer when it has one.
If nothing on the device can show a format, save or share it to open it elsewhere.

## Images and videos in messages

Select an image or video attachment or link to preview it.

- **Web and desktop**: Right-click media to save it or copy its path/URL.
- **Mobile**: Long-press a thumbnail and choose **Save or share**.

File links point to the file on the environment's machine, so moving or deleting the original breaks the preview—save a copy if you need to keep it.

## Files outside the workspace

Following an agent's file link to a file outside the workspace opens it read-only. External HTML files cannot load neighboring scripts, styles, or images.

On web and desktop, a file link can name a different Jones Code computer.
Click it to open the original file in the Files sidebar. The app uses your
existing connection to that computer and its file-read permission. It does not
save a copy to your workspace. If that computer is disconnected, reconnect it
and open the link again.

## HTML and PDF files in the file viewer

On web and desktop, HTML and PDF files open rendered (HTML previews are sandboxed from your T3 Code session; line-specific HTML links open source view). On mobile, PDFs open in the native iOS viewer or an installed Android viewer.
