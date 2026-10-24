# Workstreams

Use Workstreams to organize active conversations by an objective across repositories and connected environments. Repository projects still select where a conversation runs.

Choose **New Workstream** in the sidebar toolbar, enter a name, then choose **Create**. Escape or clicking outside dismisses the entry. Right-click a thread and open **Assign thread** below **Snooze**, then choose **Search or create Workstream…**. Search for an existing name, or choose **Create 'name'** to create and assign it together on supported servers. An exact existing name is offered first. You can also choose a destination directly from the submenu or drag its row into a group. Choose **Unassigned (remove primary)** in the submenu to remove its primary membership; secondary associations remain and are labeled in the submenu. Existing secondary associations and membership history remain available when you open the group details.

Collapse a group to save space; your choice is remembered in this browser. Drag a group's six-dot grip to reorder it, or use **Move up** and **Move down**. Within a group, drag a thread onto another thread to place it before or after that conversation. Thread ordering uses the connected environment's native active order and remains available when membership changes are unavailable, provided both threads have current thread-operation access and support manual ordering. Manual ordering is unavailable when the working shelf sorts threads by activity time.

Click a Workstream's name to open its details. Click the name again, choose **Close**, or press **Escape** while focused in the details to dismiss them. Closing details does not remove the Workstream or change its threads. **Manage and history** opens the additional tracking controls and recorded history.

Workstreams need a connected owner registry with current access. Groups show each thread's stored assignment, so threads stay in their groups when a short-lived verification expires, while reconnecting, and in large thread lists. Threads appear under **Unassigned** only after every stored assignment has loaded; otherwise the ungrouped threads appear under **Assignment unknown**. On supported servers, web, desktop and mobile assignment re-checks the current placement and registers and verifies the thread when needed, without a separate preparation step. Older servers retain their existing assignment flow. A thread from another connected environment is registered and assigned through that environment, so you need current write access there too. Nothing changes when that environment is disconnected, needs you to sign in again, has no Workstreams set up or only read access, cannot verify the thread, or uses a different owner registry than the one the sidebar shows. If Workstreams cannot be refreshed, the sidebar keeps the last loaded groups, shows **Showing the last loaded Workstreams.** with a **Refresh** button, and blocks membership changes until a refresh succeeds; losing access or signing out hides them. A thread with conflicting assignments shows a warning, and its membership cannot change until it is verified again. If verification fails or access changes, refresh before trying again. Read-only access lets you inspect groups and their history. It does not enable registry changes.

Choose **Color…** from a Workstream's menu to preview a preset or custom RGB border. **Save** shares the choice with your connected devices; **Cancel** keeps the saved color, and **Reset to automatic** restores the default after saving. On mobile, open the group's menu to choose its color. Color editing requires a registry that supports shared appearance and current write access.

Assigning through the thread menu preserves its repository, project, pinning, snooze and settlement state. Dragging a pinned thread into a Workstream assigns it first, then unpins it so it appears in the group. A failed or unconfirmed assignment leaves it pinned; dropping it onto its existing group also keeps the pin. Snoozed and settled conversations keep their native sidebar shelves.

If a thread menu cannot check access, it shows that status and attempts a bounded fresh check when opened. **Refresh** also rechecks access for the relevant connected environments. Signing out or lacking thread-operation permission keeps the actions disabled. A verification failure can include a specific source, build or generation mismatch; that environment must be current before assignment can proceed. While a change is saving or reconciling, the thread stays in its last confirmed group. Reopening the app resumes observation. Use **Retry** after an unavailable result or lost reply; the app retains the same request so creation is not duplicated. If another change moved the thread, the picker shows its observed placement and requires an explicit retry. Older servers keep the receipt-based Refresh flow.

Settings → **Jones Code** → **Workstreams setup** shows read-only diagnosis and links to the relevant setup guide. It distinguishes missing configuration, offline registry access and native source availability; Refresh does not configure, enroll or authenticate a server. Older servers may not support this diagnosis.

If the app reports that its build is not enrolled for Workstreams, the Workstreams connection still identifies a different app build. This can happen after an update. The connection needs to be renewed for the running build before either dragging or **Assign thread** can work. **Refresh** checks whether that repair has finished; it does not renew the connection itself. The message is about the app connection, rather than access to a particular Workstream.

Each group shows running threads out of its total known primary members, including members in those shelves and members whose verification has expired. Secondary associations do not increase the total. A purple dot marks threads waiting for input or approval, followed by a blue spinner while threads are running. **Failed** replaces both indicators when a member fails; the running count remains visible.

A completed turn or settled thread does not complete a Workstream. Open its details and change **Owner-declared workstream lifecycle** to record completion. Completion is shown as verified only when its owner declaration is present in the permanent history. Owner statements add context separately. T3 settlement and restore requests are explicit, separate actions.

The lifecycle is a tracking status: **planned** means intended work, **active** means in progress, **paused** means temporarily stopped, **completed** means finished, **deferred** means postponed, and **abandoned** means no longer pursued. **Record member completed** records the progress of that member; it does not move the thread or complete the whole Workstream. **Remove** removes that membership, while **Record statement** saves a note.

In **Prompts → Queued**, queue samples can show assignment status, its reason,
and the desired and last observed Workstream. Names appear when they are already
loaded for that environment; otherwise the app shows IDs. Missing assignment
information or **Not observed** does not mean the thread is unassigned. These
read-only samples can be stale and do not change or retry an assignment.

## Agents and the command line

On servers with Workstream operations support, agents can use
`t3_workstreams_list` to find a group, `t3_workstream_create` to create one,
and `t3_thread_workstream_set` to assign, move or unassign a thread.
Read the current assignment with `t3_thread_workstream_read` before choosing
an expected assignment. `t3_workstream_operation_status` reads an operation's
latest result; `t3_workstreams_status` explains setup problems and links to
repair instructions. Creating a group does not assign a thread to it.
These tools operate on their own server only. Use the thread's owning server
for a thread in another environment.

The command line uses the running local instance selected by the usual
`--base-dir` option:

```sh
t3 workstream list
t3 workstream status
t3 workstream status --thread thread-1 --environment env-1
t3 workstream status --open
```

Use `--json` for machine-readable output. Get the request registry UUID and
owner ID from `t3 workstream status --json` (`registry.registry_id` and
`registry.owner_id`), or from `t3_workstreams_status` for MCP. The list
response's `binding.registryId` is a server origin, not that UUID. Pass the
returned cursor with `list --cursor` to see more groups.
The `create`, `assign` and `unassign` commands accept `--request` JSON with
an explicit operation ID. Keep the request for retries. For example, replace
the example identifiers with the IDs returned by your server:

```sh
t3 workstream create --request '{"contract":"workstream-operations/1.0.0","operation_id":"create-example-0001","origin":{"kind":"cli"},"kind":"create_workstream","registry":{"registry_id":"11111111-1111-4111-8111-111111111111","owner_id":"owner-1"},"workstream":{"name":"Release work","lifecycle":"active","progress":{"state":"unknown"},"sort_order":0}}'
t3 workstream assign --request '{"contract":"workstream-operations/1.0.0","operation_id":"assign-example-0001","origin":{"kind":"cli"},"kind":"set_primary","thread":{"environment_id":"env-1","thread_id":"thread-1"},"target":{"kind":"workstream","registry_id":"11111111-1111-4111-8111-111111111111","owner_id":"owner-1","workstream_id":"ws-1"},"expected":{"kind":"unassigned"}}'
t3 workstream status assign-example-0001
```

To move an assigned thread, set `expected` to
`{"kind":"primary","workstream_id":"current-group-id"}` and target the new
group. To remove primary membership, use `unassign` with a `set_primary`
request whose target is `{"kind":"unassigned","registry_id":"11111111-1111-4111-8111-111111111111","owner_id":"owner-1"}`.
Use a separate operation ID for each new action. `expected: {"kind":"any"}`
explicitly permits changing whichever primary assignment is current.
MCP mutation requests use the same fields with `origin.kind` set to `agent`.

`committed` and `already_satisfied` confirm the assignment. `in_progress`
and `reconciling` mean it is not yet confirmed. Read status and resubmit the
same request with the same operation ID if needed, including after a lost
reply or server restart. A missing retained status does not prove that nothing
happened. The CLI does not automatically retry or generate a replacement ID.

Keep that ID after `foreign_thread`, `registry_mismatch`, `permission_denied`,
`offline`, `not_configured` or `registration_unavailable`; repair the reported
setup or routing problem and retry the original request. Status reads show the
last result and do not refresh a failure. Other rejected or unavailable results
are sealed against later effects from that request; resubmitting the same ID
refreshes them. A new ID is allowed only after a settled or sealed result,
and is required after `contention`. A conflict shows the observed assignment;
inspect it before issuing a new action. An older server reports unsupported
instead of attempting a partial assignment.
