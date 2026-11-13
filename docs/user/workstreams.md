# Workstreams

Workstreams group related threads across your connected environments. Create a Workstream from the sidebar, then use a thread’s menu or drag it into the group. A thread has one Workstream; move it to another group or remove it to leave it ungrouped. A pinned thread is unpinned after its move is confirmed. If unpinning fails, the thread can remain both pinned and grouped.

A group’s menu lets you rename it, choose a color, change its lifecycle or delete it. You can also reorder groups. Lifecycle describes the group; it does not settle or reopen its threads. Deleting a Workstream removes its assignments. Create a new group to use it again.

Assignments are saved on the environment that owns each thread. Clients connected to that environment see confirmed changes. Group names, colors and other details reach another environment when a client that can write to it connects to both copies. A group created through an agent or the CLI on a server stays there until such a client connects. Disconnected environments can therefore show different details temporarily. When edits conflict, one complete edit wins; changes to different fields can overwrite each other.

Read-only access lets you view groups but cannot save changes or synchronize a lagging environment. Settings → Jones Code → **Workstreams** shows each environment as **Ready**, **Offline**, **Update required**, **Read only** or **Couldn’t sync**. Use the ordinary connection or update controls for the affected environment. Older servers do not support personal Workstreams.

If a move reports that the thread changed elsewhere, check its current group before trying again. If a change cannot be confirmed, it may have been saved; check the group before retrying. No separate Workstreams registration or enrollment is needed.

Agents can list and create Workstreams with `t3_workstreams_list` and `t3_workstream_create`, read or change a thread’s assignment with `t3_thread_workstream_read` and `t3_thread_workstream_set`, and edit group details with `t3_workstream_update`. Retain the same caller-chosen ID when retrying creation. Read the current assignment revision before changing it. These tools use ordinary environment permissions and can target a linked environment.

The CLI provides `t3 workstream list`, `create`, `rename`, `recolor`, `reorder`, `delete`, `assign`, `unassign` and `read`. Use `--json` for machine-readable output. The CLI talks to the selected running server using ordinary authentication.

Historical registry memberships, proofs, edges and exports are not displayed by this feature. Existing encrypted history and keys are preserved separately; this does not make that history readable in the new client.
