# Workstreams

Workstreams group related threads across your connected environments. Create a Workstream from the sidebar, then use a thread’s menu or drag it into the group. A thread has one Workstream; move it to another group or remove it to leave it Unassigned. Dragging a pinned thread into a group unpins it after its move is confirmed. Assigning through the thread menu preserves its pin. Removing a Workstream assignment does not restore an earlier pin or pinned position. If unpinning fails, the thread can remain both pinned and grouped.

A group’s menu lets you rename it, choose a color, change its lifecycle or delete it. Double-click its title or choose Rename to edit the name in place; Enter saves and Escape cancels. Drag a group by its header to reorder it: the upper half of another header places it before that group, and the lower half places it after. The insertion line shows the destination. Lifecycle describes the group; it does not settle or reopen its threads. Deleting a Workstream requires confirmation and moves its threads to Unassigned without deleting them. Create a new group to use it again.

Each Workstream initially shows five active threads. In its options menu, choose **5**, **10** or **15** under **Visible threads** to save that Workstream’s default on this client. **Show all** reveals its remaining active threads; **Show fewer** returns to the saved default. Closing and reopening a group also returns to its default. The header retains the full thread count. The divider below the visible rows shows **Show all** or **Show fewer**. Unassigned has no display limit. Conversation shortcuts follow the visible sidebar order, with Pinned first, then the Workstreams, then Unassigned; hidden Workstreams are skipped.

Threads can be reordered within Workstreams and Unassigned when their servers support active-thread reordering and the Working section is off. With the Working section enabled, automatic ordering applies; dragging into another group still changes the assignment.

Click a Workstream’s Working, Needs input or Done count to show that status. Click the selected count again to use the global status filter, or double-click a count to exclude that status. The group’s options menu also restores the global status filter. A local choice overrides the global status filter while keeping project, environment and search scope. These choices are saved on this client.

The **Workstreams** row above the groups totals the Working, Needs input and Done counts of all Workstreams, without Unassigned. Its counts work the same way but apply to every Workstream at once. The most recent choice wins: pick a status on one group after using the Workstreams row and that group keeps its own; use the Workstreams row again and it replaces every group’s choice. Clearing a group, or choosing **Use global status filter** in its menu, returns that group to the global status filter even while the Workstreams row has a status selected. Clearing the Workstreams row returns every group to the global status filter, and earlier group choices do not come back.

Use **+** and **−** beside the Workstreams label to expand or collapse every group’s thread list; each group’s title, counts and failures stay visible, and Unassigned is unchanged. The chevron at the right of that row hides or shows all Workstreams at once. Unassigned stays visible, and showing the Workstreams again restores each group as you left it.

Drag a thread from a Workstream or Unassigned into a chat composer to reference it, just as with a pinned thread. Referencing a thread leaves its assignment and order unchanged.

Assignments are saved on the environment that owns each thread. Clients connected to that environment see confirmed changes. Group names, colors and other details reach another environment when a client that can write to it connects to both copies. A group created through an agent or the CLI on a server stays there until such a client connects. Disconnected environments can therefore show different details temporarily. When edits conflict, one complete edit wins; changes to different fields can overwrite each other.

Read-only access lets you view groups but cannot save changes or synchronize a lagging environment. Settings → Jones Code → **Workstreams** shows each environment as **Ready**, **Offline**, **Update required**, **Read only** or **Couldn’t sync**. Use the ordinary connection or update controls for the affected environment. Older servers do not support personal Workstreams.

If a move reports that the thread changed elsewhere, check its current group before trying again. If a change cannot be confirmed, it may have been saved; check the group before retrying. No separate Workstreams registration or enrollment is needed.

Agents can list and create Workstreams with `t3_workstreams_list` and `t3_workstream_create`, read or change a thread’s assignment with `t3_thread_workstream_read` and `t3_thread_workstream_set`, and edit group details with `t3_workstream_update`. Retain the same caller-chosen ID when retrying creation. Read the current assignment revision before changing it. These tools use ordinary environment permissions and can target a linked environment.

The CLI provides `t3 workstream list`, `create`, `rename`, `recolor`, `reorder`, `delete`, `assign`, `unassign` and `read`. Use `--json` for machine-readable output. The CLI talks to the selected running server using ordinary authentication.

Historical registry memberships, proofs, edges and exports are not displayed by this feature. Existing encrypted history and keys are preserved separately; this does not make that history readable in the new client.

## Automatic assignment for queued prompts

When a prompt arrives from a work queue, such as Jarvis, for a conversation that has never been in a Workstream, the server may assign that conversation to a Workstream. It does so only when one Workstream is clearly the right place, judged from Workstream names and the titles of the conversations already in them. When the match is unclear or two Workstreams are plausible, the conversation is left unassigned. Storage is never chosen.

Conversations you start or message yourself are not assigned automatically, and neither are delegated tasks or subagents. A conversation that has ever been in a Workstream, including one you removed, is never assigned automatically again.

To change an automatic assignment, move or remove the conversation as usual. The change is permanent for that conversation and helps later automatic choices. Agents can read why a conversation was or was not assigned from the `autoRoute` field of `t3_thread_workstream_read`.

The decision uses the environment's text generation model, the same one that writes thread titles. It sends the prompt, the conversation's title and project name, Workstream names and the titles of conversations in those Workstreams. It sends no other conversation content. Turn it off with Settings → Jones Code → **Assign queued prompts to a Workstream automatically**.
