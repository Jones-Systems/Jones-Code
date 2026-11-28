# Workstreams

Workstreams group related threads across your connected environments. Create a Workstream from the sidebar. In the web or desktop new-thread composer, open the Workstream picker beside the workspace controls to assign the conversation when you send your first message. You can also use an existing thread’s menu or drag it into the group. A thread has one Workstream; move it to another group or remove it to leave it Unassigned. Dragging a pinned thread into a group unpins it after its move is confirmed. Assigning through the thread menu preserves its pin. Removing a Workstream assignment does not restore an earlier pin or pinned position. If unpinning fails, the thread can remain both pinned and grouped.

On web and desktop, right-click a thread link in a conversation and choose **Assign thread** to move the linked thread without opening it.

On web and desktop, **New Workstream** opens a name field below the sidebar toolbar with an optional **Icon** picker. For an existing Workstream, right-click its header and open **Change icon** under **Workstream state**. The submenu follows the sidebar’s **Filter threads by project** order. Pick a project to apply its icon immediately or choose **None** to clear it. The selected project also becomes the default for threads started from the Workstream. Saving the icon requires a connected environment with write access and Workstream icon support.

Right-click a Workstream or open its options and choose **New thread**. The composer preselects that Workstream and its project. If the group has no project selected, the composer opens with **No project**. The thread joins the Workstream when you send your first message.

Right-click a group’s header to rename it, choose a color, snooze it, change its lifecycle or delete it. You can also focus the header and press Shift+F10. Double-click its title or choose Rename to edit the name in place. Enter saves and Escape cancels. Drag a group by its header to reorder it. The upper half of another header places it before that group, and the lower half places it after. The insertion line shows the destination. Mark a Workstream **Completed** when its work is done or **Abandoned** when you stop pursuing it. Either status moves it to **Closed Workstreams** after the environments confirm the close. Settle or move every assigned thread before closing the group.

On web and desktop, open the **Change color** submenu and choose a preset to apply it immediately. Choose **Automatic** to restore the Workstream’s assigned color. Open **Settings → Jones Code → Appearance** to preview and adjust card outlines, tint, backgrounds, text, and spacing. Choose fixed filter spacing to keep your selected gaps as the sidebar width changes. Board preferences are saved in this browser profile. Use **Copy appearance settings** to share them or **Reset board settings** to return to the defaults.

Open a closed Workstream to see its assigned threads or choose **Reopen Workstream** to return it to Active. If you unsettle a thread from a closed Workstream, it appears under **Unassigned** and stays linked in the closed Workstream’s history. Moving the thread to another Workstream or removing its saved assignment updates that link.

Choose **Snooze** from the Workstream menu and pick a preset or a custom time. The Workstream and its members leave the active sidebar together. Expand **Snoozed** and select the Workstream to bring it back early. A thread that wakes while its Workstream is still snoozed is displayed outside the group until the Workstream wakes. If you move the thread to another Workstream in the meantime, that assignment takes effect immediately. A thread’s own snooze deadline still applies.

Deleting permanently removes the Workstream and clears its saved assignments. The threads remain in Jones Code.

Each Workstream initially shows five threads. In its options menu, choose **5**, **10** or **15** under **Visible threads** to save that Workstream’s default on this client. **Show 4 more**, for example, reveals the four remaining threads that match the current filters. **Show fewer** returns to the saved default, as does closing and reopening the group. The header retains the full thread count. Unassigned has no display limit. Conversation shortcuts follow the visible sidebar order and skip hidden Workstreams.

Threads can be reordered within Workstreams and Unassigned when their servers support active-thread reordering and the Working section is off. With the Working section enabled, automatic ordering applies; dragging into another group still changes the assignment.

Click a Workstream’s Working, Needs input or Done count to show that status. Click the selected count again to use the global status filter, or double-click a count to exclude that status. A local choice overrides the global status filter while keeping project, environment and search scope. These choices are saved on this client.

The **Workstreams** row above the groups totals the Working, Needs input and Done counts of all Workstreams, without Unassigned. Its counts work the same way but apply to every Workstream at once. The most recent choice wins: pick a status on one group after using the Workstreams row and that group keeps its own; use the Workstreams row again and it replaces every group’s choice. Clearing a group’s status selection returns that group to the global status filter even while the Workstreams row has a status selected. Clearing the Workstreams row returns every group to the global status filter, and earlier group choices do not come back.

Use **+** and **−** beside the Workstreams label to expand or collapse every group’s thread list. Each group’s title, counts and failures stay visible. Click **Workstreams**, the space between controls or the chevron at the far right to hide or show the named groups. Unassigned stays visible, and showing the Workstreams again restores each group as you left it.

Drag a thread card or the active thread’s title in the chat header into the composer to add a thread mention. When you drag the header title into the sidebar, the preview becomes a card. Drop it where the sidebar shows an insertion point, or on a Workstream to move it. Sidebar drops use the same ordering rules as dragging a card.

Drag a Workstream name from the web or desktop sidebar into the conversation or composer to add it to your draft. The drop includes mentions for its unsettled threads available in the sidebar’s environments, including collapsed or filtered members. To include an excerpt from a specific thread, drag that thread by itself.

Assignments are saved on the environment that owns each thread. Clients connected to that environment see confirmed changes. Moving a thread requires a writable connection to its owner. With no Workstream authority configured, definitions sync when a writable client connects to both copies, and closing requires all configured environments to be reachable.

### Save Workstream changes while other environments are offline

On the laptop, link your other environments in **Connections** with write access. Open **Settings → Jones Code → Workstreams** and select **Use [laptop name] as authority**. The setting lists the linked environments it will enroll. Open Workstreams settings on each receiving environment and approve the exact link from the laptop. You can enroll another machine or refresh enrollment after a link changes from the same Workstreams settings.

The laptop saves changes to groups as soon as you make them. Its server delivers pending changes when enrolled environments respond, including while the user interface is closed if the laptop server is still running. Changes to a group from a phone or another computer go through the laptop. If the laptop server is unreachable, group edits require a connection to it. Each thread’s existing assignment still belongs to its own environment.

When the laptop saves a **Completed** or **Abandoned** request, the group leaves the web and desktop sidebar. Active threads remain visible under **Unassigned** and keep their saved assignments. The laptop blocks new assignments to the group while enrolled environments confirm that their threads are settled. Open **Settings → Workflows → Workstreams** to see which environments are still waiting, which have active threads, or to cancel the request. The group appears under **Closed Workstreams** once the laptop confirms the close. On mobile, open the group's details to see its pending close. If the enrollment list changes while closing, cancel and submit a new request for the current list.

Workstreams settings shows delivery failures, required updates and differing definitions. Import any groups that exist only on an enrolled environment. A closed group imports as Active so each environment can check its membership when you close it. Resolve a definition conflict there to choose which version the laptop should sync. Deleting a group clears assignments on each environment as that environment receives the deletion. To use an enrolled environment independently again, remove it from sync on the laptop, then select **Stop following** on the receiving environment.

If a move reports that the thread changed elsewhere, check its current group before trying again. If a change cannot be confirmed, it may have been saved. Check the group before retrying.

Agents can list and create Workstreams with `t3_workstreams_list` and `t3_workstream_create`, read or change a thread’s assignment with `t3_thread_workstream_read` and `t3_thread_workstream_set`, and edit group details with `t3_workstream_update`. Retain the same caller-chosen ID when retrying creation. Read the current assignment revision before changing it. These tools use ordinary environment permissions and can target a linked environment. Use the app to mark an existing Workstream Completed or Abandoned so it can check all configured environments.

The CLI provides `t3 workstream list`, `create`, `rename`, `recolor`, `reorder`, `delete`, `assign`, `unassign` and `read`. Use `--json` for machine-readable output. The CLI talks to the selected running server using ordinary authentication.

Historical registry memberships, proofs, edges and exports are not displayed by this feature. Existing encrypted history and keys are preserved separately; this does not make that history readable in the new client.

## Automatic assignment for queued prompts

When a prompt arrives from a work queue, such as Jarvis, for a conversation that has never been in a Workstream, the server may assign that conversation to a Workstream. It does so only when one Workstream is clearly the right place, judged from Workstream names and the titles of the conversations already in them. When the match is unclear or two Workstreams are plausible, the conversation is left unassigned. Storage is never chosen.

Conversations you start or message yourself are not assigned automatically, and neither are delegated tasks or subagents. A conversation that has ever been in a Workstream, including one you removed, is never assigned automatically again.

The Workstream picker identifies automatic assignments. To change one, move or remove the conversation as usual. That prevents future automatic assignment for the same conversation and helps later choices. Agents can read the latest decision in the `autoRoute` field of `t3_thread_workstream_read`.

The server uses the configured text-generation model through a text-only request. Supported routes are configured text generators and managed Codex with existing tool-free access. If the selected model has no supported route, the conversation stays unassigned. The request includes limited excerpts of the prompt and titles, the project and Workstream names, and recent Workstream corrections. The decision record stores IDs and outcome codes. Turn routing off in Settings → Jones Code → **Assign queued prompts to a Workstream automatically**.
