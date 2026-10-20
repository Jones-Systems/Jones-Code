# Jones Code UI map

Maintainer verification documentation owned by Jones Code contributors. Start
from [UI evidence](../operations/contributor-guidance.md#ui-evidence), then read
the affected entry below. Reusable principles remain in the maintained
[Frontend UI view-map guide](https://github.com/Jones-Systems/Frontend-Development/blob/main/docs/guides/ui-view-maps.md)
(access required); no private guide content or runtime assets are imported here.

## Coverage and discovery

This is a **partial** map of the shared web/desktop renderer's thread/chat and
settings routes. It records known entry paths within that boundary, not all
Jones Code views or every setting's behavior. Pull requests, usage, conversations,
work queues, design requests and native mobile screens are not mapped. Native
shells, provider execution, remote/relay/tunnel modes and authenticated external
services need separate checks; a shared-renderer result cannot qualify them.

| View | States and entry paths | Shared consumers | Evidence |
| --- | --- | --- | --- |
| [Thread/chat](thread-chat.md) | Index/empty, draft, server thread, loading/missing; direct routes, sidebar, palette/shortcuts, links, notifications and reopen | Draft and server routes share ChatView; sidebar, composer, timeline and right-panel consumers are linked in the entry | Source-discovered; runtime `unverified` |
| [Settings](settings.md) | General and all discovered settings siblings; direct routes, navigation/search, scope selectors and contextual links | Shared shell, settings rows/search targets and workspace header/container consumers are linked in the entry | Source-discovered; runtime `unverified` |

Discovery checked on **2026-10-20** against Jones Code source
`e7527d88ef438daa443e49a84d59e9891415918b`: routes, navigation callers, shared
components, development instructions and verification skills. The central
Frontend guide/template and its AGENTS, README and CUSTODY were read at
`3c34eefbf9aaa5c92fe7c7e0c8c165d68a197e84`, resolved from canonical `main`.
This is a source-guidance consumption receipt, not installed-runtime adoption.
Recheck affected entries after their source or harness changes.

## Common environment and recipes

The [development guide](../operations/development.md) owns setup, state and
ports; [test-t3-app](../../.agents/skills/test-t3-app/SKILL.md) owns Browser
interaction. Run commands from the repository root in the assigned linked
worktree. Only the primary verification owner starts a development server.

| Step | Recipe and expected result | Ownership and limits |
| --- | --- | --- |
| Prerequisites | Use the documented Node `^24.13.1` and `vp` setup. `vp i` is the documented dependency install when authorized. | Setup can access linked configuration; a docs task does not authorize installation or reading it. No general Browser doctor command is documented. |
| Launch | Reuse the task's healthy server, otherwise `vp run dev`. Read the actual web/server ports and base directory from `[dev-runner]` output; occupied ports may shift them. | Retain the terminal session/PID. A linked worktree defaults to its ignored `.t3/userdata`; confirm the resolved directory, including any explicit `--home-dir`. Leave `VITE_HTTP_URL` and `VITE_WS_URL` unset. |
| Pair/readiness | `node apps/server/src/bin.ts pair --base-dir <printed-base-directory>` issues a fresh private link. Call `preview_status`, `preview_open` if closed, then `preview_navigate` with that complete link. Use `preview_snapshot` to establish the expected page and connected environment. | Startup logs do not print the pairing URL. Prefer this explicit-base recipe over the skill's older startup-output wording. Use only the task's token; record no token in public evidence. A listener alone does not prove UI readiness. |
| Empty synthetic state | Start with the task-owned fresh home. At `/`, wait for shell bootstrap; use **Add project** with a disposable workspace, or **Start without a project** when offered. See the thread entry for outcomes. | Do not submit a provider turn merely to populate a screenshot. Provider-dependent states need their own authorized fixture and execution. |
| Rich synthetic fixtures | Follow [SQLite fixtures](../../.agents/skills/test-t3-app/references/sqlite-fixtures.md): start once for migrations, stop the owned server, inspect current V2 columns, and use the documented `t3-sqlite-state.ts` query/exec helper on the exact isolated base. Prefer rows created by the app and schema-valid synthetic payloads. | No canned full chat fixture is supplied here. Direct projection fixtures are visual-only, not event-history or backend-behavior proof. Do not read/copy live userdata, secrets or browser profiles. |
| Observe/capture | In the same Browser tab, use snapshot-derived roles/names and focused interactions. Follow [capture-ui-evidence](../../.agents/skills/capture-ui-evidence/SKILL.md); check visible outcomes and reload/backend readback where relevant. | The creator owns synthetic fixtures and captures. Retain exact candidate/build correspondence, route/state, viewport, scale/theme, text size, harness/version, date, expected/observed result and artifact location. Do not commit captures or credentials. |
| Reset/cleanup | Undo the journey's own reversible UI changes first. Keep the task server/state available for review; ending a turn is not teardown. Stop only its captured process/session. For a fresh scenario use a newly assigned isolated home via the documented `vp run dev --home-dir /absolute/task-owned/t3-home`. | No generic Browser database/profile reset command is documented. Never delete existing state to get an empty fixture. The creating owner retains evidence, then handles exact owned destinations under their agreed lifetime on success, failure or cancellation; uncertain ownership blocks cleanup. |

A missing Browser capability, usable synthetic fixture or safe reset blocks the
corresponding runtime check. Do not substitute another automation system or
another owner's server. Connections administration may require additional
explicit pairing scopes; follow [development authentication guidance](../operations/development.md#sharing-and-remote-debugging),
not live credentials. Sharing is a separately authorized route change.

For native behavior, follow the separately qualified Linux Electron route in
capture-ui-evidence or [test-t3-mobile](../../.agents/skills/test-t3-mobile/SKILL.md).
Neither is run or qualified by this map. Desktop-only Browser/Device controls
inside chat also need the appropriate host capability.

## Evidence and upkeep

The source/link review above passed only as documentation inspection. Runtime
journeys in both entries were **not run**: this docs-only lane has no launched
candidate, paired test tab or seeded fixture, and server launch belongs to the
primary verification owner. Runtime execution is `blocked` for this lane;
discovered behavior remains `unverified`. No build, viewport/device, harness
run, screenshot or observed interaction result exists for this record.

For a UI change, update affected paths and exercise the affected journeys plus
representative consumers of changed shared components. Preserve untested paths
explicitly. A full inventory/maintenance pass is separate work. Keep future
runtime records candidate-bound using the evidence fields above; a source read,
render-only test or screenshot alone is not interaction proof.
