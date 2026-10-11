# Proposal — Selectable Workstream border colors

<!-- codex-section:begin id="brief.selectable-workstream-colors#ctx.artifact-header.001" -->
Artifact Type: `design-brief`

Artifact ID: `brief.selectable-workstream-colors`

Purpose: Preserve the owner's requested color customization until Jones upstream isolation is complete.

Governing artifact: Owner request in T3 conversation “Thread Grouping Compared to Workstreams”

Decision owner: M Jones

Consumers: Jones extraction owner, future appearance feature builder, and product owner

Authority effect: none
<!-- codex-section:end id="brief.selectable-workstream-colors#ctx.artifact-header.001" -->

<!-- codex-section:begin id="brief.selectable-workstream-colors#req.product.001" -->
## Requested behavior

The current five automatically assigned tints are insufficient. The owner wants
to choose Workstream border colors deliberately, including the full rainbow,
rather than have a Workstream's identifier decide its appearance. The owner
clarified that project/workspace coloring is not requested for this proposal.

- Add **Color…** to the three-dot menu on the named Workstream group header.
- Offer a broad preset palette covering red, orange, yellow, green, cyan, blue,
  indigo, violet, pink, brown, and neutral shades, with several shades where
  useful. Five fixed choices do not satisfy this request.
- Allow custom selection from the full RGB color range, using a picker and a
  hex value entry. Presets are shortcuts rather than the limit of selection.
- Show the current choice with a selected marker and an accessible name. Show
  a preview before saving; dismissing or cancelling preserves the saved color.
- Provide **Reset to automatic** to restore the stable default tint.
- Apply the selected color consistently to the group's border in expanded and
  collapsed states. Keep the existing background treatment; keep names, text,
  focus outlines, and activity/failure indicators readable and distinguishable.
- Preserve the selection across reloads and client reconnections. Authorized
  clients viewing the same Workstream should observe the same saved choice.
- Support the equivalent selection flow on web/desktop and mobile, with a
  keyboard-accessible menu and picker on web/desktop.

Scope is Workstream borders only. Native repository/project workspace colors,
custom group backgrounds, text colors, icons, and unrelated appearance settings
remain outside this request.
<!-- codex-section:end id="brief.selectable-workstream-colors#req.product.001" -->

<!-- codex-section:begin id="brief.selectable-workstream-colors#req.dependency.001" -->
## Implementation hold: finish upstream isolation first

This is a documentation-only draft proposal. Do not begin feature
implementation or merge this proposal as a delivered color feature while the
Jones additions extraction remains in progress.

The dependency owner is the T3 conversation **“Isolate Additions for Upstream
Updates”**, native ID `c862418a-2192-4bae-b54c-2c23bda67174`. Its governing plan
is `spec.jones-code-upstream-extraction`. The observed extraction stack is
[#166](https://github.com/Jones-Systems/Jones-Code/pull/166) →
[#167](https://github.com/Jones-Systems/Jones-Code/pull/167) →
[#168](https://github.com/Jones-Systems/Jones-Code/pull/168) →
[#171](https://github.com/Jones-Systems/Jones-Code/pull/171), registered as native
stack #170. These references identify current work; merging that stack alone
does not prove that all remaining extraction is complete.

Release the implementation hold only after:

1. The extraction owner records that the intended isolation work is complete,
   including any explicitly retained boundaries and exceptions, and identifies
   the integrated repository revision and accepted ownership inventory.
2. The future builder rechecks current Workstream owners,
   open PRs, shared contracts, persistence, generated outputs, and integration
   hooks at that revision. Active collisions must be resolved before writes.
3. The feature is assigned to the extracted Jones-owned extension boundaries.
   Shared upstream edits are limited to accepted narrow hooks and contract
   seams. Do not resurrect retired paths, create duplicate owners, or move
   feature implementation back into upstream-owned UI modules.

This future feature depends on extraction; extraction, upstream updates, the
ongoing rebuild, and build adoption do not acquire a dependency on this feature.
The old implementation paths are historical evidence, not future writer cones.
Storage ownership and the final contract are chosen only after the extraction
boundaries are verified. This proposal makes no promise of conflict-free future
merges without that readback.
<!-- codex-section:end id="brief.selectable-workstream-colors#req.dependency.001" -->

<!-- codex-section:begin id="brief.selectable-workstream-colors#ac.behavior.001" -->
## Future acceptance

- The owner can use the group menu to choose any rainbow preset or a custom
  color, see the saved choice, cancel an unsaved change, and reset to automatic.
- Two Workstreams can deliberately share a color; renaming or reordering a
  Workstream does not replace its selected color.
- Saved choices persist across reloads and appear consistently on supported
  authorized clients. Read-only clients display colors without offering writes.
- Light/dark mode, expanded/collapsed groups, keyboard focus, running, waiting,
  and failure states remain readable. Color is never the only status signal.
- Existing Workstreams without a saved choice retain a compatible automatic
  fallback. Older clients and server versions degrade through the accepted
  compatibility boundary rather than failing to load groups.
- Color changes leave membership, lifecycle, pinning, settlement, repository,
  worktree identity, and execution behavior unchanged.
- Project/workspace colors and the group's existing background treatment are
  unaffected by choosing a Workstream border color.
- The implementation has focused behavior checks, before/after UI evidence,
  and the accepted extraction boundary checks at its exact candidate revision.

These are future acceptance requirements, not completed functionality or test
results. This PR changes no application code, schemas, dependencies, migrations,
runtime configuration, or installed application.
<!-- codex-section:end id="brief.selectable-workstream-colors#ac.behavior.001" -->
