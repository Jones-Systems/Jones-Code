# Interactive design reviews

Use a review preview when the owner needs to compare or adjust an existing
surface before choosing how it should look or behave. Start with the affected
view in the [UI map](../ui-map/README.md). Build from the latest intended
product revision and identify that revision in the preview. Keep the real
component, theme tokens, fonts, icons, and controls. Supply synthetic data and
simulated responses at the component's existing boundaries.

Keep the preview small enough for the decision. For a visual adjustment, show
the fixed baseline and live candidate on the same theme, state, and viewport.
Change only the properties under review. For a workflow, include the
surrounding controls that affect the decision, named starting states, and a
way to restart. Put review controls outside the product surface. Provide a
copyable summary of the chosen variant, values or flow decisions, and source
revision. Retain selectable text for clients that deny clipboard access.

## Build the thread-card brightness preview

Use the repository's [development prerequisites](development.md#setup) in a
dedicated linked worktree. From a clean, committed candidate, run:

```sh
review_dir=$(mktemp -d)
node apps/web/scripts/build-design-review.mjs thread-card "$review_dir/thread-card.html"
```

The builder bundles the production web components, assets, and stylesheet into
an HTML file for the Jones Code thread viewer. It writes to a new file outside
the checkout, identifies the source commit, and restricts the page to embedded
resources. The review uses synthetic threads and
in-memory choices.

Before publishing, verify the rendered card and the intended interactions.
For this preview, use the slider, confirm only the candidate title changes,
reset to the baseline, and confirm the copied or selectable text reflects the
current value. Check a thread-width layout and a narrow layout. Call
`html_preview` with the file, then `html_render` in the requesting thread with
the observed height. Retain the source revision and result. Use the
[UI evidence route](contributor-guidance.md#ui-evidence) for interactive
verification. Remove task-owned scratch after preserving the required evidence.

## Reuse

For another small web review, add a named `*-entry.tsx` under
`apps/web/src/jones/designReview/`. Mount the relevant product components in
`#root` and use the same builder. Prefer an isolated preview of the actual
application for flows that need a running backend or a larger workspace.
For a native review, run the actual simulator build through the Device panel
and put the scenarios, evidence, and decision summary in the thread.
