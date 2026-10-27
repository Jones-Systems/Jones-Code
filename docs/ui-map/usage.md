# Usage metrics

[Map index and shared recipes](README.md). This entry covers the shared
web/desktop renderer's totals and model breakdown, not every usage-page flow.
The `/usage` route mounts `UsagePage`; Command Palette usage actions navigate
there, and `AppSidebarLayout` recognizes the route. Those entry paths and the
native desktop shell require separate runtime checks.

## Changed journey and expected results

Use task-owned synthetic usage summaries with multiple providers and different
cost/token proportions. At a normal desktop content width, the seven totals
remain on one row: processed, cached input, uncached input, cache write, output,
cache savings and Cache %. Cache % is cached input divided by processed tokens;
processed tokens include uncached input, reads, writes and output. Empty totals
show an em dash for the percentage.

The model name opens its dialog; the adjacent inline chevron independently
opens token details. Switching Cost/Tokens changes sorting and the bar, while
both percentages remain visible. Provider focus rescales each share within that
provider. Unpriced model cost percentages remain unknown. Below a 42rem table
container, percentages stack below their amounts. Totals and legends wrap whole
items when necessary. Provider/model details and model dialogs expose cache
write; zero reads as `0 reported`, because zero cannot establish coverage.
Close details, close the dialog with Escape, and confirm focus returns.

## Focused verification

Checked on **2026-10-21** using T3 Browser and production usage components with
synthetic summaries. The fixture mounted the actual totals, table header/rows,
provider details, model dialog and share bar, with their existing Tailwind styles.
Its small shell and sorting/provider controls were fixture-owned; the full app
route/backend and installed clients were not exercised. No private usage state
was read. A fixture CSS discovery issue was isolated by compiling the production
stylesheet and candidates with the installed Tailwind compiler, without changing
production styles.

The source base was `7ae8aaa1d1d592fe633ace84f503f27b698d9041`. The combined
web-source patch against that base has SHA-256
`abc8ea85bcc5329b9efe5c4e975dd95513f463978ba6b9f91d95c9b764c981c9`.
This is source/dev-renderer correspondence, not a packaged-build attestation.
Viewports were 1111×800, 760×800, 380×800 and 320×800; device scale 2,
dark class, default text size. At 1111 and 760, all totals occupied one row.
At narrow sizes they wrapped without horizontal overflow. All chevrons were
inline. Changing the sort preserved both percentages and an open expansion.
The expansion exposed zero writes as `0 reported`; provider details and the
actual model dialog exposed positive writes. Escape closed the dialog and
returned focus to its model button. Browser captures are retained with the
change's PR/thread evidence, outside source control.

The focused automated usage group passed 25 tests, including independent share
calculations, provider focus, empty totals and the all-token cache denominator.
Web typecheck and targeted lint passed. The strict boundary check reported four
unrelated missing asset imports; no usage boundary violations were reported.
Native shells/mobile, real multi-environment transports, route entry navigation,
text enlargement and complete accessibility coverage remain unverified.
