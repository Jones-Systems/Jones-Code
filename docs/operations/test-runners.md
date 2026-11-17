# Test runners

`vp test run` finds Vitest files only. A standalone test runs only where a workflow or
package script names it, so name new ones by the pattern below.

| Kind              | Filename pattern                           | Runner command                             | Where CI runs it                                                                       |
| ----------------- | ------------------------------------------ | ------------------------------------------ | -------------------------------------------------------------------------------------- |
| Vitest            | `*.test.{ts,tsx}`                          | `vp test run <files>`                      | The package test jobs of `ci.yml`, chosen by changed files                             |
| Browser (web)     | `*.browser.test.{ts,tsx}` in `apps/web`    | `vp test run <files>`                      | `Test Web` jobs                                                                        |
| Node standalone   | `*.node-test.{mjs,cjs}`                    | `node --test --test-concurrency=1 <files>` | As the manifest's `kind` says: `ci` via `ci.yml`, `advisory` in another workflow       |
| Node, older style | `*.test.{mjs,cjs}` that import `node:test` | `node --test --test-concurrency=1 <files>` | The same. New files use `*.node-test.*`, which Vitest's default include does not match |
| Python            | `*.test.py`                                | `python3 -B <file>`                        | The same                                                                               |
| Source probe      | `*.source-test.mjs`                        | `node <file> <arguments>`                  | Not run; a person runs it with the arguments its manifest reason names                 |

`scripts/jones/ci/standalone-tests.json` classifies every standalone test by glob as
`ci`, `advisory` or `manual`. A `ci` or `advisory` entry names the runner file, which
must contain the glob or every test it matches; a `manual` entry gives the reason it has
no runner. A `ci` test is reachable from a `ci.yml` job, directly or through a workspace
package `test` script or a Vitest test that CI runs; an `advisory` test runs in another
workflow. A `ci` or `advisory` runner step must run unconditionally: the check reads text
and cannot see a step that an `if` or shell condition skips.
`scripts/jones/ci/test-inventory.node-test.cjs` runs in the `Test` job and fails on an
unclassified or stale entry, and on a workflow or package script that names a test path
that does not exist.
