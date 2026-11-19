// @effect-diagnostics nodeBuiltinImport:off - Serial disposable Python/Node protocol fixtures.
import * as NodeChildProcess from "node:child_process";
import * as NodeURL from "node:url";
import * as NodeProcess from "node:process";
import { describe, it } from "vite-plus/test";

// Keep serial subprocess groups inside the desktop test budget. The final
// group covers every case that the named database groups did not select.
const databaseGroups = [
  { group: "database admission", pattern: "^DBR T[134]:" },
  { group: "database retirement", pattern: "^DBR T1/T2:" },
  { group: "interrupted database retirement", pattern: "^DBR T5" },
  { group: "database recovery closure", pattern: "^DBR T[6-8]:" },
  { group: "database retirement CLI", pattern: "^DBR T2/T9:" },
];

describe("install and runtime retention protocol", () => {
  it.each([
    ...databaseGroups.map(({ group, pattern }) => ({
      group,
      filter: `--test-name-pattern=${pattern}`,
    })),
    {
      group: "install and runtime retention",
      filter: `--test-skip-pattern=${databaseGroups.map(({ pattern }) => pattern).join("|")}`,
    },
  ])("preserves recovery closure and refuses destructive drift ($group)", ({ filter }) => {
    NodeChildProcess.execFileSync(
      NodeProcess.execPath,
      [
        "--test",
        "--test-concurrency=1",
        filter,
        NodeURL.fileURLToPath(
          new URL(
            "../../../../../scripts/jones/updates/install-retention.node-test.mjs",
            import.meta.url,
          ),
        ),
      ],
      { encoding: "utf8", timeout: 45000, stdio: "pipe" },
    );
  });
});
