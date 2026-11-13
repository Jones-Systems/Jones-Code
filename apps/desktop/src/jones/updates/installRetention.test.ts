// @effect-diagnostics nodeBuiltinImport:off - Serial disposable Python/Node protocol fixtures.
import * as NodeChildProcess from "node:child_process";
import * as NodeURL from "node:url";
import * as NodeProcess from "node:process";
import { describe, it } from "vite-plus/test";

describe("install and runtime retention protocol", () => {
  it.each([
    { group: "database admission", filter: "--test-name-pattern=^DBR T[1-4]" },
    { group: "remaining recovery protocols", filter: "--test-skip-pattern=^DBR T[1-4]" },
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
