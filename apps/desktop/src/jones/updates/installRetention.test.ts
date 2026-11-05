// @effect-diagnostics nodeBuiltinImport:off - Serial disposable Python/Node protocol fixtures.
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { execPath } from "node:process";
import { describe, it } from "vite-plus/test";

describe("install and runtime retention protocol", () => {
  it("preserves recovery closure and refuses destructive drift at real entry points", () => {
    execFileSync(
      execPath,
      [
        "--test",
        "--test-concurrency=1",
        fileURLToPath(
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
