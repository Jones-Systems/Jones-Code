const assert = require("node:assert/strict");
const { spawnSync } = require("node:child_process");
const { resolve } = require("node:path");
const { test } = require("node:test");

test("server E2E transport guards", { timeout: 40_000 }, () => {
  const root = resolve(__dirname, "../../..");
  const result = spawnSync(
    "python3",
    ["-B", "apps/server/integration/jones-e2e/physicalHost.transport.test.py"],
    { cwd: root, encoding: "utf8", timeout: 30_000 },
  );
  assert.ifError(result.error);
  assert.equal(result.signal, null, result.stderr);
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
});
