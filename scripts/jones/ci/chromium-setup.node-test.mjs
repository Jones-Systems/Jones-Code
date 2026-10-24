import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeTest from "node:test";

const workflow = NodeFS.readFileSync(
  new URL("../../../.github/workflows/ci.yml", import.meta.url),
  "utf8",
);

NodeTest.test("CI avoids unconditional Chromium apt work and duplicate Test apt updates", () => {
  NodeAssert.doesNotMatch(workflow, /install --with-deps --only-shell chromium/);
  const packageJob = workflow.split("\n  test:\n")[1].split("\n  test_web:\n")[0];
  NodeAssert.equal(packageJob.match(/sudo apt-get update/g)?.length, 1);
  NodeAssert.match(packageJob, /build-essential libsecret-1-dev pkg-config/);
  NodeAssert.match(packageJob, /command -v cc/);
});

const { ensureChromium, resolvePlaywright } = await import("./chromium-setup.mjs");

function scenario(failures, repairError) {
  const events = [];
  let launches = 0;
  return {
    events,
    options: {
      chromium: {
        async launch(options) {
          NodeAssert.deepEqual(options, { headless: true, timeout: 30_000 });
          events.push("launch");
          if (launches++ < failures) throw new Error("browser unavailable");
          return {
            async close() {
              events.push("close");
            },
          };
        },
      },
      async installDependencies() {
        events.push("install-deps");
        if (repairError) throw repairError;
      },
      report() {
        events.push("diagnostic");
      },
    },
  };
}

NodeTest.test("a working browser closes without installing system dependencies", async () => {
  const { events, options } = scenario(0);
  await ensureChromium(options);
  NodeAssert.deepEqual(events, ["launch", "close"]);
});

NodeTest.test("failed launch repairs once and verifies a real second launch", async () => {
  const { events, options } = scenario(1);
  await ensureChromium(options);
  NodeAssert.deepEqual(events, ["launch", "diagnostic", "install-deps", "launch", "close"]);
});

NodeTest.test("an unsuccessful repair remains a failure without looping", async () => {
  const { events, options } = scenario(2);
  await NodeAssert.rejects(ensureChromium(options), /browser unavailable/);
  NodeAssert.deepEqual(events, ["launch", "diagnostic", "install-deps", "launch"]);
});

NodeTest.test("installer failure is preserved without attempting another browser", async () => {
  const failure = new Error("installer failed");
  const { events, options } = scenario(1, failure);
  await NodeAssert.rejects(ensureChromium(options), (error) => error === failure);
  NodeAssert.deepEqual(events, ["launch", "diagnostic", "install-deps"]);
});

NodeTest.test("unsupported workspaces fail before loading a package", () => {
  NodeAssert.throws(() => resolvePlaywright("apps/other"), /Unsupported Chromium workspace/);
});
