import assert from "node:assert/strict";
import test from "node:test";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { createInterface } from "node:readline";
import { lstat, mkdir, readFile, readdir, writeFile, symlink } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  assertVisible,
  assertInactiveDenied,
  assertBrowserDisabledDenied,
  atomicJson,
  redact,
  scenarioFromPrompt,
  waitFile,
} from "./install-preview-agent.mjs";
import {
  hashTree,
  isolatedEnvironment,
  readRendererManifest,
  serveRendererFixture,
  stopChild,
  withOwnedScratch,
} from "./install-preview-qualification.mjs";

const source = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

test("native ACP framing retains the exact scenario through T3's instruction wrapper", () => {
  const scenario = { mode: "active", steps: [{ action: "wait", text: "Jones main" }] };
  const marker = `JONES_PREVIEW_SCENARIO:${Buffer.from(JSON.stringify(scenario)).toString("base64")}`;
  assert.deepEqual(
    scenarioFromPrompt([
      {
        type: "text",
        text: `<t3_code_instructions>\nNative context\n</t3_code_instructions>\n\n<user_request>\n${marker}\n</user_request>`,
      },
    ]),
    scenario,
  );
  assert.throws(
    () => scenarioFromPrompt([{ type: "text", text: JSON.stringify(scenario) }]),
    /scenario marker/,
  );
});

test("a deliberately wrong renderer assertion cannot pass", () => {
  const snapshot = { visibleText: "Jones main · Setup required" };
  assertVisible(snapshot, "Setup required");
  assertVisible(snapshot, "Committed", true);
  assert.throws(() => assertVisible(snapshot, "Committed"), /Missing visible text/);
  assert.throws(() => assertVisible(snapshot, "Setup required", true), /Unexpected visible text/);
  assert.throws(
    () => assertVisible({ screenshotPath: "/owned/image.png" }, "Setup required"),
    /Missing visible text/,
  );
});

test("inactive-provider qualification rejects unrelated failures and successful calls", async () => {
  for (const response of [
    undefined,
    { isError: false },
    { isError: true, content: [{ type: "text", text: "browser crashed" }] },
  ]) {
    await assert.rejects(assertInactiveDenied({ callTool: async () => response }));
  }
  await assert.rejects(
    assertInactiveDenied({
      callTool: async () => {
        throw new Error("socket closed");
      },
    }),
  );
  await assert.rejects(
    assertInactiveDenied({
      callTool: async () => {
        throw new Error("preview failed after 4010 milliseconds");
      },
    }),
  );
  assert.equal(
    await assertInactiveDenied({
      callTool: async () => ({
        isError: true,
        content: [{ type: "text", text: "parent_not_active" }],
      }),
    }),
    "tool authorization refused",
  );
  assert.equal(
    await assertInactiveDenied({
      callTool: async () => {
        throw new Error("HTTP 401 Unauthorized");
      },
    }),
    "transport authorization refused",
  );
});

test("disabled-browser qualification requires the native preview capability refusal", () => {
  for (const result of [
    undefined,
    { isError: false },
    {
      isError: true,
      content: [{ type: "text", text: "PreviewAutomationUnavailableError: Chrome launch failed" }],
    },
  ]) {
    assert.throws(() => assertBrowserDisabledDenied(result));
  }
  assertBrowserDisabledDenied({
    isError: true,
    content: [
      {
        type: "text",
        text: "MCP credential does not grant the preview capability: browser preview tools are off for this thread.",
      },
    ],
  });
});

test("renderer manifest refuses a stale source revision, tree or undeclared boundary", async () => {
  await withOwnedScratch(os.tmpdir(), async (root) => {
    const manifest = {
      schema: "jones-install-renderer-fixture/v1",
      sourceSha: "a".repeat(40),
      sourceTree: "b".repeat(40),
      entry: "jones-install-fixture.html",
      boundary:
        "Production renderer components with synthetic desktop bridge; no Electron IPC or native installation",
    };
    const file = path.join(root, "jones-install-fixture-manifest.json");
    await atomicJson(file, manifest);
    assert.deepEqual(
      await readRendererManifest(root, manifest.sourceSha, manifest.sourceTree),
      manifest,
    );
    await assert.rejects(
      readRendererManifest(root, "c".repeat(40), manifest.sourceTree),
      /candidate source revision/,
    );
    await assert.rejects(
      readRendererManifest(root, manifest.sourceSha, "d".repeat(40)),
      /candidate source tree/,
    );
    await atomicJson(file, { ...manifest, boundary: "Native installation verified" });
    await assert.rejects(readRendererManifest(root, manifest.sourceSha, manifest.sourceTree));
  });
});

test("owned scratch removes only its exact directory after success, failure and cancellation", async () => {
  await withOwnedScratch(os.tmpdir(), async (parent) => {
    const sibling = path.join(parent, "preserved");
    await writeFile(sibling, "keep");
    for (const result of ["success", "failure", "cancellation"]) {
      let owned;
      const run = withOwnedScratch(parent, async (root) => {
        owned = root;
        await mkdir(path.join(root, "nested"));
        await writeFile(path.join(root, "nested/evidence"), result);
        if (result !== "success") throw new Error(result);
      });
      if (result === "success") await run;
      else await assert.rejects(run, new RegExp(result));
      await assert.rejects(lstat(owned), { code: "ENOENT" });
      assert.equal(await readFile(sibling, "utf8"), "keep");
    }
  });
});

test(
  "atomic milestone observation closes watchers for existing, newly written and cancelled files",
  { timeout: 10_000 },
  async () => {
    await withOwnedScratch(os.tmpdir(), async (root) => {
      const file = path.join(root, "milestone.json");
      const waiting = waitFile(file, AbortSignal.timeout(5_000));
      await atomicJson(file, { ready: true });
      assert.deepEqual(await waiting, { ready: true });
      assert.deepEqual(await waitFile(file, AbortSignal.timeout(5_000)), { ready: true });
      const cancellation = new AbortController();
      const pending = waitFile(path.join(root, "absent.json"), cancellation.signal);
      cancellation.abort(new Error("owned cancellation"));
      await assert.rejects(pending, /owned cancellation|aborted/i);
      assert.deepEqual(await readdir(root), ["milestone.json"]);
    });
  },
);

test("unresolved process or evidence cleanup retains its owned scratch without masking the failure", async () => {
  await withOwnedScratch(os.tmpdir(), async (parent) => {
    let owned;
    const original = Object.assign(new Error("native cleanup failed"), { retainScratch: true });
    await assert.rejects(
      withOwnedScratch(parent, async (root) => {
        owned = root;
        await writeFile(path.join(root, "evidence"), "preserve");
        throw original;
      }),
      (error) => error === original && error.cleanupError.includes(owned),
    );
    assert.equal(await readFile(path.join(owned, "evidence"), "utf8"), "preserve");
  });
});

test("artifact identity detects a changed served bundle and refuses symlink traversal", async () => {
  await withOwnedScratch(os.tmpdir(), async (root) => {
    await writeFile(path.join(root, "index.html"), "first");
    const before = await hashTree(root);
    await writeFile(path.join(root, "index.html"), "second");
    assert.notDeepEqual(await hashTree(root), before);
    await symlink(path.join(root, "index.html"), path.join(root, "linked.html"));
    await assert.rejects(hashTree(root), /contains a symlink/);
  });
});

test("fixture environment and evidence exclude ambient credentials", () => {
  const environment = isolatedEnvironment("/owned/fixture");
  assert.equal(environment.HOME, "/owned/fixture/home");
  assert.equal(environment.T3CODE_SERVER_BROWSER_SANDBOX, "1");
  assert.deepEqual(
    Object.keys(environment).sort(),
    [
      "CI",
      "HOME",
      "LANG",
      "NO_COLOR",
      "PATH",
      "T3CODE_DISABLE_TELEMETRY",
      "T3CODE_SERVER_BROWSER_SANDBOX",
      "TMPDIR",
      "XDG_CACHE_HOME",
      "XDG_CONFIG_HOME",
      "XDG_DATA_HOME",
    ].sort(),
  );
  assert.equal(
    redact("Bearer own-session token=PAIRCODE wsTicket=WS own-secret", ["own-secret"]),
    "Bearer [redacted] token=[redacted] wsTicket=[redacted] [redacted]",
  );
});

test("owned renderer serving records byte identity and rejects paths outside its output", async () => {
  await withOwnedScratch(os.tmpdir(), async (parent) => {
    const root = path.join(parent, "renderer");
    await mkdir(root);
    await writeFile(path.join(root, "page.html"), "<title>Owned renderer fixture</title>");
    await writeFile(path.join(parent, "outside"), "must not serve");
    await symlink(path.join(parent, "outside"), path.join(root, "outside"));
    const server = await serveRendererFixture(root);
    try {
      const response = await fetch(`${server.origin}/page.html`);
      assert.equal(response.status, 200);
      assert.match(await response.text(), /Owned renderer fixture/);
      assert.equal((await fetch(`${server.origin}/outside`)).status, 404);
      assert.equal((await fetch(`${server.origin}/page.html`, { method: "POST" })).status, 404);
      assert.deepEqual(
        server.evidence().requests.map(({ path }) => path),
        ["page.html"],
      );
      assert.match(server.evidence().requests[0].sha256, /^[a-f0-9]{64}$/);
    } finally {
      await server.close();
    }
  });
});

test(
  "real verifier process refuses an ACP session without native T3 MCP injection",
  { timeout: 15_000 },
  async () => {
    await withOwnedScratch(os.tmpdir(), async (root) => {
      const report = path.join(root, "report.json");
      const child = spawn(
        process.execPath,
        [path.join(source, "scripts/jones/updates/install-preview-agent.mjs")],
        {
          env: {
            ...isolatedEnvironment(root),
            JONES_PREVIEW_SOURCE: source,
            JONES_PREVIEW_REPORT: report,
            JONES_PREVIEW_RELEASE: path.join(root, "release.json"),
          },
          stdio: ["pipe", "pipe", "pipe"],
        },
      );
      const lines = createInterface({ input: child.stdout });
      const pending = new Map();
      let next = 0;
      let stderr = "";
      child.stderr.on("data", (bytes) => {
        stderr += bytes;
      });
      lines.on("line", (line) => {
        const response = JSON.parse(line);
        pending.get(response.id)?.(response);
        pending.delete(response.id);
      });
      const request = (method, params) =>
        new Promise((resolve, reject) => {
          const id = ++next;
          const timer = setTimeout(() => {
            pending.delete(id);
            reject(new Error(`ACP ${method} timeout`));
          }, 5_000);
          pending.set(id, (value) => {
            clearTimeout(timer);
            resolve(value);
          });
          child.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", id, method, params })}\n`);
        });
      try {
        await once(child, "spawn");
        const initialized = await request("initialize", {
          protocolVersion: 1,
          clientCapabilities: {},
        });
        assert.equal(initialized.result.protocolVersion, 1);
        const created = await request("session/new", { cwd: root, mcpServers: [] });
        const scenario = Buffer.from(JSON.stringify({ mode: "active", steps: [] })).toString(
          "base64",
        );
        const result = await request("session/prompt", {
          sessionId: created.result.sessionId,
          prompt: [{ type: "text", text: `JONES_PREVIEW_SCENARIO:${scenario}` }],
        });
        assert.match(result.error.message, /Normal T3 MCP injection is required/);
        const final = await waitFile(`${report}.final`, AbortSignal.timeout(5_000));
        assert.equal(final.outcome, "failed");
        assert.equal(final.providerPid, child.pid);
        assert.equal(final.transportCreated, false);
        assert.equal(final.transportClosed, false);
        assert.equal(final.mcpBridgePid, undefined);
        assert.deepEqual(final.calls, []);
        assert.equal(stderr, "");
      } finally {
        child.stdin.end();
        await stopChild(child);
        lines.close();
      }
      assert.ok(child.exitCode !== null || child.signalCode !== null);
    });
  },
);
