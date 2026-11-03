import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeURL from "node:url";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import {
  admitPreparation,
  assertPreparationIdentity,
  cleanupHistoricalPreparation,
  createHistoricalPreparation,
  finishHistoricalPreparation,
  inspectHistoricalToolchain,
  inspectPreparation,
  preparationEvidencePath,
  preservePreparationEvidence,
  runPreparationProcess,
  validatePreparationBounds,
} from "./historical-preparation.mjs";

const python = NodeFS.realpathSync("/usr/bin/python3");
const bounds = {
  maxBytes: 4 * 1024 * 1024,
  maxEntries: 1000,
  maxFileBytes: 1024 * 1024,
  maxCacheBytes: 1024 * 1024,
  maxOutputBytes: 128 * 1024,
  deadlineMs: 2000,
  terminateGraceMs: 100,
  reapTimeoutMs: 1000,
  maxProcesses: 32,
  estimatedPeakBytes: 10 * 1024 ** 3,
};
function fixture(t, overrides = {}) {
  const base = NodeFS.mkdtempSync(
    NodePath.join(NodeFS.realpathSync(process.env.TMPDIR ?? NodeOS.tmpdir()), "hp-"),
  );
  const parentPath = NodePath.join(base, "runs"),
    evidenceParent = NodePath.join(base, "evidence");
  NodeFS.mkdirSync(parentPath, { mode: 0o700 });
  NodeFS.mkdirSync(evidenceParent, { mode: 0o700 });
  const owner = createHistoricalPreparation({
    parentPath,
    evidenceParent,
    taskId: "synthetic-controls",
    runId: NodeCrypto.randomUUID(),
    bounds: { ...bounds, ...overrides },
    retention: "release-after-closed-evidence",
  });
  let retained = false;
  t.after(() => {
    const cleanup = cleanupHistoricalPreparation(owner, {
      evidencePreserved: true,
      consumerClosed: true,
    });
    if (!cleanup.absent && NodeFS.existsSync(owner.creation.root)) {
      retained = true;
      t.diagnostic(JSON.stringify({ retainedSyntheticRoot: base, cleanup }));
    }
    if (!retained) NodeFS.rmSync(base, { recursive: true });
  });
  return { owner, root: owner.creation.root, base, parentPath, evidenceParent };
}
const run = (owner, program, signal) =>
  runPreparationProcess(owner, {
    executable: python,
    args: ["-c", program],
    cwd: owner.creation.root,
    signal,
  });
function assertGone(report) {
  NodeAssert.equal(report.familyClosed, true);
  NodeAssert.equal(report.streamsClosed, true);
  NodeAssert.equal(report.reaped, true);
  for (const [pid] of report.identities)
    NodeAssert.equal(
      NodeFS.existsSync(`/proc/${pid}`),
      false,
      `owned family member ${pid} survived`,
    );
}

NodeTest.test(
  "missing finite allocation evidence and toolchain refuse before producers",
  async (t) => {
    for (const key of Object.keys(bounds))
      NodeAssert.throws(
        () => validatePreparationBounds({ ...bounds, [key]: undefined }),
        /required/,
      );
    NodeAssert.throws(
      () => validatePreparationBounds({ ...bounds, estimatedPeakBytes: 1 }),
      /10 GiB/,
    );
    const { owner } = fixture(t);
    await NodeAssert.rejects(inspectHistoricalToolchain(owner, {}, undefined), /verified node/);
    NodeAssert.equal(NodeFS.readdirSync(preparationEvidencePath(owner)).length, 1);
  },
);

NodeTest.test(
  "fresh reserve/aggregate admission refuses a producer without touching its output",
  async (t) => {
    const { owner, root } = fixture(t, { estimatedPeakBytes: Number.MAX_SAFE_INTEGER });
    NodeAssert.throws(() => admitPreparation(owner), { code: "storage_refused" });
    await NodeAssert.rejects(run(owner, "open('unexpected','w').write('bad')"), {
      code: "storage_refused",
    });
    NodeAssert.equal(NodeFS.existsSync(NodePath.join(root, "unexpected")), false);
  },
);

NodeTest.test(
  "serial success and failure close/reap and preserve original failure outside removed scratch",
  async (t) => {
    for (const code of [0, 17]) {
      const { owner, root } = fixture(t);
      const result = await run(owner, `import sys;print('proof');sys.exit(${code})`);
      NodeAssert.equal(result.outcome, code === 0 ? "success" : "failed");
      NodeAssert.equal(result.installerExitCode, code);
      assertGone(result);
      const finalized = finishHistoricalPreparation(owner, {
        originalError: code
          ? { code: "exit_17", message: "deliberate original failure" }
          : undefined,
        consumerClosed: true,
        value: result.outcome,
      });
      NodeAssert.equal(finalized.cleanup.absent, true);
      NodeAssert.equal(NodeFS.existsSync(root), false);
      NodeAssert.equal(
        JSON.parse(NodeFS.readFileSync(finalized.evidence.path)).originalError?.code,
        code ? "exit_17" : undefined,
      );
      NodeAssert.ok(NodeFS.existsSync(finalized.finalEvidence.path));
    }
  },
);

NodeTest.test(
  "deadline and bounded output stop an otherwise live family and drain both streams",
  async (t) => {
    const timed = fixture(t, { deadlineMs: 100 });
    const deadline = await run(timed.owner, "import time;time.sleep(100)");
    NodeAssert.equal(deadline.outcome, "deadline");
    assertGone(deadline);
    const limited = fixture(t, { maxOutputBytes: 256 });
    const output = await run(
      limited.owner,
      "import os,time;os.write(1,b'x'*4096);os.write(2,b'y'*4096);time.sleep(100)",
    );
    NodeAssert.equal(output.outcome, "output_limit");
    NodeAssert.ok(output.outputBytes > 256);
    NodeAssert.ok(Buffer.byteLength(output.stdout + output.stderr) <= 256);
    assertGone(output);
  },
);

NodeTest.test(
  "ordinary cancellation kills identity-qualified TERM-resistant descendants",
  async (t) => {
    const { owner, root } = fixture(t);
    const abort = new AbortController();
    const watcher = NodeFS.watch(root, (_event, name) => {
      if (name === "ready") abort.abort();
    });
    try {
      const result = await run(
        owner,
        "import os,signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);open('ready','w').close();time.sleep(100)",
        abort.signal,
      );
      NodeAssert.equal(result.outcome, "cancelled");
      assertGone(result);
    } finally {
      watcher.close();
    }
  },
);

NodeTest.test(
  "subreaper adopts double-forked detached closed-stdio survivors before declaring closure",
  async (t) => {
    const { owner } = fixture(t);
    const result = await run(
      owner,
      `import os,signal,time
if os.fork(): os._exit(0)
os.setsid()
if os.fork(): os._exit(0)
signal.signal(signal.SIGTERM,signal.SIG_IGN)
os.close(1);os.close(2)
time.sleep(100)
`,
    );
    NodeAssert.equal(result.outcome, "survivors");
    NodeAssert.ok(result.identities.length >= 2);
    assertGone(result);
  },
);

NodeTest.test(
  "link-aware consumption rejects sibling/parent escape; cleanup unlinks without following",
  (t) => {
    const { owner, root, base } = fixture(t);
    const canary = NodePath.join(base, "canary");
    NodeFS.writeFileSync(canary, "untouched");
    NodeFS.symlinkSync(canary, NodePath.join(root, "outside"));
    NodeAssert.throws(() => inspectPreparation(owner, { consumption: true }), /escapes/);
    const evidence = preservePreparationEvidence(owner, { proof: "refused external symlink" });
    const cleanup = cleanupHistoricalPreparation(owner, {
      evidencePreserved: true,
      consumerClosed: true,
    });
    NodeAssert.equal(cleanup.absent, true);
    NodeAssert.equal(NodeFS.readFileSync(canary, "utf8"), "untouched");
    NodeAssert.ok(NodeFS.existsSync(evidence.path));
  },
);

NodeTest.test(
  "private pnpm-style hardlinks are fully accounted; an external hardlink retains both errors",
  (t) => {
    const { owner, root, base } = fixture(t);
    const file = NodePath.join(root, "store", "module.js");
    NodeFS.writeFileSync(file, "module");
    NodeFS.linkSync(file, NodePath.join(root, "sources/history/module.js"));
    NodeAssert.ok(inspectPreparation(owner, { consumption: true }).bytes > 0);
    const external = NodePath.join(base, "external");
    NodeFS.linkSync(file, external);
    const result = finishHistoricalPreparation(owner, {
      originalError: { code: "original", message: "preserve this" },
      consumerClosed: true,
    });
    NodeAssert.equal(result.originalError.code, "original");
    NodeAssert.equal(result.cleanup.outcome, "retained");
    NodeAssert.equal(result.cleanup.error.code, "external_hardlink");
    NodeAssert.equal(NodeFS.readFileSync(external, "utf8"), "module");
    NodeAssert.ok(NodeFS.existsSync(result.evidence.path));
    // Reconcile the exact test-created external link, then let the original owner retry.
    NodeFS.unlinkSync(external);
    NodeAssert.equal(
      cleanupHistoricalPreparation(owner, { evidencePreserved: true, consumerClosed: true }).absent,
      true,
    );
  },
);

NodeTest.test(
  "inspection overflow and replaced root retain exact custody without deleting canaries",
  (t) => {
    const capped = fixture(t, { maxEntries: 20 });
    for (let i = 0; i < 20; i++)
      NodeFS.writeFileSync(NodePath.join(capped.root, `entry-${i}`), "x");
    const refused = cleanupHistoricalPreparation(capped.owner, {
      evidencePreserved: true,
      consumerClosed: true,
    });
    NodeAssert.equal(refused.outcome, "retained");
    NodeAssert.equal(refused.error.code, "entry_limit");
    for (let i = 0; i < 20; i++) NodeFS.unlinkSync(NodePath.join(capped.root, `entry-${i}`));
    const replaced = fixture(t);
    const moved = `${replaced.root}-captured`;
    NodeFS.renameSync(replaced.root, moved);
    NodeFS.mkdirSync(replaced.root);
    NodeFS.writeFileSync(NodePath.join(replaced.root, "canary"), "replacement");
    NodeAssert.throws(() => assertPreparationIdentity(replaced.owner), /identity changed/);
    NodeAssert.equal(
      cleanupHistoricalPreparation(replaced.owner, {
        evidencePreserved: true,
        consumerClosed: true,
      }).outcome,
      "retained",
    );
    NodeAssert.equal(
      NodeFS.readFileSync(NodePath.join(replaced.root, "canary"), "utf8"),
      "replacement",
    );
    NodeFS.unlinkSync(NodePath.join(replaced.root, "canary"));
    NodeFS.rmdirSync(replaced.root);
    NodeFS.renameSync(moved, replaced.root);
  },
);

NodeTest.test(
  "cross-invocation cancellation and cleanup leave the other owner's canary intact",
  async (t) => {
    const a = fixture(t),
      b = fixture(t);
    const canary = NodePath.join(b.root, "canary");
    NodeFS.writeFileSync(canary, "owned by b");
    const result = await run(a.owner, "raise SystemExit(9)");
    assertGone(result);
    NodeAssert.equal(
      cleanupHistoricalPreparation(a.owner, { evidencePreserved: true, consumerClosed: true })
        .absent,
      true,
    );
    NodeAssert.equal(NodeFS.readFileSync(canary, "utf8"), "owned by b");
  },
);

NodeTest.test(
  "lost supervisor retains unknown custody and separate durable evidence",
  async (t) => {
    const { owner, root } = fixture(t);
    await NodeAssert.rejects(
      run(owner, "import os,signal;os.kill(os.getppid(),signal.SIGKILL);os._exit(0)"),
      { code: "supervisor_lost" },
    );
    const result = finishHistoricalPreparation(owner, {
      originalError: { code: "supervisor_lost", message: "unknown family closure" },
      consumerClosed: true,
    });
    NodeAssert.equal(result.cleanup.outcome, "retained");
    NodeAssert.equal(result.cleanup.reason, "unknown_closure_or_evidence");
    NodeAssert.ok(NodeFS.existsSync(root));
    NodeAssert.ok(NodeFS.existsSync(result.evidence.path));
  },
);

NodeTest.test(
  "parent process exit closes the control pipe while supervisor drains adopted family",
  async (t) => {
    const { owner, root } = fixture(t);
    const evidenceRoot = preparationEvidencePath(owner);
    const receiptPath = NodePath.join(evidenceRoot, "disconnected.process.json");
    const requestPath = NodePath.join(evidenceRoot, "disconnected.request.json");
    const helper = NodeURL.fileURLToPath(new URL("./historical-process.py", import.meta.url));
    NodeFS.writeFileSync(
      requestPath,
      JSON.stringify({
        root,
        rootIdentity: Object.values(owner.creation.rootIdentity),
        evidenceRoot,
        evidenceIdentity: Object.values(owner.creation.evidenceIdentity),
        bounds,
        token: "parent-exit",
        executable: python,
        args: [
          "-c",
          "import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);open('disconnect-ready','w').close();time.sleep(100)",
        ],
        cwd: root,
        env: { PATH: "/usr/bin:/bin", HOME: NodePath.join(root, "home") },
        receiptPath,
      }),
    );
    const controllerPath = NodePath.join(evidenceRoot, "disconnect-controller.mjs");
    NodeFS.writeFileSync(
      controllerPath,
      `import { spawn } from "node:child_process";import { watch } from "node:fs";
const watcher=watch(${JSON.stringify(root)},(_event,name)=>{if(name==="disconnect-ready")process.exit(23)});
spawn(${JSON.stringify(python)},["-B",${JSON.stringify(helper)},${JSON.stringify(requestPath)}],{stdio:["ignore","ignore","ignore","pipe"]});`,
    );
    let listener;
    const receiptReady = new Promise((resolve, reject) => {
      listener = NodeFS.watch(
        evidenceRoot,
        { signal: AbortSignal.timeout(5000) },
        (_event, name) => {
          if (name === "disconnected.process.json" && NodeFS.existsSync(receiptPath)) resolve();
        },
      );
      listener.on("error", reject);
      listener.on("close", () => {
        if (!NodeFS.existsSync(receiptPath))
          reject(new Error("no disconnected supervisor receipt"));
      });
    });
    const controller = NodeChildProcess.spawn(process.execPath, [controllerPath], {
      stdio: "ignore",
    });
    try {
      const code = await new Promise((resolve) => controller.once("close", resolve));
      NodeAssert.equal(code, 23);
      await receiptReady;
      const result = JSON.parse(NodeFS.readFileSync(receiptPath));
      NodeAssert.equal(result.outcome, "parent_disconnected");
      assertGone(result);
    } finally {
      listener.close();
    }
  },
);

NodeTest.test(
  "failed evidence preservation refuses deletion while preserving primary and cleanup results",
  (t) => {
    const { owner, root } = fixture(t, { maxOutputBytes: 16 });
    const result = finishHistoricalPreparation(owner, {
      originalError: { code: "primary", message: "original failure" },
      consumerClosed: true,
    });
    NodeAssert.equal(result.originalError.code, "primary");
    NodeAssert.match(result.evidenceError.message, /evidence output bound/);
    NodeAssert.equal(result.cleanup.reason, "unknown_closure_or_evidence");
    NodeAssert.ok(NodeFS.existsSync(root));
    t.diagnostic(JSON.stringify({ preservedFailureResult: result }));
  },
);

NodeTest.test(
  "process identity cap overflow fences retry and retains the exact invocation",
  async (t) => {
    const { owner, root } = fixture(t, { maxProcesses: 1 });
    const result = await run(
      owner,
      `import os,time
if os.fork() == 0:
 time.sleep(1)
 os._exit(0)
time.sleep(100)
`,
    );
    NodeAssert.equal(result.outcome, "process_limit");
    NodeAssert.equal(result.processOverflow, true);
    NodeAssert.equal(result.familyClosed, false);
    NodeAssert.equal(
      cleanupHistoricalPreparation(owner, { evidencePreserved: true, consumerClosed: true })
        .outcome,
      "retained",
    );
    await NodeAssert.rejects(run(owner, "raise SystemExit(0)"), /unknown prior effect/);
    NodeAssert.ok(NodeFS.existsSync(root));
  },
);
