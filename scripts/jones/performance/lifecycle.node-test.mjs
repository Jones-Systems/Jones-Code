import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import * as NodeURL from "node:url";

import { createOwnedRoot, disposeOwnedRoot, validateSyntheticFixture } from "./guard.mjs";
import { runOwnedChild } from "./lifecycle.mjs";

const testDirectory = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const sourceRevision = "e5a31aceec91484b64315c63dcce80f6e7581604";

async function withScratch(body) {
  const scratch = NodeFS.mkdtempSync(NodePath.join(testDirectory, ".lifecycle-test-"));
  const fixtureAbort = new AbortController();
  const children = [];
  let bodyFailure;
  let result;
  try {
    const binding = {
      repository: "Jones-Systems/Jones-Code",
      sourceRevision,
      taskRef: "spec.jones-performance-portfolio#task.e-root.001",
      runId: NodePath.basename(scratch),
    };
    const policy = {
      homePath: scratch,
      worktreePaths: [NodePath.resolve(testDirectory, "../..")],
      protectedPaths: [NodePath.join(scratch, "synthetic-protected-store")],
    };
    const owner = createOwnedRoot({ parentPath: scratch, childName: "owned", binding, policy });
    const run = (childOptions) => {
      NodeAssert.equal(childOptions.owner, owner);
      const signal = childOptions.signal
        ? AbortSignal.any([childOptions.signal, fixtureAbort.signal])
        : fixtureAbort.signal;
      const pending = runOwnedChild({ ...childOptions, signal });
      children.push(pending);
      return pending;
    };
    result = await body({ scratch, owner, binding, policy, run });
  } catch (error) {
    bodyFailure = error;
  } finally {
    fixtureAbort.abort();
    const settled = await Promise.allSettled(children);
    let cleanupFailure;
    if (
      settled.some(
        (child) =>
          child.status !== "fulfilled" ||
          child.value.outcome === "unknown" ||
          !child.value.closed ||
          !child.value.reaped,
      )
    ) {
      cleanupFailure = new Error(`child closure is unknown; retained exact test root ${scratch}`);
    } else {
      try {
        NodeFS.rmSync(scratch, { recursive: true, force: true });
      } catch (error) {
        cleanupFailure = error;
      }
    }
    if (cleanupFailure && bodyFailure) console.error(cleanupFailure.message);
    else if (cleanupFailure) bodyFailure = cleanupFailure;
  }
  if (bodyFailure) throw bodyFailure;
  return result;
}

function options(owner, code, overrides = {}) {
  return {
    owner,
    executable: process.execPath,
    args: ["--input-type=module", "-e", code],
    env: {},
    timeoutMs: 5000,
    terminateGraceMs: 100,
    reapTimeoutMs: 1500,
    maxOutputBytes: 32 * 1024,
    ...overrides,
  };
}

function cleanup(owner, receipt) {
  NodeAssert.equal(receipt.closed, true);
  NodeAssert.equal(receipt.reaped, true);
  const result = disposeOwnedRoot(owner, { childReceipts: [receipt] });
  NodeAssert.equal(result.outcome, "complete");
  NodeAssert.equal(result.absent, true);
  NodeAssert.equal(NodeFS.existsSync(owner.creationReceipt.canonicalRootPath), false);
}

NodeTest.test(
  "offline leaf success uses the exact owned cwd and explicit environment",
  async () => {
    await withScratch(async ({ owner, run }) => {
      const receipt = await run(
        options(owner, "console.log(JSON.stringify({cwd:process.cwd(),env:process.env}))", {
          env: { SYNTHETIC_INPUT: "bounded" },
        }),
      );
      NodeAssert.equal(receipt.outcome, "success");
      NodeAssert.ok(Number.isInteger(receipt.pid));
      NodeAssert.deepEqual(JSON.parse(receipt.stdout), {
        cwd: owner.creationReceipt.canonicalRootPath,
        env: { SYNTHETIC_INPUT: "bounded" },
      });
      NodeAssert.equal(receipt.truncated, false);
      cleanup(owner, receipt);
    });
  },
);

NodeTest.test(
  "nonzero exit keeps the original failure and bounded diagnostic before cleanup",
  async () => {
    await withScratch(async ({ owner, run }) => {
      const receipt = await run(
        options(owner, "console.error('synthetic failure');process.exitCode=7"),
      );
      NodeAssert.equal(receipt.outcome, "failed");
      NodeAssert.equal(receipt.exitCode, 7);
      NodeAssert.equal(receipt.stderr, "synthetic failure\n");
      cleanup(owner, receipt);
      NodeAssert.equal(receipt.exitCode, 7);
      NodeAssert.equal(receipt.stderr, "synthetic failure\n");
    });
  },
);

NodeTest.test("missing executable and invalid options are known spawn refusals", async () => {
  await withScratch(async ({ owner, run }) => {
    const receipt = await run(
      options(owner, "", {
        executable: NodePath.join(owner.creationReceipt.canonicalRootPath, "absent-executable"),
      }),
    );
    NodeAssert.equal(receipt.outcome, "spawn_refused");
    NodeAssert.equal(receipt.pid, null);
    cleanup(owner, receipt);
  });
  await withScratch(async ({ owner, run }) => {
    const receipt = await run(
      options(owner, "throw new Error('must not run')", { env: undefined }),
    );
    NodeAssert.equal(receipt.outcome, "spawn_refused");
    NodeAssert.equal(receipt.stopReason, "invalid_options");
    cleanup(owner, receipt);
  });
});

NodeTest.test("a blocked audited leaf is terminated and reaped on the hard deadline", async () => {
  await withScratch(async ({ owner, run }) => {
    const receipt = await run(options(owner, "setInterval(()=>{},1000)", { timeoutMs: 100 }));
    NodeAssert.equal(receipt.outcome, "timed_out");
    NodeAssert.equal(receipt.stopReason, "timed_out");
    NodeAssert.equal(receipt.terminated, true);
    cleanup(owner, receipt);
  });
});

NodeTest.test(
  "pre-cancelled work never spawns and cancellation after spawn is reaped",
  async () => {
    await withScratch(async ({ owner, run }) => {
      const controller = new AbortController();
      controller.abort();
      const receipt = await run(
        options(owner, "throw new Error('must not run')", { signal: controller.signal }),
      );
      NodeAssert.equal(receipt.outcome, "cancelled");
      NodeAssert.equal(receipt.pid, null);
      cleanup(owner, receipt);
    });
    await withScratch(async ({ owner, run }) => {
      const controller = new AbortController();
      const pending = run(
        options(owner, "setInterval(()=>{},1000)", { signal: controller.signal }),
      );
      try {
        NodeAssert.equal(disposeOwnedRoot(owner).outcome, "retained");
        controller.abort();
        const receipt = await pending;
        NodeAssert.equal(receipt.outcome, "cancelled");
        NodeAssert.ok(Number.isInteger(receipt.pid));
        cleanup(owner, receipt);
      } finally {
        controller.abort();
        await pending;
      }
    });
  },
);

NodeTest.test("an acknowledged SIGTERM-ignoring leaf receives bounded escalation", async () => {
  await withScratch(async ({ owner, run }) => {
    const controller = new AbortController();
    const root = owner.creationReceipt.canonicalRootPath;
    const watcher = NodeFS.watch(root, (_event, filename) => {
      if (String(filename) === "armed" && NodeFS.existsSync(NodePath.join(root, "armed"))) {
        NodeAssert.equal(NodeFS.readFileSync(NodePath.join(root, "armed"), "utf8"), "ready");
        controller.abort();
      }
    });
    try {
      const receipt = await run(
        options(
          owner,
          "import{writeFileSync,renameSync}from'node:fs';process.on('SIGTERM',()=>{});writeFileSync('arming','ready');renameSync('arming','armed');setInterval(()=>{},1000)",
          { signal: controller.signal },
        ),
      );
      NodeAssert.equal(receipt.outcome, "cancelled");
      NodeAssert.equal(receipt.terminated, true);
      NodeAssert.equal(receipt.escalated, true);
      NodeAssert.equal(receipt.signal, "SIGKILL");
      cleanup(owner, receipt);
    } finally {
      watcher.close();
    }
  });
});

NodeTest.test(
  "combined output is counted before accumulation and overflow stops the child",
  async () => {
    await withScratch(async ({ owner, run }) => {
      const receipt = await run(
        options(
          owner,
          "process.stdout.write(Buffer.alloc(512*1024,65));process.stderr.write(Buffer.alloc(512*1024,66));setInterval(()=>{},1000)",
          { maxOutputBytes: 32 },
        ),
      );
      NodeAssert.equal(receipt.outcome, "output_limited");
      NodeAssert.ok(receipt.observedBytes > 32);
      NodeAssert.equal(receipt.capturedBytes, 32);
      NodeAssert.equal(Buffer.byteLength(receipt.stdout) + Buffer.byteLength(receipt.stderr), 32);
      NodeAssert.equal(receipt.truncated, true);
      cleanup(owner, receipt);
    });
  },
);

NodeTest.test(
  "invalid UTF-8 cannot expand the retained text beyond the raw byte budget",
  async () => {
    await withScratch(async ({ owner, run }) => {
      const receipt = await run(
        options(owner, "process.stdout.write(Buffer.alloc(4,255))", { maxOutputBytes: 4 }),
      );
      NodeAssert.equal(receipt.outcome, "success");
      NodeAssert.equal(receipt.capturedBytes, 4);
      NodeAssert.ok(Buffer.byteLength(receipt.stdout) + Buffer.byteLength(receipt.stderr) <= 4);
      NodeAssert.equal(receipt.truncated, true);
      cleanup(owner, receipt);
    });
  },
);

NodeTest.test(
  "forged ownership returns unknown without spawning and cloned child receipts retain",
  async () => {
    await withScratch(async ({ owner, scratch, run }) => {
      const canary = NodePath.join(scratch, "canary");
      NodeFS.writeFileSync(canary, "unchanged");
      const fake = JSON.parse(JSON.stringify(owner));
      const unknown = await runOwnedChild(
        options(
          fake,
          `import{writeFileSync}from'node:fs';writeFileSync(${JSON.stringify(canary)},'changed')`,
        ),
      );
      NodeAssert.equal(unknown.outcome, "unknown");
      NodeAssert.equal(unknown.closed, false);
      NodeAssert.equal(unknown.pid, null);
      NodeAssert.equal(NodeFS.readFileSync(canary, "utf8"), "unchanged");
      const receipt = await run(options(owner, "console.log('done')"));
      NodeAssert.equal(
        disposeOwnedRoot(owner, { childReceipts: [JSON.parse(JSON.stringify(receipt))] }).outcome,
        "retained",
      );
      cleanup(owner, receipt);
    });
  },
);

NodeTest.test(
  "captured close gates cross-process nested fixture validation and parent cleanup",
  async () => {
    await withScratch(async ({ owner, binding, policy, scratch, run }) => {
      const nestedBinding = { ...binding, runId: `${binding.runId}:leaf` };
      const input = { binding: nestedBinding, policy };
      const worker = `
      import {openSync,closeSync,writeFileSync} from 'node:fs';
      import {createOwnedRoot,assertOwnedDatabase,observeSyntheticClose,sealSyntheticFixture,syntheticFixtureReceiptSha256} from ${JSON.stringify(new URL("./guard.mjs", import.meta.url).href)};
      const input=JSON.parse(process.argv[1]);
      const owner=createOwnedRoot({parentPath:process.cwd(),childName:'nested',...input});
      const permit=assertOwnedDatabase(owner,{databaseRelativePath:'state.sqlite',access:'create'});
      const resource={fd:openSync(permit.canonicalPath,'r+')};
      writeFileSync(resource.fd,'leaf synthetic bytes');
      const producerStep='spec.jones-performance-portfolio#task.e-root.001:offline-leaf';
      const closedProof=await observeSyntheticClose(owner,{permit,resource,producerStep,close:r=>closeSync(r.fd)});
      const receipt=await sealSyntheticFixture(owner,{databaseRelativePath:'state.sqlite',producerStep,closedProof});
      console.log(JSON.stringify({receipt,digest:syntheticFixtureReceiptSha256(receipt)}));
    `;
      const child = await run(
        options(owner, worker, {
          args: ["--input-type=module", "-e", worker, JSON.stringify(input)],
        }),
      );
      NodeAssert.equal(child.outcome, "success", child.stderr);
      NodeAssert.equal(child.closed, true);
      NodeAssert.equal(child.reaped, true);
      const output = JSON.parse(child.stdout);
      NodeAssert.equal(
        output.receipt.creationReceipt.canonicalParentPath,
        owner.creationReceipt.canonicalRootPath,
      );
      const verified = await validateSyntheticFixture({
        receipt: output.receipt,
        expectedReceiptSha256: output.digest,
        expectedBinding: nestedBinding,
        policy,
      });
      NodeAssert.equal(
        verified.canonicalPath,
        NodePath.join(owner.creationReceipt.canonicalRootPath, "nested", "state.sqlite"),
      );
      const independent = createOwnedRoot({
        parentPath: scratch,
        childName: "independent",
        binding,
        policy,
      });
      cleanup(owner, child);
      NodeAssert.equal(NodeFS.existsSync(independent.creationReceipt.canonicalRootPath), true);
      NodeAssert.equal(disposeOwnedRoot(independent).outcome, "complete");
    });
  },
);

NodeTest.test(
  "a deliberate body failure aborts and reaps the captured child before outer scratch cleanup",
  async () => {
    const failure = new Error("deliberate failure after owned child spawn");
    let scratchPath;
    let receipt;
    let existedAtClose;
    await NodeAssert.rejects(
      withScratch(async ({ owner, scratch, run }) => {
        scratchPath = scratch;
        const pending = run(options(owner, "setInterval(()=>{},1000)"));
        pending.then(
          (value) => {
            receipt = value;
            existedAtClose = NodeFS.existsSync(scratch);
          },
          () => {},
        );
        throw failure;
      }),
      (error) => error === failure,
    );
    NodeAssert.equal(receipt.outcome, "cancelled");
    NodeAssert.equal(receipt.closed, true);
    NodeAssert.equal(receipt.reaped, true);
    NodeAssert.equal(existedAtClose, true);
    NodeAssert.equal(NodeFS.existsSync(scratchPath), false);
  },
);
