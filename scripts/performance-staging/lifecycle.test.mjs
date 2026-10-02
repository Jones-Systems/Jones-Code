import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readFileSync, rmSync, watch, writeFileSync } from "node:fs";
import { basename, dirname, join, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { createOwnedRoot, disposeOwnedRoot, validateSyntheticFixture } from "./guard.mjs";
import { runOwnedChild } from "./lifecycle.mjs";

const testDirectory = dirname(fileURLToPath(import.meta.url));
const sourceRevision = "e5a31aceec91484b64315c63dcce80f6e7581604";

async function withScratch(body) {
  const scratch = mkdtempSync(join(testDirectory, ".lifecycle-test-"));
  const fixtureAbort = new AbortController();
  const children = [];
  let bodyFailure;
  let result;
  try {
    const binding = {
      repository: "Jones-Systems/Jones-Code",
      sourceRevision,
      taskRef: "spec.jones-performance-portfolio#task.e-root.001",
      runId: basename(scratch),
    };
    const policy = {
      homePath: scratch,
      worktreePaths: [resolve(testDirectory, "../..")],
      protectedPaths: [join(scratch, "synthetic-protected-store")],
    };
    const owner = createOwnedRoot({ parentPath: scratch, childName: "owned", binding, policy });
    const run = (childOptions) => {
      assert.equal(childOptions.owner, owner);
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
        rmSync(scratch, { recursive: true, force: true });
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
  assert.equal(receipt.closed, true);
  assert.equal(receipt.reaped, true);
  const result = disposeOwnedRoot(owner, { childReceipts: [receipt] });
  assert.equal(result.outcome, "complete");
  assert.equal(result.absent, true);
  assert.equal(existsSync(owner.creationReceipt.canonicalRootPath), false);
}

test("offline leaf success uses the exact owned cwd and explicit environment", async () => {
  await withScratch(async ({ owner, run }) => {
    const receipt = await run(
      options(owner, "console.log(JSON.stringify({cwd:process.cwd(),env:process.env}))", {
        env: { SYNTHETIC_INPUT: "bounded" },
      }),
    );
    assert.equal(receipt.outcome, "success");
    assert.ok(Number.isInteger(receipt.pid));
    assert.deepEqual(JSON.parse(receipt.stdout), {
      cwd: owner.creationReceipt.canonicalRootPath,
      env: { SYNTHETIC_INPUT: "bounded" },
    });
    assert.equal(receipt.truncated, false);
    cleanup(owner, receipt);
  });
});

test("nonzero exit keeps the original failure and bounded diagnostic before cleanup", async () => {
  await withScratch(async ({ owner, run }) => {
    const receipt = await run(
      options(owner, "console.error('synthetic failure');process.exitCode=7"),
    );
    assert.equal(receipt.outcome, "failed");
    assert.equal(receipt.exitCode, 7);
    assert.equal(receipt.stderr, "synthetic failure\n");
    cleanup(owner, receipt);
    assert.equal(receipt.exitCode, 7);
    assert.equal(receipt.stderr, "synthetic failure\n");
  });
});

test("missing executable and invalid options are known spawn refusals", async () => {
  await withScratch(async ({ owner, run }) => {
    const receipt = await run(
      options(owner, "", {
        executable: join(owner.creationReceipt.canonicalRootPath, "absent-executable"),
      }),
    );
    assert.equal(receipt.outcome, "spawn_refused");
    assert.equal(receipt.pid, null);
    cleanup(owner, receipt);
  });
  await withScratch(async ({ owner, run }) => {
    const receipt = await run(
      options(owner, "throw new Error('must not run')", { env: undefined }),
    );
    assert.equal(receipt.outcome, "spawn_refused");
    assert.equal(receipt.stopReason, "invalid_options");
    cleanup(owner, receipt);
  });
});

test("a blocked audited leaf is terminated and reaped on the hard deadline", async () => {
  await withScratch(async ({ owner, run }) => {
    const receipt = await run(options(owner, "setInterval(()=>{},1000)", { timeoutMs: 100 }));
    assert.equal(receipt.outcome, "timed_out");
    assert.equal(receipt.stopReason, "timed_out");
    assert.equal(receipt.terminated, true);
    cleanup(owner, receipt);
  });
});

test("pre-cancelled work never spawns and cancellation after spawn is reaped", async () => {
  await withScratch(async ({ owner, run }) => {
    const controller = new AbortController();
    controller.abort();
    const receipt = await run(
      options(owner, "throw new Error('must not run')", { signal: controller.signal }),
    );
    assert.equal(receipt.outcome, "cancelled");
    assert.equal(receipt.pid, null);
    cleanup(owner, receipt);
  });
  await withScratch(async ({ owner, run }) => {
    const controller = new AbortController();
    const pending = run(options(owner, "setInterval(()=>{},1000)", { signal: controller.signal }));
    try {
      assert.equal(disposeOwnedRoot(owner).outcome, "retained");
      controller.abort();
      const receipt = await pending;
      assert.equal(receipt.outcome, "cancelled");
      assert.ok(Number.isInteger(receipt.pid));
      cleanup(owner, receipt);
    } finally {
      controller.abort();
      await pending;
    }
  });
});

test("an acknowledged SIGTERM-ignoring leaf receives bounded escalation", async () => {
  await withScratch(async ({ owner, run }) => {
    const controller = new AbortController();
    const root = owner.creationReceipt.canonicalRootPath;
    const watcher = watch(root, (_event, filename) => {
      if (String(filename) === "armed" && existsSync(join(root, "armed"))) {
        assert.equal(readFileSync(join(root, "armed"), "utf8"), "ready");
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
      assert.equal(receipt.outcome, "cancelled");
      assert.equal(receipt.terminated, true);
      assert.equal(receipt.escalated, true);
      assert.equal(receipt.signal, "SIGKILL");
      cleanup(owner, receipt);
    } finally {
      watcher.close();
    }
  });
});

test("combined output is counted before accumulation and overflow stops the child", async () => {
  await withScratch(async ({ owner, run }) => {
    const receipt = await run(
      options(
        owner,
        "process.stdout.write(Buffer.alloc(512*1024,65));process.stderr.write(Buffer.alloc(512*1024,66));setInterval(()=>{},1000)",
        { maxOutputBytes: 32 },
      ),
    );
    assert.equal(receipt.outcome, "output_limited");
    assert.ok(receipt.observedBytes > 32);
    assert.equal(receipt.capturedBytes, 32);
    assert.equal(Buffer.byteLength(receipt.stdout) + Buffer.byteLength(receipt.stderr), 32);
    assert.equal(receipt.truncated, true);
    cleanup(owner, receipt);
  });
});

test("invalid UTF-8 cannot expand the retained text beyond the raw byte budget", async () => {
  await withScratch(async ({ owner, run }) => {
    const receipt = await run(
      options(owner, "process.stdout.write(Buffer.alloc(4,255))", { maxOutputBytes: 4 }),
    );
    assert.equal(receipt.outcome, "success");
    assert.equal(receipt.capturedBytes, 4);
    assert.ok(Buffer.byteLength(receipt.stdout) + Buffer.byteLength(receipt.stderr) <= 4);
    assert.equal(receipt.truncated, true);
    cleanup(owner, receipt);
  });
});

test("forged ownership returns unknown without spawning and cloned child receipts retain", async () => {
  await withScratch(async ({ owner, scratch, run }) => {
    const canary = join(scratch, "canary");
    writeFileSync(canary, "unchanged");
    const fake = JSON.parse(JSON.stringify(owner));
    const unknown = await runOwnedChild(
      options(
        fake,
        `import{writeFileSync}from'node:fs';writeFileSync(${JSON.stringify(canary)},'changed')`,
      ),
    );
    assert.equal(unknown.outcome, "unknown");
    assert.equal(unknown.closed, false);
    assert.equal(unknown.pid, null);
    assert.equal(readFileSync(canary, "utf8"), "unchanged");
    const receipt = await run(options(owner, "console.log('done')"));
    assert.equal(
      disposeOwnedRoot(owner, { childReceipts: [JSON.parse(JSON.stringify(receipt))] }).outcome,
      "retained",
    );
    cleanup(owner, receipt);
  });
});

test("captured close gates cross-process nested fixture validation and parent cleanup", async () => {
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
    assert.equal(child.outcome, "success", child.stderr);
    assert.equal(child.closed, true);
    assert.equal(child.reaped, true);
    const output = JSON.parse(child.stdout);
    assert.equal(
      output.receipt.creationReceipt.canonicalParentPath,
      owner.creationReceipt.canonicalRootPath,
    );
    const verified = await validateSyntheticFixture({
      receipt: output.receipt,
      expectedReceiptSha256: output.digest,
      expectedBinding: nestedBinding,
      policy,
    });
    assert.equal(
      verified.canonicalPath,
      join(owner.creationReceipt.canonicalRootPath, "nested", "state.sqlite"),
    );
    const independent = createOwnedRoot({
      parentPath: scratch,
      childName: "independent",
      binding,
      policy,
    });
    cleanup(owner, child);
    assert.equal(existsSync(independent.creationReceipt.canonicalRootPath), true);
    assert.equal(disposeOwnedRoot(independent).outcome, "complete");
  });
});

test("a deliberate body failure aborts and reaps the captured child before outer scratch cleanup", async () => {
  const failure = new Error("deliberate failure after owned child spawn");
  let scratchPath;
  let receipt;
  let existedAtClose;
  await assert.rejects(
    withScratch(async ({ owner, scratch, run }) => {
      scratchPath = scratch;
      const pending = run(options(owner, "setInterval(()=>{},1000)"));
      pending.then(
        (value) => {
          receipt = value;
          existedAtClose = existsSync(scratch);
        },
        () => {},
      );
      throw failure;
    }),
    (error) => error === failure,
  );
  assert.equal(receipt.outcome, "cancelled");
  assert.equal(receipt.closed, true);
  assert.equal(receipt.reaped, true);
  assert.equal(existedAtClose, true);
  assert.equal(existsSync(scratchPath), false);
});
