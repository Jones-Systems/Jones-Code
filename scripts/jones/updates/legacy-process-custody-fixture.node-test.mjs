import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import {
  runLegacyProcessCustodyFixture,
  validateProcessCustodyResult,
  withProcessCustodyScratch,
} from "./legacy-process-custody-fixture.mjs";

const darwin = { skip: process.platform !== "darwin" };

NodeTest.test(
  "rootless native audit-token signaling rejects wrong and pre-exec identities",
  async () => {
    const result = await runLegacyProcessCustodyFixture();
    NodeAssert.equal(result.activationAllowed, false);
    NodeAssert.equal(result.productionMembershipQualified, false);
    NodeAssert.equal(result.hardenedAppQualified, false);
    NodeAssert.equal(result.pidOnlyAdmissionFallback, false);
    if (process.platform !== "darwin") {
      NodeAssert.equal(result.status, "unsupported");
      NodeAssert.equal(result.reason, "darwin-required");
      return;
    }
    NodeAssert.equal(result.status, "passed", result.reason);
    NodeAssert.equal(result.cleanupProved, true);
    NodeAssert.equal(result.proof.wrongTokenErrno, NodeOS.constants.errno.ESRCH);
    NodeAssert.equal(result.proof.staleTokenErrno, NodeOS.constants.errno.ESRCH);
    NodeAssert.notEqual(result.proof.beforePidversion, result.proof.afterPidversion);
    for (const key of [
      "stopObserved",
      "continueSurvivor",
      "wrongTokenSurvivor",
      "staleTokenSurvivor",
      "termReaped",
    ])
      NodeAssert.equal(result.proof[key], true);
    NodeAssert.match(result.sourceSha256, /^[a-f0-9]{64}$/);
    NodeAssert.match(result.binarySha256, /^[a-f0-9]{64}$/);
  },
);

for (const [scenario, reason] of [
  ["missing-symbol", "direct-symbol-unavailable"],
  ["missing-flavor", "kernel-identity-schema-unavailable"],
]) {
  NodeTest.test(`${scenario} refuses qualification without PID-only fallback`, darwin, async () => {
    const result = await runLegacyProcessCustodyFixture({ scenario });
    NodeAssert.equal(result.status, "unsupported");
    NodeAssert.equal(result.reason, reason);
    NodeAssert.equal(result.pidOnlyAdmissionFallback, false);
    NodeAssert.equal(result.cleanupProved, true);
    NodeAssert.equal(result.proof, undefined);
  });
}

for (const kind of ["cancel", "callback-failure"]) {
  NodeTest.test(
    `${kind} at the captured-child barrier reaps the child and removes scratch`,
    darwin,
    async () => {
      await withProcessCustodyScratch(async (parent) => {
        const cancellation = new AbortController();
        const original = new Error(`deliberate ${kind}`);
        let ready = false;
        await NodeAssert.rejects(
          runLegacyProcessCustodyFixture({
            signal: cancellation.signal,
            temporaryDirectory: parent,
            onReady: () => {
              ready = true;
              if (kind === "cancel") cancellation.abort(original);
              else throw original;
            },
          }),
          (error) => error === original && !error.preserveScratch,
        );
        NodeAssert.equal(ready, true);
        NodeAssert.deepEqual(await NodeFSP.readdir(parent), []);
      });
    },
  );
}

NodeTest.test("invalid scenario and pre-cancellation create no resources", async () => {
  await withProcessCustodyScratch(async (parent) => {
    await NodeAssert.rejects(
      runLegacyProcessCustodyFixture({ scenario: "adopt-production", temporaryDirectory: parent }),
    );
    const original = new Error("already cancelled");
    const signal = AbortSignal.abort(original);
    await NodeAssert.rejects(
      runLegacyProcessCustodyFixture({ signal, temporaryDirectory: parent }),
      (error) => error === original,
    );
    NodeAssert.deepEqual(await NodeFSP.readdir(parent), []);
  });
});

NodeTest.test("scratch owners clean success and failure independently", async () => {
  const roots = [];
  await Promise.all(
    ["first", "second"].map((value) =>
      withProcessCustodyScratch(async (root) => {
        roots.push(root);
        await NodeFSP.writeFile(NodePath.join(root, "owned"), value, { flag: "wx" });
        await new Promise((resolve) => setImmediate(resolve));
        NodeAssert.equal(await NodeFSP.readFile(NodePath.join(root, "owned"), "utf8"), value);
      }),
    ),
  );
  NodeAssert.equal(new Set(roots).size, 2);
  for (const root of roots) await NodeAssert.rejects(NodeFSP.lstat(root), { code: "ENOENT" });
  let failedRoot;
  const original = new Error("owned fixture failed");
  await NodeAssert.rejects(
    withProcessCustodyScratch(async (root) => {
      failedRoot = root;
      throw original;
    }),
    (error) => error === original,
  );
  await NodeAssert.rejects(NodeFSP.lstat(failedRoot), { code: "ENOENT" });
});

NodeTest.test("cleanup failure preserves the original error", async () => {
  const original = new Error("operation failed first");
  await NodeAssert.rejects(
    withProcessCustodyScratch(async (root) => {
      await NodeFSP.rmdir(root);
      throw original;
    }),
    (error) => {
      NodeAssert.ok(error instanceof AggregateError);
      NodeAssert.equal(error.cause, original);
      NodeAssert.equal(error.errors[0], original);
      NodeAssert.equal(error.errors[1].code, "ENOENT");
      return true;
    },
  );
});

NodeTest.test("a partial or contradictory native result cannot qualify", () => {
  const ready = { event: "ready", pid: 42, pidversion: 10, uniqueId: "100" };
  const proof = {
    event: "proof",
    samePid: 42,
    beforePidversion: 10,
    afterPidversion: 11,
    beforeUniqueId: "100",
    afterUniqueId: "100",
    wrongTokenErrno: NodeOS.constants.errno.ESRCH,
    staleTokenErrno: NodeOS.constants.errno.ESRCH,
    stopObserved: true,
    continueSurvivor: true,
    wrongTokenSurvivor: true,
    staleTokenSurvivor: true,
    termReaped: true,
  };
  const final = { event: "result", status: "passed", cleanupProved: true };
  const result = { code: 0, signal: null, events: [ready, proof, final], stderr: "" };
  NodeAssert.equal(validateProcessCustodyResult(result).status, "passed");
  for (const change of [
    { staleTokenErrno: 0 },
    { wrongTokenSurvivor: false },
    { termReaped: false },
    { samePid: 43 },
    { afterPidversion: 10 },
    { afterPidversion: undefined },
  ])
    NodeAssert.throws(() =>
      validateProcessCustodyResult({ ...result, events: [ready, { ...proof, ...change }, final] }),
    );
  NodeAssert.throws(() => validateProcessCustodyResult({ ...result, events: [ready, proof] }));
  NodeAssert.throws(() =>
    validateProcessCustodyResult({
      ...result,
      events: [ready, proof, { ...final, cleanupProved: false }],
    }),
  );
  NodeAssert.throws(() => validateProcessCustodyResult({ ...result, code: 77 }));
});
