import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeTest from "node:test";
import {
  runLegacyIdentityNativeFixture,
  validateLegacyIdentityObservation,
  withLegacyIdentityScratch,
} from "./legacy-identity-native-fixture.mjs";

// oxlint-disable-next-line t3code/no-global-process-runtime -- Standalone native ABI tests require the actual host, without an application runtime dependency.
const nativePlatform = process.platform;

NodeTest.test(
  "native APFS identities survive one remount without rewriting mount observations",
  { timeout: 120000 },
  async (context) => {
    const result = await runLegacyIdentityNativeFixture({ signal: context.signal });
    NodeAssert.equal(result.activationAllowed, false);
    NodeAssert.equal(result.observationOnly, true);
    if (nativePlatform !== "darwin") {
      NodeAssert.equal(result.status, "unsupported");
      NodeAssert.equal(result.reason, "darwin-required");
      return;
    }
    NodeAssert.equal(result.status, "passed");
    NodeAssert.equal(result.cleanupProved, true);
    NodeAssert.equal(result.productionMembershipQualified, false);
    NodeAssert.equal(result.offlineAdmissionQualified, false);
    NodeAssert.equal(result.sameKernelRemountOnly, true);
    NodeAssert.deepEqual(result.before.persistent, result.after.persistent);
    NodeAssert.equal(result.before.stat.birthtimeNs, result.after.stat.birthtimeNs);
    NodeAssert.equal(result.after.stat.mode, 0o640);
    NodeAssert.equal(result.unreadableEndpointRefused, true);
    NodeAssert.equal(result.restored.stat.mode, 0o750);
    NodeAssert.deepEqual(result.refusalReasons, [
      "fd-path-alias",
      "endpoint-type-links-or-ownership",
      "endpoint-type-links-or-ownership",
      "duplicate-inherited-fd",
    ]);
    NodeAssert.match(result.sourceSha256, /^[a-f0-9]{64}$/);
    NodeAssert.match(result.binarySha256, /^[a-f0-9]{64}$/);
  },
);

NodeTest.test("pre-cancellation starts no identity qualifier resources", async () => {
  const original = new Error("already cancelled");
  await NodeAssert.rejects(
    runLegacyIdentityNativeFixture({ signal: AbortSignal.abort(original) }),
    (cause) => cause === original,
  );
});

NodeTest.test(
  "identity scratch owners clean success and failure, preserving the cause",
  async () => {
    let successful;
    await withLegacyIdentityScratch(async (root) => {
      successful = root;
    });
    await NodeAssert.rejects(NodeFSP.lstat(successful), { code: "ENOENT" });
    let failed;
    const original = new Error("synthetic identity failure");
    await NodeAssert.rejects(
      withLegacyIdentityScratch(async (root) => {
        failed = root;
        throw original;
      }),
      (cause) => cause === original,
    );
    await NodeAssert.rejects(NodeFSP.lstat(failed), { code: "ENOENT" });
  },
);

NodeTest.test(
  "unknown image custody retains its exact root and cleanup errors preserve the cause",
  async () => {
    let retained;
    const unknown = Object.assign(new Error("synthetic unknown custody"), {
      preserveScratch: true,
    });
    try {
      await NodeAssert.rejects(
        withLegacyIdentityScratch(async (root) => {
          retained = root;
          throw unknown;
        }),
        (cause) => cause === unknown && cause.scratchRoot === retained,
      );
      NodeAssert.equal((await NodeFSP.lstat(retained)).isDirectory(), true);
    } finally {
      // This case creates an empty directory only; no image or process exists.
      if (retained) await NodeFSP.rmdir(retained);
    }
    const original = new Error("operation failed");
    await NodeAssert.rejects(
      withLegacyIdentityScratch(async (root) => {
        await NodeFSP.rmdir(root);
        throw original;
      }),
      (cause) =>
        cause instanceof AggregateError && cause.cause === original && cause.errors[0] === original,
    );
  },
);

NodeTest.test("schema validation never upgrades unavailable or malformed identity evidence", () => {
  const boot = {
    schema: 1,
    operation: "boot",
    status: "observed",
    bootSessionUuid: "11111111-2222-3333-4444-555555555555",
    ancestorPolicy: "nofollow-currentness-only",
    objects: [],
    observationOnly: true,
    activationAllowed: false,
  };
  NodeAssert.equal(validateLegacyIdentityObservation(boot), boot);
  for (const changed of [
    { status: "unsupported" },
    { activationAllowed: true },
    { observationOnly: false },
    { bootSessionUuid: "" },
    { bootSessionUuid: "00000000-0000-0000-0000-000000000000" },
    { ancestorPolicy: "exclusive" },
    { objects: [{}] },
  ])
    NodeAssert.throws(() => validateLegacyIdentityObservation({ ...boot, ...changed }));
});
