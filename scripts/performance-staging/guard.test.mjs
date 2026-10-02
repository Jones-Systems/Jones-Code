import assert from "node:assert/strict";
import {
  chmodSync,
  closeSync,
  cpSync,
  existsSync,
  linkSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  openSync,
  readFileSync,
  renameSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { basename, dirname, join, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import {
  assertOwnedDatabase,
  createOwnedRoot,
  disposeOwnedRoot,
  observeSyntheticClose,
  sealSyntheticFixture,
  syntheticFixtureReceiptSha256,
  validateSyntheticFixture,
} from "./guard.mjs";

const testDirectory = dirname(fileURLToPath(import.meta.url));
const sourceRevision = "e5a31aceec91484b64315c63dcce80f6e7581604";
const producerStep = "spec.jones-performance-portfolio#task.e-root.001:synthetic-file-producer";
const testOwners = new WeakMap();

function closeResource(resource) {
  if (resource.fd !== null) {
    closeSync(resource.fd);
    resource.fd = null;
  }
}

function openResource(owner, permit) {
  const resource = { fd: openSync(permit.canonicalPath, "r+") };
  testOwners.get(owner).resources.add(resource);
  return resource;
}

async function withScratch(body) {
  const scratch = mkdtempSync(join(testDirectory, ".guard-test-"));
  const resources = new Set();
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
    await body({ scratch, binding, policy, resources });
  } finally {
    for (const resource of resources) closeResource(resource);
    // This outer test root also owns intentionally retained adversarial trees and canaries.
    rmSync(scratch, { recursive: true, force: true });
  }
}

function ownerAt(context, childName = "fixture") {
  const owner = createOwnedRoot({
    parentPath: context.scratch,
    childName,
    binding: context.binding,
    policy: context.policy,
  });
  testOwners.set(owner, context);
  return owner;
}

function createdFile(owner) {
  const permit = assertOwnedDatabase(owner, {
    databaseRelativePath: "state.sqlite",
    access: "create",
  });
  const resource = openResource(owner, permit);
  writeFileSync(resource.fd, "deterministic synthetic bytes");
  return { permit, resource };
}

async function closedFixture(context, childName = "fixture") {
  const owner = ownerAt(context, childName);
  const { permit, resource } = createdFile(owner);
  const closedProof = await observeSyntheticClose(owner, {
    permit,
    producerStep,
    resource,
    close: closeResource,
  });
  const receipt = await sealSyntheticFixture(owner, {
    databaseRelativePath: permit.relativePath,
    producerStep,
    closedProof,
  });
  return {
    owner,
    permit,
    receipt,
    closedProof,
    expectedReceiptSha256: syntheticFixtureReceiptSha256(receipt),
  };
}

function refusalBeforeOpen(owner, options, code) {
  let opens = 0;
  assert.throws(
    () => {
      const permit = assertOwnedDatabase(owner, options);
      opens++;
      return permit;
    },
    { code },
  );
  assert.equal(opens, 0);
}

test("fresh HOME/worktree children are private and exclusively created", async () => {
  await withScratch(async (context) => {
    const owner = ownerAt(context);
    assert.equal(lstatSync(owner.creationReceipt.canonicalRootPath).mode & 0o777, 0o700);
    const permit = assertOwnedDatabase(owner, {
      databaseRelativePath: "state.sqlite",
      access: "create",
    });
    assert.equal(lstatSync(permit.canonicalPath).mode & 0o777, 0o600);
    const resource = openResource(owner, permit);
    await observeSyntheticClose(owner, { permit, producerStep, resource, close: closeResource });
    refusalBeforeOpen(
      owner,
      { databaseRelativePath: "state.sqlite", access: "create" },
      "existing_destination",
    );
    assert.throws(() => ownerAt(context), { code: "existing_destination" });
    const cleanup = disposeOwnedRoot(owner);
    assert.equal(cleanup.outcome, "complete");
    assert.equal(cleanup.absent, true);
    assert.equal(existsSync(owner.creationReceipt.canonicalRootPath), false);
    const worktreeOwner = createOwnedRoot({
      parentPath: context.scratch,
      childName: "worktree-fixture",
      binding: context.binding,
      policy: {
        ...context.policy,
        homePath: join(context.scratch, "other-home"),
        worktreePaths: [context.scratch],
      },
    });
    assert.equal(disposeOwnedRoot(worktreeOwner).outcome, "complete");
  });
});

test("lexical protected overlap refuses ancestors and descendants before candidate inspection", async () => {
  await withScratch(async (context) => {
    for (const protectedPath of [
      join(context.scratch, "absent", "root"),
      join(context.scratch, "absent", "root", "store"),
    ]) {
      assert.throws(
        () =>
          createOwnedRoot({
            parentPath: join(context.scratch, "absent"),
            childName: "root",
            binding: context.binding,
            policy: { ...context.policy, protectedPaths: [protectedPath] },
          }),
        { code: "protected_path" },
      );
    }
    assert.throws(
      () =>
        createOwnedRoot({
          parentPath: context.scratch,
          childName: "synthetic-protected-store",
          binding: context.binding,
          policy: context.policy,
        }),
      { code: "protected_path" },
    );
    assert.equal(existsSync(join(context.scratch, "absent")), false);
  });
});

test("source binding accepts exact object ID lengths and refuses a 41-digit value", async () => {
  await withScratch(async (context) => {
    assert.throws(
      () =>
        createOwnedRoot({
          parentPath: context.scratch,
          childName: "invalid-binding",
          binding: { ...context.binding, sourceRevision: "a".repeat(41) },
          policy: context.policy,
        }),
      { code: "invalid_binding" },
    );
    assert.equal(existsSync(join(context.scratch, "invalid-binding")), false);
    const owner = createOwnedRoot({
      parentPath: context.scratch,
      childName: "sha256-binding",
      binding: { ...context.binding, sourceRevision: "a".repeat(64) },
      policy: context.policy,
    });
    assert.equal(disposeOwnedRoot(owner).outcome, "complete");
  });
});

test("parent aliases, escapes and existing roots refuse without modifying canaries", async () => {
  await withScratch(async (context) => {
    const canonical = join(context.scratch, "canonical");
    mkdirSync(canonical);
    const alias = join(context.scratch, "alias");
    symlinkSync(canonical, alias);
    writeFileSync(join(canonical, "canary"), "keep");
    assert.throws(
      () =>
        createOwnedRoot({
          parentPath: alias,
          childName: "fixture",
          binding: context.binding,
          policy: context.policy,
        }),
      { code: "aliased_path" },
    );
    assert.throws(
      () =>
        createOwnedRoot({
          parentPath: context.scratch,
          childName: "../escape",
          binding: context.binding,
          policy: context.policy,
        }),
      { code: "invalid_path" },
    );
    const owner = ownerAt(context);
    refusalBeforeOpen(
      owner,
      { databaseRelativePath: "../state.sqlite", access: "create" },
      "invalid_path",
    );
    assert.equal(readFileSync(join(canonical, "canary"), "utf8"), "keep");
    assert.equal(disposeOwnedRoot(owner).outcome, "complete");
  });
});

test("nested, database and sidecar symlinks refuse before a database opener", async () => {
  await withScratch(async (context) => {
    const canary = join(context.scratch, "canary");
    writeFileSync(canary, "unchanged");
    for (const [index, relativePath] of [
      "nested-alias",
      "state.sqlite",
      "state.sqlite-wal",
      "state.sqlite-shm",
      "state.sqlite-journal",
    ].entries()) {
      const owner = ownerAt(context, `links-${index}`);
      symlinkSync(canary, join(owner.creationReceipt.canonicalRootPath, relativePath));
      refusalBeforeOpen(
        owner,
        { databaseRelativePath: "state.sqlite", access: "create" },
        "symlink",
      );
      assert.equal(disposeOwnedRoot(owner).outcome, "retained");
    }
    assert.equal(readFileSync(canary, "utf8"), "unchanged");
  });
});

test("hardlinked files refuse and cleanup retains both links", async () => {
  await withScratch(async (context) => {
    const owner = ownerAt(context);
    const canary = join(context.scratch, "canary");
    writeFileSync(canary, "unchanged");
    linkSync(canary, join(owner.creationReceipt.canonicalRootPath, "state.sqlite"));
    refusalBeforeOpen(
      owner,
      { databaseRelativePath: "state.sqlite", access: "create" },
      "hardlink",
    );
    assert.equal(disposeOwnedRoot(owner).outcome, "retained");
    assert.equal(readFileSync(canary, "utf8"), "unchanged");
  });
});

test("serialized, cloned and observational handles never authorize writes or cleanup", async () => {
  await withScratch(async (context) => {
    const owner = ownerAt(context);
    for (const forged of [owner.creationReceipt, { ...owner }, JSON.parse(JSON.stringify(owner))]) {
      refusalBeforeOpen(
        forged,
        { databaseRelativePath: "state.sqlite", access: "create" },
        "invalid_owner",
      );
      assert.equal(disposeOwnedRoot(forged).outcome, "unknown");
    }
    assert.equal(existsSync(owner.creationReceipt.canonicalRootPath), true);
    assert.equal(disposeOwnedRoot(owner).outcome, "complete");
  });
});

test("observed sync close produces a pinned readonly fixture with exact layout and hashes", async () => {
  await withScratch(async (context) => {
    const fixture = await closedFixture(context);
    const result = await validateSyntheticFixture({
      receipt: fixture.receipt,
      expectedReceiptSha256: fixture.expectedReceiptSha256,
      expectedBinding: context.binding,
      policy: context.policy,
    });
    assert.equal(result.access, "readonly");
    assert.equal(result.canonicalPath, fixture.permit.canonicalPath);
    assert.equal(result.verifiedFiles, 2);
    assert.ok(result.verifiedBytes > 0);
    assert.deepEqual(
      result.layout.map((entry) => entry.present),
      [true, false, false, false],
    );
    assert.equal(disposeOwnedRoot(fixture.owner).outcome, "complete");
  });
});

test("async resource closure executes exactly once and refuses concurrent or repeated closure", async () => {
  await withScratch(async (context) => {
    const owner = ownerAt(context);
    const { permit, resource } = createdFile(owner);
    let finish;
    let calls = 0;
    const pending = observeSyntheticClose(owner, {
      permit,
      producerStep,
      resource,
      close: async (value) => {
        calls++;
        await new Promise((resolve) => {
          finish = resolve;
        });
        closeResource(value);
      },
    });
    await assert.rejects(
      observeSyntheticClose(owner, {
        permit,
        producerStep,
        resource,
        close: () => {
          calls++;
        },
      }),
      { code: "invalid_close" },
    );
    assert.equal(disposeOwnedRoot(owner).outcome, "retained");
    finish();
    const proof = await pending;
    await assert.rejects(
      observeSyntheticClose(owner, {
        permit,
        producerStep,
        resource,
        close: () => {
          calls++;
        },
      }),
      { code: "invalid_close" },
    );
    assert.equal(calls, 1);
    assert.ok(proof);
    assert.equal(disposeOwnedRoot(owner).outcome, "complete");
  });
});

test("failed close yields no proof, refuses retry and retains the tree", async () => {
  await withScratch(async (context) => {
    const owner = ownerAt(context);
    const { permit, resource } = createdFile(owner);
    const failure = new Error("synthetic close failure");
    let calls = 0;
    await assert.rejects(
      observeSyntheticClose(owner, {
        permit,
        producerStep,
        resource,
        close: (value) => {
          calls++;
          closeResource(value);
          throw failure;
        },
      }),
      (error) => error === failure,
    );
    await assert.rejects(
      observeSyntheticClose(owner, {
        permit,
        producerStep,
        resource,
        close: () => {
          calls++;
        },
      }),
      { code: "invalid_close" },
    );
    await assert.rejects(
      sealSyntheticFixture(owner, {
        databaseRelativePath: "state.sqlite",
        producerStep,
        closedProof: true,
      }),
      { code: "invalid_close_proof" },
    );
    assert.equal(calls, 1);
    assert.equal(disposeOwnedRoot(owner).outcome, "retained");
  });
});

test("file replacement during actual close cannot produce a proof", async () => {
  await withScratch(async (context) => {
    const owner = ownerAt(context);
    const { permit, resource } = createdFile(owner);
    await assert.rejects(
      observeSyntheticClose(owner, {
        permit,
        producerStep,
        resource,
        close: (value) => {
          closeResource(value);
          renameSync(permit.canonicalPath, `${permit.canonicalPath}.previous`);
          writeFileSync(permit.canonicalPath, "replacement");
        },
      }),
      { code: "changed_identity" },
    );
    refusalBeforeOpen(
      owner,
      { databaseRelativePath: "state.sqlite", access: "readwrite" },
      "unregistered_database",
    );
    assert.equal(disposeOwnedRoot(owner).outcome, "retained");
  });
});

test("proof forgery, foreign ownership, mismatches and concurrent consumption refuse", async () => {
  await withScratch(async (context) => {
    const owner = ownerAt(context);
    const other = ownerAt(context, "other");
    const { permit, resource } = createdFile(owner);
    const proof = await observeSyntheticClose(owner, {
      permit,
      producerStep,
      resource,
      close: closeResource,
    });
    for (const fake of [true, {}, JSON.parse(JSON.stringify(proof))]) {
      await assert.rejects(
        sealSyntheticFixture(owner, {
          databaseRelativePath: "state.sqlite",
          producerStep,
          closedProof: fake,
        }),
        { code: "invalid_close_proof" },
      );
    }
    await assert.rejects(
      sealSyntheticFixture(other, {
        databaseRelativePath: "state.sqlite",
        producerStep,
        closedProof: proof,
      }),
      { code: "invalid_close_proof" },
    );
    await assert.rejects(
      sealSyntheticFixture(owner, {
        databaseRelativePath: "other.sqlite",
        producerStep,
        closedProof: proof,
      }),
      { code: "invalid_close_proof" },
    );
    await assert.rejects(
      sealSyntheticFixture(owner, {
        databaseRelativePath: "state.sqlite",
        producerStep: "wrong-step",
        closedProof: proof,
      }),
      { code: "invalid_close_proof" },
    );
    const sealing = sealSyntheticFixture(owner, {
      databaseRelativePath: "state.sqlite",
      producerStep,
      closedProof: proof,
    });
    await assert.rejects(
      sealSyntheticFixture(owner, {
        databaseRelativePath: "state.sqlite",
        producerStep,
        closedProof: proof,
      }),
      { code: "invalid_close_proof" },
    );
    await sealing;
    await assert.rejects(
      sealSyntheticFixture(owner, {
        databaseRelativePath: "state.sqlite",
        producerStep,
        closedProof: proof,
      }),
      { code: "invalid_close_proof" },
    );
    assert.equal(disposeOwnedRoot(owner).outcome, "complete");
    assert.equal(disposeOwnedRoot(other).outcome, "complete");
  });
});

test("a new producer needs a fresh permit and invalidates an old same-size close proof", async () => {
  await withScratch(async (context) => {
    const owner = ownerAt(context);
    const first = createdFile(owner);
    const oldProof = await observeSyntheticClose(owner, {
      ...first,
      producerStep,
      close: closeResource,
    });
    await assert.rejects(
      observeSyntheticClose(owner, {
        permit: first.permit,
        resource: {},
        producerStep: "second-producer",
        close: () => {},
      }),
      { code: "invalid_close" },
    );
    const permit = assertOwnedDatabase(owner, {
      databaseRelativePath: "state.sqlite",
      access: "readwrite",
    });
    const resource = openResource(owner, permit);
    writeFileSync(resource.fd, "Deterministic synthetic bytes");
    const nextProof = await observeSyntheticClose(owner, {
      permit,
      resource,
      producerStep: "second-producer",
      close: closeResource,
    });
    await assert.rejects(
      sealSyntheticFixture(owner, {
        databaseRelativePath: "state.sqlite",
        producerStep,
        closedProof: oldProof,
      }),
      { code: "invalid_close_proof" },
    );
    await sealSyntheticFixture(owner, {
      databaseRelativePath: "state.sqlite",
      producerStep: "second-producer",
      closedProof: nextProof,
    });
    const thirdPermit = assertOwnedDatabase(owner, {
      databaseRelativePath: "state.sqlite",
      access: "readwrite",
    });
    const thirdResource = openResource(owner, thirdPermit);
    const thirdProof = await observeSyntheticClose(owner, {
      permit: thirdPermit,
      resource: thirdResource,
      producerStep: "third-producer",
      close: closeResource,
    });
    const resealed = await sealSyntheticFixture(owner, {
      databaseRelativePath: "state.sqlite",
      producerStep: "third-producer",
      closedProof: thirdProof,
    });
    assert.equal(resealed.producerStep, "third-producer");
    assert.equal(disposeOwnedRoot(owner).outcome, "complete");
  });
});

test("unpinned, stale and mutated receipts refuse before readonly qualification", async () => {
  await withScratch(async (context) => {
    const fixture = await closedFixture(context);
    const options = {
      receipt: fixture.receipt,
      expectedBinding: context.binding,
      policy: context.policy,
    };
    await assert.rejects(validateSyntheticFixture(options), { code: "unpinned_receipt" });
    await assert.rejects(
      validateSyntheticFixture({
        ...options,
        expectedReceiptSha256: fixture.expectedReceiptSha256,
        expectedBinding: { ...context.binding, sourceRevision: "a".repeat(40) },
      }),
      { code: "invalid_receipt" },
    );
    const changed = JSON.parse(JSON.stringify(fixture.receipt));
    changed.producerStep = "untrusted-import";
    await assert.rejects(
      validateSyntheticFixture({
        ...options,
        receipt: changed,
        expectedReceiptSha256: fixture.expectedReceiptSha256,
      }),
      { code: "receipt_mismatch" },
    );
    assert.equal(disposeOwnedRoot(fixture.owner).outcome, "complete");
  });
});

test("copied roots and markers, changed bytes and sidecar layout never qualify", async () => {
  await withScratch(async (context) => {
    const fixture = await closedFixture(context);
    const copiedPath = join(context.scratch, "copied");
    cpSync(fixture.owner.creationReceipt.canonicalRootPath, copiedPath, { recursive: true });
    const copied = JSON.parse(JSON.stringify(fixture.receipt));
    copied.creationReceipt.canonicalRootPath = copiedPath;
    await assert.rejects(
      validateSyntheticFixture({
        receipt: copied,
        expectedReceiptSha256: syntheticFixtureReceiptSha256(copied),
        expectedBinding: context.binding,
        policy: context.policy,
      }),
      { code: "changed_identity" },
    );
    writeFileSync(fixture.permit.canonicalPath, "Deterministic synthetic bytes");
    await assert.rejects(
      validateSyntheticFixture({
        receipt: fixture.receipt,
        expectedReceiptSha256: fixture.expectedReceiptSha256,
        expectedBinding: context.binding,
        policy: context.policy,
      }),
      { code: "hash_mismatch" },
    );
    writeFileSync(`${fixture.permit.canonicalPath}-wal`, "unexpected sidecar");
    await assert.rejects(
      validateSyntheticFixture({
        receipt: fixture.receipt,
        expectedReceiptSha256: fixture.expectedReceiptSha256,
        expectedBinding: context.binding,
        policy: context.policy,
      }),
      { code: "manifest_mismatch" },
    );
  });
});

test("changed root identity or private mode retains the exact tree", async () => {
  await withScratch(async (context) => {
    const owner = ownerAt(context);
    const original = owner.creationReceipt.canonicalRootPath;
    const moved = `${original}-moved`;
    renameSync(original, moved);
    mkdirSync(original, { mode: 0o700 });
    assert.equal(disposeOwnedRoot(owner).outcome, "retained");
    assert.equal(existsSync(moved), true);
    assert.equal(existsSync(original), true);
    const changedMode = ownerAt(context, "changed-mode");
    chmodSync(changedMode.creationReceipt.canonicalRootPath, 0o755);
    assert.equal(disposeOwnedRoot(changedMode).outcome, "retained");
  });
});

test("validation cancellation and manifest limits refuse without weakening receipt checks", async () => {
  await withScratch(async (context) => {
    const fixture = await closedFixture(context);
    const controller = new AbortController();
    controller.abort();
    await assert.rejects(
      validateSyntheticFixture({
        receipt: fixture.receipt,
        expectedReceiptSha256: fixture.expectedReceiptSha256,
        expectedBinding: context.binding,
        policy: context.policy,
        signal: controller.signal,
      }),
      { code: "cancelled" },
    );
    await assert.rejects(
      validateSyntheticFixture({
        receipt: fixture.receipt,
        expectedReceiptSha256: fixture.expectedReceiptSha256,
        expectedBinding: context.binding,
        policy: { ...context.policy, maxFileBytes: 1 },
      }),
      { code: "manifest_limit" },
    );
    assert.equal(disposeOwnedRoot(fixture.owner).outcome, "complete");
  });
});
