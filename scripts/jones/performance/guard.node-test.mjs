import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import * as NodeURL from "node:url";

import {
  assertOwnedDatabase,
  createOwnedRoot,
  disposeOwnedRoot,
  observeSyntheticClose,
  sealSyntheticFixture,
  syntheticFixtureReceiptSha256,
  validateSyntheticFixture,
} from "./guard.mjs";

const testDirectory = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const sourceRevision = "e5a31aceec91484b64315c63dcce80f6e7581604";
const producerStep = "spec.jones-performance-portfolio#task.e-root.001:synthetic-file-producer";
const testOwners = new WeakMap();

function closeResource(resource) {
  if (resource.fd !== null) {
    NodeFS.closeSync(resource.fd);
    resource.fd = null;
  }
}

function openResource(owner, permit) {
  const resource = { fd: NodeFS.openSync(permit.canonicalPath, "r+") };
  testOwners.get(owner).resources.add(resource);
  return resource;
}

async function withScratch(body) {
  const scratch = NodeFS.mkdtempSync(NodePath.join(testDirectory, ".guard-test-"));
  const resources = new Set();
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
    await body({ scratch, binding, policy, resources });
  } finally {
    for (const resource of resources) closeResource(resource);
    // This outer test root also owns intentionally retained adversarial trees and canaries.
    NodeFS.rmSync(scratch, { recursive: true, force: true });
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
  NodeFS.writeFileSync(resource.fd, "deterministic synthetic bytes");
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
  NodeAssert.throws(
    () => {
      const permit = assertOwnedDatabase(owner, options);
      opens++;
      return permit;
    },
    { code },
  );
  NodeAssert.equal(opens, 0);
}

NodeTest.test("fresh HOME/worktree children are private and exclusively created", async () => {
  await withScratch(async (context) => {
    const owner = ownerAt(context);
    NodeAssert.equal(NodeFS.lstatSync(owner.creationReceipt.canonicalRootPath).mode & 0o777, 0o700);
    const permit = assertOwnedDatabase(owner, {
      databaseRelativePath: "state.sqlite",
      access: "create",
    });
    NodeAssert.equal(NodeFS.lstatSync(permit.canonicalPath).mode & 0o777, 0o600);
    const resource = openResource(owner, permit);
    await observeSyntheticClose(owner, { permit, producerStep, resource, close: closeResource });
    refusalBeforeOpen(
      owner,
      { databaseRelativePath: "state.sqlite", access: "create" },
      "existing_destination",
    );
    NodeAssert.throws(() => ownerAt(context), { code: "existing_destination" });
    const cleanup = disposeOwnedRoot(owner);
    NodeAssert.equal(cleanup.outcome, "complete");
    NodeAssert.equal(cleanup.absent, true);
    NodeAssert.equal(NodeFS.existsSync(owner.creationReceipt.canonicalRootPath), false);
    const worktreeOwner = createOwnedRoot({
      parentPath: context.scratch,
      childName: "worktree-fixture",
      binding: context.binding,
      policy: {
        ...context.policy,
        homePath: NodePath.join(context.scratch, "other-home"),
        worktreePaths: [context.scratch],
      },
    });
    NodeAssert.equal(disposeOwnedRoot(worktreeOwner).outcome, "complete");
  });
});

NodeTest.test(
  "lexical protected overlap refuses ancestors and descendants before candidate inspection",
  async () => {
    await withScratch(async (context) => {
      for (const protectedPath of [
        NodePath.join(context.scratch, "absent", "root"),
        NodePath.join(context.scratch, "absent", "root", "store"),
      ]) {
        NodeAssert.throws(
          () =>
            createOwnedRoot({
              parentPath: NodePath.join(context.scratch, "absent"),
              childName: "root",
              binding: context.binding,
              policy: { ...context.policy, protectedPaths: [protectedPath] },
            }),
          { code: "protected_path" },
        );
      }
      NodeAssert.throws(
        () =>
          createOwnedRoot({
            parentPath: context.scratch,
            childName: "synthetic-protected-store",
            binding: context.binding,
            policy: context.policy,
          }),
        { code: "protected_path" },
      );
      NodeAssert.equal(NodeFS.existsSync(NodePath.join(context.scratch, "absent")), false);
    });
  },
);

NodeTest.test(
  "source binding accepts exact object ID lengths and refuses a 41-digit value",
  async () => {
    await withScratch(async (context) => {
      NodeAssert.throws(
        () =>
          createOwnedRoot({
            parentPath: context.scratch,
            childName: "invalid-binding",
            binding: { ...context.binding, sourceRevision: "a".repeat(41) },
            policy: context.policy,
          }),
        { code: "invalid_binding" },
      );
      NodeAssert.equal(NodeFS.existsSync(NodePath.join(context.scratch, "invalid-binding")), false);
      const owner = createOwnedRoot({
        parentPath: context.scratch,
        childName: "sha256-binding",
        binding: { ...context.binding, sourceRevision: "a".repeat(64) },
        policy: context.policy,
      });
      NodeAssert.equal(disposeOwnedRoot(owner).outcome, "complete");
    });
  },
);

NodeTest.test(
  "parent aliases, escapes and existing roots refuse without modifying canaries",
  async () => {
    await withScratch(async (context) => {
      const canonical = NodePath.join(context.scratch, "canonical");
      NodeFS.mkdirSync(canonical);
      const alias = NodePath.join(context.scratch, "alias");
      NodeFS.symlinkSync(canonical, alias);
      NodeFS.writeFileSync(NodePath.join(canonical, "canary"), "keep");
      NodeAssert.throws(
        () =>
          createOwnedRoot({
            parentPath: alias,
            childName: "fixture",
            binding: context.binding,
            policy: context.policy,
          }),
        { code: "aliased_path" },
      );
      NodeAssert.throws(
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
      NodeAssert.equal(NodeFS.readFileSync(NodePath.join(canonical, "canary"), "utf8"), "keep");
      NodeAssert.equal(disposeOwnedRoot(owner).outcome, "complete");
    });
  },
);

NodeTest.test("nested, database and sidecar symlinks refuse before a database opener", async () => {
  await withScratch(async (context) => {
    const canary = NodePath.join(context.scratch, "canary");
    NodeFS.writeFileSync(canary, "unchanged");
    for (const [index, relativePath] of [
      "nested-alias",
      "state.sqlite",
      "state.sqlite-wal",
      "state.sqlite-shm",
      "state.sqlite-journal",
    ].entries()) {
      const owner = ownerAt(context, `links-${index}`);
      NodeFS.symlinkSync(
        canary,
        NodePath.join(owner.creationReceipt.canonicalRootPath, relativePath),
      );
      refusalBeforeOpen(
        owner,
        { databaseRelativePath: "state.sqlite", access: "create" },
        "symlink",
      );
      NodeAssert.equal(disposeOwnedRoot(owner).outcome, "retained");
    }
    NodeAssert.equal(NodeFS.readFileSync(canary, "utf8"), "unchanged");
  });
});

NodeTest.test("hardlinked files refuse and cleanup retains both links", async () => {
  await withScratch(async (context) => {
    const owner = ownerAt(context);
    const canary = NodePath.join(context.scratch, "canary");
    NodeFS.writeFileSync(canary, "unchanged");
    NodeFS.linkSync(canary, NodePath.join(owner.creationReceipt.canonicalRootPath, "state.sqlite"));
    refusalBeforeOpen(
      owner,
      { databaseRelativePath: "state.sqlite", access: "create" },
      "hardlink",
    );
    NodeAssert.equal(disposeOwnedRoot(owner).outcome, "retained");
    NodeAssert.equal(NodeFS.readFileSync(canary, "utf8"), "unchanged");
  });
});

NodeTest.test(
  "serialized, cloned and observational handles never authorize writes or cleanup",
  async () => {
    await withScratch(async (context) => {
      const owner = ownerAt(context);
      for (const forged of [
        owner.creationReceipt,
        { ...owner },
        JSON.parse(JSON.stringify(owner)),
      ]) {
        refusalBeforeOpen(
          forged,
          { databaseRelativePath: "state.sqlite", access: "create" },
          "invalid_owner",
        );
        NodeAssert.equal(disposeOwnedRoot(forged).outcome, "unknown");
      }
      NodeAssert.equal(NodeFS.existsSync(owner.creationReceipt.canonicalRootPath), true);
      NodeAssert.equal(disposeOwnedRoot(owner).outcome, "complete");
    });
  },
);

NodeTest.test(
  "observed sync close produces a pinned readonly fixture with exact layout and hashes",
  async () => {
    await withScratch(async (context) => {
      const fixture = await closedFixture(context);
      const result = await validateSyntheticFixture({
        receipt: fixture.receipt,
        expectedReceiptSha256: fixture.expectedReceiptSha256,
        expectedBinding: context.binding,
        policy: context.policy,
      });
      NodeAssert.equal(result.access, "readonly");
      NodeAssert.equal(result.canonicalPath, fixture.permit.canonicalPath);
      NodeAssert.equal(result.verifiedFiles, 2);
      NodeAssert.ok(result.verifiedBytes > 0);
      NodeAssert.deepEqual(
        result.layout.map((entry) => entry.present),
        [true, false, false, false],
      );
      NodeAssert.equal(disposeOwnedRoot(fixture.owner).outcome, "complete");
    });
  },
);

NodeTest.test(
  "async resource closure executes exactly once and refuses concurrent or repeated closure",
  async () => {
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
          await new Promise((resolveClose) => {
            finish = resolveClose;
          });
          closeResource(value);
        },
      });
      await NodeAssert.rejects(
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
      NodeAssert.equal(disposeOwnedRoot(owner).outcome, "retained");
      finish();
      const proof = await pending;
      await NodeAssert.rejects(
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
      NodeAssert.equal(calls, 1);
      NodeAssert.ok(proof);
      NodeAssert.equal(disposeOwnedRoot(owner).outcome, "complete");
    });
  },
);

NodeTest.test("failed close yields no proof, refuses retry and retains the tree", async () => {
  await withScratch(async (context) => {
    const owner = ownerAt(context);
    const { permit, resource } = createdFile(owner);
    const failure = new Error("synthetic close failure");
    let calls = 0;
    await NodeAssert.rejects(
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
    await NodeAssert.rejects(
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
    await NodeAssert.rejects(
      sealSyntheticFixture(owner, {
        databaseRelativePath: "state.sqlite",
        producerStep,
        closedProof: true,
      }),
      { code: "invalid_close_proof" },
    );
    NodeAssert.equal(calls, 1);
    NodeAssert.equal(disposeOwnedRoot(owner).outcome, "retained");
  });
});

NodeTest.test("file replacement during actual close cannot produce a proof", async () => {
  await withScratch(async (context) => {
    const owner = ownerAt(context);
    const { permit, resource } = createdFile(owner);
    await NodeAssert.rejects(
      observeSyntheticClose(owner, {
        permit,
        producerStep,
        resource,
        close: (value) => {
          closeResource(value);
          NodeFS.renameSync(permit.canonicalPath, `${permit.canonicalPath}.previous`);
          NodeFS.writeFileSync(permit.canonicalPath, "replacement");
        },
      }),
      { code: "changed_identity" },
    );
    refusalBeforeOpen(
      owner,
      { databaseRelativePath: "state.sqlite", access: "readwrite" },
      "unregistered_database",
    );
    NodeAssert.equal(disposeOwnedRoot(owner).outcome, "retained");
  });
});

NodeTest.test(
  "proof forgery, foreign ownership, mismatches and concurrent consumption refuse",
  async () => {
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
        await NodeAssert.rejects(
          sealSyntheticFixture(owner, {
            databaseRelativePath: "state.sqlite",
            producerStep,
            closedProof: fake,
          }),
          { code: "invalid_close_proof" },
        );
      }
      await NodeAssert.rejects(
        sealSyntheticFixture(other, {
          databaseRelativePath: "state.sqlite",
          producerStep,
          closedProof: proof,
        }),
        { code: "invalid_close_proof" },
      );
      await NodeAssert.rejects(
        sealSyntheticFixture(owner, {
          databaseRelativePath: "other.sqlite",
          producerStep,
          closedProof: proof,
        }),
        { code: "invalid_close_proof" },
      );
      await NodeAssert.rejects(
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
      await NodeAssert.rejects(
        sealSyntheticFixture(owner, {
          databaseRelativePath: "state.sqlite",
          producerStep,
          closedProof: proof,
        }),
        { code: "invalid_close_proof" },
      );
      await sealing;
      await NodeAssert.rejects(
        sealSyntheticFixture(owner, {
          databaseRelativePath: "state.sqlite",
          producerStep,
          closedProof: proof,
        }),
        { code: "invalid_close_proof" },
      );
      NodeAssert.equal(disposeOwnedRoot(owner).outcome, "complete");
      NodeAssert.equal(disposeOwnedRoot(other).outcome, "complete");
    });
  },
);

NodeTest.test(
  "a new producer needs a fresh permit and invalidates an old same-size close proof",
  async () => {
    await withScratch(async (context) => {
      const owner = ownerAt(context);
      const first = createdFile(owner);
      const oldProof = await observeSyntheticClose(owner, {
        ...first,
        producerStep,
        close: closeResource,
      });
      await NodeAssert.rejects(
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
      NodeFS.writeFileSync(resource.fd, "Deterministic synthetic bytes");
      const nextProof = await observeSyntheticClose(owner, {
        permit,
        resource,
        producerStep: "second-producer",
        close: closeResource,
      });
      await NodeAssert.rejects(
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
      NodeAssert.equal(resealed.producerStep, "third-producer");
      NodeAssert.equal(disposeOwnedRoot(owner).outcome, "complete");
    });
  },
);

NodeTest.test(
  "unpinned, stale and mutated receipts refuse before readonly qualification",
  async () => {
    await withScratch(async (context) => {
      const fixture = await closedFixture(context);
      const options = {
        receipt: fixture.receipt,
        expectedBinding: context.binding,
        policy: context.policy,
      };
      await NodeAssert.rejects(validateSyntheticFixture(options), { code: "unpinned_receipt" });
      await NodeAssert.rejects(
        validateSyntheticFixture({
          ...options,
          expectedReceiptSha256: fixture.expectedReceiptSha256,
          expectedBinding: { ...context.binding, sourceRevision: "a".repeat(40) },
        }),
        { code: "invalid_receipt" },
      );
      const changed = JSON.parse(JSON.stringify(fixture.receipt));
      changed.producerStep = "untrusted-import";
      await NodeAssert.rejects(
        validateSyntheticFixture({
          ...options,
          receipt: changed,
          expectedReceiptSha256: fixture.expectedReceiptSha256,
        }),
        { code: "receipt_mismatch" },
      );
      NodeAssert.equal(disposeOwnedRoot(fixture.owner).outcome, "complete");
    });
  },
);

NodeTest.test(
  "copied roots and markers, changed bytes and sidecar layout never qualify",
  async () => {
    await withScratch(async (context) => {
      const fixture = await closedFixture(context);
      const copiedPath = NodePath.join(context.scratch, "copied");
      NodeFS.cpSync(fixture.owner.creationReceipt.canonicalRootPath, copiedPath, {
        recursive: true,
      });
      const copied = JSON.parse(JSON.stringify(fixture.receipt));
      copied.creationReceipt.canonicalRootPath = copiedPath;
      await NodeAssert.rejects(
        validateSyntheticFixture({
          receipt: copied,
          expectedReceiptSha256: syntheticFixtureReceiptSha256(copied),
          expectedBinding: context.binding,
          policy: context.policy,
        }),
        { code: "changed_identity" },
      );
      NodeFS.writeFileSync(fixture.permit.canonicalPath, "Deterministic synthetic bytes");
      await NodeAssert.rejects(
        validateSyntheticFixture({
          receipt: fixture.receipt,
          expectedReceiptSha256: fixture.expectedReceiptSha256,
          expectedBinding: context.binding,
          policy: context.policy,
        }),
        { code: "hash_mismatch" },
      );
      NodeFS.writeFileSync(`${fixture.permit.canonicalPath}-wal`, "unexpected sidecar");
      await NodeAssert.rejects(
        validateSyntheticFixture({
          receipt: fixture.receipt,
          expectedReceiptSha256: fixture.expectedReceiptSha256,
          expectedBinding: context.binding,
          policy: context.policy,
        }),
        { code: "manifest_mismatch" },
      );
    });
  },
);

NodeTest.test("changed root identity or private mode retains the exact tree", async () => {
  await withScratch(async (context) => {
    const owner = ownerAt(context);
    const original = owner.creationReceipt.canonicalRootPath;
    const moved = `${original}-moved`;
    NodeFS.renameSync(original, moved);
    NodeFS.mkdirSync(original, { mode: 0o700 });
    NodeAssert.equal(disposeOwnedRoot(owner).outcome, "retained");
    NodeAssert.equal(NodeFS.existsSync(moved), true);
    NodeAssert.equal(NodeFS.existsSync(original), true);
    const changedMode = ownerAt(context, "changed-mode");
    NodeFS.chmodSync(changedMode.creationReceipt.canonicalRootPath, 0o755);
    NodeAssert.equal(disposeOwnedRoot(changedMode).outcome, "retained");
  });
});

NodeTest.test(
  "validation cancellation and manifest limits refuse without weakening receipt checks",
  async () => {
    await withScratch(async (context) => {
      const fixture = await closedFixture(context);
      const controller = new AbortController();
      controller.abort();
      await NodeAssert.rejects(
        validateSyntheticFixture({
          receipt: fixture.receipt,
          expectedReceiptSha256: fixture.expectedReceiptSha256,
          expectedBinding: context.binding,
          policy: context.policy,
          signal: controller.signal,
        }),
        { code: "cancelled" },
      );
      await NodeAssert.rejects(
        validateSyntheticFixture({
          receipt: fixture.receipt,
          expectedReceiptSha256: fixture.expectedReceiptSha256,
          expectedBinding: context.binding,
          policy: { ...context.policy, maxFileBytes: 1 },
        }),
        { code: "manifest_limit" },
      );
      NodeAssert.equal(disposeOwnedRoot(fixture.owner).outcome, "complete");
    });
  },
);

NodeTest.test("named protected stores refuse before filesystem inspection", async () => {
  await withScratch(async (context) => {
    for (const relativePath of [".t3", ".codex", ".ssh", ".config", ".t3/userdata"]) {
      const protectedPath = NodePath.join(context.scratch, relativePath);
      const policy = { ...context.policy, protectedPaths: [protectedPath] };
      for (const candidate of [protectedPath, NodePath.join(protectedPath, "fixture")]) {
        NodeAssert.throws(
          () =>
            createOwnedRoot({
              parentPath: NodePath.dirname(candidate),
              childName: NodePath.basename(candidate),
              binding: context.binding,
              policy,
            }),
          { code: "protected_path" },
        );
      }
      NodeAssert.equal(NodeFS.existsSync(protectedPath), false);
    }
  });
});
