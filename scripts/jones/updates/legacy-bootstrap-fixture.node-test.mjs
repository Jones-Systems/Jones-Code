import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import {
  createJonesLegacyBootstrapBundle,
  JONES_PREPARATION_SOURCE_INPUTS,
} from "./legacy-bootstrap-bundle.ts";
import {
  assertBootstrapResponse,
  assertInventoryResponse,
  closeStartupObservation,
  verifyPackagedBootstrap,
} from "./legacy-bootstrap-fixture.mjs";
import { FixtureOwnership } from "./native-startup-fixture.mjs";

const metadata = {
  version: "1.0.0-preview.20261011.12.1",
  jonesSource: {
    repository: "Jones-Systems/Jones-Code",
    sha: "a".repeat(40),
    tree: "b".repeat(40),
  },
};

async function withBundle(body, withNative = false) {
  const ownership = new FixtureOwnership();
  const root = await NodeFSP.realpath(
    await ownership.temporary(NodePath.join(NodeOS.tmpdir(), "jones-bootstrap-bundle-")),
  );
  try {
    const app = NodePath.join(root, "Candidate.app");
    const resources = NodePath.join(app, "Contents/Resources");
    await NodeFSP.mkdir(NodePath.join(resources, "jones-bootstrap"), { recursive: true });
    const identity = {
      repository: metadata.jonesSource.repository,
      sourceSha: metadata.jonesSource.sha,
      sourceTree: metadata.jonesSource.tree,
      version: metadata.version,
    };
    let files = createJonesLegacyBootstrapBundle(identity);
    if (withNative) {
      const binary = Buffer.alloc(160);
      [0xfeedfacf, 0x0100000c, 0, 2, 2, 96, 0, 0].forEach((value, index) =>
        binary.writeUInt32LE(value, index * 4),
      );
      binary.writeUInt32LE(0x19, 32);
      binary.writeUInt32LE(72, 36);
      binary.write("__TEXT", 40, "ascii");
      binary.writeBigUInt64LE(160n, 80);
      binary.writeUInt32LE(0x80000028, 104);
      binary.writeUInt32LE(24, 108);
      binary.writeBigUInt64LE(128n, 112);
      const source = await NodeFSP.readFile(
        new URL(
          "../../../apps/desktop/src/jones/updates/jonesLegacyIdentityNative.c",
          import.meta.url,
        ),
      );
      const { jonesLegacyMaintenanceSource } =
        await import("../../../apps/desktop/src/jones/updates/jonesLegacyMaintenanceSource.ts");
      files = createJonesLegacyBootstrapBundle(identity, {
        identityNative: binary,
        identityNativeSourceSha256: NodeCrypto.createHash("sha256").update(source).digest("hex"),
        maintenanceSource: jonesLegacyMaintenanceSource,
      });
    }
    const nativeProof = withNative ? files[2].content : undefined;
    for (const file of files) {
      const filename = NodePath.join(resources, file.relativePath);
      await NodeFSP.writeFile(filename, file.content, { flag: "wx", mode: file.mode });
    }
    return await body({ root, app, resources, files, nativeProof });
  } finally {
    await ownership.cleanup();
    await NodeAssert.rejects(NodeFSP.lstat(root), { code: "ENOENT" });
  }
}

NodeTest.test("packaged bootstrap proves actual source bytes, provenance, and modes", async () => {
  await withBundle(async ({ app }) => {
    const bundle = await verifyPackagedBootstrap(app, metadata);
    NodeAssert.equal(bundle.sourceSha, metadata.jonesSource.sha);
    NodeAssert.equal(bundle.version, metadata.version);
    NodeAssert.match(bundle.executorSha256, /^[a-f0-9]{64}$/);
  });
});

NodeTest.test(
  "v3 packaged qualification binds the sixth resource to independent canonical proof",
  async () => {
    await withBundle(async ({ app, resources, nativeProof }) => {
      const hash = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");
      // Synthetic packaging data are not a qualified preparation worker.
      const bytes = Buffer.from("inert preparation resource; never execute\n");
      const config =
        '{"main":"offlinePreparationEntry.mjs","mainFormat":"module","executable":"node","output":"jones-offline-preparation","disableExperimentalSEAWarning":true,"useSnapshot":false,"useCodeCache":false,"execArgv":[],"execArgvExtension":"none"}\n';
      const proof = {
        ...JSON.parse(nativeProof),
        schema: 3,
        preparation: {
          schema: 1,
          protocol: 1,
          provenanceSha256: "1".repeat(64),
          sourceInputHashes: Object.fromEntries(
            JONES_PREPARATION_SOURCE_INPUTS.map((name) => [name, "2".repeat(64)]),
          ),
          entry: { sha256: "3".repeat(64), size: 4096 },
          seaConfig: { sha256: hash(config), size: Buffer.byteLength(config) },
          executable: {
            name: "jones-offline-preparation",
            sha256: hash(bytes),
            size: bytes.length,
          },
          node: {
            version: "26.8.2",
            platform: "darwin",
            architecture: "arm64",
            archiveSha256: "4".repeat(64),
            archiveBytes: 4096,
            checksumsSha256: "5".repeat(64),
            runtimeSha256: "6".repeat(64),
            runtimeBytes: 4096,
          },
          sqlite: { version: "3.50.4", sourceId: "2025-07-30 19:33:53 " + "7".repeat(64) },
        },
      };
      const raw = JSON.stringify(proof) + "\n";
      const manifest = NodePath.join(resources, "jones-bootstrap/BUNDLE.json");
      await NodeFSP.chmod(manifest, 0o600);
      await NodeFSP.writeFile(manifest, raw);
      await NodeFSP.chmod(manifest, 0o400);
      const filename = NodePath.join(resources, "jones-bootstrap/jones-offline-preparation");
      await NodeFSP.writeFile(filename, bytes, { flag: "wx", mode: 0o500 });
      NodeAssert.equal((await verifyPackagedBootstrap(app, metadata, raw)).schema, 3);
      await NodeAssert.rejects(verifyPackagedBootstrap(app, metadata));
      await NodeAssert.rejects(verifyPackagedBootstrap(app, metadata, raw.trimEnd()));
      await NodeAssert.rejects(
        verifyPackagedBootstrap(
          app,
          { ...metadata, jonesSource: { ...metadata.jonesSource, sha: "c".repeat(40) } },
          raw,
        ),
      );
      await NodeFSP.chmod(filename, 0o600);
      await NodeFSP.writeFile(filename, Buffer.alloc(bytes.length, 0));
      await NodeFSP.chmod(filename, 0o500);
      await NodeAssert.rejects(verifyPackagedBootstrap(app, metadata, raw), /digest/);
      await NodeFSP.unlink(filename);
      await NodeAssert.rejects(verifyPackagedBootstrap(app, metadata, raw), { code: "ENOENT" });
    }, true);
  },
);

NodeTest.test("missing bundle and substituted executor cannot qualify", async () => {
  await withBundle(async ({ app, resources }) => {
    const manifest = NodePath.join(resources, "jones-bootstrap/BUNDLE.json");
    await NodeFSP.unlink(manifest);
    await NodeAssert.rejects(verifyPackagedBootstrap(app, metadata), { code: "ENOENT" });
  });
  await withBundle(async ({ app, resources }) => {
    const executor = NodePath.join(resources, "jones-bootstrap/executor.py");
    const original = await NodeFSP.readFile(executor, "utf8");
    await NodeFSP.chmod(executor, 0o600);
    try {
      await NodeFSP.writeFile(executor, "unexpected executor\n");
    } finally {
      await NodeFSP.chmod(executor, 0o500);
    }
    await NodeAssert.rejects(verifyPackagedBootstrap(app, metadata), {
      code: "ERR_ASSERTION",
      actual: "unexpected executor\n",
      expected: original,
    });
  });
});

NodeTest.test("wrong source binding and writable bootstrap cannot qualify", async () => {
  await withBundle(async ({ app, resources }) => {
    await NodeAssert.rejects(
      verifyPackagedBootstrap(app, {
        ...metadata,
        jonesSource: { ...metadata.jonesSource, sha: "c".repeat(40) },
      }),
      (error) => {
        NodeAssert.equal(error.code, "ERR_ASSERTION");
        NodeAssert.equal(JSON.parse(error.actual).sourceSha, metadata.jonesSource.sha);
        NodeAssert.equal(JSON.parse(error.expected).sourceSha, "c".repeat(40));
        return true;
      },
    );
    await NodeFSP.chmod(NodePath.join(resources, "jones-bootstrap/bootstrap.py"), 0o700);
    await NodeAssert.rejects(verifyPackagedBootstrap(app, metadata), {
      code: "ERR_ASSERTION",
      actual: 0o700,
      expected: 0o500,
    });
  });
});

function result(response, audit = { positiveControl: true, violations: [] }, code = 0) {
  return {
    code,
    signal: null,
    stdout: JSON.stringify(response),
    stderr: `JONES_BOOTSTRAP_AUDIT=${JSON.stringify(audit)}\n`,
  };
}
const inspected = {
  status: "bootstrap-required",
  result: "inspected",
  activationAllowed: false,
  blocker: "DESK-FIRST-01",
  observationOnly: true,
  nextRequirement: "qualified-maintenance-admission",
  bindingDigest: "c".repeat(64),
};

const inventory = {
  schema: 1,
  operation: "inventory-routes",
  status: "partial",
  observationOnly: true,
  activationAllowed: false,
  launchMembershipComplete: false,
  processCustody: "unobserved",
  blocker: "DESK-FIRST-01",
  coverage: {
    history: "partial",
    orphanSiblingApps: "unobserved",
    manualCopies: "not-enumerated",
    interpreterIdentity: "unobserved",
    payloadDigests: "not-verified",
  },
  observations: [{ kind: "helper", path: "/synthetic/helper.py", binding: "content-addressed" }],
  issues: [{ code: "receipt-missing", path: "/synthetic/stage" }],
};
const inventoryAudit = {
  positiveControl: true,
  violations: [],
  readOnly: true,
  writeControl: true,
};

NodeTest.test("inventory cannot qualify activation, complete membership, or custody", () => {
  NodeAssert.deepEqual(assertInventoryResponse(result(inventory, inventoryAudit, 2)), inventory);
  const observed = { ...inventory, status: "observed", issues: [] };
  assertInventoryResponse(result(observed, inventoryAudit), "observed");
  for (const changed of [
    { activationAllowed: true },
    { launchMembershipComplete: true },
    { processCustody: "verified" },
    { observationOnly: false },
    { coverage: { ...inventory.coverage, history: "complete" } },
  ])
    NodeAssert.throws(() =>
      assertInventoryResponse(result({ ...observed, ...changed }, inventoryAudit), "observed"),
    );
  NodeAssert.throws(() => assertInventoryResponse(result(inventory, inventoryAudit), "partial"));
});

NodeTest.test("inventory qualification requires independent read and write audit controls", () => {
  for (const changed of [
    { positiveControl: false },
    { writeControl: false },
    { readOnly: false },
    { violations: ["filesystem-write"] },
    { violations: ["protected-path"] },
    { violations: ["child-execution"] },
  ])
    NodeAssert.throws(() =>
      assertInventoryResponse(result(inventory, { ...inventoryAudit, ...changed }, 2)),
    );
  NodeAssert.throws(() => assertInventoryResponse(result(inventory, undefined, 2)));
  NodeAssert.throws(() =>
    assertInventoryResponse({ ...result(inventory, inventoryAudit, 2), stderr: "" }),
  );
});

NodeTest.test("inspection and executor staging are never counted as activation", () => {
  NodeAssert.deepEqual(assertBootstrapResponse(result(inspected), "inspected"), inspected);
  for (const expected of ["created", "no-change"]) {
    const staged = { ...inspected, result: expected, status: "executor-ready" };
    assertBootstrapResponse(result(staged), expected);
    NodeAssert.throws(() =>
      assertBootstrapResponse(result({ ...staged, activationAllowed: true }), expected),
    );
    NodeAssert.throws(() =>
      assertBootstrapResponse(result({ ...staged, status: "upgraded" }), expected),
    );
  }
});

NodeTest.test(
  "protected access, child execution, and missing positive controls fail qualification",
  () => {
    for (const audit of [
      { positiveControl: false, violations: [] },
      { positiveControl: true, violations: ["protected-path"] },
      { positiveControl: true, violations: ["child-execution"] },
    ])
      NodeAssert.throws(() => assertBootstrapResponse(result(inspected, audit), "inspected"));
    NodeAssert.throws(() =>
      assertBootstrapResponse({ ...result(inspected), stderr: "" }, "inspected"),
    );
  },
);

NodeTest.test("a partial stage must refuse without reporting creation or activation", () => {
  const refused = { status: "refused", activationAllowed: false, blocker: "DESK-FIRST-01" };
  assertBootstrapResponse(result(refused, undefined, 1), "refused");
  NodeAssert.throws(() => assertBootstrapResponse(result(refused), "refused"));
  NodeAssert.throws(() => assertBootstrapResponse(result(refused, undefined, 1), "created"));
  NodeAssert.throws(() =>
    assertBootstrapResponse(
      result({ ...refused, activationAllowed: true }, undefined, 1),
      "refused",
    ),
  );
});

NodeTest.test("fixture success and deliberate failure clean only their own roots", async () => {
  let survivor;
  let removed;
  await withBundle(async (outer) => {
    survivor = outer.root;
    const failure = new Error("deliberate fixture failure");
    await NodeAssert.rejects(
      withBundle(async (inner) => {
        removed = inner.root;
        NodeAssert.notEqual(removed, survivor);
        throw failure;
      }),
      (error) => error === failure,
    );
    await NodeAssert.rejects(NodeFSP.lstat(removed), { code: "ENOENT" });
    NodeAssert.ok((await NodeFSP.lstat(survivor)).isDirectory());
  });
  await NodeAssert.rejects(NodeFSP.lstat(survivor), { code: "ENOENT" });
});

NodeTest.test(
  "startup closure rejects history loss and protected mutations received during close",
  async () => {
    for (const event of [
      { path: "/unrelated", flags: 0x01 },
      { path: "/protected/transient", flags: 0x100 },
    ]) {
      const observer = {
        events: [],
        async close() {
          this.events.push(event);
        },
      };
      await NodeAssert.rejects(closeStartupObservation(observer, 0, ["/protected"]));
    }
    const failure = new Error("observer failed while closing");
    await NodeAssert.rejects(
      closeStartupObservation(
        {
          events: [],
          async close() {
            throw failure;
          },
        },
        0,
        ["/protected"],
      ),
      (error) => error === failure,
    );
  },
);

NodeTest.test(
  "startup evidence is fixed at observer closure and excludes subsequent bootstrap activity",
  async () => {
    let closed = false;
    const observer = {
      events: [],
      async close() {
        this.events.push({ path: "/unrelated", flags: 0x100 });
        closed = true;
      },
    };
    const evidence = await closeStartupObservation(observer, 0, ["/protected"]);
    NodeAssert.equal(closed, true);
    NodeAssert.deepEqual(evidence, {
      scope: "packaged-normal-startup-refusal",
      historyLoss: false,
      eventCount: 1,
      closedBeforeBootstrap: true,
    });
    observer.events.push({ path: "/unrelated", flags: 0x01 });
    NodeAssert.equal(evidence.eventCount, 1);
    NodeAssert.ok(Object.isFrozen(evidence));
  },
);

NodeTest.test(
  "v2 packaged bootstrap requires the independent proof and exact final native bytes",
  async () => {
    await withBundle(async ({ app, resources, files, nativeProof }) => {
      const bundle = await verifyPackagedBootstrap(app, metadata, nativeProof);
      NodeAssert.equal(bundle.schema, 2);
      NodeAssert.equal(bundle.nativeArchitecture, "arm64");
      await NodeAssert.rejects(verifyPackagedBootstrap(app, metadata));
      const binary = Buffer.from(files[4].content);
      binary[159] = 255;
      const changed = {
        ...bundle,
        identityNativeSha256: NodeCrypto.createHash("sha256").update(binary).digest("hex"),
      };
      for (const [index, content] of [
        [4, binary],
        [2, JSON.stringify(changed) + "\n"],
      ]) {
        const filename = NodePath.join(resources, files[index].relativePath);
        await NodeFSP.chmod(filename, 0o600);
        await NodeFSP.writeFile(filename, content);
        await NodeFSP.chmod(filename, files[index].mode);
      }
      await NodeAssert.rejects(verifyPackagedBootstrap(app, metadata, nativeProof));
    }, true);
  },
);
