import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import { createJonesLegacyBootstrapBundle } from "./legacy-bootstrap-bundle.ts";
import {
  assertBootstrapResponse,
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

async function withBundle(body) {
  const ownership = new FixtureOwnership();
  const root = await ownership.temporary(NodePath.join(NodeOS.tmpdir(), "jones-bootstrap-bundle-"));
  try {
    const app = NodePath.join(root, "Candidate.app");
    const resources = NodePath.join(app, "Contents/Resources");
    await NodeFSP.mkdir(NodePath.join(resources, "jones-bootstrap"), { recursive: true });
    const files = createJonesLegacyBootstrapBundle({
      repository: metadata.jonesSource.repository,
      sourceSha: metadata.jonesSource.sha,
      sourceTree: metadata.jonesSource.tree,
      version: metadata.version,
    });
    for (const file of files) {
      const filename = NodePath.join(resources, file.relativePath);
      await NodeFSP.writeFile(filename, file.content, { flag: "wx", mode: file.mode });
    }
    return await body({ root, app, resources });
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
