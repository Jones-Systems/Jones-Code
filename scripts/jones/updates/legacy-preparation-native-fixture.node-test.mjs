import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeTest from "node:test";
import {
  PreparationNativeUnavailable,
  preparationFixtureLiteral,
  preparationNativeSourceInputs,
  runLegacyPreparationNativeFixture,
  validatePreparationNativeClosure,
  validatePreparationNativeInput,
} from "./legacy-preparation-native-fixture.mjs";

function input() {
  const file = (name, maximum = 32) => ({
    path: "/owned-fixture/" + name,
    sha256: "a".repeat(64),
    size: maximum,
  });
  return {
    schema: 1,
    classification: "synthetic-preparation-native-input",
    sourceSha: "b".repeat(40),
    sourceTree: "c".repeat(40),
    nativeSourceSha256: "d".repeat(64),
    preparation: {
      schema: 1,
      protocol: 1,
      sourceInputHashes: Object.fromEntries(
        preparationNativeSourceInputs.map((name) => [name, "e".repeat(64)]),
      ),
      executable: { name: "jones-offline-preparation", sha256: "a".repeat(64), size: 32 },
      entry: { sha256: "a".repeat(64), size: 32 },
      seaConfig: { sha256: "a".repeat(64), size: 32 },
      provenanceSha256: "a".repeat(64),
      node: {
        version: "26.8.2",
        platform: "darwin",
        architecture: "arm64",
        runtimeSha256: "a".repeat(64),
        runtimeBytes: 32,
      },
    },
    files: Object.fromEntries(
      ["executable", "node", "native", "entry", "seaConfig", "provenance"].map((name) => [
        name,
        file(name),
      ]),
    ),
  };
}

NodeTest.test(
  "artifact binding accepts a bounded fixture record without granting producer qualification",
  () => {
    const value = input();
    NodeAssert.equal(validatePreparationNativeInput(value), value);
    NodeAssert.equal(Object.hasOwn(value, "producerQualified"), false);
  },
);
for (const [name, mutate] of [
  [
    "missing final SEA",
    (value) => {
      delete value.files.executable;
    },
  ],
  [
    "held classification",
    (value) => {
      value.classification = "held-entry-only";
    },
  ],
  [
    "runtime rebound",
    (value) => {
      value.files.node.sha256 = "f".repeat(64);
    },
  ],
  [
    "SEA digest rebound",
    (value) => {
      value.files.executable.sha256 = "f".repeat(64);
    },
  ],
  [
    "oversized runtime",
    (value) => {
      value.files.executable.size = 256 * 1024 * 1024 + 1;
    },
  ],
  [
    "relative artifact path",
    (value) => {
      value.files.native.path = "../observer";
    },
  ],
  [
    "missing worker input",
    (value) => {
      delete value.preparation.sourceInputHashes[
        "apps/server/src/jones/updates/offlinePreparationWorker.ts"
      ];
    },
  ],
  [
    "unbounded source path",
    (value) => {
      value.preparation.sourceInputHashes["/foreign/private"] = "e".repeat(64);
    },
  ],
])
  NodeTest.test("artifact binding refuses " + name, () => {
    const value = input();
    mutate(value);
    NodeAssert.throws(() => validatePreparationNativeInput(value));
  });

NodeTest.test(
  "absent artifacts report qualification unavailable before scratch or processes",
  async () => {
    await NodeAssert.rejects(
      runLegacyPreparationNativeFixture(),
      (failure) =>
        failure instanceof PreparationNativeUnavailable &&
        failure.code === "JONES_PREPARATION_QUALIFICATION_UNAVAILABLE",
    );
  },
);

NodeTest.test(
  "literal data reader consumes actual historical tables, ledgers and provider capabilities",
  async () => {
    const core = await NodeFSP.readFile(
      new URL("../../../apps/server/src/jones/updates/offlinePreparationCore.ts", import.meta.url),
      "utf8",
    );
    const test = await NodeFSP.readFile(
      new URL(
        "../../../apps/server/src/jones/updates/offlinePreparationWorker.test.ts",
        import.meta.url,
      ),
      "utf8",
    );
    const tables = preparationFixtureLiteral(
      core,
      "const legacyTables: Readonly<Record<string, string>> = ",
      ";",
    );
    const ledgers = preparationFixtureLiteral(core, "const legacyLedgers = ", " as const;");
    const capabilities = preparationFixtureLiteral(
      test,
      "const historicalCapabilities = ",
      " as const satisfies",
    );
    NodeAssert.equal(Object.keys(tables).length, 13);
    NodeAssert.match(tables.orchestration_v2_effect_outbox, /payload_json TEXT NOT NULL/);
    NodeAssert.deepEqual(ledgers.upstream.at(-1), [59, "McpAppModelContext"]);
    NodeAssert.deepEqual(ledgers.jones.at(-1), [105, "UpstreamReferenceIsolation"]);
    NodeAssert.equal(capabilities.sessions.pendingRequestsSurviveRestart, false);
  },
);

for (const source of [
  "const item = {value: invoke()};",
  "const item = {value: 1, value: 2};",
  "const item = {value: true};const item = {};",
  "const item = {value: 1} + {};",
])
  NodeTest.test(
    "literal data reader rejects executable, duplicate or trailing input: " + source,
    () => {
      NodeAssert.throws(() => preparationFixtureLiteral(source, "const item = ", ";"));
    },
  );

function closure() {
  return {
    schema: 1,
    complete: true,
    capturedChildren: 1,
    constructorUnknown: false,
    pidSignalAuthority: false,
    processes: [
      {
        pid: 123,
        spawnAttempted: true,
        spawnUnknown: false,
        reaped: true,
        streamsClosed: true,
        signals: [],
        pidSignalAuthority: false,
      },
    ],
  };
}
NodeTest.test("closure requires real reaping and descriptor closure independently", () => {
  NodeAssert.equal(validatePreparationNativeClosure(closure()).complete, true);
  for (const field of ["reaped", "streamsClosed"])
    NodeAssert.throws(() =>
      validatePreparationNativeClosure({
        ...closure(),
        processes: [{ ...closure().processes[0], [field]: false }],
      }),
    );
});
NodeTest.test("unreturned constructor and PID-only evidence cannot settle fixture custody", () => {
  NodeAssert.throws(() =>
    validatePreparationNativeClosure({ ...closure(), constructorUnknown: true }),
  );
  NodeAssert.throws(() =>
    validatePreparationNativeClosure({ ...closure(), processes: [{ pid: 123 }] }),
  );
  NodeAssert.throws(() =>
    validatePreparationNativeClosure({
      ...closure(),
      processes: [{ ...closure().processes[0], spawnUnknown: true }],
    }),
  );
});

// Native cases run through the fixture CLI with a required artifact record. These
// ordinary tests establish data/cleanup refusal contracts, never a native pass.
