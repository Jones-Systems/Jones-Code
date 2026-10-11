import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import {
  PreparationNativeUnavailable,
  preparationFixtureLiteral,
  preparationNativeSourceInputs,
  preparationInputOpenFlags,
  preparationRepeatOracleSource,
  runLegacyPreparationNativeFixture,
  validatePreparationNativeClosure,
  validatePreparationNativeInput,
} from "./legacy-preparation-native-fixture.mjs";
import {
  runLegacyIdentityFixtureCommand,
  withLegacyIdentityScratch,
} from "./legacy-identity-native-fixture.mjs";

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

NodeTest.test("all fixture input acquisition uses nonblocking nofollow descriptors", () => {
  NodeAssert.equal(
    preparationInputOpenFlags & NodeFS.constants.O_NONBLOCK,
    NodeFS.constants.O_NONBLOCK,
  );
  NodeAssert.equal(
    preparationInputOpenFlags & NodeFS.constants.O_NOFOLLOW,
    NodeFS.constants.O_NOFOLLOW,
  );
});

NodeTest.test(
  "repeat oracle rejects accepting peers outside the controller catch-all",
  async () => {
    await withLegacyIdentityScratch(async (root) => {
      const script = NodePath.join(root, "oracle.py");
      await NodeFSP.writeFile(
        script,
        String.raw`
import json, struct, time
class Refused(Exception): pass
` +
          preparationRepeatOracleSource +
          String.raw`
class Process:
    def __init__(self, response, code=70, stdin=True, signals=None):
        self.response, self.code = response, code
        self.input_open, self.signals = stdin, [] if signals is None else signals
        self.output, self.error, self.eof = bytearray(), bytearray(), set()
        self.child, self.reaped, self.waits = self, False, 0
    def pump(self, deadline):
        assert time.monotonic() < deadline
        if self.response is not None:
            raw = json.dumps(self.response, sort_keys=True, separators=(',', ':')).encode()
            self.output.extend(struct.pack('>I', len(raw)) + raw)
        else:
            self.error.extend(b'Closed preparation worker refused.\n')
            self.eof = {'out', 'err'}
    def wait(self, timeout):
        assert timeout > 0
        self.waits += 1
        return self.code
def caught(process, evidence):
    try: observe_repeat_rejection(process, evidence)
    except BaseException as cause: return {'wrappedControllerFailure': cause}
    raise AssertionError('Expected controller failure')
controls = 0
for operation, sequence in (('PLAN', 2), ('APPLY', 3)):
    for response in ('SQL_SCOPE_OPENED', 'CLOSED_RESULT'):
        header = dict(schema=1, sessionId='a'*64, sequence=sequence, type=response, payloadBytes=0)
        if response == 'SQL_SCOPE_OPENED': header.update(scopeId='b'*64, purpose='plan' if operation == 'PLAN' else 'apply', access='read-write')
        else: header.update(scopeId='b'*64, planSha256='c'*64, outcome='applied', errorCode=None)
        process, evidence = Process(header), {}
        assert caught(process, evidence)['wrappedControllerFailure'] is not None
        assert evidence['firstHeader']['type'] == response and evidence['outputBytes'] > 0
        # This is deliberately outside caught(): the original false-positive catch-all must not absorb it.
        try: require_repeat_rejection(evidence)
        except AssertionError: pass
        else: raise AssertionError('An accepting peer passed the repeat oracle')
        assert process.waits == 0 and not process.reaped
        controls += 1
process, evidence = Process(None), {}
caught(process, evidence); require_repeat_rejection(evidence)
assert process.waits == 1 and process.reaped and process.input_open
for process in (Process(None, code=0), Process(None, stdin=False), Process(None, signals=['parent-stop'])):
    evidence = {}; caught(process, evidence)
    try: require_repeat_rejection(evidence)
    except AssertionError: pass
    else: raise AssertionError('Controller-induced closure passed the repeat oracle')
print(json.dumps({'acceptingControlsRejected': controls, 'realRefusalShapeAccepted': True, 'inducedClosureRejected': 3}))
`,
        { flag: "wx", mode: 0o400 },
      );
      const result = await runLegacyIdentityFixtureCommand(
        "/usr/bin/python3",
        ["-I", "-B", script],
        root,
      );
      NodeAssert.equal(result.code, 0, result.stderr);
      NodeAssert.deepEqual(JSON.parse(result.stdout), {
        acceptingControlsRejected: 4,
        realRefusalShapeAccepted: true,
        inducedClosureRejected: 3,
      });
    });
  },
);

NodeTest.test("FIFO inputs refuse before reading or creating a copied artifact", async () => {
  await withLegacyIdentityScratch(async (root) => {
    const fifo = NodePath.join(root, "input.fifo");
    const created = await runLegacyIdentityFixtureCommand("/usr/bin/mkfifo", [fifo], root);
    NodeAssert.equal(created.code, 0, created.stderr);
    const moduleUrl = new URL("./legacy-preparation-native-fixture.mjs", import.meta.url).href;
    const script = String.raw`
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
const fixture = await import(process.argv[1]);
const fifo = process.argv[2], target = process.argv[3];
await assert.rejects(fixture.readPreparationNativeInputFile(fifo), { code: 'ERR_ASSERTION' });
await assert.rejects(fixture.copyPreparationNativeArtifact({path: fifo, sha256: 'a'.repeat(64), size: 32}, target), { code: 'ERR_ASSERTION' });
await assert.rejects(fs.lstat(target), { code: 'ENOENT' });
assert.equal((await fs.lstat(fifo)).isFIFO(), true);
process.stdout.write('fifo-refused');
`;
    // A regressed blocking open is contained in this exact captured child, never in the test runner.
    const result = await runLegacyIdentityFixtureCommand(
      process.execPath,
      ["--input-type=module", "-e", script, moduleUrl, fifo, NodePath.join(root, "copied")],
      root,
    );
    NodeAssert.equal(result.code, 0, result.stderr);
    NodeAssert.equal(result.stdout, "fifo-refused");
  });
});
