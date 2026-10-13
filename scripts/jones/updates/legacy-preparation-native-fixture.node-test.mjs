import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import {
  PreparationNativeUnavailable,
  preparationFailureCaptureSource,
  preparationNativeDriver,
  publishPreparationNativeFailure,
  validatePreparationNativeFailure,
  preparationFixtureLiteral,
  preparationNativeSourceInputs,
  preparationNativeQualifierSourceInputs,
  preparationNativeCases,
  readPreparationNativeQualifierSourceHashes,
  validatePreparationNativeResult,
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
    nativeSourceSha256: "e".repeat(64),
    qualifierSourceHashes: Object.fromEntries(
      preparationNativeQualifierSourceInputs.map((name) => [name, "e".repeat(64)]),
    ),
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
      sqlite: { version: "3.50.0", sourceId: "2026-01-01 00:00:00 " + "f".repeat(64) },
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

// Representative fixed C records and Python barrier order; these are validator controls, not native evidence.
function nativeCaseEvidence(name, template, boot) {
  const prefix = ["ready", "closed-supplement", "plan-open"];
  const planned = [...prefix, "PLAN_READY", "intent-durable"];
  const applied = [...planned, "apply-open", "CLOSED_RESULT"];
  const settled = [...applied, "reaped", "observed"];
  const events = {
    apply: settled,
    reconcile: [...settled, "ready", "reconcile-open", "CLOSED_RESULT", "reaped", "observed"],
    "repeat-plan": planned,
    "durable-plan-drift": planned,
    "repeat-apply": applied,
    "close-uncertainty": applied,
    "wrong-scope": prefix,
    "paused-cancel": prefix,
    "custody-uncertainty": prefix,
    "closed-query-failure": [...prefix, "CLOSED_RESULT", "reaped"],
  }[name];
  let session = 0;
  const nativeObservations = [],
    observationContexts = [];
  for (const [eventIndex, phase] of events.entries()) {
    if (["intent-durable", "reaped", "observed"].includes(phase)) continue;
    if (phase === "ready") session += 1;
    const planning =
      ["closed-supplement", "plan-open", "PLAN_READY"].includes(phase) ||
      (phase === "CLOSED_RESULT" && name === "closed-query-failure");
    const databasePath = planning ? "/owned/preparation/state.sqlite" : "/owned/live/state.sqlite";
    const access = phase.endsWith("-open")
      ? phase === "reconcile-open"
        ? "read-only"
        : "read-write"
      : null;
    const observation = structuredClone(template);
    observation.operation = phase === "ready" ? "capture-worker" : "observe-worker";
    observation.processes[1].pid = 123 + session;
    observation.processes[1].uniqueId = String(123 + session);
    observation.excludedWorkerFds = [3, 4];
    observation.family = ["database", "wal", "shm"].map((role, index) => ({
      role,
      object: {
        index,
        path: databasePath + ["", "-wal", "-shm"][index],
        kind: "file",
        current: true,
        persistent: { volumeUuid: boot, fileId: String(index + 1) },
        stat: {
          device: "1",
          inode: String(index + 1),
          uid: 501,
          gid: 20,
          nlink: "1",
          mode: 384,
          flags: 0,
          size: "4096",
          mtimeNs: "1",
          ctimeNs: "1",
          birthtimeNs: "1",
        },
        mount: {
          filesystem: "apfs",
          flags: 0,
          mountPoint: "/",
          local: true,
          ownershipHonored: true,
          executable: true,
          persistentObjectIds: true,
          objectIds64Bit: true,
        },
      },
    }));
    observation.matches =
      access === null
        ? []
        : [
            {
              process: "worker",
              familyRole: "database",
              fd: 12,
              access,
              shared: false,
              eligibleOpenObservation: true,
              exclusion: null,
            },
          ];
    const sessionId = String(session).repeat(64),
      sequence = planning || session === 2 ? 1 : 2;
    let frame = null;
    if (!["ready", "closed-supplement"].includes(phase)) {
      frame = {
        schema: 1,
        type: access !== null ? "SQL_SCOPE_OPENED" : phase,
        sessionId,
        sequence,
        payloadBytes: phase === "PLAN_READY" ? 256 : 0,
      };
      if (access !== null)
        Object.assign(frame, {
          scopeId: String(sequence).repeat(64),
          purpose: phase.slice(0, -5),
          access,
        });
      else if (phase === "PLAN_READY") frame.planSha256 = "a".repeat(64);
      else
        Object.assign(frame, {
          scopeId: String(sequence).repeat(64),
          planSha256: name === "closed-query-failure" ? null : "a".repeat(64),
          outcome: name === "closed-query-failure" ? "query-failed" : "applied",
          errorCode: name === "closed-query-failure" ? "query-rejected" : null,
        });
    }
    nativeObservations.push(observation);
    observationContexts.push({
      eventIndex,
      sessionId,
      workerPid: observation.processes[1].pid,
      databasePath,
      databaseKind: planning ? "planning" : "live",
      access,
      frame,
    });
  }
  const closed = closure();
  closed.capturedChildren = session;
  closed.processes = Array.from({ length: session }, (_, index) => ({
    ...closure().processes[0],
    pid: 124 + index,
  }));
  return {
    events,
    nativeObservations,
    observationContexts,
    observerCalls: nativeObservations.length,
    closure: closed,
  };
}

function aggregate(value = input()) {
  const boot = "12345678-1234-1234-1234-123456789abc";
  const observation = {
    schema: 1,
    operation: "capture-worker",
    status: "observed",
    bootSessionUuid: boot,
    processes: [
      {
        role: "owner",
        pid: 123,
        parentPid: 1,
        uniqueId: "123",
        parentUniqueId: "1",
        pidVersion: 1,
        uid: 501,
        fdCount: 3,
      },
      {
        role: "worker",
        pid: 124,
        parentPid: 123,
        uniqueId: "124",
        parentUniqueId: "123",
        pidVersion: 1,
        uid: 501,
        fdCount: 3,
      },
    ],
    excludedWorkerFds: [],
    family: [],
    matches: [],
    suppliedFamilyOnly: true,
    familyMembershipComplete: false,
    observationOnly: true,
    sqliteOpenProved: false,
    databaseSyncCloseProved: false,
    workerReaped: false,
    stateAdmissionAllowed: false,
    launchMembershipComplete: false,
    processCustody: "unobserved",
    activationAllowed: false,
  };
  return {
    schema: 1,
    qualification: "synthetic-native-preparation",
    fixtureClosure: true,
    producerQualified: false,
    priorBootModeled: true,
    nativeColdOwnerQualified: false,
    stateAdmissionAllowed: false,
    activationAllowed: false,
    qualifierSourceHashes: value.qualifierSourceHashes,
    fixtureDataSourceHashes: {
      core: value.qualifierSourceHashes["apps/server/src/jones/updates/offlinePreparationCore.ts"],
      workerTests:
        value.qualifierSourceHashes[
          "apps/server/src/jones/updates/offlinePreparationWorker.test.ts"
        ],
    },
    artifact: {
      ...Object.fromEntries(
        Object.entries(value.files).map(([name, file]) => [name + "Sha256", file.sha256]),
      ),
      fileSizes: Object.fromEntries(
        Object.entries(value.files).map(([name, file]) => [name, file.size]),
      ),
      sourceSha: value.sourceSha,
      sourceTree: value.sourceTree,
      nativeSourceSha256: value.nativeSourceSha256,
    },
    cases: preparationNativeCases.map((name) => {
      const held = !["apply", "reconcile", "closed-query-failure"].includes(name);
      const evidence = nativeCaseEvidence(name, observation, boot);
      return {
        schema: 1,
        qualification: "synthetic-native-preparation",
        fixtureClosure: true,
        producerQualified: false,
        nativeColdOwnerQualified: false,
        stateAdmissionAllowed: false,
        activationAllowed: false,
        uri: {
          ...value.preparation.sqlite,
          nodeVersion: "26.8.2",
          missingFileCreated: false,
          existingUriRead: true,
        },
        closure: evidence.closure,
        cases: [
          {
            case: name,
            status: held
              ? "expected-refusal"
              : name === "closed-query-failure"
                ? "query-failed"
                : "applied",
            events: evidence.events,
            observerCalls: evidence.observerCalls,
            nativeObservations: evidence.nativeObservations,
            observationContexts: evidence.observationContexts,
            sqliteDescriptorObservations: true,
            rawPairPreserved: true,
            currentBootObserved: boot,
            priorBootModeled: true,
            workerReaped: true,
            productionAdmission: false,
            productionCustodyHeld: held,
            independentCleanupRecovered: name === "custody-uncertainty",
            heldCustodyReceipt: held ? { phase: "custody-held" } : null,
            repeatRejection: name.startsWith("repeat-")
              ? {
                  started: true,
                  outputBytes: 0,
                  firstHeader: null,
                  exitCode: 70,
                  reapedBeforeCleanup: true,
                  stdinStayedOpen: true,
                  signalsBeforeCleanup: [],
                  stderr: "Closed preparation worker refused.\n",
                }
              : null,
          },
        ],
      };
    }),
  };
}
NodeTest.test("result structure binds all ten cases without minting native authority", () => {
  validatePreparationNativeResult(aggregate(), input());
});
NodeTest.test(
  "result boot identity accepts case variants without rewriting raw observations",
  () => {
    const result = aggregate();
    for (const qualified of result.cases) {
      const item = qualified.cases[0];
      for (const observation of item.nativeObservations)
        observation.bootSessionUuid = observation.bootSessionUuid.toUpperCase();
    }
    const before = JSON.stringify(result);
    validatePreparationNativeResult(result, input());
    NodeAssert.equal(JSON.stringify(result), before);
  },
);
for (const [name, boot] of [
  ["different", "12345678-1234-1234-1234-123456789abd"],
  ["malformed", "not-a-uuid"],
  ["non-string", null],
  ["braced", "{12345678-1234-1234-1234-123456789abc}"],
  ["no-hyphens", "12345678123412341234123456789abc"],
  ["trailing-newline", "12345678-1234-1234-1234-123456789abc\n"],
]) {
  NodeTest.test("result boot identity refuses " + name, () => {
    const result = aggregate();
    result.cases[0].cases[0].nativeObservations[0].bootSessionUuid = boot;
    NodeAssert.throws(() => validatePreparationNativeResult(result, input()));
  });
}
for (const [name, mutate] of [
  [
    "unknown field",
    (value) => {
      value.admission = true;
    },
  ],
  [
    "sparse case",
    (value) => {
      delete value.cases[0];
    },
  ],
  ["missing case", (value) => value.cases.pop()],
  [
    "duplicate case",
    (value) => {
      value.cases[1] = value.cases[0];
    },
  ],
  [
    "unknown case",
    (value) => {
      value.cases[0].cases[0].case = "parent-death";
    },
  ],
  [
    "closure false",
    (value) => {
      value.cases[0].closure.complete = false;
    },
  ],
  [
    "missing close",
    (value) => {
      value.cases[0].closure.processes[0].streamsClosed = false;
    },
  ],
  [
    "producer authority",
    (value) => {
      value.producerQualified = true;
    },
  ],
  [
    "unmodeled boot claim",
    (value) => {
      value.priorBootModeled = false;
    },
  ],
  [
    "artifact rebound",
    (value) => {
      value.artifact.entrySha256 = "0".repeat(64);
    },
  ],
  [
    "qualifier drift",
    (value) => {
      value.qualifierSourceHashes = {
        ...value.qualifierSourceHashes,
        [preparationNativeQualifierSourceInputs[0]]: "0".repeat(64),
      };
    },
  ],
  [
    "oversized report",
    (value) => {
      value.cases[0].cases[0].events = ["x".repeat(262145)];
    },
  ],
])
  NodeTest.test("result structure refuses " + name, () => {
    const result = aggregate();
    mutate(result);
    NodeAssert.throws(() => validatePreparationNativeResult(result, input()));
  });
NodeTest.test(
  "fixed source snapshot detects actual seed drift and refuses replaced symlinks",
  async () => {
    await withLegacyIdentityScratch(async (root) => {
      for (const name of preparationNativeQualifierSourceInputs) {
        const path = NodePath.join(root, name);
        await NodeFSP.mkdir(NodePath.dirname(path), { recursive: true });
        await NodeFSP.writeFile(path, "owned source", { flag: "wx", mode: 0o600 });
      }
      const before = await readPreparationNativeQualifierSourceHashes(root);
      const name = "apps/server/src/jones/updates/offlinePreparationWorker.test.ts";
      const path = NodePath.join(root, name);
      await NodeFSP.writeFile(path, "changed seed");
      const after = await readPreparationNativeQualifierSourceHashes(root);
      NodeAssert.notEqual(before[name], after[name]);
      const value = input();
      value.qualifierSourceHashes = before;
      value.preparation.sourceInputHashes = Object.fromEntries(
        preparationNativeSourceInputs.map((key) => [key, before[key]]),
      );
      value.nativeSourceSha256 =
        before["apps/desktop/src/jones/updates/jonesLegacyIdentityNative.c"];
      const result = aggregate(value);
      result.qualifierSourceHashes = after;
      NodeAssert.throws(() => validatePreparationNativeResult(result, value));
      await NodeFSP.unlink(path);
      await NodeFSP.symlink(NodePath.join(root, preparationNativeQualifierSourceInputs[0]), path);
      await NodeAssert.rejects(readPreparationNativeQualifierSourceHashes(root));
    });
  },
);

for (const [name, mutate] of [
  [
    "malformed excluded FD",
    (item) => {
      item.nativeObservations[0].excludedWorkerFds = ["3"];
    },
  ],
  [
    "duplicate excluded FD",
    (item) => {
      item.nativeObservations[0].excludedWorkerFds = [3, 3];
    },
  ],
  [
    "unknown family role",
    (item) => {
      item.nativeObservations[0].family[0].role = "other";
    },
  ],
  [
    "extra object field",
    (item) => {
      item.nativeObservations[0].family[0].object.admitted = true;
    },
  ],
  [
    "malformed stat",
    (item) => {
      item.nativeObservations[0].family[0].object.stat.size = -1;
    },
  ],
  [
    "malformed mount",
    (item) => {
      item.nativeObservations[0].family[0].object.mount.local = false;
    },
  ],
  [
    "wrong sidecar path",
    (item) => {
      item.nativeObservations[0].family[1].object.path = "/foreign/wal";
    },
  ],
  [
    "extra match field",
    (item) => {
      item.nativeObservations[2].matches[0].proof = true;
    },
  ],
  [
    "malformed match FD",
    (item) => {
      item.nativeObservations[2].matches[0].fd = -1;
    },
  ],
  [
    "excluded FD presented eligible",
    (item) => {
      item.nativeObservations[2].matches[0].fd = 3;
    },
  ],
  [
    "missing database open",
    (item) => {
      item.nativeObservations[2].matches = [];
    },
  ],
  [
    "capture only",
    (item) => {
      item.nativeObservations.forEach((observation) => {
        observation.operation = "capture-worker";
        observation.matches = [];
      });
    },
  ],
  [
    "empty family",
    (item) => {
      item.nativeObservations[0].family = [];
    },
  ],
  [
    "missing events",
    (item) => {
      item.events = [];
    },
  ],
  [
    "worker identity drift",
    (item) => {
      item.nativeObservations[2].processes[1].uniqueId = "999";
    },
  ],
  [
    "worker PID mismatch",
    (item) => {
      item.observationContexts[2].workerPid = 999;
    },
  ],
  [
    "wrong database context",
    (item) => {
      item.observationContexts[2].databasePath = "/foreign/database";
    },
  ],
  [
    "wrong open purpose",
    (item) => {
      item.observationContexts[2].frame.purpose = "apply";
    },
  ],
  [
    "wrong closed scope",
    (item) => {
      item.observationContexts.at(-1).frame.scopeId = "f".repeat(64);
    },
  ],
])
  NodeTest.test("native evidence validator refuses " + name, () => {
    const result = aggregate();
    mutate(result.cases[0].cases[0]);
    NodeAssert.throws(() => validatePreparationNativeResult(result, input()));
  });
NodeTest.test(
  "reconcile requires its own captured identity, read-only live open and closed result",
  () => {
    const result = aggregate();
    const item = result.cases[1].cases[0];
    const at = item.observationContexts.findIndex(
      (context) => item.events[context.eventIndex] === "reconcile-open",
    );
    item.nativeObservations[at].matches[0].access = "read-write";
    NodeAssert.throws(() => validatePreparationNativeResult(result, input()));
  },
);

function failedFixture() {
  const snapshot = {
    stdoutBytes: 2,
    stdoutEof: true,
    stderrBytes: 100,
    stderrEof: true,
    returncode: 70,
    signal: null,
    stopRecords: 0,
    stderr: "JONES_PREPARATION_FAILURE phase=owner-current kind=refused",
  };
  return {
    schema: 1,
    classification: "native-preparation-failure",
    controllerFailure: { records: [], truncated: false },
    case: "apply",
    completedCases: 0,
    expectedFrames: ["PLAN_READY", "CLOSED_RESULT"],
    lastPhase: "sent-CONTINUE",
    processes: [
      {
        beforeCleanup: { ...snapshot, returncode: null, stderrEof: false },
        beforeCleanupTiming: "receive-failure",
        afterCleanup: snapshot,
      },
    ],
    closure: closure(),
  };
}
NodeTest.test(
  "failure record validates fixed phase and actual closure without private payloads",
  () => {
    NodeAssert.equal(validatePreparationNativeFailure(failedFixture()).case, "apply");
    for (const mutate of [
      (value) => {
        value.settings = "private";
      },
      (value) => {
        value.processes[0].afterCleanup.stderr = "Bearer private";
      },
      (value) => {
        value.processes[0].afterCleanup.stderr =
          "JONES_PREPARATION_FAILURE phase=private kind=refused";
      },
      (value) => {
        value.expectedFrames = ["private-frame"];
      },
      (value) => {
        value.closure.processes[0].reaped = false;
      },
      (value) => {
        value.processes[0].afterCleanup.stdoutBytes = -1;
      },
    ]) {
      const value = failedFixture();
      mutate(value);
      NodeAssert.throws(() => validatePreparationNativeFailure(value));
    }
  },
);
NodeTest.test("durable failure sidecar survives exact inner scratch cleanup", async () => {
  await withLegacyIdentityScratch(async (parent) => {
    let inner, caught;
    try {
      await withLegacyIdentityScratch(
        async (root) => {
          inner = root;
          await NodeFSP.writeFile(
            NodePath.join(root, "fixture-failure.json"),
            JSON.stringify(failedFixture()),
          );
          throw await publishPreparationNativeFailure(root, new Error("synthetic original"), 3);
        },
        { temporaryDirectory: parent },
      );
    } catch (cause) {
      caught = cause;
    }
    NodeAssert.equal(caught.cause.message, "synthetic original");
    await NodeAssert.rejects(NodeFSP.stat(inner), { code: "ENOENT" });
    NodeAssert.deepEqual(JSON.parse(await NodeFSP.readFile(caught.failureEvidencePath, "utf8")), {
      ...failedFixture(),
      completedCases: 3,
    });
    NodeAssert.equal((await NodeFSP.stat(caught.failureEvidencePath)).mode & 0o777, 0o600);
  });
});
NodeTest.test(
  "failed exclusive publication retains exact scratch and existing evidence",
  async () => {
    await withLegacyIdentityScratch(async (parent) => {
      let inner, caught;
      try {
        await withLegacyIdentityScratch(
          async (root) => {
            inner = root;
            await NodeFSP.writeFile(
              NodePath.join(root, "fixture-failure.json"),
              JSON.stringify(failedFixture()),
            );
            await NodeFSP.writeFile(root + "-failure.json", "existing evidence");
            throw await publishPreparationNativeFailure(root, new Error("synthetic original"));
          },
          { temporaryDirectory: parent },
        );
      } catch (cause) {
        caught = cause;
      }
      NodeAssert.equal(caught.preserveScratch, true);
      NodeAssert.equal(caught.scratchRoot, inner);
      NodeAssert.equal(
        await NodeFSP.readFile(inner + "-failure.json", "utf8"),
        "existing evidence",
      );
      NodeAssert.ok((await NodeFSP.stat(inner)).isDirectory());
      // No child or descriptor is uncertain in this synthetic conflict; the enclosing test owns cleanup.
    });
  },
);
NodeTest.test("failure capture drains bounded stderr and separates precleanup status", async () => {
  await withLegacyIdentityScratch(async (root) => {
    const script = NodePath.join(root, "failure-capture.py");
    await NodeFSP.writeFile(
      script,
      "import json, time\n" +
        preparationFailureCaptureSource +
        String.raw`
class Process:
    def __init__(self):
        self.child, self.output, self.error = self, bytearray(b'private-frame'), bytearray(b'Bearer private-secret\n')
        self.eof, self.signals, self.closed, self.code = {'out'}, [], False, None
    def poll(self): return self.code
    def pump(self, deadline):
        assert time.monotonic() < deadline
        self.error.extend(b'JONES_PREPARATION_FAILURE phase=owner-current kind=refused\n')
        self.eof.add('err'); self.code = 70
process = Process()
before = failure_process_snapshot(process)
drain_failure_stderr(process)
after = failure_process_snapshot(process)
assert before['returncode'] is None and after['returncode'] == 70
assert before['stdoutEof'] and not before['stderrEof'] and after['stderrEof']
assert 'private' not in json.dumps([before, after])
assert after['stderr'].endswith('phase=owner-current kind=refused')
assert len(safe_worker_stderr(b'private\n' * 5000)) <= 2048
for gate in _FAILURE_GATES:
    marker = 'JONES_PREPARATION_GATE gate=' + gate + ' command=plan scope=succeeded'
    assert safe_worker_stderr(marker.encode()) == marker
for marker in ('JONES_PREPARATION_GATE gate=private command=plan scope=succeeded',
               'JONES_PREPARATION_GATE gate=runtime command=private scope=succeeded',
               'JONES_PREPARATION_GATE gate=runtime command=plan scope=private',
               'JONES_PREPARATION_GATE gate=runtime command=plan scope=succeeded private'):
    assert safe_worker_stderr(marker.encode()) == '[redacted]'

print(json.dumps({'passed': True}))
`,
      { flag: "wx", mode: 0o400 },
    );
    const result = await runLegacyIdentityFixtureCommand(
      "/usr/bin/python3",
      ["-I", "-B", script],
      root,
    );
    NodeAssert.equal(result.code, 0, result.stderr);
    NodeAssert.deepEqual(JSON.parse(result.stdout), { passed: true });
  });
});

NodeTest.test("incomplete captured closure keeps exact inner failure evidence", async () => {
  await withLegacyIdentityScratch(async (parent) => {
    let inner, caught;
    try {
      await withLegacyIdentityScratch(
        async (root) => {
          inner = root;
          const value = failedFixture();
          value.closure.processes[0].streamsClosed = false;
          await NodeFSP.writeFile(
            NodePath.join(root, "fixture-failure.json"),
            JSON.stringify(value),
          );
          throw await publishPreparationNativeFailure(root, new Error("synthetic original"));
        },
        { temporaryDirectory: parent },
      );
    } catch (cause) {
      caught = cause;
    }
    NodeAssert.equal(caught.preserveScratch, true);
    NodeAssert.equal(caught.scratchRoot, inner);
    NodeAssert.equal(
      JSON.parse(await NodeFSP.readFile(NodePath.join(inner, "fixture-failure.json"), "utf8"))
        .closure.processes[0].streamsClosed,
      false,
    );
    await NodeAssert.rejects(NodeFSP.stat(inner + "-failure.json"), { code: "ENOENT" });
    // This test models closure failure without launching a child; its outer scope owns recovery cleanup.
  });
});
NodeTest.test(
  "failure validation refuses hidden serializers and inconsistent signal claims",
  () => {
    const value = failedFixture();
    value.processes.toJSON = () => ({ secret: "private serializer payload" });
    NodeAssert.throws(() => validatePreparationNativeFailure(value));
    const signalled = failedFixture();
    signalled.processes[0].afterCleanup.returncode = -15;
    NodeAssert.throws(() => validatePreparationNativeFailure(signalled));
    signalled.processes[0].afterCleanup.signal = 15;
    NodeAssert.equal(
      validatePreparationNativeFailure(signalled).processes[0].afterCleanup.signal,
      15,
    );
  },
);

NodeTest.test(
  "stop records and unavailable timing distinguish natural exit, failed stop and observation refusal",
  async () => {
    await withLegacyIdentityScratch(async (root) => {
      const source = await NodeFSP.readFile(
        new URL(
          "../../../apps/desktop/src/jones/updates/jonesPreparationWorkerSource.ts",
          import.meta.url,
        ),
        "utf8",
      );
      const marker = "String.raw`";
      NodeAssert.equal(source.split(marker).length, 2);
      const workerSource = source.slice(
        source.indexOf(marker) + marker.length,
        source.lastIndexOf("`;"),
      );
      const workerPath = NodePath.join(root, "worker.py");
      const script = NodePath.join(root, "diagnostic-boundaries.py");
      await NodeFSP.writeFile(workerPath, workerSource, { flag: "wx", mode: 0o400 });
      await NodeFSP.writeFile(
        script,
        String.raw`
import errno, json, pathlib, subprocess, sys, time, types
class Refused(Exception): pass
def require(value):
    if not value: raise Refused('Synthetic controller refusal')
scope = dict(__name__='owned_diagnostic_test', require=require, Refused=Refused, subprocess=subprocess,
             hex_value=lambda value, length: value == 'a' * length)
exec(compile(pathlib.Path(sys.argv[1]).read_text(), 'worker.py', 'exec'), scope)
` +
          preparationFailureCaptureSource +
          String.raw`
Process, Worker = scope['_PreparationProcess'], scope['_PreparationWorker']
class Child:
    def __init__(self, code=None, deny=False):
        self.code, self.deny, self.calls = code, deny, 0
        self.stdin = self.stdout = self.stderr = None
    def poll(self): return self.code
    def terminate(self):
        self.calls += 1
        if self.deny: raise PermissionError(errno.EPERM, 'synthetic denied stop')
        self.code = -15
    def kill(self): raise AssertionError('Unexpected escalation')
    def wait(self, timeout):
        if self.code is None: self.code = 70
        return self.code

def process(child):
    value = object.__new__(Process)
    value.child, value.spawn_unknown, value.reaped = child, False, False
    value.closed, value.selector = False, types.SimpleNamespace(close=lambda: None)
    value.output, value.error, value.eof, value.signals = bytearray(), bytearray(), set(), []
    return value
natural = process(Child(code=0)); natural.stop(); natural.close()
assert natural.child.calls == 0 and len(natural.signals) == 1
assert failure_process_snapshot(natural)['stopRecords'] == 1
assert 'signalsSent' not in failure_process_snapshot(natural)
denied = process(Child(deny=True)); denied.stop(); denied.close()
assert denied.child.calls == 1 and denied.signals[0]['outcome'] == 'failed'
assert failure_process_snapshot(denied)['stopRecords'] == 1
worker = object.__new__(Worker)
worker.process, worker.observers = process(Child()), []
worker.session = types.SimpleNamespace(poison=lambda failure: None)
worker.receipt, worker.receipt_path = {}, pathlib.Path('synthetic-receipt')
worker.save_receipt = lambda phase: None
sent = []
worker.receive = lambda kinds: (dict(purpose='plan', access='read-write', scopeId='a'*64), b'')
def observation_failure(*args, **kwargs): raise Refused('Synthetic observation failure while peer awaits CONTINUE')
worker.observe, worker.send = observation_failure, lambda *args, **kwargs: sent.append(args)
worker.perform = lambda recovery: worker.scope('plan', 'synthetic-database')
assert worker.process.child.poll() is None
try: worker.run(False)
except scope['_PreparationFailure']: pass
else: raise AssertionError('Expected observation refusal')
assert sent == [] and worker.process.reaped and worker.process.closed
record = failure_process_record(worker.process, None)
assert record['beforeCleanup'] is None and record['beforeCleanupTiming'] == 'unavailable'
assert record['afterCleanup']['returncode'] == -15
print(json.dumps({'naturalExitCalls': 0, 'failedStopRecords': 1, 'observationTimingUnavailable': True}))
`,
        { flag: "wx", mode: 0o400 },
      );
      const result = await runLegacyIdentityFixtureCommand(
        "/usr/bin/python3",
        ["-I", "-B", script, workerPath],
        root,
      );
      NodeAssert.equal(result.code, 0, result.stderr);
      NodeAssert.deepEqual(JSON.parse(result.stdout), {
        naturalExitCalls: 0,
        failedStopRecords: 1,
        observationTimingUnavailable: true,
      });
    });
  },
);

NodeTest.test(
  "failure schema refuses a fabricated precleanup snapshot for unavailable timing",
  () => {
    const value = failedFixture();
    value.processes[0].beforeCleanupTiming = "unavailable";
    NodeAssert.throws(() => validatePreparationNativeFailure(value));
    value.processes[0].beforeCleanup = null;
    NodeAssert.equal(
      validatePreparationNativeFailure(value).processes[0].beforeCleanupTiming,
      "unavailable",
    );
  },
);

for (const mode of ["deadline", "cancel"]) {
  NodeTest.test(
    "missing log callback retains primary and durable evidence on " + mode,
    async (test) => {
      await withLegacyIdentityScratch(async (parent) => {
        let inner,
          caught,
          publicationWrites = 0;
        const original = new Error("synthetic original");
        const controller = new AbortController();
        const write = process.stderr.write;
        const mock = test.mock.method(process.stderr, "write", function (chunk, ...args) {
          if (
            typeof chunk === "string" &&
            chunk.startsWith('{"event":"preparation-native-failure"')
          ) {
            publicationWrites += 1;
            if (mode === "cancel") controller.abort();
            return true;
          }
          return Reflect.apply(write, this, [chunk, ...args]);
        });
        try {
          await withLegacyIdentityScratch(
            async (root) => {
              inner = root;
              await NodeFSP.writeFile(
                NodePath.join(root, "fixture-failure.json"),
                JSON.stringify(failedFixture()),
              );
              throw await publishPreparationNativeFailure(root, original, 0, {
                signal: controller.signal,
                publicationTimeoutMs: 10,
              });
            },
            { temporaryDirectory: parent },
          );
        } catch (cause) {
          caught = cause;
        } finally {
          mock.mock.restore();
        }
        NodeAssert.equal(publicationWrites, 1);
        NodeAssert.equal(caught.cause, original);
        NodeAssert.equal(caught.preserveScratch, true);
        NodeAssert.equal(caught.scratchRoot, inner);
        NodeAssert.match(
          caught.errors[1].message,
          mode === "cancel" ? /cancelled/ : /deadline exceeded/,
        );
        NodeAssert.deepEqual(
          JSON.parse(await NodeFSP.readFile(caught.failureEvidencePath, "utf8")),
          failedFixture(),
        );
        NodeAssert.ok((await NodeFSP.stat(inner)).isDirectory());
        // Both retained paths belong to this synthetic test; no child or unknown write remains after observation.
      });
    },
  );
}

NodeTest.test("fixed currentness diagnostic schema rejects unknown and private fields", () => {
  const gates = [
    "parent",
    "generation",
    "state",
    "database-parent",
    "inherited-directory",
    "lock-descriptor",
    "lock-path",
    "runtime",
    "retained-plan",
    "dispatch",
    "intent",
    "pair",
    "cursor",
    "owner-journal",
    "manifest",
    "pair-journal",
    "sql-directory",
    "scope-result",
  ];
  for (const gate of gates) {
    const evidence = failedFixture();
    evidence.processes[0].afterCleanup.stderr =
      "JONES_PREPARATION_FAILURE phase=post-sql-current kind=refused\nJONES_PREPARATION_GATE gate=" +
      gate +
      " command=plan scope=succeeded";
    NodeAssert.deepEqual(validatePreparationNativeFailure(evidence), evidence);
  }
  for (const line of [
    "JONES_PREPARATION_GATE gate=private command=plan scope=succeeded",
    "JONES_PREPARATION_GATE gate=runtime command=private scope=succeeded",
    "JONES_PREPARATION_GATE gate=runtime command=plan scope=private",
    "JONES_PREPARATION_GATE gate=runtime command=plan scope=succeeded private",
  ]) {
    const evidence = failedFixture();
    evidence.processes[0].afterCleanup.stderr = line;
    NodeAssert.throws(() => validatePreparationNativeFailure(evidence));
  }
});

NodeTest.test(
  "controller cause capture preserves originals, cleanup ordering and bounded static evidence",
  async () => {
    await withLegacyIdentityScratch(async (root) => {
      const script = NodePath.join(root, "controller-cause.py");
      const driver = NodePath.join(root, "native-driver-syntax.py");
      await NodeFSP.writeFile(driver, preparationNativeDriver, { flag: "wx", mode: 0o400 });
      await NodeFSP.writeFile(
        script,
        "import json\n" +
          preparationFailureCaptureSource +
          String.raw`
import pathlib, sys
compile(pathlib.Path(sys.argv[1]).read_text(), 'fixture.py', 'exec')
class Refused(Exception): pass
class _PreparationFailure(Refused):
    def __init__(self, errors):
        self.errors = tuple(errors)
        self.__cause__ = self.errors[0]
private = 'Bearer private-secret /private/raw/path'
scope = dict(Refused=Refused, private=private)
exec(compile('def clean_work():\n    raise Refused(private)\n', 'maintenance.py', 'exec'), scope)
original = None
try: at_failure_stage('work-cleanup', scope['clean_work'])
except Refused as cause: original = cause
assert original is not None and str(original) == private
cleanup = OSError(private)
aggregate = _PreparationFailure([original, cleanup])
try: at_failure_stage('worker-perform', lambda: (_ for _ in ()).throw(aggregate))
except _PreparationFailure as caught: assert caught is aggregate and caught.__cause__ is original
report = capture_controller_failure([aggregate, cleanup], aggregate)
assert report['records'][0]['relation'] == 'primary'
assert report['records'][1]['type'] == 'Refused' and report['records'][1]['stage'] == 'work-cleanup'
assert report['records'][1]['frames'][-1] == dict(source='maintenance.py', function='clean_work', line=2)
assert report['records'][2]['type'] == 'OSError' and report['records'][2]['relation'] == 'aggregate'
assert report['records'][-1]['relation'] == 'cleanup' and report['records'][-1]['duplicateOf'] == 2
assert aggregate.errors == (original, cleanup) and str(original) == private
original.__cause__ = aggregate
cycle = capture_controller_failure([aggregate, cleanup], aggregate)
assert any(item['duplicateOf'] is not None for item in cycle['records'])
large = capture_controller_failure([_PreparationFailure(tuple(ValueError(private) for _ in range(100)))], None)
assert len(large['records']) <= 16 and large['truncated']
class PrivateExceptionName(Exception): pass
unknown = capture_controller_failure([PrivateExceptionName(private)], None)
assert unknown['records'][0]['type'] == 'unknown'
encoded = json.dumps([report, cycle, large, unknown])
assert 'private' not in encoded and 'Bearer' not in encoded and 'PrivateExceptionName' not in encoded
print(json.dumps(report))
`,
        { flag: "wx", mode: 0o400 },
      );
      const result = await runLegacyIdentityFixtureCommand(
        "/usr/bin/python3",
        ["-I", "-B", script, driver],
        root,
      );
      NodeAssert.equal(result.code, 0, result.stderr);
      const evidence = failedFixture();
      evidence.controllerFailure = JSON.parse(result.stdout);
      NodeAssert.deepEqual(validatePreparationNativeFailure(evidence), evidence);
    });
  },
);

NodeTest.test("controller cause validator refuses private, forged and unbounded metadata", () => {
  const cause = {
    parent: null,
    relation: "primary",
    type: "Refused",
    stage: "work-cleanup",
    frames: [{ source: "maintenance.py", function: "clean_work", line: 2 }],
    duplicateOf: null,
    sqliteErrorCode: null,
  };
  for (const mutate of [
    (record) => {
      record.sqliteErrorCode = "private";
    },
    (record) => {
      record.sqliteErrorCode = -1;
    },
    (record) => {
      record.sqliteErrorCode = 2147483648;
    },
    (record) => {
      record.sqliteErrorCode = 1;
    },
    (record) => {
      delete record.sqliteErrorCode;
    },
    (record) => {
      record.type = "PrivateExceptionName";
    },
    (record) => {
      record.stage = "private-stage";
    },
    (record) => {
      record.frames[0].source = "/private/maintenance.py";
    },
    (record) => {
      record.frames[0].function = "privateFunction";
    },
    (record) => {
      record.frames[0].line = 0;
    },
    (record) => {
      record.frames = Array.from({ length: 9 }, () => structuredClone(record.frames[0]));
    },
    (record) => {
      record.message = "Bearer private-secret";
    },
    (record) => {
      record.parent = 0;
    },
    (record) => {
      record.duplicateOf = 0;
    },
    (record) => {
      record.relation = "unknown";
    },
  ]) {
    const evidence = failedFixture();
    const record = structuredClone(cause);
    mutate(record);
    evidence.controllerFailure = { records: [record], truncated: false };
    NodeAssert.throws(() => validatePreparationNativeFailure(evidence));
  }
  const evidence = failedFixture();
  evidence.controllerFailure = {
    records: Array.from({ length: 17 }, () => structuredClone(cause)),
    truncated: true,
  };
  NodeAssert.throws(() => validatePreparationNativeFailure(evidence));
});

NodeTest.test(
  "reader diagnostics preserve SQLite failure and superseding cleanup context without private data",
  async () => {
    await withLegacyIdentityScratch(async (root) => {
      const script = NodePath.join(root, "reader-diagnostic.py");
      const writeSource = preparationNativeDriver
        .split("def write(path, raw, mode=0o600):")[1]
        .split("\ndef command(args):")[0];
      const readerSource = preparationNativeDriver
        .split("def readonly_rows(database):")[1]
        .split("\ndef refusal(body):")[0];
      await NodeFSP.writeFile(
        script,
        "import json, os, pathlib, sqlite3, stat, sys\n" +
          preparationFailureCaptureSource +
          "\ndef write(path, raw, mode=0o600):" +
          writeSource +
          "\ndef readonly_rows(database):" +
          readerSource +
          String.raw`
root = pathlib.Path(sys.argv[1])
database = root / 'owned.sqlite'
target = root / 'assertion-copy.sqlite'
payload = dict(tables={'private_table_name': None})
with sqlite3.connect(database) as seed: seed.execute('CREATE TABLE actual_table(value TEXT)')
seed.close()
reports = []
def failed(stage, operation):
    try: operation()
    except BaseException as failure:
        report = capture_controller_failure([failure], failure)
        assert report['records'][0]['stage'] == stage
        reports.append(report)
        return failure, report
    raise AssertionError('Expected owned reader failure')
query, report = failed('reader-query', lambda: readonly_rows(database))
assert isinstance(query, sqlite3.OperationalError)
assert report['records'][0]['sqliteErrorCode'] == getattr(query, 'sqlite_errorcode', None)
assert any(frame['function'] == 'readonly_rows' for frame in report['records'][0]['frames'])
if sys.version_info < (3, 12): assert any(frame['function'] == '<dictcomp>' for frame in report['records'][0]['frames'])
assert not target.exists()
opened, report = failed('reader-open', lambda: readonly_rows(root / 'absent.sqlite'))
assert isinstance(opened, sqlite3.OperationalError)
assert report['records'][0]['sqliteErrorCode'] == getattr(opened, 'sqlite_errorcode', None)
source_directory = root / 'source-directory'; source_directory.mkdir()
copied, report = failed('reader-copy', lambda: readonly_rows(source_directory))
assert isinstance(copied, IsADirectoryError) and report['records'][0]['sqliteErrorCode'] is None
private = 'Bearer private-secret /private/raw/path'
real_connect = sqlite3.connect
class Closing(sqlite3.Connection):
    def close(self):
        super().close()
        raise OSError(private)
sqlite3.connect = lambda *args, **kwargs: real_connect(*args, **kwargs, factory=Closing)
try: closed, report = failed('reader-close', lambda: readonly_rows(database))
finally: sqlite3.connect = real_connect
assert isinstance(closed, OSError) and str(closed) == private
assert isinstance(closed.__context__, sqlite3.OperationalError)
assert report['records'][1]['relation'] == 'context' and report['records'][1]['stage'] == 'reader-query'
assert target.exists()
target.unlink()
real_unlink = pathlib.Path.unlink
def deny_unlink(self, *args, **kwargs): raise PermissionError(private)
pathlib.Path.unlink = deny_unlink
try: removed, report = failed('reader-removal', lambda: readonly_rows(database))
finally: pathlib.Path.unlink = real_unlink
assert isinstance(removed, PermissionError) and isinstance(removed.__context__, sqlite3.OperationalError)
assert report['records'][1]['stage'] == 'reader-query' and target.exists()
target.unlink()
for value in (True, -1, 2147483648, private):
    query.sqlite_errorcode = value
    assert capture_controller_failure([query], query)['records'][0]['sqliteErrorCode'] is None
forged = ValueError(private); forged.sqlite_errorcode = 1
assert capture_controller_failure([forged], forged)['records'][0]['sqliteErrorCode'] is None
encoded = json.dumps(reports)
assert 'private' not in encoded and 'Bearer' not in encoded and str(root) not in encoded
print(encoded)
`,
        { flag: "wx", mode: 0o400 },
      );
      const result = await runLegacyIdentityFixtureCommand(
        "/usr/bin/python3",
        ["-I", "-B", script, root],
        root,
      );
      NodeAssert.equal(result.code, 0, result.stderr);
      for (const report of JSON.parse(result.stdout)) {
        const evidence = failedFixture();
        evidence.controllerFailure = report;
        NodeAssert.deepEqual(validatePreparationNativeFailure(evidence), evidence);
      }
    });
  },
);
