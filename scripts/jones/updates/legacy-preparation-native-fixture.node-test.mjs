import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import {
  PreparationNativeUnavailable,
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
