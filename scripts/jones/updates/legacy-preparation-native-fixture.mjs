import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { readColdMaintenanceSources } from "./legacy-cold-maintenance-fixture.mjs";
import {
  runLegacyIdentityFixtureCommand,
  withLegacyIdentityScratch,
} from "./legacy-identity-native-fixture.mjs";

// oxlint-disable-next-line t3code/no-global-process-runtime -- This standalone native qualifier observes its actual host without an application runtime.
const nativePlatform = process.platform;
// oxlint-disable-next-line t3code/no-global-process-runtime -- The retained SEA and observer require the actual arm64 host.
const nativeArchitecture = process.arch;
const repository = NodeURL.fileURLToPath(new URL("../../../", import.meta.url));
const hash = (value) => NodeCrypto.createHash("sha256").update(value).digest("hex");
const hex = /^[a-f0-9]{64}$/;
const maximumRuntime = 256 * 1024 * 1024;
export const preparationNativeCases = Object.freeze([
  "apply",
  "reconcile",
  "repeat-plan",
  "repeat-apply",
  "durable-plan-drift",
  "wrong-scope",
  "paused-cancel",
  "closed-query-failure",
  "close-uncertainty",
  "custody-uncertainty",
]);

export class PreparationNativeUnavailable extends Error {
  constructor(reason) {
    super("Native preparation qualification unavailable: " + reason);
    this.code = "JONES_PREPARATION_QUALIFICATION_UNAVAILABLE";
  }
}
function exact(value, keys) {
  NodeAssert.ok(value && typeof value === "object" && !Array.isArray(value));
  NodeAssert.deepEqual(Object.keys(value).sort(), [...keys].sort());
  return value;
}

// Literal-only fixture data extraction; no TypeScript evaluation or imported application runtime.
export function preparationFixtureLiteral(source, marker, end) {
  NodeAssert.equal(source.split(marker).length, 2);
  const tail = source.slice(source.indexOf(marker) + marker.length);
  const stop = tail.indexOf(end);
  NodeAssert.ok(stop >= 0);
  const input = tail.slice(0, stop);
  let at = 0;
  const whitespace = () => {
    while (/\s/.test(input[at] ?? "") && at < input.length) at += 1;
  };
  function value() {
    whitespace();
    if (input[at] === '"') {
      const match = /^"(?:[^"\\\r\n]|\\["\\/bfnrt]|\\u[\da-fA-F]{4})*"/.exec(input.slice(at));
      NodeAssert.ok(match);
      at += match[0].length;
      return JSON.parse(match[0]);
    }
    const open = input[at];
    if (open === "{" || open === "[") {
      at += 1;
      const result = open === "{" ? {} : [];
      const close = open === "{" ? "}" : "]";
      whitespace();
      while (input[at] !== close) {
        if (open === "{") {
          const name = /^[A-Za-z_][A-Za-z_0-9]*/.exec(input.slice(at));
          NodeAssert.ok(name);
          at += name[0].length;
          whitespace();
          NodeAssert.equal(input[at++], ":");
          NodeAssert.ok(!Object.hasOwn(result, name[0]) && name[0] !== "__proto__");
          result[name[0]] = value();
        } else result.push(value());
        whitespace();
        if (input[at] === close) break;
        NodeAssert.equal(input[at++], ",");
        whitespace();
      }
      at += 1;
      return result;
    }
    const token = /^(?:true|false|null|0|[1-9][0-9]*)(?![\w.])/.exec(input.slice(at));
    NodeAssert.ok(token);
    at += token[0].length;
    return JSON.parse(token[0]);
  }
  const result = value();
  whitespace();
  NodeAssert.equal(at, input.length);
  return result;
}

export function validatePreparationNativeInput(value) {
  exact(value, [
    "schema",
    "classification",
    "sourceSha",
    "sourceTree",
    "preparation",
    "nativeSourceSha256",
    "files",
  ]);
  NodeAssert.equal(value.schema, 1);
  NodeAssert.equal(value.classification, "synthetic-preparation-native-input");
  for (const key of ["sourceSha", "sourceTree"]) NodeAssert.match(value[key], /^[a-f0-9]{40}$/);
  NodeAssert.match(value.nativeSourceSha256, hex);
  exact(value.files, ["executable", "node", "native", "entry", "seaConfig", "provenance"]);
  for (const [name, file] of Object.entries(value.files)) {
    exact(file, ["path", "sha256", "size"]);
    NodeAssert.ok(NodePath.isAbsolute(file.path) && NodePath.normalize(file.path) === file.path);
    NodeAssert.match(file.sha256, hex);
    const bound =
      name === "native"
        ? 4 * 1024 * 1024
        : name === "entry"
          ? 32 * 1024 * 1024
          : name === "seaConfig"
            ? 16384
            : name === "provenance"
              ? 65536
              : maximumRuntime;
    NodeAssert.ok(Number.isSafeInteger(file.size) && file.size > 0 && file.size <= bound);
  }
  const proof = value.preparation;
  NodeAssert.ok(proof && proof.schema === 1 && proof.protocol === 1);
  exact(proof.sourceInputHashes, preparationNativeSourceInputs);
  for (const digest of Object.values(proof.sourceInputHashes)) NodeAssert.match(digest, hex);
  for (const name of ["executable", "entry", "seaConfig"]) {
    NodeAssert.equal(proof[name].sha256, value.files[name].sha256);
    NodeAssert.equal(proof[name].size, value.files[name].size);
  }
  NodeAssert.equal(proof.provenanceSha256, value.files.provenance.sha256);
  NodeAssert.equal(proof.node.runtimeSha256, value.files.node.sha256);
  NodeAssert.equal(proof.node.runtimeBytes, value.files.node.size);
  NodeAssert.equal(proof.node.version, "26.8.2");
  NodeAssert.equal(proof.node.platform, "darwin");
  NodeAssert.equal(proof.node.architecture, "arm64");
  return value;
}

export const preparationInputOpenFlags =
  NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW | NodeFS.constants.O_NONBLOCK;

async function openRegularInput(path, maximum, signal) {
  signal?.throwIfAborted();
  const handle = await NodeFSP.open(path, preparationInputOpenFlags);
  try {
    signal?.throwIfAborted();
    const info = await handle.stat({ bigint: true });
    NodeAssert.ok(
      info.isFile() && info.nlink === 1n && info.size > 0n && info.size <= BigInt(maximum),
    );
    return { handle, info };
  } catch (primary) {
    try {
      await handle.close();
    } catch (cleanup) {
      const failure = new AggregateError(
        [primary, cleanup],
        "Fixture input descriptor closure failed.",
        { cause: primary },
      );
      failure.preserveScratch = true;
      throw failure;
    }
    throw primary;
  }
}

export async function readPreparationNativeInputFile(path, signal) {
  const { handle, info } = await openRegularInput(path, 65536, signal);
  let raw, primary;
  try {
    signal?.throwIfAborted();
    const buffer = Buffer.alloc(Number(info.size) + 1);
    let offset = 0;
    while (offset < buffer.length) {
      signal?.throwIfAborted();
      const read = await handle.read(buffer, offset, buffer.length - offset, offset);
      if (read.bytesRead === 0) break;
      offset += read.bytesRead;
    }
    NodeAssert.equal(offset, Number(info.size));
    raw = buffer.subarray(0, offset);
  } catch (error) {
    primary = error;
  }
  try {
    await handle.close();
  } catch (cleanup) {
    const failure = new AggregateError(
      primary ? [primary, cleanup] : [cleanup],
      "Fixture input descriptor closure failed.",
      { cause: primary ?? cleanup },
    );
    failure.preserveScratch = true;
    throw failure;
  }
  if (primary) throw primary;
  return raw;
}

export async function copyPreparationNativeArtifact(file, destination, signal) {
  const { handle: source, info: before } = await openRegularInput(
    file.path,
    maximumRuntime,
    signal,
  );
  let target, primary;
  try {
    NodeAssert.equal(before.size, BigInt(file.size));
    target = await NodeFSP.open(destination, "wx", 0o600);
    const digest = NodeCrypto.createHash("sha256"),
      buffer = Buffer.alloc(1024 * 1024);
    let offset = 0;
    while (offset < file.size) {
      signal?.throwIfAborted();
      const read = await source.read(
        buffer,
        0,
        Math.min(buffer.length, file.size - offset),
        offset,
      );
      NodeAssert.ok(read.bytesRead > 0);
      digest.update(buffer.subarray(0, read.bytesRead));
      let written = 0;
      while (written < read.bytesRead) {
        const part = await target.write(
          buffer,
          written,
          read.bytesRead - written,
          offset + written,
        );
        NodeAssert.ok(part.bytesWritten > 0);
        written += part.bytesWritten;
      }
      offset += read.bytesRead;
    }
    const after = await source.stat({ bigint: true }),
      current = await NodeFSP.lstat(file.path, { bigint: true });
    for (const key of ["dev", "ino", "size", "mtimeNs", "ctimeNs", "mode", "nlink"])
      NodeAssert.ok(before[key] === after[key] && before[key] === current[key]);
    NodeAssert.equal(digest.digest("hex"), file.sha256);
    await target.chmod(0o500);
    await target.sync();
  } catch (error) {
    primary = error;
  } finally {
    const closed = await Promise.allSettled([source.close(), ...(target ? [target.close()] : [])]);
    const failures = closed.filter((item) => item.status === "rejected").map((item) => item.reason);
    if (failures.length) {
      const failure = new AggregateError(
        primary ? [primary, ...failures] : failures,
        "Fixture artifact descriptor closure failed.",
        { cause: primary ?? failures[0] },
      );
      failure.preserveScratch = true;
      throw failure;
    }
  }
  if (primary) throw primary;
}

// https://github.com/nodejs/node/blob/v26.8.2/src/node_sqlite.cc#L1147-L1150 preserves strings;
// Open995–1002 uses SQLITE_OPEN_URI. This primitive still requires an actual runtime result.
export const preparationUriProbe = String.raw`
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { DatabaseSync } from 'node:sqlite';
assert.equal(process.versions.node, '26.8.2');
const root = fs.realpathSync(process.argv[2]);
const absent = path.join(root, 'absent.sqlite');
const uri = pathToFileURL(absent); uri.searchParams.set('mode', 'rw');
const before = fs.readdirSync(root).sort();
assert.throws(() => new DatabaseSync(uri.href, { readOnly: false, allowExtension: false }));
assert.deepEqual(fs.readdirSync(root).sort(), before);
const existing = path.join(root, 'existing ? #.sqlite');
let db = new DatabaseSync(existing);
try { db.exec('CREATE TABLE sentinel(value); INSERT INTO sentinel VALUES (73)'); } finally { db.close(); }
const readable = pathToFileURL(existing); readable.searchParams.set('mode', 'ro');
db = new DatabaseSync(readable.href, { readOnly: true, allowExtension: false });
let version, sourceId;
try {
  assert.equal(db.prepare('SELECT value FROM sentinel').get().value, 73);
  assert.throws(() => db.exec('INSERT INTO sentinel VALUES (74)'));
  const row = db.prepare('SELECT sqlite_version() AS version, sqlite_source_id() AS sourceId').get();
  version = row.version; sourceId = row.sourceId;
} finally { db.close(); }
fs.unlinkSync(existing);
assert.deepEqual(fs.readdirSync(root).sort(), before);
process.stdout.write(JSON.stringify({ version, sourceId, nodeVersion: process.versions.node, missingFileCreated: false, existingUriRead: true }));
`;

function seedRows(capabilities) {
  const now = "2026-10-11T12:00:00.000Z",
    id = "thread:worker",
    run = "run:" + id;
  const session = "session:" + id,
    providerThread = "provider-thread:" + id;
  const attempt = "attempt:" + id,
    turn = "turn:" + id,
    node = "node:" + id;
  const modelSelection = { instanceId: "codex", model: "synthetic" };
  const payload = JSON.stringify;
  return [
    [
      "orchestration_v2_projection_threads",
      {
        thread_id: id,
        project_id: "project:synthetic",
        title: id,
        default_provider: "codex",
        runtime_mode: "full-access",
        interaction_mode: "default",
        active_provider_thread_id: providerThread,
        created_at: now,
        updated_at: now,
        archived_at: null,
        deleted_at: null,
        payload_json: payload({
          id,
          projectId: "project:synthetic",
          title: id,
          providerInstanceId: "codex",
          modelSelection,
          createdBy: "user",
          creationSource: "web",
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          activeProviderThreadId: providerThread,
          lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: id },
          forkedFrom: null,
          createdAt: now,
          updatedAt: now,
          archivedAt: null,
          deletedAt: null,
        }),
      },
    ],
    [
      "orchestration_v2_projection_runs",
      {
        run_id: run,
        thread_id: id,
        ordinal: 1,
        provider: "codex",
        provider_thread_id: providerThread,
        status: "running",
        requested_at: now,
        completed_at: null,
        payload_json: payload({
          id: run,
          threadId: id,
          ordinal: 1,
          providerInstanceId: "codex",
          modelSelection,
          providerThreadId: providerThread,
          userMessageId: "message:" + id,
          rootNodeId: node,
          activeAttemptId: attempt,
          status: "running",
          requestedAt: now,
          startedAt: now,
          completedAt: null,
          checkpointId: null,
          contextHandoffId: null,
        }),
      },
    ],
    [
      "orchestration_v2_projection_run_attempts",
      {
        attempt_id: attempt,
        thread_id: id,
        run_id: run,
        attempt_ordinal: 1,
        root_node_id: node,
        provider: "codex",
        provider_thread_id: providerThread,
        provider_turn_id: turn,
        status: "running",
        payload_json: payload({
          id: attempt,
          runId: run,
          attemptOrdinal: 1,
          rootNodeId: node,
          providerInstanceId: "codex",
          providerThreadId: providerThread,
          providerTurnId: turn,
          reason: "initial",
          status: "running",
          startedAt: now,
          completedAt: null,
        }),
      },
    ],
    [
      "orchestration_v2_projection_provider_sessions",
      {
        provider_session_id: session,
        thread_id: id,
        provider: "codex",
        status: "ready",
        model: "synthetic",
        updated_at: now,
        payload_json: payload({
          id: session,
          driver: "codex",
          providerInstanceId: "codex",
          status: "ready",
          cwd: "/synthetic",
          model: "synthetic",
          capabilities,
          createdAt: now,
          updatedAt: now,
          lastError: null,
        }),
      },
    ],
    [
      "orchestration_v2_projection_provider_session_bindings",
      { provider_session_id: session, thread_id: id },
    ],
    [
      "orchestration_v2_projection_provider_threads",
      {
        provider_thread_id: providerThread,
        thread_id: id,
        owner_node_id: null,
        provider: "codex",
        provider_session_id: session,
        status: "active",
        first_run_ordinal: 1,
        last_run_ordinal: 1,
        updated_at: now,
        payload_json: payload({
          id: providerThread,
          driver: "codex",
          providerInstanceId: "codex",
          providerSessionId: session,
          appThreadId: id,
          ownerNodeId: null,
          nativeThreadRef: { driver: "codex", nativeId: "synthetic:" + id, strength: "strong" },
          nativeConversationHeadRef: null,
          status: "active",
          firstRunOrdinal: 1,
          lastRunOrdinal: 1,
          handoffIds: [],
          forkedFrom: null,
          createdAt: now,
          updatedAt: now,
        }),
      },
    ],
    [
      "orchestration_v2_projection_provider_turns",
      {
        provider_turn_id: turn,
        thread_id: id,
        provider_thread_id: providerThread,
        node_id: node,
        run_attempt_id: attempt,
        ordinal: 1,
        status: "running",
        started_at: now,
        completed_at: null,
        payload_json: payload({
          id: turn,
          providerThreadId: providerThread,
          nodeId: node,
          runAttemptId: attempt,
          nativeTurnRef: null,
          ordinal: 1,
          status: "running",
          startedAt: now,
          completedAt: null,
        }),
      },
    ],
  ];
}

export const preparationRepeatOracleSource = String.raw`
def observe_repeat_rejection(process, evidence):
    # Keep stdin open and send no signal: EOF must originate from the worker's own refusal.
    assert process.input_open and not process.signals
    evidence.update(started=True, outputBytes=0, firstHeader=None, exitCode=None,
                    reapedBeforeCleanup=False, stdinStayedOpen=True, signalsBeforeCleanup=[])
    deadline = time.monotonic() + 5
    while True:
        evidence['outputBytes'] = len(process.output)
        if process.output:
            # Observe bytes before any controller allowed-kinds filter. Every response,
            # including a valid SQL_SCOPE_OPENED or CLOSED_RESULT, disproves this oracle.
            if len(process.output) >= 4:
                length = struct.unpack('>I', process.output[:4])[0]
                if 1 <= length <= 16384 and len(process.output) >= 4 + length:
                    try:
                        header = json.loads(bytes(process.output[4:4 + length]))
                        if isinstance(header, dict):
                            evidence['firstHeader'] = {key: header.get(key) for key in ('schema', 'type', 'sequence', 'payloadBytes')}
                    except (UnicodeError, ValueError): pass
                    break
                if length < 1 or length > 16384: break
        if process.eof == {'out', 'err'}: break
        process.pump(deadline)
    if process.output:
        evidence['outputBytes'] = len(process.output)
        raise Refused('Repeated command produced response bytes before refusal.')
    evidence['stdinStayedOpen'] = process.input_open
    evidence['signalsBeforeCleanup'] = list(process.signals)
    evidence['exitCode'] = process.child.wait(timeout=max(0.001, deadline - time.monotonic()))
    process.reaped = True
    evidence['reapedBeforeCleanup'] = True
    evidence['stderr'] = bytes(process.error).decode('utf8', errors='replace')
    # The outer oracle runs after the controller's catch-all, so neither this
    # assertion nor an accepting peer can be relabeled an expected refusal.
    raise Refused('Repeated command peer exited; evaluate its independent evidence.')

def require_repeat_rejection(evidence):
    assert evidence.get('started') is True
    assert evidence.get('outputBytes') == 0 and evidence.get('firstHeader') is None
    assert evidence.get('exitCode') == 70 and evidence.get('reapedBeforeCleanup') is True
    assert evidence.get('stdinStayedOpen') is True and evidence.get('signalsBeforeCleanup') == []
    assert evidence.get('stderr') == 'Closed preparation worker refused.\n'
`;

export const preparationNativeDriver =
  String.raw`
import sys
sys.dont_write_bytecode = True
import contextlib, errno, hashlib, io, json, os, pathlib, plistlib, shutil, signal, sqlite3, stat, struct, subprocess, time, uuid, zipfile
root = pathlib.Path(sys.argv[2]).resolve(strict=True)
payload = json.loads(pathlib.Path(sys.argv[1]).read_bytes())
scope = {'__name__': 'owned_preparation_native_fixture'}
exec(compile(payload['maintenance'], 'maintenance.py', 'exec'), scope)
encode, sha, Refused = scope['encoded'], scope['sha'], scope['Refused']
Process, Worker, Maintenance = scope['_PreparationProcess'], scope['_PreparationWorker'], scope['Maintenance']
` +
  preparationRepeatOracleSource +
  String.raw`
commands, children, constructor_unknown, results = [], [], [], []
real_popen, real_start, real_close, real_stop = subprocess.Popen, Process.start, Process.close, Process.stop

# The fixture owns every actual handle, including a Popen interrupted after initialization.
# Capturing that handle is test cleanup only; it never repairs the production UNKNOWN result.
class Captured(real_popen):
    def __init__(self, *args, **kwargs):
        children.append(self)
        try: super().__init__(*args, **kwargs)
        except BaseException:
            constructor_unknown.append(self)
            raise
subprocess.Popen = Captured
def tracked_start(self):
    commands.append(self)
    return real_start(self)
Process.start = tracked_start
def interrupted(number, frame): raise KeyboardInterrupt('Owned native fixture cancellation')
signal.signal(signal.SIGTERM, interrupted)
signal.signal(signal.SIGINT, interrupted)

def write(path, raw, mode=0o600):
    assert path.is_relative_to(root)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.exists():
        info = path.lstat()
        assert stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and info.st_nlink == 1
        path.chmod(stat.S_IMODE(info.st_mode) | 0o200)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, mode)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(raw); stream.flush(); os.fchmod(stream.fileno(), mode); os.fsync(stream.fileno())

def command(args):
    process = Process(args, [], 262144, False)
    failures = []
    try:
        process.start()
        assert process.finish(time.monotonic() + 30) == 0, bytes(process.error)
        return bytes(process.output)
    except BaseException as failure: failures.append(failure)
    finally:
        try: process.stop()
        except BaseException as failure: failures.append(failure)
        try: process.close()
        except BaseException as failure: failures.append(failure)
        if failures: raise scope['_PreparationFailure'](failures)

def closure():
    complete = not constructor_unknown and all(not item.spawn_unknown and (not item.spawn_attempted or item.reaped) and item.closed for item in commands)
    complete = complete and all(child.returncode is not None for child in children)
    value = {'schema': 1, 'complete': complete, 'processes': [item.evidence() for item in commands],
             'capturedChildren': len(children), 'constructorUnknown': bool(constructor_unknown), 'pidSignalAuthority': False}
    write(root / 'fixture-closure.json', encode(value))
    return complete

def make_bundle():
    artifact = payload['artifact']
    bundle = dict(schema=3, repository='Jones-Systems/Jones-Code', sourceSha=artifact['sourceSha'], sourceTree=artifact['sourceTree'],
                  version='0.0.45-preview.20261011.123.1', bootstrapSha256=sha(payload['bootstrap'].encode()), executorSha256=sha(payload['executor'].encode()),
                  maintenanceSha256=sha(payload['maintenance'].encode()), identityNativeSha256=artifact['files']['native']['sha256'],
                  identityNativeSourceSha256=artifact['nativeSourceSha256'], nativeArchitecture='arm64', preparation=artifact['preparation'])
    scope['_preparation_bundle'](bundle)
    return bundle

def seed_database(database):
    source = database.parent / 'seed.sqlite'
    db = sqlite3.connect(source)
    try:
        db.execute('PRAGMA journal_mode=wal'); db.execute('PRAGMA wal_autocheckpoint=0')
        for ddl in payload['tables'].values(): db.execute(ddl)
        for table, ledger in (('effect_sql_migrations', payload['ledgers']['upstream']), ('jones_sql_migrations', payload['ledgers']['jones'])):
            db.execute('CREATE TABLE ' + table + ' (migration_id integer PRIMARY KEY NOT NULL, created_at datetime NOT NULL DEFAULT current_timestamp, name VARCHAR(255) NOT NULL)')
            db.executemany('INSERT INTO ' + table + '(migration_id,name) VALUES (?,?)', ledger)
        db.commit(); db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        for table, values in payload['rows'] + [['orchestration_v2_effect_outbox', payload['preserved']]]:
            db.execute('INSERT INTO ' + table + '(' + ','.join(values) + ') VALUES (' + ','.join('?' for _ in values) + ')', tuple(values.values()))
        db.commit()
        assert pathlib.Path(str(source) + '-wal').stat().st_size > 32
        for suffix in ('', '-wal', '-shm'):
            write(pathlib.Path(str(database) + suffix), pathlib.Path(str(source) + suffix).read_bytes())
    finally: db.close()
    # The copied main alone excludes the committed selection; WAL is semantically necessary.
    main = database.parent / 'main-only.sqlite'
    write(main, database.read_bytes())
    db = sqlite3.connect(main.as_uri() + '?immutable=1', uri=True)
    try: assert db.execute('SELECT count(*) FROM orchestration_v2_projection_threads').fetchone()[0] == 0
    finally: db.close()
    main.unlink(); source.unlink()

def setup(base, bundle):
    home, profile = base / 'home', base / 'profile'
    updater = home / 'runtime/jones-updates'
    updater.mkdir(parents=True, mode=0o700)
    database = home / 'userdata/statev2.sqlite'
    database.parent.mkdir(mode=0o700)
    seed_database(database)
    write(database.parent / 'settings.json', b'{"continueThreadsAfterServerUpdate":true}')
    write(profile / 'sentinel', b'owned synthetic profile')
    def app(path, source, tree, version, tools=False):
        package = {'version': version, 'jonesSource': {'repository': 'Jones-Systems/Jones-Code', 'sha': source, 'tree': tree}}
        if tools: package['startupGateProtocol'] = 1
        packed = encode(package); header = encode({'files': {'package.json': {'size': len(packed), 'offset': '0'}}})
        write(path / 'Contents/Resources/app.asar', struct.pack('<IIII', 4, len(header)+8, len(header)+4, len(header)) + header + packed)
        write(path / 'Contents/Info.plist', plistlib.dumps({'CFBundleExecutable': 'Jones', 'CFBundleIdentifier': 'test.jones.cold', 'CFBundleShortVersionString': version}))
        write(path / 'Contents/MacOS/Jones', b'owned nonexecuted app', 0o500)
        if tools:
            resources = path / 'Contents/Resources/jones-bootstrap'
            for name in ('bootstrap', 'executor', 'maintenance'): write(resources / (name + '.py'), payload[name].encode(), 0o500)
            for name, resource in (('native', 'identity-native'), ('executable', 'jones-offline-preparation')):
                resources.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(root / 'inputs' / name, resources / resource)
                (resources / resource).chmod(0o500)
            write(resources / 'BUNDLE.json', encode(bundle) + b'\n', 0o400)
    old = base / 'Applications/Installed.app'
    old_version = '0.0.45-preview.20261011.38108866317.1'
    app(old, scope['LEGACY_SOURCE'], 'b' * 40, old_version)
    active = dict(protocol=1, owner='desktop', generation='1'*64, transactionId='bootstrap', home=str(home), databasePath=str(database), profile=str(profile),
                  environmentId='synthetic-native-preparation', appPath=str(old), executablePath=str(old / 'Contents/MacOS/Jones'), version=old_version,
                  sourceSha=scope['LEGACY_SOURCE'], sourceTree='b'*40, bundleIdentifier='test.jones.cold')
    manifest = home / 'runtime/jones-active-install.json'; write(manifest, encode(active))
    candidate = dict(schema=1, repository='Jones-Systems/Jones-Code', source=bundle['sourceSha'], tree=bundle['sourceTree'], installedSource=active['sourceSha'],
                     workflow='.github/workflows/artifact-desktop-mac.yml', workflowId=11, runId=123, runAttempt=1, ciRunId=456, artifactId=789,
                     artifactName='desktop-mac-arm64-123-1--'+bundle['version'], artifactDigest='', artifactBytes=0, expiresAt='2099-12-01T00:00:00Z',
                     version=bundle['version'], platform='darwin', architecture='arm64')
    dmg = b'owned synthetic packaging receipt'
    receipt = {key: candidate[key] for key in ('repository', 'source', 'tree', 'workflow', 'version', 'platform', 'architecture')}
    receipt.update(schema=1, event='push', ref='refs/heads/main', runId='123', runAttempt='1', artifact='T3-Code-'+bundle['version']+'-arm64.dmg', sha256=sha(dmg))
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, 'w') as zipped:
        zipped.writestr('ARTIFACT.json', encode(receipt)); zipped.writestr(receipt['artifact'], dmg)
    raw = archive.getvalue(); candidate.update(artifactDigest=sha(raw), artifactBytes=len(raw))
    handle = sha(encode([candidate[key] for key in scope['CANDIDATE_FIELDS']]))
    artifacts, apps = updater / 'artifacts' / handle, updater / 'apps' / handle
    candidate_app = apps / 'Candidate.app'; app(candidate_app, candidate['source'], candidate['tree'], candidate['version'], True)
    artifact = dict(schema=1, source='jones-actions', channel='jones-main', stagedHandle=handle, candidate=candidate, receipt=receipt, payloadPath=str(artifacts / receipt['artifact']))
    write(artifacts / receipt['artifact'], dmg); write(artifacts / 'github-artifact.zip', raw); write(artifacts / 'jones-actions-stage.json', encode(artifact))
    staged = dict(handle=handle, receiptPath=str(apps / 'mac-app-receipt.json'), appPath=str(candidate_app), executablePath=str(candidate_app / 'Contents/MacOS/Jones'),
                  version=candidate['version'], sourceSha=candidate['source'], sourceTree=candidate['tree'], appDigest=scope['app_digest'](scope['Observation'](), candidate_app),
                  asarDigest=sha((candidate_app / 'Contents/Resources/app.asar').read_bytes()), executableDigest=sha((candidate_app / 'Contents/MacOS/Jones').read_bytes()),
                  bundleIdentifier='test.jones.cold', startupGateProtocol=1)
    write(apps / 'mac-app-receipt.json', encode(dict(app=staged, artifact=artifact, candidate=candidate)))
    selection = dict(schema=1, source='jones-actions', home=str(home), profile=str(profile), currentVersion=active['version'], installedSource=active['sourceSha'], active=active, artifactDirectory=str(artifacts), app=staged)
    selection_raw = encode(selection); selection_path = updater / 'staging' / (active['sourceSha']+'-'+'e'*64+'.json')
    write(selection_path, selection_raw); write(home / 'bin/t3', scope['expected_shim'](home, active), 0o750)
    scope['__file__'] = str(candidate_app / 'Contents/Resources/jones-bootstrap/maintenance.py')
    output = io.StringIO()
    with contextlib.redirect_stdout(output): code = scope['maintenance_main'](['arm', '--manifest', str(manifest), '--selection', str(selection_path), '--selection-sha256', sha(selection_raw)])
    assert code == 0, output.getvalue()
    pointer = json.loads((home / 'runtime/jones-maintenance/current.json').read_bytes())
    directory = home / 'runtime/jones-maintenance' / pointer['generation']
    scope['__file__'] = str(directory / 'tools/maintenance.py')
    # Only the owned prior epoch is modeled. Native current boot and all verification stay real.
    journal = json.loads((directory / 'journal.json').read_bytes())
    actual_boot = str(uuid.UUID(journal['armedBootSessionUuid']))
    modeled = str(uuid.uuid4()); assert modeled != actual_boot
    journal['armedBootSessionUuid'] = modeled
    write(directory / 'journal.json', encode(journal) + b'\n')
    return manifest, directory, database, actual_boot

def readonly_rows(database):
    # Observe a disposable copy only after the SEA is reaped; inspection cannot normalize live WAL.
    target = database.parent / 'assertion-copy.sqlite'
    for suffix in ('', '-wal', '-shm'):
        source = pathlib.Path(str(database) + suffix)
        if source.exists(): write(pathlib.Path(str(target) + suffix), source.read_bytes())
    db = sqlite3.connect(target.as_uri() + '?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        return {table: [dict(row) for row in db.execute('SELECT * FROM ' + table + ' ORDER BY 1')] for table in payload['tables']}
    finally:
        db.close()
        for suffix in ('', '-wal', '-shm'):
            path = pathlib.Path(str(target) + suffix)
            if path.exists(): path.unlink()

def refusal(body):
    try: body()
    except BaseException as failure: return failure
    raise AssertionError('Expected fixture refusal')

def run_case(case, bundle):
    base = root / case; base.mkdir(mode=0o700)
    manifest, directory, database, actual_boot = setup(base, bundle)
    baseline = readonly_rows(database)
    events, observations, sessions, sent, faults = [], [], [], {}, []
    repeat_evidence = {}
    held_receipt = None
    original_receive, original_observe, original_send, original_finish = Worker.receive, Worker.observe, Worker.send, Worker.finish
    original_durable = scope['durable_file']
    first_command = len(commands)
    def observe(self, path, capture=False, access=None):
        original_observe(self, path, capture, access)
        report = json.loads(bytes(self.observers[-1].output))
        assert report['bootSessionUuid'] == actual_boot and report['sqliteOpenProved'] is False
        observations.append(report)
        events.append('ready' if capture else 'closed-supplement' if access is None else ('plan-open' if path != database else 'reconcile-open' if access == 'read-only' else 'apply-open'))
        if access is not None:
            assert report['processes'][1]['pid'] == self.process.child.pid
            if path == database:
                paired = json.loads((directory / 'state/journal.json').read_bytes())
                assert paired['phase'] in ('preparation-intent', 'prepared')
                assert (directory / 'state/preparation-plan.json').is_file()
            if case in ('paused-cancel', 'custody-uncertainty') and not faults:
                faults.append('cancel'); raise KeyboardInterrupt('Owned cancellation while SEA awaits CONTINUE')
    Worker.observe = observe
    def receive(self, kinds):
        result, raw = original_receive(self, kinds)
        if result['type'] in ('PLAN_READY', 'CLOSED_RESULT'):
            target = self.work / 'state.sqlite' if result['type'] == 'PLAN_READY' or result.get('planSha256') is None else database
            original_observe(self, target)
            report = json.loads(bytes(self.observers[-1].output)); observations.append(report)
            assert not any(item['eligibleOpenObservation'] and item['process'] == 'worker' for item in report['matches'])
            events.append(result['type'])
        return result, raw
    Worker.receive = receive
    def send(self, kind, **fields):
        sent[kind] = fields.copy()
        if kind == 'CONTINUE' and case == 'wrong-scope' and not faults:
            faults.append('scope'); fields['scopeId'] = 'f' * 64 if fields['scopeId'] != 'f' * 64 else 'e' * 64
        if kind == 'APPLY':
            assert 'intent-durable' in events
            if case == 'repeat-plan':
                faults.append('repeat')
                original_send(self, 'PLAN', **sent['PLAN'])
                observe_repeat_rejection(self.process, repeat_evidence)
            if case == 'durable-plan-drift':
                faults.append('plan-drift')
                plan_path = directory / 'state/preparation-plan.json'
                write(plan_path, plan_path.read_bytes() + b'\n', 0o400)
        return original_send(self, kind, **fields)
    Worker.send = send
    def finish(self):
        if case == 'repeat-apply' and not faults:
            faults.append('repeat'); self.sequence += 1
            original_send(self, 'APPLY', **sent['APPLY'])
            observe_repeat_rejection(self.process, repeat_evidence)
        original_finish(self)
        assert self.process.reaped and self.process.closed
        events.append('reaped')
    Worker.finish = finish
    def durable(path, raw, mode, previous=None):
        original_durable(path, raw, mode, previous)
        if path == directory / 'state/journal.json' and json.loads(raw)['phase'] == 'preparation-intent':
            assert (directory / 'state/preparation-plan.json').is_file() and (directory / 'state/preparation-envelope.json').is_file()
            events.append('intent-durable')
        if path.name.startswith('preparation-evidence-'):
            assert 'reaped' in events and all(item.reaped and item.closed for item in commands[first_command:])
            events.append('observed')
    scope['durable_file'] = durable
    def close(self):
        real_close(self)
        if case == 'close-uncertainty' and sessions and sessions[-1].worker is not None and self is sessions[-1].worker.process and not faults:
            faults.append('close'); raise OSError(errno.EIO, 'Owned failure after actual SEA stream closure')
    Process.close = close
    def stop(self):
        if case == 'custody-uncertainty' and sessions and sessions[-1].worker is not None and self is sessions[-1].worker.process:
            faults.append('custody'); raise OSError(errno.EIO, 'Owned fixture withholds the captured wait result')
        return real_stop(self)
    Process.stop = stop
    def enter(operation):
        with scope['fixed_lock'](manifest):
            with scope['_cold_owner_session']() as session:
                sessions.append(session)
                assert session.record['bootSessionUuid'] == actual_boot
                return operation(session)
    try:
        if case == 'closed-query-failure':
            # Modify only fixture seed data before snapshot; its invalid ledger causes a settled SQL query rejection.
            db = sqlite3.connect(database)
            try:
                db.execute('UPDATE effect_sql_migrations SET name=? WHERE migration_id=1', ('fixture-drift',)); db.commit()
            finally: db.close()
            def rejected(session):
                result = session.prepare()
                assert result['status'] == 'query-failed' and result['workerReaped']
                assert not (directory / 'state/preparation-plan.json').exists()
                session.begin_restore(); session.reconcile_restore()
                return result
            result = enter(rejected)
        elif case in ('apply', 'reconcile'):
            result = enter(lambda session: session.prepare())
            assert result['status'] == 'applied' and result['workerReaped']
        else:
            failure = refusal(lambda: enter(lambda session: session.prepare()))
            if case in ('repeat-plan', 'repeat-apply'): require_repeat_rejection(repeat_evidence)
            assert isinstance(failure, scope['_PreparationFailure']), type(failure).__name__
            assert faults and sessions[-1].failure is not None and not sessions[-1].live
            assert json.loads((directory / 'journal.json').read_bytes())['coldOwner']['phase'] == 'entered'
            assert not list((directory / 'state').glob('preparation-evidence-*.json'))
            assert (directory / 'state/previous-pair/database').is_file()
            receipt = json.loads(sessions[-1].worker.receipt_path.read_bytes())
            held_receipt = receipt
            assert receipt['phase'] == 'custody-held' and receipt['failures']
            if case == 'close-uncertainty': assert receipt['failures'][0]['errno'] == errno.EIO
            if case == 'custody-uncertainty':
                worker = sessions[-1].worker
                assert not receipt['reaped'] and not worker.settled
                assert worker.work is not None and worker.work.exists()
                refusal(lambda: worker.clean_work())
                retained = directory / 'state/fixture-uncertain-custody.json'
                write(retained, encode(receipt), 0o400)
                # The independent fixture registry still owns this exact handle. Its real
                # cleanup does not turn the production session's failure into settlement.
                real_stop(worker.process); real_close(worker.process)
                assert worker.process.reaped and worker.process.closed and worker.failed
                assert sessions[-1].failure is not None and retained.exists()
            result = {'status': 'expected-refusal'}
        assert all(item.reaped and item.closed and not item.spawn_unknown for item in commands[first_command:])
        before_reentry = (directory / 'journal.json').read_bytes()
        refusal(lambda: enter(lambda session: session.prepare()))
        assert (directory / 'journal.json').read_bytes() == before_reentry
        if case in ('apply', 'reconcile', 'repeat-apply', 'close-uncertainty'):
            plan_bytes = (directory / 'state/preparation-plan.json').read_bytes()
            plan = json.loads(plan_bytes)
            assert len(plan['selections']) == len(plan['expectedRows']) == 1
            assert plan['selections'][0]['threadId'] == 'thread:worker'
            assert plan['preservedRows'] == [payload['preserved']]
            after = readonly_rows(database)
            for table in payload['tables']:
                if table != 'orchestration_v2_effect_outbox': assert after[table] == baseline[table]
            assert after['orchestration_v2_effect_outbox'] == sorted([payload['preserved']] + plan['expectedRows'], key=lambda row: row['effect_id'])
            if case in ('apply', 'reconcile'):
                assert events.index('PLAN_READY') < events.index('intent-durable') < events.index('apply-open') < events.index('reaped') < events.index('observed')
            if case == 'reconcile':
                envelope = (directory / 'state/preparation-envelope.json').read_bytes()
                # Model only a prior process epoch. This is expressly not a second kernel observation.
                journal = json.loads((directory / 'journal.json').read_bytes())
                previous = str(uuid.uuid4()); assert previous != actual_boot
                journal['coldOwner']['bootSessionUuid'] = previous
                write(directory / 'journal.json', encode(journal) + b'\n')
                recovery = enter(lambda session: session.recover_preparation())
                assert recovery['status'] == 'applied' and recovery['workerReaped']
                assert (directory / 'state/preparation-plan.json').read_bytes() == plan_bytes
                assert (directory / 'state/preparation-envelope.json').read_bytes() == envelope
                assert readonly_rows(database) == after
                assert 'reconcile-open' in events
        elif case != 'closed-query-failure': assert readonly_rows(database) == baseline
        pair = json.loads((directory / 'state/pair-descriptor.json').read_bytes())
        for name in ('database', 'wal', 'shm', 'settings.json'):
            entry = pair['copied'][name]
            if entry is not None:
                assert sha((directory / 'state/previous-pair' / name).read_bytes()) == entry['content'][0][4]
        assert (base / 'profile/sentinel').read_bytes() == b'owned synthetic profile'
        for session in sessions:
            if session.worker is not None:
                receipt = json.loads(session.worker.receipt_path.read_bytes())
                if case == 'custody-uncertainty': assert receipt['reaped'] is False
                else: assert receipt['reaped'] and receipt['streamsClosed']
                assert receipt['pidSignalAuthority'] is False
                assert all(item['reaped'] and item['streamsClosed'] for item in receipt['observers'])
        return dict(case=case, status=result['status'], events=events,
                    observerCalls=len(observations), nativeObservations=observations,
                    sqliteDescriptorObservations=True, rawPairPreserved=True, currentBootObserved=actual_boot,
                    priorBootModeled=True, workerReaped=True, productionAdmission=False,
                    productionCustodyHeld=held_receipt is not None,
                    independentCleanupRecovered=case == 'custody-uncertainty', heldCustodyReceipt=held_receipt,
                    repeatRejection=repeat_evidence if case in ('repeat-plan', 'repeat-apply') else None)
    finally:
        Worker.receive, Worker.observe, Worker.send, Worker.finish = original_receive, original_observe, original_send, original_finish
        scope['durable_file'], Process.close, Process.stop = original_durable, real_close, real_stop

failure = None
try:
    bundle = make_bundle()
    probe = root / 'uri-probe.mjs'; write(probe, payload['uriProbe'].encode(), 0o400)
    uri_root = root / 'uri'; uri_root.mkdir(mode=0o700)
    uri = json.loads(command([str(root / 'inputs/node'), '--no-warnings', str(probe), str(uri_root)]))
    assert uri['nodeVersion'] == bundle['preparation']['node']['version']
    assert uri['version'] == bundle['preparation']['sqlite']['version'] and uri['sourceId'] == bundle['preparation']['sqlite']['sourceId']
    for case in payload['cases']: results.append(run_case(case, bundle))
except BaseException as cause: failure = cause
finally:
    failures = [] if failure is None else [failure]
    # Only captured handles are signalled; an uncertain constructor is retained, never adopted by PID.
    for process in commands:
        try: real_stop(process)
        except BaseException as cause: failures.append(cause)
        try: real_close(process)
        except BaseException as cause: failures.append(cause)
    for child in children:
        try:
            if getattr(child, 'pid', None) is not None and child.returncode is None:
                child.terminate()
                try: child.wait(timeout=5)
                except subprocess.TimeoutExpired: child.kill(); child.wait(timeout=5)
        except BaseException as cause: failures.append(cause)
    try:
        if not closure(): failures.append(AssertionError('Captured fixture closure remains unproved'))
    except BaseException as cause: failures.append(cause)
    if failures: raise scope['_PreparationFailure'](failures, str(root / 'fixture-closure.json'))
print(json.dumps(dict(schema=1, qualification='synthetic-native-preparation', producerQualified=False,
                     nativeColdOwnerQualified=False, stateAdmissionAllowed=False, activationAllowed=False,
                     uri=uri, cases=results, fixtureClosure=True)))
`;

export const preparationNativeSourceInputs = Object.freeze([
  "pnpm-lock.yaml",
  "pnpm-workspace.yaml",
  "package.json",
  "apps/server/package.json",
  "apps/desktop/package.json",
  "apps/web/package.json",
  "packages/contracts/package.json",
  "apps/server/vite.config.ts",
  "vite.config.ts",
  "scripts/jones/updates/offline-preparation-build.ts",
  "apps/server/src/jones/updates/offlinePreparationEntry.ts",
  "apps/server/src/jones/updates/offlinePreparation.ts",
  "apps/server/src/jones/updates/offlinePreparationCore.ts",
  "apps/server/src/jones/updates/offlinePreparationPlan.ts",
  "apps/server/src/jones/updates/offlinePreparationWorker.ts",
]);

export function validatePreparationNativeClosure(value) {
  exact(value, [
    "schema",
    "complete",
    "processes",
    "capturedChildren",
    "constructorUnknown",
    "pidSignalAuthority",
  ]);
  NodeAssert.equal(value.schema, 1);
  NodeAssert.equal(value.complete, true);
  NodeAssert.equal(value.constructorUnknown, false);
  NodeAssert.equal(value.pidSignalAuthority, false);
  NodeAssert.ok(
    Number.isSafeInteger(value.capturedChildren) &&
      value.capturedChildren >= 1 &&
      value.capturedChildren <= 4096,
  );
  NodeAssert.ok(
    Array.isArray(value.processes) && value.processes.length > 0 && value.processes.length <= 64,
  );
  for (const item of value.processes) {
    NodeAssert.equal(item.spawnUnknown, false);
    NodeAssert.equal(item.spawnAttempted, true);
    NodeAssert.equal(item.reaped, true);
    NodeAssert.equal(item.streamsClosed, true);
    NodeAssert.equal(item.pidSignalAuthority, false);
  }
  return value;
}

export async function runLegacyPreparationNativeFixture({
  input,
  signal,
  temporaryDirectory,
} = {}) {
  if (!input) throw new PreparationNativeUnavailable("final SEA input record required");
  if (nativePlatform !== "darwin" || nativeArchitecture !== "arm64")
    throw new PreparationNativeUnavailable("darwin arm64 required");
  const artifact = validatePreparationNativeInput(input);
  exact(artifact.preparation.sourceInputHashes, preparationNativeSourceInputs);
  const sources = await readColdMaintenanceSources();
  NodeAssert.equal(artifact.nativeSourceSha256, sources.nativeSourceSha256);
  const inputBytes = new Map();
  for (const name of preparationNativeSourceInputs) {
    NodeAssert.match(artifact.preparation.sourceInputHashes[name], hex);
    const bytes = await NodeFSP.readFile(NodePath.join(repository, name));
    NodeAssert.equal(hash(bytes), artifact.preparation.sourceInputHashes[name], name);
    inputBytes.set(name, bytes);
  }
  const core = inputBytes
    .get("apps/server/src/jones/updates/offlinePreparationCore.ts")
    .toString("utf8");
  const test = await NodeFSP.readFile(
    NodePath.join(repository, "apps/server/src/jones/updates/offlinePreparationWorker.test.ts"),
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
  // This retained unrelated row deliberately contains opaque payload bytes.
  const preserved = {
    effect_id: "effect:preserved",
    command_id: "command:ordinary",
    thread_id: "thread:other",
    effect_type: "provider-turn.start",
    payload_json: "opaque-unrelated-payload",
    status: "succeeded",
    attempt_count: 1,
    available_at: "2019-01-01T00:00:00.000Z",
    created_at: "2019-01-01T00:00:00.000Z",
    updated_at: "2019-01-02T00:00:00.000Z",
    lease_owner: null,
    lease_expires_at: null,
    completed_at: "2019-01-02T00:00:00.000Z",
    last_error: null,
  };
  const records = [];
  // One command/root per case keeps the existing command deadline and the 256MiB real reserve bounded.
  for (const scenario of preparationNativeCases) {
    signal?.throwIfAborted();
    const result = await withLegacyIdentityScratch(
      async (root) => {
        await NodeFSP.mkdir(NodePath.join(root, "inputs"), { mode: 0o700 });
        for (const [name, file] of Object.entries(artifact.files))
          await copyPreparationNativeArtifact(file, NodePath.join(root, "inputs", name), signal);
        const payload = NodePath.join(root, "fixture.json"),
          driver = NodePath.join(root, "fixture.py");
        const evidence = NodePath.join(root, "fixture-closure.json");
        const fixtureBytes = JSON.stringify({
          ...sources,
          artifact,
          tables,
          ledgers,
          rows: seedRows(capabilities),
          preserved,
          cases: [scenario],
          uriProbe: preparationUriProbe,
        });
        NodeAssert.ok(Buffer.byteLength(fixtureBytes) <= 4 * 1024 * 1024);
        await NodeFSP.writeFile(payload, fixtureBytes, { mode: 0o400, flag: "wx" });
        await NodeFSP.writeFile(driver, preparationNativeDriver, { mode: 0o400, flag: "wx" });
        await NodeFSP.writeFile(evidence, JSON.stringify({ schema: 1, complete: false }), {
          mode: 0o600,
          flag: "wx",
        });
        for (const [name, before] of inputBytes)
          NodeAssert.ok(
            before.equals(await NodeFSP.readFile(NodePath.join(repository, name))),
            "Source changed before native fixture: " + name,
          );
        let result, primary;
        try {
          result = await runLegacyIdentityFixtureCommand(
            "/usr/bin/python3",
            ["-I", "-B", driver, payload, root],
            root,
            { signal },
          );
          NodeAssert.equal(result.code, 0, result.stderr.slice(0, 8192));
          NodeAssert.equal(result.stoppedBy, null);
        } catch (error) {
          primary = error;
        }
        try {
          const raw = await NodeFSP.readFile(evidence);
          NodeAssert.ok(raw.length <= 65536);
          validatePreparationNativeClosure(JSON.parse(raw));
        } catch (cleanup) {
          const failure = new AggregateError(
            primary ? [primary, cleanup] : [cleanup],
            "Native preparation closure is unproved; exact scratch retained.",
            { cause: primary ?? cleanup },
          );
          failure.preserveScratch = true;
          failure.evidencePath = evidence;
          throw failure;
        }
        if (primary) throw primary;
        NodeAssert.ok(result.stdout.length <= 262144);
        const report = JSON.parse(result.stdout);
        NodeAssert.equal(report.fixtureClosure, true);
        NodeAssert.equal(report.producerQualified, false);
        NodeAssert.equal(report.nativeColdOwnerQualified, false);
        NodeAssert.equal(report.stateAdmissionAllowed, false);
        NodeAssert.equal(report.activationAllowed, false);
        NodeAssert.equal(report.cases.length, 1);
        NodeAssert.equal(report.cases[0].case, scenario);
        // Publish bounded evidence before its creator removes this exact scratch root.
        process.stderr.write(JSON.stringify({ event: "preparation-native-case", report }) + "\n");
        return report;
      },
      { temporaryDirectory },
    );
    records.push(result);
  }
  return {
    schema: 1,
    qualification: "synthetic-native-preparation",
    cases: records,
    artifact: {
      executableSha256: artifact.files.executable.sha256,
      nativeSha256: artifact.files.native.sha256,
      nodeSha256: artifact.files.node.sha256,
      entrySha256: artifact.files.entry.sha256,
      sourceSha: artifact.sourceSha,
      sourceTree: artifact.sourceTree,
    },
    fixtureDataSourceHashes: { core: hash(core), workerTests: hash(test) },
    producerQualified: false,
    priorBootModeled: true,
    nativeColdOwnerQualified: false,
    stateAdmissionAllowed: false,
    activationAllowed: false,
  };
}

if (
  process.argv[1] &&
  NodeURL.pathToFileURL(NodePath.resolve(process.argv[1])).href === import.meta.url
) {
  const abort = new AbortController();
  const cancel = () => abort.abort(new Error("Native fixture cancelled"));
  process.once("SIGTERM", cancel);
  process.once("SIGINT", cancel);
  try {
    const args = process.argv.slice(2);
    if (args.length !== 2 || args[0] !== "--input")
      throw new PreparationNativeUnavailable("use --input with a source-bound final SEA record");
    const raw = await readPreparationNativeInputFile(args[1], abort.signal);
    const result = await runLegacyPreparationNativeFixture({
      input: JSON.parse(raw),
      signal: abort.signal,
    });
    process.stdout.write(JSON.stringify(result) + "\n");
  } catch (error) {
    process.stderr.write(
      JSON.stringify({
        status: "qualification-unavailable-or-failed",
        code: error.code ?? "FIXTURE_FAILED",
        message: error.message,
        scratchRoot: error.scratchRoot,
        evidencePath: error.evidencePath,
        stateAdmissionAllowed: false,
      }) + "\n",
    );
    process.exitCode = 1;
  } finally {
    process.removeListener("SIGTERM", cancel);
    process.removeListener("SIGINT", cancel);
  }
}
