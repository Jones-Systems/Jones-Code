// @effect-diagnostics-next-line nodeBuiltinImport:off -- This creating layer owns its Python fixture files and captured processes.
import * as NodeFSP from "node:fs/promises";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Every fixture path is beneath the owned scratch root.
import * as NodePath from "node:path";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Bounded polling observes only this fixture's readiness marker.
import * as NodeTimersPromises from "node:timers/promises";
import { describe, expect, it } from "vite-plus/test";
// @ts-expect-error -- The existing owned fixture has no declarations.
import * as IdentityFixture from "../../../../../scripts/jones/updates/legacy-identity-native-fixture.mjs";
import { jonesColdTrialCustodySource } from "./jonesColdTrialCustodySource.ts";

const driver = String.raw`
import sys, pathlib, json, os, types, fcntl, signal
root = pathlib.Path(sys.argv[1])
payload = json.loads((root / 'payload.json').read_text())
scenario = payload['scenario']
scope = {'__name__':'owned_cold_candidate_fixture'}
exec(compile(payload['source'],'cold-custody.py','exec'),scope)
try: scope['_CapturedColdCandidate']()
except RuntimeError: pass
else: raise AssertionError('Production candidate issuer was enabled')
binding = dict(transactionId='1'*64,transferId='2'*64,handoffSha256='3'*64,sourceSha='4'*40,sourceTree='5'*40,artifactSha256='6'*64,bootSessionUuid='00000000-0000-4000-8000-000000000001')
# The runtime issuer is replaced only in this owned lexical fixture namespace.
# Its identity strings exercise decoding, never kernel identity qualification.
lock = os.open(root / 'fixed.lock',os.O_CREAT|os.O_RDWR,0o600)
fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
lock_identity = os.fstat(lock)
def check():
    current = os.fstat(lock)
    assert (current.st_dev,current.st_ino) == (lock_identity.st_dev,lock_identity.st_ino)
runtime = types.SimpleNamespace(binding=binding,check=check,lock_fd=lock,directory=root,
    executable=root / ('absent' if scenario == 'unknown-spawn' else 'candidate.py'),
    environment=dict(PATH='/usr/bin:/bin',HOME=str(root),TMPDIR=str(root),SCENARIO=scenario,BINDING=json.dumps(binding),FIXED_LOCK_FD=str(lock)))
scope['_cold_trial_runtime'] = lambda: runtime
custody = scope['_CapturedColdCandidate']()
cancelled = False
marker = root / 'custody.json'
def write_marker(value):
    pending = root / 'custody.next'
    pending.write_text(json.dumps(value))
    pending.replace(marker)
def record(result):
    known_absent = custody.child is None and scenario == 'unknown-spawn' and isinstance(custody.failure, FileNotFoundError)
    write_marker(dict(state='settled' if result['settled'] or known_absent else 'held',result=result))
def cancel(signum, frame):
    global cancelled
    if cancelled: return
    cancelled = True
    signal.signal(signal.SIGTERM,signal.SIG_IGN)
    signal.signal(signal.SIGINT,signal.SIG_IGN)
    custody.poison(RuntimeError('Owned fixture cancelled.'))
    record(custody.stop())
    raise SystemExit(128 + signum)
signal.signal(signal.SIGTERM,cancel)
signal.signal(signal.SIGINT,cancel)
rejected = False
try:
    custody.start()
    custody.ready()
    write_marker(dict(state='ready'))
    if scenario == 'ready-reuse': custody.ready()
    if scenario == 'start-reuse':
        try: custody.start()
        except RuntimeError: rejected = True
        else: raise AssertionError('Candidate launched twice')
    if scenario == 'cancel': result = custody.stop()
    else:
        custody.send('STOP_REQUEST')
        custody.receive(('BACKEND_SETTLED',))
        custody.receive(('TERMINAL_CLOSED',))
except BaseException as failure:
    custody.poison(failure)
    rejected = True
    if cancelled: raise
finally:
    result = custody.stop() if scenario == 'cancel' else custody.finish()
    record(result)
    os.close(lock)
assert result['held'] is True
for flag in ('nativeIdentityQualified','chromiumWritersQualified','stateAdmissionAllowed','activationAllowed','restorationAllowed'): assert result[flag] is False
assert result['settled'] is (scenario != 'unknown-spawn'), result
assert result['lifecycleFailed'] is (scenario not in ('success','start-reuse')), result
assert result['abnormalExit'] is (scenario in ('unknown-spawn','cancel','exit-one','exit-signal')), result
assert result['stopRequested'] is (scenario == 'cancel'), result
if scenario == 'exit-one': assert result['exitStatus'] == dict(code=1,signal=None), result
if scenario == 'exit-signal': assert result['exitStatus'] == dict(code=None,signal='SIGTERM'), result
if scenario in ('wrong-generation','duplicate','truncated','lost-app','unknown-spawn','lost-backend','unknown-type','start-reuse','ready-reuse'): assert rejected
print(json.dumps(dict(scenario=scenario,settled=result['settled'],held=result['held'],lifecycleFailed=result['lifecycleFailed'])))
`;

const candidate = String.raw`#!/usr/bin/python3
import json, os, signal, socket, time
scenario = os.environ['SCENARIO']
if scenario == 'external-cancel-escalation': signal.signal(signal.SIGTERM,signal.SIG_IGN)
channel = socket.socket(fileno=int(os.environ['T3CODE_JONES_COLD_CHANNEL_FD']))
os.fstat(int(os.environ['FIXED_LOCK_FD']))
binding = json.loads(os.environ['BINDING'])
identity = dict(pid=os.getpid(),parentPid=os.getppid(),uniqueId='1',parentUniqueId='2',pidVersion=1,uid=os.getuid())
if scenario == 'wrong-generation': binding['transferId'] = '9'*64
def frame(sequence,kind,identity=None):
    return (json.dumps(dict(protocol=1,binding=binding,sequence=sequence,type=kind,identity=identity))+'\n').encode()
if scenario == 'truncated': channel.sendall(b'{"protocol":')
elif scenario != 'lost-app': channel.sendall(frame(1,'READY' if scenario != 'unknown-type' else 'EXECUTE',identity))
if scenario == 'duplicate': channel.sendall(frame(1,'READY',identity))
if scenario == 'ready-reuse': channel.sendall(frame(2,'READY',dict(identity,uniqueId='3')))
if scenario in ('success','start-reuse','exit-one','exit-signal'):
    raw = b''
    while b'\n' not in raw: raw += channel.recv(16384)
    value = json.loads(raw)
    assert value['type'] == 'STOP_REQUEST' and value['sequence'] == 1 and value['binding'] == binding
    channel.sendall(frame(2,'BACKEND_SETTLED')+frame(3,'TERMINAL_CLOSED'))
if scenario == 'cancel' or scenario.startswith('external-cancel'): time.sleep(10)
channel.close()
if scenario == 'exit-one': raise SystemExit(1)
if scenario == 'exit-signal': os.kill(os.getpid(),signal.SIGTERM)
`;

async function runOwnedFixture(root: string, scenario: string, signal: AbortSignal) {
  const marker = NodePath.join(root, "custody.json");
  // An absent completion marker retains this exact root even if the outer driver was killed.
  await NodeFSP.writeFile(marker, JSON.stringify({ state: "held" }));
  const cancel = new AbortController();
  let failure: unknown;
  let result;
  try {
    const command = IdentityFixture.runLegacyIdentityFixtureCommand(
      "/usr/bin/python3",
      ["-I", "-B", NodePath.join(root, "driver.py"), root],
      root,
      { signal: AbortSignal.any([signal, cancel.signal]) },
    ).then(
      (value: { code: number; stdout: string; stderr: string }) => ({ value }),
      (cause: unknown) => ({ cause }),
    );
    try {
      if (scenario.startsWith("external-cancel")) {
        const deadline = Date.now() + 5000;
        while (JSON.parse(await NodeFSP.readFile(marker, "utf8")).state !== "ready") {
          if (Date.now() >= deadline) throw new Error("Captured fixture did not reach READY.");
          await NodeTimersPromises.setTimeout(5);
        }
        cancel.abort(new Error("Owned external cancellation after READY."));
      }
    } finally {
      if (scenario.startsWith("external-cancel")) cancel.abort();
      result = await command;
    }
  } catch (cause) {
    failure = cause;
  }
  const custody = await NodeFSP.readFile(marker, "utf8")
    .then((value) => JSON.parse(value))
    .catch(() => null);
  if (custody?.state !== "settled") {
    const uncertain = new Error("Captured fixture descendant closure is unproved.", {
      cause: failure,
    });
    Object.assign(uncertain, { preserveScratch: true });
    throw uncertain;
  }
  if (failure) throw failure;
  if (scenario.startsWith("external-cancel")) {
    expect(result).toHaveProperty("cause");
    expect(custody.result.settled).toBe(true);
    expect(custody.result.stopRequested).toBe(true);
    expect(custody.result.exitStatus).toEqual({
      code: null,
      signal: scenario === "external-cancel-escalation" ? "SIGKILL" : "SIGTERM",
    });
    expect(custody.result.lifecycleFailed).toBe(true);
    return;
  }
  if (!result || "cause" in result) throw result?.cause ?? new Error("Missing fixture result.");
  expect(result.value.code, result.value.stderr).toBe(0);
  expect(result.value.stderr).toBe("");
  expect(JSON.parse(result.value.stdout.trim())).toEqual({
    scenario,
    settled: scenario !== "unknown-spawn",
    held: true,
    lifecycleFailed: scenario !== "success" && scenario !== "start-reuse",
  });
}

describe("private captured cold candidate", () => {
  it.for([
    "success",
    "wrong-generation",
    "duplicate",
    "truncated",
    "lost-app",
    "lost-backend",
    "unknown-spawn",
    "start-reuse",
    "unknown-type",
    "cancel",
    "ready-reuse",
    "exit-one",
    "exit-signal",
    "external-cancel",
    "external-cancel-escalation",
  ])(
    "retains real Popen and channel custody for %s",
    { timeout: 30000 },
    async (scenario, { signal }) => {
      await IdentityFixture.withLegacyIdentityScratch(async (root: string) => {
        const entry = NodePath.join(root, "driver.py");
        await NodeFSP.writeFile(entry, driver);
        await NodeFSP.writeFile(NodePath.join(root, "candidate.py"), candidate, { mode: 0o700 });
        await NodeFSP.writeFile(
          NodePath.join(root, "payload.json"),
          JSON.stringify({ source: jonesColdTrialCustodySource, scenario }),
        );
        await runOwnedFixture(root, scenario, signal);
      });
    },
  );
});
