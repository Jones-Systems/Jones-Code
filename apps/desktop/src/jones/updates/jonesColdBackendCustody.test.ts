// @effect-diagnostics-next-line nodeBuiltinImport:off -- The fixture owns the emitted module and every child under its scratch root.
import * as NodeFSP from "node:fs/promises";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Fixture paths are bounded by the creating layer.
import * as NodePath from "node:path";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The fixture uses the current qualified test interpreter.
import * as NodeProcess from "node:process";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Bounded polling observes only this fixture's readiness marker.
import * as NodeTimersPromises from "node:timers/promises";
import { describe, expect, it } from "vite-plus/test";
// @ts-expect-error -- The existing owned fixture has no declarations.
import * as IdentityFixture from "../../../../../scripts/jones/updates/legacy-identity-native-fixture.mjs";
import { jonesColdBackendCustodySource } from "./jonesColdBackendCustody.ts";

// Only this lexical test harness reaches the private constructor. There is no
// production permit issuer, exported test mint, supplied spawner, or PID adoption.
const harness = String.raw`
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
const scenario = process.argv[2];
const root = process.argv[3];
const binding = {transactionId:'1'.repeat(64),transferId:'2'.repeat(64),handoffSha256:'3'.repeat(64),sourceSha:'4'.repeat(40),sourceTree:'5'.repeat(40),artifactSha256:'6'.repeat(64),bootSessionUuid:'00000000-0000-4000-8000-000000000001'};
assert.throws(() => startJonesColdBackend(null));
assert.throws(() => startJonesColdBackend({binding, qualified:true}));
const controller = new AbortController();
const marker = root + '/custody.json';
let custody;
let cancelling = false;
function writeMarker(value) {
 fs.writeFileSync(marker + '.next',JSON.stringify(value));
 fs.renameSync(marker + '.next',marker);
}
function record(result) {
 writeMarker({state:result.settled || (!result.spawned && result.closeObserved) ? 'settled' : 'held',result});
}
async function cancel(signal) {
 if (cancelling) return;
 cancelling = true;
 controller.abort();
 try { if (custody) record(await custody.stop()); }
 finally { process.exitCode = signal === 'SIGINT' ? 130 : 143; }
}
process.on('SIGTERM',() => { void cancel('SIGTERM'); });
process.on('SIGINT',() => { void cancel('SIGINT'); });
custody = new CapturedColdBackend({executable:scenario === 'unknown-spawn' ? root + '/absent' : process.execPath,
 args:[root + '/child.mjs'],cwd:root,env:{HOME:root,TMPDIR:root,BINDING:JSON.stringify(binding),SCENARIO:scenario}},binding,controller.signal);
let rejected = false;
try {
 await custody.ready();
 writeMarker({state:'ready'});
 if (scenario === 'ready-reuse') { try { await custody.ready(); } catch { rejected = true; } }
 if (scenario === 'cancel') controller.abort();
} catch { rejected = true; }
const result = scenario === 'cancel' ? await custody.stop() : await custody.wait();
record(result);
if (!cancelling) {
assert.equal(result.held,true);
for (const flag of ['nativeIdentityQualified','chromiumWritersQualified','stateAdmissionAllowed','activationAllowed','restorationAllowed']) assert.equal(result[flag],false);
if (scenario === 'unknown-spawn') { assert.equal(result.spawnUnknown,true); assert.equal(result.settled,false); }
else {
 assert.equal(result.settled,true);
 assert.equal(result.exitObserved,true);
 assert.equal(result.closeObserved,true);
 assert.equal(result.pipesClosed,true);
 assert.ok(result.events.indexOf('EXIT_OBSERVED') < result.events.indexOf('CLOSE_OBSERVED'));
}
assert.equal(result.lifecycleFailed,scenario !== 'success');
if (scenario !== 'output-failure') assert.equal(result.abnormalExit,['exit-one','exit-signal','cancel','unknown-spawn'].includes(scenario));
if (scenario === 'exit-one') assert.deepEqual(result.exitStatus,{code:1,signal:null});
if (scenario === 'exit-signal') assert.deepEqual(result.exitStatus,{code:null,signal:'SIGTERM'});
assert.equal(result.stopRequested,scenario === 'cancel');
if (['wrong-generation','duplicate','truncated','lost-app','unknown-spawn','ready-reuse'].includes(scenario)) assert.equal(rejected,true);
console.log(JSON.stringify({scenario,settled:result.settled,held:result.held,lifecycleFailed:result.lifecycleFailed}));
}
`;

const child = String.raw`
import * as fs from 'node:fs';
const scenario = process.env.SCENARIO;
if (scenario === 'external-cancel-escalation') process.on('SIGTERM',() => {});
const binding = JSON.parse(process.env.BINDING);
const identity = {pid:process.pid,parentPid:process.ppid,uniqueId:'1',parentUniqueId:'2',pidVersion:1,uid:process.getuid()};
if (scenario === 'wrong-generation') binding.transferId = '9'.repeat(64);
const frame = JSON.stringify({protocol:1,binding,sequence:1,type:'READY',identity}) + '\n';
if (scenario === 'truncated') fs.writeSync(8,'{"protocol":');
else if (scenario !== 'lost-app') fs.writeSync(8,frame + (scenario === 'duplicate' ? frame : ''));
if (scenario === 'output-failure') {
 const chunk = Buffer.alloc(65536);
 for (let i=0;i<17;i++) fs.writeSync(1,chunk);
}
if (scenario === 'cancel' || scenario.startsWith('external-cancel')) setInterval(() => {},1000);
if (scenario === 'exit-one') process.exitCode = 1;
if (scenario === 'exit-signal') process.kill(process.pid,'SIGTERM');
`;

async function runOwnedFixture(root: string, scenario: string, signal: AbortSignal) {
  const marker = NodePath.join(root, "custody.json");
  // Only the creating layer decides cleanup. A killed driver cannot erase uncertainty.
  await NodeFSP.writeFile(marker, JSON.stringify({ state: "held" }));
  const cancel = new AbortController();
  let failure: unknown;
  let result;
  try {
    const command = IdentityFixture.runLegacyIdentityFixtureCommand(
      NodeProcess.execPath,
      [NodePath.join(root, "custody.mjs"), scenario, root],
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
    lifecycleFailed: scenario !== "success",
  });
}

describe("private captured cold backend", () => {
  it.for([
    "success",
    "wrong-generation",
    "duplicate",
    "truncated",
    "lost-app",
    "unknown-spawn",
    "ready-reuse",
    "cancel",
    "output-failure",
    "exit-one",
    "exit-signal",
    "external-cancel",
    "external-cancel-escalation",
  ])(
    "retains real child and pipe custody for %s",
    { timeout: 30000 },
    async (scenario, { signal }) => {
      await IdentityFixture.withLegacyIdentityScratch(async (root: string) => {
        const entry = NodePath.join(root, "custody.mjs");
        await NodeFSP.writeFile(entry, jonesColdBackendCustodySource + harness);
        await NodeFSP.writeFile(NodePath.join(root, "child.mjs"), child);
        await runOwnedFixture(root, scenario, signal);
      });
    },
  );
});
