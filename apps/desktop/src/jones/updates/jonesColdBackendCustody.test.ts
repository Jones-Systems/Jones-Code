// @effect-diagnostics-next-line nodeBuiltinImport:off -- The fixture owns the emitted module and every child under its scratch root.
import * as NodeFSP from "node:fs/promises";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Fixture paths are bounded by the creating layer.
import * as NodePath from "node:path";
import * as NodeProcess from "node:process";
import { observeOwnedFixture } from "./jonesColdBackendCustody.testFixtures.ts";
import { describe, expect, it } from "vite-plus/test";
// @ts-expect-error -- The existing owned fixture has no declarations.
import * as IdentityFixture from "../../../../../scripts/jones/updates/native-fixture-support.mjs";
import * as NodeModule from "node:module";

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
const capturedOutput = new Map();
const drains = [1,2,5,7].map(async fd => {
 const chunks = [];
 for await (const chunk of custody.output(fd)) chunks.push(chunk);
 capturedOutput.set(fd,Buffer.concat(chunks).toString());
});
// Attach rejection handlers immediately; the owner preserves stream failures independently.
const draining = Promise.allSettled(drains);
if (scenario === 'fd-roundtrip') for (const fd of [3,4,6]) custody.input(fd).end('owned-fd-' + fd + '\n');
let rejected = false;
try {
 await custody.ready();
 writeMarker({state:'ready'});
 if (scenario === 'ready-reuse') { try { await custody.ready(); } catch { rejected = true; } }
 if (scenario === 'cancel') controller.abort();
} catch { rejected = true; }
const result = scenario === 'cancel' ? await custody.stop() : await custody.wait();
await draining;
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
assert.equal(result.lifecycleFailed,!['success','fd-roundtrip'].includes(scenario));
if (scenario === 'fd-roundtrip') { assert.equal(capturedOutput.get(5),'owned-fd-3\nowned-fd-4\n'); assert.equal(capturedOutput.get(7),'owned-fd-6\n'); }
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
if (scenario === 'fd-roundtrip') {
 const read = fd => { let line = ''; const byte = Buffer.alloc(1); while (!line.endsWith('\n')) { if (!fs.readSync(fd,byte)) throw new Error('Missing owned input'); line += byte.toString(); } return line; };
 fs.writeSync(5,read(3) + read(4)); fs.writeSync(7,read(6));
}
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
  const { result, custody } = await observeOwnedFixture(root, scenario, signal);
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
    lifecycleFailed: scenario !== "success" && scenario !== "fd-roundtrip",
  });
}

describe("private captured cold backend", () => {
  it("rejects failed or altered readback across the whole fixed hardening operation", async () => {
    const controls = String.raw`
import * as assert from 'node:assert/strict';
const original = [0,2,4];
const expected = [1,3,5];
for (const slot of [0,1,2]) {
 for (const invalid of [-1,NaN,1.5,0x80000000,expected[slot]^2]) {
  let reads=0; const writes=[];
  const verify = coldHardenFixedDescriptors(fd => {
   const index=fd-3; return reads++<3 ? original[index] : index===slot ? invalid : expected[index];
  },fd=>{writes.push(fd);return 0;});
  assert.deepEqual(writes,[3,4,5]);
  assert.throws(verify);
 }
}
for (const invalid of [-1,NaN,1.5,0x80000000]) {
 let writes=0;
 assert.throws(()=>coldHardenFixedDescriptors(()=>invalid,()=>{writes++;return 0;}));
 assert.equal(writes,0);
}
let reads=0;
coldHardenFixedDescriptors(fd=>reads++<3?original[fd-3]:expected[fd-3],()=>0)();
assert.throws(()=>coldHardenFixedDescriptors(()=>0,()=>-1));
`;
    await IdentityFixture.withNativeFixtureScratch(async (root: string) => {
      const source = await NodeFSP.readFile(
        new URL("./jonesColdBackendCustody.ts", import.meta.url),
        "utf8",
      );
      const layout = await NodeFSP.readFile(
        new URL("./jonesColdDarwinLayout.ts", import.meta.url),
        "utf8",
      );
      const entry = NodePath.join(root, "hardening.mjs");
      await NodeFSP.writeFile(
        NodePath.join(root, "jonesColdDarwinLayout.mjs"),
        NodeModule.stripTypeScriptTypes(layout),
      );
      await NodeFSP.writeFile(
        entry,
        NodeModule.stripTypeScriptTypes(
          source.replace('"./jonesColdDarwinLayout.ts"', '"./jonesColdDarwinLayout.mjs"') +
            controls,
        ),
      );
      const result = await IdentityFixture.runNativeFixtureCommand(
        NodeProcess.execPath,
        [entry],
        root,
      );
      expect(result.code).toBe(0);
      expect(result.stdout).toBe("");
    });
  });
  it.for([
    "success",
    "fd-roundtrip",
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
    async (scenario, { signal }) => {
      await IdentityFixture.withNativeFixtureScratch(async (root: string) => {
        const entry = NodePath.join(root, "custody.mjs");
        const source = await NodeFSP.readFile(
          new URL("./jonesColdBackendCustody.ts", import.meta.url),
          "utf8",
        );
        const layout = await NodeFSP.readFile(
          new URL("./jonesColdDarwinLayout.ts", import.meta.url),
          "utf8",
        );
        await NodeFSP.writeFile(
          NodePath.join(root, "jonesColdDarwinLayout.mjs"),
          NodeModule.stripTypeScriptTypes(layout),
        );
        await NodeFSP.writeFile(
          entry,
          NodeModule.stripTypeScriptTypes(
            source.replace('"./jonesColdDarwinLayout.ts"', '"./jonesColdDarwinLayout.mjs"') +
              harness,
          ),
        );
        await NodeFSP.writeFile(NodePath.join(root, "child.mjs"), child);
        await runOwnedFixture(root, scenario, signal);
      });
    },
  );
});
