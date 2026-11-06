// Explicit source integration probe. No dependency installation, foreign checkout
// mutation, live registry, provider, or configured-runner qualification is implied.
import { installEffectSeam } from "./EffectSeam.node-test-support.mjs";
installEffectSeam();
import * as assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as childProcess from "node:child_process";
import { createHash } from "node:crypto";
const { fixture, command } = await import("./Conformance.testkit.ts");
const { acquireReservation, releaseReservation, holderIdentity } = await import("./Reservation.ts");
const { incarnation } = await import("./OutputAttestation.ts");
const { participantGuard, reservationTransaction } = await import("./ParticipantGuard.ts");
const source = process.argv[2],
  python = process.argv[3];
assert.ok(
  source && python && path.isAbsolute(source) && path.isAbsolute(python),
  "explicit pinned V3 source repository and qualified interpreter paths required",
);
const revision = "8b4abb6d24da5f994ea66e6cc7c591e645754f43";
const tree = "3459d9c63dbbd428952e69f3738c96ef77853ec7";
assert.equal(await command(source, ["rev-parse", revision + "^{tree}"]), tree);
const files = [
  "workspace_lifecycle.py",
  "workspace_reservation.py",
  "worktree_readiness.py",
  "build_outputs/tree.py",
  "git/process.py",
];
const program = String.raw`
import sys,json,sqlite3,hashlib
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from codex_v3.workspace_lifecycle import LifecycleRegistry,ConflictError
from codex_v3.worktree_readiness import Registry
from codex_v3.workspace_reservation import Reservation
r=json.load(sys.stdin)
registry=LifecycleRegistry(r['database'])
b=r['binding']
def snapshot():
 p=Path(r['database'])
 return (p.read_bytes(),p.stat().st_mtime_ns,sorted(x.name for x in p.parent.iterdir()))
if r['action']=='initialize':
 with Registry(r['database'])._connection(): pass
 registry.migrate(expected_version=1)
 print(json.dumps(registry.assess(r['assessment'],expected_generation=None)))
elif r['action']=='refuse':
 before=snapshot()
 opens=[]
 original=registry._connection
 def forbidden(*args,**kwargs):
  opens.append(True)
  return original(*args,**kwargs)
 registry._connection=forbidden
 for name in ('assess','resume'):
  try:
   if name=='assess': registry.assess(r['assessment'],expected_generation=r['generation'])
   else: registry.resume(b,expected_generation=r['generation'])
   raise AssertionError(name+' wrote after FC5')
  except ConflictError as exc: assert str(exc)=='retiring',str(exc)
 assert not opens,'participant SQLite opened before reservation refusal'
 assert snapshot()==before,'participant SQLite bytes/mtime/directory changed'
 print(json.dumps({'assess':'retiring','resume':'retiring','participant_opens':len(opens),'sqlite_write':False}))
elif r['action']=='write':
 print(json.dumps(registry.resume(b,expected_generation=r['generation'])))
elif r['action']=='acquire':
 print(json.dumps(Reservation(r['physical']).acquire(**r['holder'])))
elif r['action']=='release':
 Reservation(r['physical']).release(r['record'])
 print('{}')
elif r['action']=='busy':
 before=snapshot()
 try: registry.resume(b,expected_generation=r['generation'])
 except sqlite3.OperationalError as exc:
  assert (exc.sqlite_errorcode & 255) in (sqlite3.SQLITE_BUSY,sqlite3.SQLITE_LOCKED)
 else: raise AssertionError('busy common store permitted writer')
 assert snapshot()==before
 print(json.dumps({'busy':True,'sqlite_write':False}))
`;
await fixture(async ({ base, repo, worktree, input }) => {
  const extracted = path.join(base, "pinned-source"),
    database = path.join(base, "registry", "lifecycle.sqlite3");
  await fs.mkdir(path.dirname(database));
  const blobs = {};
  for (const file of files) {
    const target = path.join(extracted, "codex_v3", file);
    await fs.mkdir(path.dirname(target), { recursive: true });
    const body = await new Promise((resolve, reject) =>
      childProcess.execFile(
        "git",
        ["-C", source, "show", `${revision}:src/codex_v3/${file}`],
        { encoding: "buffer", timeout: 10000, maxBuffer: 1024 * 1024 },
        (error, out) => (error ? reject(error) : resolve(out)),
      ),
    );
    await fs.writeFile(target, body);
    blobs[file] = await command(source, ["rev-parse", `${revision}:src/codex_v3/${file}`]);
    assert.equal(
      createHash("sha1").update(`blob ${body.length}\0`).update(body).digest("hex"),
      blobs[file],
    );
  }
  for (const sub of ["", "build_outputs", "git"])
    await fs.writeFile(path.join(extracted, "codex_v3", sub, "__init__.py"), "");
  const bound = input.expected,
    binding = {
      environment_id: "fixture",
      thread_id: "fixture",
      incarnation_id: incarnation(bound),
      repository: "Jones-Systems/Synthetic",
      worktree,
      git_common_dir: bound.git_common_dir,
      git_dir: bound.git_dir,
      branch: "refs/heads/task",
      head: await command(repo, ["rev-parse", "HEAD"]),
      source_id: "native-fixture",
      source_generation: "1",
    };
  const assessment = {
    schema: "workspace-assessment/v1",
    binding,
    role: "authoring",
    disposition: "cleanup_candidate",
    remaining_work: [],
    preservation_refs: [],
    handoff_refs: [],
    artifact_destinations: [],
    unresolved_obligations: [],
  };
  const run = (action, extra = {}) =>
    new Promise((resolve, reject) => {
      const child = childProcess.execFile(
        python,
        ["-B", "-c", program, extracted],
        { timeout: 10000, maxBuffer: 65536, encoding: "utf8" },
        (error, out, err) =>
          error
            ? reject(new Error(`${action}: ${err || error.message}`))
            : resolve(JSON.parse(out)),
      );
      child.stdin.end(JSON.stringify({ action, database, binding, assessment, ...extra }));
    });
  let { generation } = await run("initialize");
  const holder = await holderIdentity(),
    record = {
      schema: "workspace-reservation/v1",
      admin_id: bound.admin_id,
      incarnation: incarnation(bound),
      executor: "jones-fixture",
      receipt: "post-FC5",
      ...holder,
    };
  // RV02: Jones owns FC5 through the removal window. Both actual A3 C11
  // writers refuse before even opening their participant SQLite connection.
  await acquireReservation(bound, record);
  try {
    assert.deepEqual(await run("refuse", { generation }), {
      assess: "retiring",
      resume: "retiring",
      participant_opens: 0,
      sqlite_write: false,
    });
  } finally {
    await releaseReservation(bound, record);
  }
  // RV01: an actual writer committed before acquisition changes generation;
  // retirement's subsequent generation read must therefore reject its old token.
  const before = generation;
  ({ generation } = await run("write", { generation }));
  assert.notEqual(generation, before);
  await acquireReservation(bound, record);
  try {
    await run("refuse", { generation });
  } finally {
    await releaseReservation(bound, record);
  }
  // Reverse RV02: the actual A3 reservation row excludes the Jones writer;
  // its callback never runs and its independent SQLite store is unchanged.
  const reverse = await run("acquire", {
    physical: bound,
    holder: { executor: "v3-fixture", receipt: "reverse-FC5", ...holder },
  });
  const jonesPath = path.join(base, "jones-writer.sqlite3");
  await fs.writeFile(jonesPath, "untouched participant");
  const digest = () =>
    fs.readFile(jonesPath).then((bytes) => createHash("sha256").update(bytes).digest("hex"));
  const initial = await digest();
  let writes = 0;
  try {
    await assert.rejects(
      participantGuard(bound, async () => {
        writes++;
        await fs.writeFile(jonesPath, "incorrect");
      }),
      /retiring/,
    );
    assert.equal(writes, 0);
    assert.equal(await digest(), initial);
  } finally {
    await run("release", { physical: bound, record: reverse });
  }
  // RV03: busy admission is refused without participant writes.
  await reservationTransaction(bound, async () => {
    assert.deepEqual(await run("busy", { generation }), { busy: true, sqlite_write: false });
  });
  // The absent-store vector must not create a common directory.
  const absent = path.join(base, "absent-common");
  await participantGuard({ ...bound, git_common_dir: absent }, async () => {});
  await assert.rejects(fs.lstat(absent), { code: "ENOENT" });
  console.log(
    JSON.stringify({
      source: { revision, tree, blobs },
      vectors: {
        RV01: "actual resume generation advanced before acquisition",
        RV02: "bilateral post-FC5 refusal; zero participant SQLite opens/writes",
        RV03: "busy refusal and absent-store no-create",
      },
      fixtureCleanup: "owned fixture finally with root identity and absence readback",
    }),
  );
});
