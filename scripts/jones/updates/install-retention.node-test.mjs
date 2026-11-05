import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import * as FS from "node:fs/promises";
import { rmSync } from "node:fs";
import * as OS from "node:os";
import * as Path from "node:path";
import { jonesNativeHelperSource } from "../../../apps/desktop/src/jones/updates/jonesNativeHelperSource.ts";
import { proposeRuntimeStagingRetention } from "../../../apps/server/src/jones/cloud/runtimeStagingRetention.ts";
import { withQualifiedRuntimeLock } from "../../../apps/server/src/jones/cloud/qualifiedRuntime.ts";

const definitions = jonesNativeHelperSource.slice(0, jonesNativeHelperSource.indexOf("\nparser = argparse.ArgumentParser()"));
const seed = String.raw`
root = pathlib.Path(sys.argv[1])
manifest_path = root / 'runtime' / 'jones-active-install.json'
retention_exclusion_held = True
ids = {letter: letter * 64 for letter in 'abcdef'}
manual = manifest_path.parent / 'jones-manual-installs'
def receipt(letter, phase='launched', dependencies=None):
    identifier = ids[letter]
    directory = manual / identifier
    directory.mkdir(parents=True, exist_ok=True)
    value = {'protocol': 1, 'operation': 'manual-install', 'transactionId': identifier,
             'phase': phase, 'installed': {}, 'candidate': {}, 'originalManifest': json.dumps({'generation': 'bootstrap'}), 'dependencies': dependencies or []}
    durable(directory / 'receipt.json', value)
    if phase == 'launched': durable(directory / 'previous-pair' / 'pair.json', {'expected': {'generation': 'bootstrap'}, 'stateScope': 'database-settings'})
    return directory
durable(manifest_path, {'protocol': 1, 'owner': 'desktop', 'home': str(root), 'generation': ids['a'], 'transactionId': 'manual-' + ids['a']})
retention_lock = open(manifest_path.with_name('jones-activation.lock'), 'w+')
retention_lock.write('jones-activation-lock-v1\n'); retention_lock.flush()
fcntl.flock(retention_lock, fcntl.LOCK_EX)
retention_exclusion_descriptor = retention_lock.fileno()
receipt('a', dependencies=[{'generation': ids['b']}, {'transactionId': 'manual-' + ids['c']}])
receipt('b', dependencies=[{'generation': ids['e']}])
receipt('c'); receipt('d'); receipt('e')
for letter in 'bce':
    pair = manual / ids[letter] / 'previous-pair'
    pair.mkdir(exist_ok=True)
    (pair / 'state.sqlite').write_text('state-' + letter)
    (pair / 'app-preimage').write_text('app-' + letter)
def enable_retire():
    durable(root / 'userdata' / 'settings.json', {'storageCleanup': {'installRetention': 'retire'}})
`;

async function fixture(action) {
  const root = await FS.realpath(await FS.mkdtemp(Path.join(OS.tmpdir(), "jones-install-retention-")));
  const cancelled = () => {
    rmSync(root, { recursive: true, force: true });
    process.exit(143);
  };
  process.once("SIGTERM", cancelled);
  process.once("SIGINT", cancelled);
  let primary;
  try { return await action(root); }
  catch (error) { primary = error; throw error; }
  finally {
    process.removeListener("SIGTERM", cancelled);
    process.removeListener("SIGINT", cancelled);
    try {
      await FS.rm(root, { recursive: true });
      await assert.rejects(FS.lstat(root), { code: "ENOENT" });
    } catch (error) {
      if (primary) throw new AggregateError([primary, error], "Test failed and fixture cleanup failed");
      throw error;
    }
  }
}

async function python(body) {
  await fixture(async (root) => {
    execFileSync("python3", ["-B", "-", root], {
      input: `${definitions}\n${seed}\n${body}\n`, encoding: "utf8", timeout: 15000,
    });
  });
}

test("JR32: active, both previous members and transitive payloads survive exact retirement", () => python(String.raw`
plan = install_retention_plan()
assert plan['keep'] == [ids[c] for c in 'abce']
assert [c['transactionId'] for c in plan['candidates']] == [ids['d']]
assert plan['mode'] == 'propose'
try: apply_install_retention(plan)
except RuntimeError: pass
else: raise AssertionError('default propose removed material')
assert (manual / ids['d']).exists()
enable_retire()
plan = install_retention_plan()
result = apply_install_retention(plan)
assert result['transactions'] == [ids['d']]
for letter in 'bce':
    assert (manual / ids[letter] / 'previous-pair' / 'state.sqlite').read_text() == 'state-' + letter
    assert (manual / ids[letter] / 'previous-pair' / 'app-preimage').read_text() == 'app-' + letter
assert not (manual / ids['d']).exists()
assert install_retention_plan()['candidates'] == []
`));

test("JR33: warm recovery and embedded original manifests pin manual dependencies", () => python(String.raw`
warm = manifest_path.parent / 'jones-updates' / 'transactions' / ('f' * 64)
durable(warm / 'journal.json', {'phase': 'blocked', 'previous': {'generation': ids['d']}})
assert install_retention_plan()['candidates'] == []
(warm / 'journal.json').unlink()
value = read(manual / ids['a'] / 'receipt.json')
value['originalManifest'] = json.dumps({'transactionId': 'manual-' + ids['d']})
durable(manual / ids['a'] / 'receipt.json', value)
durable(manual / ids['a'] / 'previous-pair' / 'pair.json', {'expected': json.loads(value['originalManifest']), 'stateScope': 'database-settings'})
assert install_retention_plan()['candidates'] == []
`));

test("JR35: real previous state and app restoration still works after superseded retention", () => python(String.raw`
live = root / 'userdata'; live.mkdir()
database = live / 'state.sqlite'; database.write_text('advanced-state')
stable, previous, incoming = root / 'active.app', root / 'previous.app', root / 'incoming.app'
stable.mkdir(); previous.mkdir()
(stable / 'binary').write_text('new-app'); (previous / 'binary').write_text('previous-app')
expected = {'generation': ids['b'], 'databasePath': str(database), 'profile': str(root / 'profile'), 'appPath': str(stable)}
pair = manual / ids['a'] / 'previous-pair'
(pair / 'state.sqlite').write_text('previous-state')
durable(pair / 'pair.json', {'expected': expected, 'stateScope': 'database-settings', 'files': {'': True, '-wal': False, '-shm': False}, 'settings': {name: False for name in SETTINGS}})
value = read(manual / ids['a'] / 'receipt.json')
durable(manual / ids['a'] / 'receipt.json', dict(value, originalManifest=json.dumps(expected), previousBundle=str(previous), pairedState=str(pair)))
enable_retire(); apply_install_retention(install_retention_plan())
# Host quiescence/signature/authority are native boundaries, not exercised on
# synthetic files. The production pair and app restore functions run unchanged.
prove_quiescence = lambda expected: None
native_authority_transition = lambda *args: {'kind': 'absent'}
verify_rollback_signature = lambda *args: None
restore_pair(expected, pair, manual / ids['a'] / 'advanced-state', {})
restore_bundle(expected, incoming, previous)
assert database.read_text() == 'previous-state'
assert (stable / 'binary').read_text() == 'previous-app'
assert (incoming / 'binary').read_text() == 'new-app'
assert (manual / ids['a'] / 'advanced-state' / 'state.sqlite').read_text() == 'advanced-state'
`));

test("JR33: unfinished, unknown and held records keep their full dependency closure", () => python(String.raw`
receipt('f', 'held', [{'generation': ids['d']}])
assert install_retention_plan()['candidates'] == []
receipt('f', 'future-phase', [{'generation': ids['d']}])
assert install_retention_plan()['candidates'] == []
(manual / ids['f'] / 'receipt.json').unlink()
assert propose_install_retention()['status'] == 'held'
`));

test("JR33: the three latest failed receipts and logs remain present", () => python(String.raw`
for index in range(4):
    identifier = str(index) * 64
    path = manual / identifier
    durable(path / 'receipt.json', {'protocol': 1, 'operation': 'manual-install', 'transactionId': identifier, 'phase': 'failed'})
    (path / 'manual-install.log').write_text('failure-' + str(index))
plan = install_retention_plan()
assert all(str(index) * 64 in plan['keep'] for index in range(4))
assert all((manual / (str(index) * 64) / 'manual-install.log').is_file() for index in range(4))
`));

for (const change of ["manifest", "receipt", "replacement", "symlink"]) {
  test(`JR36: per-candidate CAS refuses ${change} drift`, () => python(String.raw`
enable_retire()
plan = install_retention_plan()
candidate = manual / ids['d']
change = '${change}'
if change == 'manifest': durable(manifest_path, dict(read(manifest_path), generation=ids['d']))
elif change == 'receipt': durable(candidate / 'receipt.json', dict(read(candidate / 'receipt.json'), phase='held'))
elif change == 'replacement':
    candidate.rename(root / 'saved')
    candidate.mkdir(); durable(candidate / 'receipt.json', read(root / 'saved' / 'receipt.json'))
else:
    candidate.rename(root / 'saved'); candidate.symlink_to(root / 'saved', target_is_directory=True)
try: apply_install_retention(plan)
except (RuntimeError, OSError): pass
else: raise AssertionError('stale plan removed material')
assert candidate.exists()
assert not (manifest_path.parent / 'jones-install-retention' / (ids['d'] + '.json')).exists()
`));
}

test("JR36: change after intent publication refuses removal and holds unknown effect", () => python(String.raw`
enable_retire(); plan = install_retention_plan()
original = durable
def concurrent_change(path, value, exclusive=False):
    original(path, value, exclusive)
    if value.get('status') == 'intent':
        original(manifest_path, dict(read(manifest_path), generation=ids['d']))
durable = concurrent_change
try: apply_install_retention(plan)
except RuntimeError: pass
else: raise AssertionError('post-intent generation race was ignored')
assert (manual / ids['d'] / 'receipt.json').exists()
marker = manifest_path.parent / 'jones-install-retention' / (ids['d'] + '.json')
assert read(marker)['status'] == 'held'
assert propose_install_retention()['status'] == 'held'
`));

test("JR36: link payload never follows outside the candidate and partial effects never replay", () => python(String.raw`
enable_retire()
external = root / 'outside'; external.write_text('preserved')
(manual / ids['d'] / 'previous-pair').mkdir(exist_ok=True)
(manual / ids['d'] / 'previous-pair' / 'link').symlink_to(external)
plan = install_retention_plan()
try: apply_install_retention(plan)
except RuntimeError: pass
else: raise AssertionError('symlink payload was removed')
assert external.read_text() == 'preserved'
assert propose_install_retention()['status'] == 'held'
`));

test("JR36: replacement at the destructive open seam refuses symlink traversal", () => python(String.raw`
enable_retire(); plan = install_retention_plan()
candidate = manual / ids['d']
external = root / 'external'; external.mkdir(); (external / 'secret').write_text('untouched')
original_open = os.open
def replace_at_open(path, flags, *args, **kwargs):
    if pathlib.Path(path) == candidate and flags & os.O_DIRECTORY:
        candidate.rename(root / 'saved-candidate')
        candidate.symlink_to(external, target_is_directory=True)
    return original_open(path, flags, *args, **kwargs)
os.open = replace_at_open
try: apply_install_retention(plan)
except OSError: pass
else: raise AssertionError('replacement at destructive seam was accepted')
assert (external / 'secret').read_text() == 'untouched'
assert read(manifest_path.parent / 'jones-install-retention' / (ids['d'] + '.json'))['status'] == 'held'
`));

test("JR36: each removal compares the remaining closure rather than trusting a batch", () => python(String.raw`
receipt('f'); enable_retire(); plan = install_retention_plan()
original = durable
def change_after_first_removal(path, value, exclusive=False):
    original(path, value, exclusive)
    if value.get('status') == 'removed' and value['candidate']['transactionId'] == ids['d']:
        original(manifest_path, dict(read(manifest_path), generation=ids['f']))
durable = change_after_first_removal
try: apply_install_retention(plan)
except RuntimeError: pass
else: raise AssertionError('second candidate used a stale batch closure')
assert (manual / ids['f'] / 'receipt.json').exists()
assert not (manual / ids['d']).exists()
assert propose_install_retention()['status'] == 'held'
`));

test("JR34/36: real admission, startup recovery and retention entries share lock; process exit releases it", () => fixture(async (root) => {
  const helper = Path.join(root, "helper.py");
  // Only the platform dispatch gate is adapted. No native action is reached.
  await FS.writeFile(helper, jonesNativeHelperSource.replace("if sys.platform != 'darwin':", "if False:"));
  execFileSync("python3", ["-B", "-", root, helper], { encoding: "utf8", timeout: 15000, input: `${definitions}\n${seed}\n` + String.raw`
helper = sys.argv[2]
retention_lock.close()
retention_exclusion_held = False
try: install_retention_plan()
except RuntimeError: pass
else: raise AssertionError('retention bypassed exclusion')
lock_path = manifest_path.with_name('jones-activation.lock')
lock_path.write_text('jones-activation-lock-v1\n')
with open(lock_path, 'r+') as lock:
    fcntl.flock(lock, fcntl.LOCK_EX)
    for action in (['--inspect-manual-install', str(root / 'absent')], ['--reconcile-running', str(root / 'absent')], ['--inspect-install-retention']):
        value = subprocess.run([sys.executable, '-B', helper, '--manifest', str(manifest_path)] + action, capture_output=True, text=True, timeout=3)
        assert value.returncode != 0 or json.loads(value.stdout).get('reason') == 'busy', value
    assert not (manifest_path.parent / 'jones-install-retention-plan.json').exists()
value = subprocess.run([sys.executable, '-B', helper, '--manifest', str(manifest_path), '--inspect-install-retention'], capture_output=True, text=True, timeout=3)
assert value.returncode == 0, value.stderr
assert json.loads(value.stdout)['mode'] == 'propose'
# Admission has retained exactly its original launched-only rule; held material
# still refuses before the rest of the install plan can be inspected.
receipt('f', 'held')
plan_path = root / 'manual-plan.json'
durable(plan_path, {'protocol': 1, 'restart': 'ordinary', 'expectedManifestSha256': digest(manifest_path)})
value = subprocess.run([sys.executable, '-B', helper, '--manifest', str(manifest_path), '--inspect-manual-install', str(plan_path), '--plan-sha256', digest(plan_path)], capture_output=True, text=True, timeout=3)
assert value.returncode != 0 and 'Unfinished manual installation requires reconciliation.' in value.stderr, value.stderr
` });
}));

test("runtime generations: known state and staged references are reported; unknown consumers hold", () => fixture(async (root) => {
  const runtime = Path.join(root, "runtime");
  await FS.mkdir(Path.join(runtime, "versions", "old"), { recursive: true });
  await FS.mkdir(Path.join(runtime, "versions", "active"));
  await FS.writeFile(Path.join(runtime, "service-state.json"), JSON.stringify({ activeVersion: "active", update: { fromVersion: "old" } }));
  await FS.mkdir(Path.join(runtime, "staged-updates"));
  await FS.writeFile(Path.join(runtime, "staged-updates", "stage.json"), JSON.stringify({ receipt: { version: "old" } }));
  await proposeRuntimeStagingRetention(root);
  const plan = JSON.parse(await FS.readFile(Path.join(runtime, "jones-runtime-retention-plan.json"), "utf8"));
  assert.equal(plan.held.length, 2);
  assert.deepEqual(plan.candidates, []);
  assert.deepEqual(plan.references[0].versions, ["active", "old"]);
  await withQualifiedRuntimeLock(root, "qualified-runtime-lock", async () => {
    await assert.rejects(proposeRuntimeStagingRetention(root), /already owns/);
  });
  await proposeRuntimeStagingRetention(root);
  assert.equal((await FS.readdir(runtime)).filter((name) => name.startsWith("jones-runtime-retention-plan.json.")).length, 0);
}));
