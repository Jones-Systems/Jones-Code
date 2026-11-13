import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeProcess from "node:process";
import { jonesNativeHelperSource } from "../../../apps/desktop/src/jones/updates/jonesNativeHelperSource.ts";
import { proposeRuntimeStagingRetention } from "../../../apps/server/src/jones/cloud/runtimeStagingRetention.ts";
import { withQualifiedRuntimeLock } from "../../../apps/server/src/jones/cloud/qualifiedRuntime.ts";

const definitions = jonesNativeHelperSource.slice(
  0,
  jonesNativeHelperSource.indexOf("\nparser = argparse.ArgumentParser()"),
);
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
def warm(identifier, prior):
    directory = manifest_path.parent / 'jones-updates' / 'transactions' / identifier
    prior = dict({'home': str(root), 'databasePath': str(root / 'userdata' / 'state.sqlite'),
                  'profile': str(root / 'profile'), 'appPath': str(root / 'Jones.app'), 'environmentId': 'fixture'}, **prior)
    handle = '9' * 64
    claim = preparation_claim(prior, handle, identifier, manifest_path.parent / 'jones-updates' / 'staging' / 'fixture.json', '0' * 64)
    durable(directory / 'prepare-intent.json', claim)
    durable(directory / 'prepare-dispatched.json', claim)
    durable(directory / 'continuation.json', dict(claim, prepared=True))
    intent = {'protocol': 1, 'transactionId': identifier, 'expected': prior, 'staged': {'handle': handle},
              'continuationReceipt': str(directory / 'continuation.json'), 'processes': [{'pid': 1, 'identity': 'fixture'}], 'listener': 'fixture'}
    durable(directory / 'intent.json', intent)
    pair = directory / 'previous-pair'
    durable(pair / 'pair.json', {'expected': prior, 'stateScope': 'database-settings'})
    durable(directory / 'journal.json', {'intent': intent, 'phase': 'resumed', 'pairedState': str(pair)})
    return directory
def warm_chain():
    active = dict(read(manifest_path), databasePath=str(root / 'userdata' / 'state.sqlite'),
                  profile=str(root / 'profile'), appPath=str(root / 'Jones.app'), environmentId='fixture')
    durable(manifest_path, active)
    first, second = '1' * 64, '2' * 64
    baseline = dict(active, generation='bootstrap', transactionId='bootstrap')
    warm(first, baseline)
    prior = dict(active, generation=first, transactionId=first)
    directory = warm(second, prior)
    pair = directory / 'previous-pair'
    durable(pair / 'pair.json', {'expected': prior, 'stateScope': 'database-settings'})
    (pair / 'state.sqlite').write_text('W1-state')
    previous = pathlib.Path(active['appPath'] + '.jones-previous-' + first)
    previous.mkdir(); (previous / 'binary').write_text('W1-app')
    durable(directory / 'journal.json', dict(read(directory / 'journal.json'), pairedState=str(pair), previousBundle=str(previous)))
    value = read(manual / ids['a'] / 'receipt.json')
    expected = dict(active, generation=second, transactionId=second)
    durable(manual / ids['a'] / 'receipt.json', dict(value, originalManifest=json.dumps(expected)))
    durable(manual / ids['a'] / 'previous-pair' / 'pair.json', {'expected': expected, 'stateScope': 'database-settings'})
    return active, first, second, pair, previous
def unreferenced_warm():
    active, _, _, _, _ = warm_chain()
    identifier = '3' * 64
    prior = dict(active, generation='unreferenced', transactionId='bootstrap')
    directory = warm(identifier, prior)
    pair = directory / 'previous-pair'
    (pair / 'state.sqlite').write_text('unreferenced-state')
    previous = pathlib.Path(active['appPath'] + '.jones-previous-unreferenced')
    previous.mkdir(); (previous / 'binary').write_text('unreferenced-app')
    durable(directory / 'journal.json', dict(read(directory / 'journal.json'), previousBundle=str(previous)))
    acquire = globals()
    acquire['acquire_writer_exclusion'] = lambda *args, **kwargs: []
    acquire['prove_quiescence'] = lambda *args: None
    return active, identifier, pair, previous
`;

async function fixture(action) {
  const root = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-install-retention-")),
  );
  const cancelled = () => {
    NodeFS.rmSync(root, { recursive: true, force: true });
    process.exit(143);
  };
  process.once("SIGTERM", cancelled);
  process.once("SIGINT", cancelled);
  let primary;
  try {
    return await action(root);
  } catch (error) {
    primary = error;
    throw error;
  } finally {
    process.removeListener("SIGTERM", cancelled);
    process.removeListener("SIGINT", cancelled);
    try {
      await NodeFSP.rm(root, { recursive: true });
      await NodeAssert.strict.rejects(NodeFSP.lstat(root), { code: "ENOENT" });
    } catch (error) {
      if (primary)
        throw new AggregateError([primary, error], "Test failed and fixture cleanup failed");
      throw error;
    }
  }
}

async function python(body) {
  await fixture(async (root) => {
    NodeChildProcess.execFileSync("python3", ["-B", "-", root], {
      input: `${definitions}\n${seed}\n${body}\n`,
      encoding: "utf8",
      timeout: 15000,
    });
  });
}

NodeTest.test(
  "JR32: active, both previous members and transitive payloads survive exact retirement",
  () =>
    python(String.raw`
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
`),
);

NodeTest.test("JR33: warm recovery and embedded original manifests pin manual dependencies", () =>
  python(String.raw`
warm_path = warm('f' * 64, {'generation': ids['d'], 'transactionId': 'manual-' + ids['d']})
assert install_retention_plan()['candidates'] == []
shutil.rmtree(warm_path)
value = read(manual / ids['a'] / 'receipt.json')
value['originalManifest'] = json.dumps({'transactionId': 'manual-' + ids['d']})
durable(manual / ids['a'] / 'receipt.json', value)
durable(manual / ids['a'] / 'previous-pair' / 'pair.json', {'expected': json.loads(value['originalManifest']), 'stateScope': 'database-settings'})
assert install_retention_plan()['candidates'] == []
`),
);

NodeTest.test("J6-COR-01: activation-request-only warm recovery holds manual retirement", () =>
  python(String.raw`
enable_retire(); plan = install_retention_plan()
directory = manifest_path.parent / 'jones-updates' / 'transactions' / ('f' * 64)
durable(directory / 'activation-request.json', {'protocol': 1, 'transactionId': directory.name, 'expected': {'transactionId': 'manual-' + ids['d']}})
assert propose_install_retention()['status'] == 'held'
try: apply_install_retention(plan)
except RuntimeError: pass
else: raise AssertionError('incomplete warm recovery deleted its manual dependency')
assert (manual / ids['d'] / 'receipt.json').exists()
assert not (manifest_path.parent / 'jones-install-retention' / (ids['d'] + '.json')).exists()
`),
);

NodeTest.test(
  "J6-COR-01: all warm metadata pins dependencies and incomplete or unknown sets hold",
  () =>
    python(String.raw`
directory = warm('f' * 64, {'generation': 'bootstrap'})
# This recovery guard was outside the original five-file inventory.
guard = directory / 'advanced-state-authority-guard.json'
durable(guard, {'protocol': 1, 'guard': {'transactionId': 'manual-' + ids['d']}})
durable(directory / 'advanced-state' / 'retained.json', {'protocol': 1, 'previous': read(directory / 'intent.json')['expected']})
assert install_retention_plan()['candidates'] == []
guard.unlink(); shutil.rmtree(directory / 'advanced-state')
for fault in ('missing-claim', 'unknown-record', 'mismatched-request', 'missing-pair'):
    claim = directory / 'prepare-intent.json'
    pair = directory / 'previous-pair' / 'pair.json'
    if fault == 'missing-claim': claim.rename(root / 'saved-claim')
    elif fault == 'unknown-record': durable(directory / 'future-recovery.json', {'transactionId': 'manual-' + ids['d']})
    elif fault == 'mismatched-request': durable(directory / 'activation-request.json', {'transactionId': 'manual-' + ids['d']})
    else: pair.rename(root / 'saved-pair')
    assert propose_install_retention()['status'] == 'held', fault
    if fault == 'missing-claim': (root / 'saved-claim').rename(claim)
    elif fault == 'unknown-record': (directory / 'future-recovery.json').unlink()
    elif fault == 'mismatched-request': (directory / 'activation-request.json').unlink()
    else: (root / 'saved-pair').rename(pair)
assert [value['transactionId'] for value in install_retention_plan()['candidates']] == [ids['d']]
`),
);

NodeTest.test(
  "J6-COR-01: identical warm record replacement invalidates manual removal after intent",
  () =>
    python(String.raw`
directory = warm('f' * 64, {'generation': 'bootstrap'})
enable_retire(); plan = install_retention_plan()
original = durable
def replace_record(path, value, exclusive=False):
    original(path, value, exclusive)
    if value.get('status') == 'intent':
        claim = directory / 'prepare-intent.json'
        raw = claim.read_bytes(); claim.rename(root / 'saved-claim'); claim.write_bytes(raw)
durable = replace_record
try: apply_install_retention(plan)
except RuntimeError: pass
else: raise AssertionError('same-content warm record replacement was ignored')
assert (manual / ids['d'] / 'receipt.json').exists()
assert read(manifest_path.parent / 'jones-install-retention' / (ids['d'] + '.json'))['status'] == 'held'
assert propose_install_retention()['status'] == 'held'
`),
);

NodeTest.test(
  "J6-COR-02: existing warm retirement preserves active manual transitive recovery",
  () =>
    python(String.raw`
active, first, second, pair, previous = warm_chain()
acquire_writer_exclusion = lambda *args, **kwargs: []
prove_quiescence = lambda *args: None
for identifier in (second, first):
    result = retirement_command('inspect-retirement', active, identifier)
    assert result['status'] == 'refused' and result['reason'] == 'protected-recovery-parent', result
    result = retirement_command('retire-transaction', active, identifier, '0' * 64)
    assert result['status'] == 'refused' and result['reason'] == 'protected-recovery-parent', result
assert (pair / 'state.sqlite').read_text() == 'W1-state'
assert (previous / 'binary').read_text() == 'W1-app'
`),
);

NodeTest.test("J6-COR-02: a manual app preimage path alone pins the warm pair", () =>
  python(String.raw`
active, first, second, pair, previous = warm_chain()
value = read(manual / ids['a'] / 'receipt.json')
expected = {'generation': 'bootstrap'}
durable(manual / ids['a'] / 'receipt.json', dict(value, originalManifest=json.dumps(expected), previousBundle=str(previous)))
durable(manual / ids['a'] / 'previous-pair' / 'pair.json', {'expected': expected, 'stateScope': 'database-settings'})
for identifier in (second, first):
    result = retirement_command('inspect-retirement', active, identifier)
    assert result['reason'] == 'protected-recovery-parent', result
assert (pair / 'state.sqlite').exists() and (previous / 'binary').exists()
`),
);

for (const target of ["held", "dependency"]) {
  NodeTest.test(
    `J6-COR-R2-01: old warm retirement preserves ${target} recovery under an unresolved hold`,
    () =>
      python(String.raw`
active, dependency, pair, previous = unreferenced_warm()
held = '4' * 64
prior = dict(active, generation=dependency, transactionId=dependency)
directory = warm(held, prior)
held_pair = directory / 'previous-pair'
(held_pair / 'state.sqlite').write_text('held-state')
held_app = pathlib.Path(active['appPath'] + '.jones-previous-' + dependency)
held_app.mkdir(); (held_app / 'binary').write_text('held-app')
durable(directory / 'journal.json', dict(read(directory / 'journal.json'),
        recoveryReserveCleanup='held', previousBundle=str(held_app), dependencies=[{'generation': ids['d']}]))
identifier = held if '${target}' == 'held' else dependency
plan = retirement_command('inspect-retirement', active, identifier)
result = retirement_command('retire-transaction', active, identifier, plan.get('planSha256', '0' * 64))
assert (held_pair / 'state.sqlite').read_text() == 'held-state', result
assert (held_app / 'binary').read_text() == 'held-app', result
assert (pair / 'state.sqlite').read_text() == 'unreferenced-state', result
assert (previous / 'binary').read_text() == 'unreferenced-app', result
assert plan['status'] == result['status'] == 'refused', (plan, result)
assert plan['reason'] == result['reason'] == 'protected-recovery-parent', (plan, result)
assert read(directory / 'journal.json')['recoveryReserveCleanup'] == 'held'
assert not (directory / 'retirement-intent.json').exists()
assert not (pair.parent / 'retirement-intent.json').exists()
manual_plan = install_retention_plan()
assert held in manual_plan['snapshot']['warmKeep'] and dependency in manual_plan['snapshot']['warmKeep']
assert ids['d'] in manual_plan['keep'] and manual_plan['candidates'] == []
`),
  );
}

NodeTest.test(
  "J6-COR-02: unreferenced warm retirement completes and reads back its durable receipt",
  () =>
    python(String.raw`
active, identifier, pair, previous = unreferenced_warm()
result = retirement_command('inspect-retirement', active, identifier)
assert result['status'] == 'ready' and len(result['targets']) == 2, result
result = retirement_command('retire-transaction', active, identifier, result['planSha256'])
assert result['status'] == 'retired', result
assert not (pair / 'state.sqlite').exists() and not previous.exists()
assert (pair / 'pair.json').exists()
assert retirement_command('retire-transaction', active, identifier, result['planSha256'])['status'] == 'retired'
`),
);

for (const change of ["warm-record-identity", "manual-record", "between-targets"]) {
  NodeTest.test(`J6-COR-02: fresh cross-store generation refuses ${change} drift`, () =>
    python(String.raw`
active, identifier, pair, previous = unreferenced_warm()
approved = retirement_command('inspect-retirement', active, identifier)
assert approved['status'] == 'ready', approved
original_durable, original_remove = durable, remove_retirement_payload
def drift():
    if '${change}' == 'warm-record-identity':
        path = pair.parent / 'prepare-intent.json'
        raw = path.read_bytes(); path.rename(root / 'saved-claim'); path.write_bytes(raw)
    else:
        path = manual / ids['a'] / 'receipt.json'
        original_durable(path, dict(read(path), note='generation changed'))
def after_intent(path, value, exclusive=False):
    original_durable(path, value, exclusive)
    if path.name == 'retirement-intent.json' and '${change}' != 'between-targets': drift()
def after_target(target):
    original_remove(target)
    if '${change}' == 'between-targets': drift()
durable, remove_retirement_payload = after_intent, after_target
result = retirement_command('retire-transaction', active, identifier, approved['planSha256'])
assert result['status'] == 'uncertain' and result['reason'] == 'plan-changed', result
assert previous.exists()
assert (pair / 'state.sqlite').exists() == ('${change}' != 'between-targets')
directory = pair.parent
assert (directory / 'retirement-intent.json').exists() and not (directory / 'retirement-receipt.json').exists()
assert retirement_command('retire-transaction', active, identifier, approved['planSha256'])['status'] == 'uncertain'
`),
  );
}

NodeTest.test(
  "JR35: real previous state and app restoration still works after superseded retention",
  () =>
    python(String.raw`
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
`),
);

NodeTest.test("JR33: unfinished, unknown and held records keep their full dependency closure", () =>
  python(String.raw`
receipt('f', 'held', [{'generation': ids['d']}])
assert install_retention_plan()['candidates'] == []
receipt('f', 'future-phase', [{'generation': ids['d']}])
assert install_retention_plan()['candidates'] == []
(manual / ids['f'] / 'receipt.json').unlink()
assert propose_install_retention()['status'] == 'held'
`),
);

NodeTest.test("JR33: the three latest failed receipts and logs remain present", () =>
  python(String.raw`
for index in range(4):
    identifier = str(index) * 64
    path = manual / identifier
    durable(path / 'receipt.json', {'protocol': 1, 'operation': 'manual-install', 'transactionId': identifier, 'phase': 'failed'})
    (path / 'manual-install.log').write_text('failure-' + str(index))
plan = install_retention_plan()
assert all(str(index) * 64 in plan['keep'] for index in range(4))
assert all((manual / (str(index) * 64) / 'manual-install.log').is_file() for index in range(4))
`),
);

for (const change of ["manifest", "receipt", "replacement", "symlink"]) {
  NodeTest.test(`JR36: per-candidate CAS refuses ${change} drift`, () =>
    python(String.raw`
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
`),
  );
}

NodeTest.test(
  "JR36: change after intent publication refuses removal and holds unknown effect",
  () =>
    python(String.raw`
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
`),
);

NodeTest.test(
  "JR36: link payload never follows outside the candidate and partial effects never replay",
  () =>
    python(String.raw`
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
`),
);

NodeTest.test("JR36: replacement at the destructive open seam refuses symlink traversal", () =>
  python(String.raw`
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
`),
);

NodeTest.test(
  "JR36: each removal compares the remaining closure rather than trusting a batch",
  () =>
    python(String.raw`
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
`),
);

NodeTest.test(
  "JR34/36: real admission, startup recovery and retention entries share lock; process exit releases it",
  () =>
    fixture(async (root) => {
      const helper = NodePath.join(root, "helper.py");
      const platformGuard =
        "\nif sys.platform != 'darwin': raise SystemExit('The Jones native helper requires macOS.')\n";
      NodeAssert.strict.equal(jonesNativeHelperSource.split(platformGuard).length, 2);
      // Adapt only the unique CLI gate in this disposable copy. Kernel guards stay intact.
      await NodeFSP.writeFile(helper, jonesNativeHelperSource.replace(platformGuard, "\n"));
      if (NodeProcess.platform !== "darwin") {
        const guardManifest = NodePath.join(root, "guard-manifest.json");
        await NodeFSP.writeFile(guardManifest, "{}");
        const native = NodeChildProcess.spawnSync(
          "python3",
          ["-B", "-", "--manifest", guardManifest, "--inspect-install-retention"],
          { input: jonesNativeHelperSource, encoding: "utf8", timeout: 3000 },
        );
        NodeAssert.strict.notEqual(native.status, 0);
        NodeAssert.strict.match(native.stderr, /The Jones native helper requires macOS\./);
      }
      NodeChildProcess.execFileSync("python3", ["-B", "-", root, helper], {
        encoding: "utf8",
        timeout: 15000,
        input:
          `${definitions}\n${seed}\n` +
          String.raw`
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
    for action in (['--inspect-manual-install', str(root / 'absent')], ['--reconcile-running', str(root / 'absent')], ['--inspect-install-retention'], ['--retire-transaction', 'f' * 64]):
        value = subprocess.run([sys.executable, '-B', helper, '--manifest', str(manifest_path)] + action, capture_output=True, text=True, timeout=3)
        assert (value.returncode == 0 and json.loads(value.stdout).get('reason') == 'busy') or (value.returncode != 0 and 'BlockingIOError' in value.stderr and 'Resource temporarily unavailable' in value.stderr), value
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
`,
      });
    }),
);

NodeTest.test(
  "runtime generations: known state and staged references are reported; unknown consumers hold",
  () =>
    fixture(async (root) => {
      const runtime = NodePath.join(root, "runtime");
      await NodeFSP.mkdir(NodePath.join(runtime, "versions", "old"), { recursive: true });
      await NodeFSP.mkdir(NodePath.join(runtime, "versions", "active"));
      await NodeFSP.writeFile(
        NodePath.join(runtime, "service-state.json"),
        JSON.stringify({ activeVersion: "active", update: { fromVersion: "old" } }),
      );
      await NodeFSP.mkdir(NodePath.join(runtime, "staged-updates"));
      await NodeFSP.writeFile(
        NodePath.join(runtime, "staged-updates", "stage.json"),
        JSON.stringify({ receipt: { version: "old" } }),
      );
      await proposeRuntimeStagingRetention(root);
      const plan = JSON.parse(
        await NodeFSP.readFile(NodePath.join(runtime, "jones-runtime-retention-plan.json"), "utf8"),
      );
      NodeAssert.strict.equal(plan.held.length, 2);
      NodeAssert.strict.deepEqual(plan.candidates, []);
      NodeAssert.strict.deepEqual(plan.references[0].versions, ["active", "old"]);
      await withQualifiedRuntimeLock(root, "qualified-runtime-lock", async () => {
        await NodeAssert.strict.rejects(proposeRuntimeStagingRetention(root), /already owns/);
      });
      await proposeRuntimeStagingRetention(root);
      NodeAssert.strict.equal(
        (await NodeFSP.readdir(runtime)).filter((name) =>
          name.startsWith("jones-runtime-retention-plan.json."),
        ).length,
        0,
      );
    }),
);

// DBR fixtures use the production pair writer; only its native clone boundary is synthetic.
const databaseSeed = String.raw`
root = pathlib.Path(sys.argv[1])
manifest_path = root / 'runtime' / 'jones-active-install.json'
live = root / 'userdata'; live.mkdir()
for name in ('state.sqlite', 'state.sqlite-wal', 'state.sqlite-shm', 'settings.json', 'environment-id'):
    (live / name).write_bytes(b'{}')
active = {'protocol': 1, 'owner': 'desktop', 'home': str(root), 'databasePath': str(live / 'state.sqlite'),
          'profile': str(root / 'profile'), 'environmentId': 'synthetic', 'appPath': str(root / 'Jones.app'),
          'generation': 'a' * 64, 'transactionId': 'manual-' + 'a' * 64}
durable(manifest_path, active)
lock_path = manifest_path.with_name('jones-activation.lock')
lock_path.write_bytes(b'jones-activation-lock-v1\n')
lock = open(lock_path, 'r'); fcntl.flock(lock, fcntl.LOCK_EX)
retention_exclusion_held, retention_exclusion_descriptor = True, lock.fileno()
clone_file = lambda source, target, **kwargs: bool(shutil.copy2(source, target))
manual = manifest_path.parent / 'jones-manual-installs'
warm_root = manifest_path.parent / 'jones-updates' / 'transactions'
def make_backup(store, identifier, prior=None):
    expected = dict(active, generation='bootstrap', transactionId='bootstrap') if prior is None else prior
    directory = (manual if store == 'manual' else warm_root) / identifier
    directory.mkdir(parents=True)
    pair_state(expected, directory / 'previous-pair')
    if store == 'manual':
        durable(directory / 'receipt.json', {'protocol': 1, 'operation': 'manual-install', 'transactionId': identifier,
                'phase': 'launched', 'originalManifest': json.dumps(expected), 'pairedState': str(directory / 'previous-pair')})
    else:
        claim = preparation_claim(expected, '9' * 64, identifier, root / 'selection.json', '0' * 64)
        for name, value in (('prepare-intent.json', claim), ('prepare-dispatched.json', claim), ('continuation.json', dict(claim, prepared=True))): durable(directory / name, value)
        intent = {'protocol': 1, 'transactionId': identifier, 'expected': expected, 'staged': {'handle': '9' * 64},
                  'continuationReceipt': str(directory / 'continuation.json'), 'processes': [{'pid': 1, 'identity': 'synthetic'}], 'listener': 'synthetic'}
        durable(directory / 'intent.json', intent)
        durable(directory / 'journal.json', {'intent': intent, 'phase': 'resumed', 'pairedState': str(directory / 'previous-pair')})
    return directory / 'previous-pair'
pairs = [make_backup('manual', 'a' * 64), make_backup('manual', 'b' * 64), make_backup('warm', 'c' * 64)]
selection = ['manual:' + 'a' * 64, 'manual:' + 'b' * 64, 'warm:' + 'c' * 64]
def tree():
    return {str(p.relative_to(root)): (p.lstat().st_mode, p.lstat().st_ino, p.lstat().st_size, p.lstat().st_mtime_ns)
            for p in [root] + sorted(root.rglob('*'))}
def inspect():
    before = tree()
    value = database_retirement_command(False, selection)
    assert tree() == before, 'inspect changed the tree'
    return value
def ready():
    value = inspect(); assert value['status'] == 'ready', value
    return value['planSha256']
def retire(p): return database_retirement_command(True, selection, p)
def complete(p):
    value = retire(p); assert value['status'] == 'retired', value
    for pair in pairs:
        assert not any((pair / ('state.sqlite' + s)).exists() for s in ('', '-wal', '-shm'))
        assert read(pair / 'database-retirement.json')['status'] == 'retired'
    before = tree()
    assert retire(p)['status'] == inspect()['status'] == 'retired'
    assert tree() == before
`;
async function databasePython(body) {
  await fixture(async (root) => {
    NodeChildProcess.execFileSync("python3", ["-B", "-", root], {
      input: `${definitions}\n${databaseSeed}\n${body}\n`,
      encoding: "utf8",
      timeout: 20000,
    });
  });
}

NodeTest.test("DBR T8: restore rejects retirement evidence before quiescence or mutation", () =>
  databasePython(String.raw`
pair = pairs[0]
prove_quiescence = lambda *args: (_ for _ in ()).throw(AssertionError('quiescence reached'))
native_authority_transition = lambda *args: (_ for _ in ()).throw(AssertionError('transition reached'))
for name in ('database-retirement.json', '.database-retirement.json.retiring-' + 'f' * 64 + '.pending'):
    marker = pair / name; marker.write_bytes(b'{}')
    before = tree()
    try: restore_pair(read(pair / 'pair.json')['expected'], pair, pair.parent / 'advanced-state', {})
    except RuntimeError as error: assert str(error) == 'Recovery database snapshot was retired by its owner; restore held before state mutation.', str(error)
    else: raise AssertionError('restore accepted retirement evidence')
    assert tree() == before
    marker.unlink()
`),
);

NodeTest.test("DBR T1/T2: exact DB-only retirement survives generation changes and is idempotent", () =>
  databasePython(String.raw`
p = ready()
protected = {path: (database_file(path), path.read_bytes()) for pair in pairs for path in pair.iterdir() if not path.name.startswith('state.sqlite')}
protected.update({path: (database_file(path), None) for path in live.iterdir()})
for pair in pairs:
    for path in pair.parent.iterdir():
        if path.is_file(): protected[path] = (database_file(path), path.read_bytes())
make_backup('manual', 'd' * 64)
durable(manifest_path, dict(active, generation='d' * 64, transactionId='manual-' + 'd' * 64))
complete(p)
for path, (metadata, raw) in protected.items():
    assert database_file(path) == metadata, path
    if raw is not None: assert path.read_bytes() == raw
assert not list(root.rglob('*.pending'))
assert inspect()['logicalBytes'] == 18
`),
);

const databaseDrifts = {
  binding: "durable(manifest_path, dict(active, environmentId='changed'))",
  receipt: "durable(pairs[0].parent / 'receipt.json', dict(read(pairs[0].parent / 'receipt.json'), note='changed'))",
  journal: "durable(pairs[2].parent / 'journal.json', dict(read(pairs[2].parent / 'journal.json'), note='changed'))",
  pair: "path = pairs[0] / 'pair.json'; path.write_bytes(path.read_bytes() + b' ')",
  settings: "path = pairs[0] / 'settings.json'; path.rename(root / 'saved'); path.write_bytes(b'{}')",
  replacement: "path = pairs[0] / 'state.sqlite'; path.rename(root / 'saved'); path.write_bytes(b'{}')",
  resized: "(pairs[0] / 'state.sqlite').write_bytes(b'changed')",
  touched: "path = pairs[0] / 'state.sqlite'; os.utime(path, ns=(1, 1))",
  extra: "(pairs[0] / 'extra').write_bytes(b'')",
  symlink: "path = pairs[0] / 'state.sqlite'; path.unlink(); path.symlink_to(live / 'state.sqlite')",
  hardlink: "os.link(pairs[0] / 'state.sqlite', root / 'extra-link')",
  scope: "path = pairs[0] / 'pair.json'; durable(path, dict(read(path), stateScope='profile'))",
  flags: "path = pairs[0] / 'pair.json'; value = read(path); value['files']['-wal'] = False; durable(path, value)",
  unfinished: "path = pairs[0].parent / 'receipt.json'; durable(path, dict(read(path), phase='preparing'))",
};
for (const [name, mutation] of Object.entries(databaseDrifts)) {
  NodeTest.test(`DBR T3: pre-effect ${name} drift preserves all targets`, () =>
    databasePython(String.raw`
p = ready()
${mutation}
before = tree()
value = retire(p)
assert value['status'] == 'refused' and value['reason'], value
assert tree() == before
assert not (manifest_path.parent / DATABASE_RETIREMENT_STORE).exists()
assert all((pair / 'state.sqlite-wal').exists() for pair in pairs)
`),
  );
}

const databaseHolds = {
  trial: "d = make_backup('warm', 'e' * 64).parent; path = d / 'journal.json'; durable(path, dict(read(path), phase='trial'))",
  blocked: "d = make_backup('warm', 'e' * 64).parent; path = d / 'journal.json'; durable(path, dict(read(path), phase='blocked'))",
  intent: "d = make_backup('warm', 'e' * 64).parent; (d / 'journal.json').unlink()",
  preparation: "d = warm_root / ('e' * 64); durable(d / 'prepare-intent.json', {})",
  request: "d = warm_root / ('e' * 64); durable(d / 'activation-request.json', {'protocol': 1, 'transactionId': d.name, 'expected': active})",
  empty: "(warm_root / ('e' * 64)).mkdir()",
  retirement: "d = make_backup('warm', 'e' * 64).parent; durable(d / 'retirement-intent.json', {})",
  cleanup: "path = pairs[2].parent / 'journal.json'; durable(path, dict(read(path), recoveryReserveCleanup='held'))",
  cold: "path = pairs[2].parent / 'intent.json'; value = dict(read(path), maintenanceGeneration='cold'); durable(path, value); path = pairs[2].parent / 'journal.json'; durable(path, dict(read(path), intent=value))",
  held: "d = make_backup('manual', 'e' * 64).parent; path = d / 'receipt.json'; durable(path, dict(read(path), phase='held'))",
  preparing: "d = make_backup('manual', 'e' * 64).parent; path = d / 'receipt.json'; durable(path, dict(read(path), phase='preparing'))",
  audit_intent: "durable(manifest_path.parent / 'jones-install-retention' / ('e' * 64 + '.json'), {'protocol': 1, 'status': 'intent'})",
  audit_held: "durable(manifest_path.parent / 'jones-install-retention' / ('e' * 64 + '.json'), {'protocol': 1, 'status': 'held'})",
  maintenance: "(manifest_path.parent / 'jones-maintenance').mkdir()",
};
for (const [name, mutation] of Object.entries(databaseHolds)) {
  NodeTest.test(`DBR T4: global ${name} recovery holds without inspecting sibling payloads`, () =>
    databasePython(String.raw`
${mutation}
before = tree(); value = inspect()
assert value['status'] == 'refused' and value['reason'], value
assert tree() == before and not (manifest_path.parent / DATABASE_RETIREMENT_STORE).exists()
`),
  );
}
for (const payload of ["empty", "partial", "absent"]) {
  NodeTest.test(`DBR T4: terminal pre-effect sibling with ${payload} backup passes bounded admission`, () =>
    databasePython(String.raw`
pair = make_backup('warm', 'e' * 64); directory = pair.parent
for path in pair.iterdir(): path.unlink()
if '${payload}' == 'partial': (pair / 'state.sqlite').write_bytes(b'partial')
if '${payload}' == 'absent': pair.rmdir()
path = directory / 'journal.json'; value = read(path)
value.update(phase='rolled-back', preSwapPreparationRecovery={'settled': True})
value.pop('pairedState'); durable(path, value)
ready()
`),
  );
}

// Forks die inside the real filesystem calls, rather than after a publisher returns.
const databaseCrashes = {
  directory: "if event == 'fsync' and recdir.exists() and not list(recdir.iterdir()): crash()",
  record_scratch: "if event == 'replace' and str(dst) == str(rec) and '.intent-' in str(src): crash()",
  record_rename: "if event == 'fsync' and rec.exists() and not (pairs[0] / DATABASE_RETIREMENT_MARKER).exists(): crash()",
  marker_scratch: "if event == 'replace' and str(dst) == str(marker) and '.retiring-' in str(src): crash()",
  marker_rename: "if event == 'fsync' and marker.exists() and (pairs[0] / 'state.sqlite').exists(): crash()",
  unlink: "if event == 'fsync' and marker.exists() and not (pairs[0] / 'state.sqlite').exists(): crash()",
  marker_retired: "if event == 'fsync' and marker.exists() and read(marker)['status'] == 'retired': crash()",
  record_retired: "if event == 'replace' and str(dst) == str(rec) and '.retired-' in str(src): crash()",
};
for (const [name, point] of Object.entries(databaseCrashes)) {
  NodeTest.test(`DBR T5: process death at ${name} publication window resumes exact P`, () =>
    databasePython(String.raw`
p = ready()
recdir = manifest_path.parent / DATABASE_RETIREMENT_STORE
rec, marker = recdir / (p + '.json'), pairs[0] / DATABASE_RETIREMENT_MARKER
pid = os.fork()
if pid == 0:
    original_fsync, original_replace = os.fsync, os.replace
    def crash(): os._exit(91)
    def fsync(fd):
        event, src, dst = 'fsync', None, None
        ${point}
        return original_fsync(fd)
    def replace(src, dst):
        event = 'replace'
        ${point}
        return original_replace(src, dst)
    os.fsync, os.replace = fsync, replace
    retire(p)
    os._exit(92)
_, status = os.waitpid(pid, 0)
assert os.WEXITSTATUS(status) == 91, status
value = inspect(); assert value['status'] == 'ready' and value['reconciliation'], value
complete(p)
`),
  );
}
for (const partial of [0, 17]) {
  NodeTest.test(`DBR T5: ${partial}-byte record scratch reconciles without inspect writes`, () =>
    databasePython(String.raw`
p = ready(); plan = database_fresh_plan(active, database_selection(selection))
directory = manifest_path.parent / DATABASE_RETIREMENT_STORE; directory.mkdir()
rec = directory / (p + '.json'); raw = database_record(plan, p, 'intent')
database_scratch(rec, p, 'intent').write_bytes(raw[:${partial}])
value = inspect(); assert value['status'] == 'ready' and value['reconciliation'], value
complete(p)
`),
  );
}

for (const fault of ["foreign", "other-P", "unknown", "orphan", "missing-unmarked", "selection"]) {
  NodeTest.test(`DBR T5n: ${fault} state holds without further effects`, () =>
    databasePython(String.raw`
p = ready(); plan = database_fresh_plan(active, database_selection(selection))
directory = manifest_path.parent / DATABASE_RETIREMENT_STORE
if '${fault}' != 'orphan': directory.mkdir()
rec = directory / (p + '.json')
if '${fault}' == 'foreign': database_scratch(rec, p, 'intent').write_bytes(b'foreign')
elif '${fault}' == 'other-P': database_scratch(directory / ('f' * 64 + '.json'), 'f' * 64, 'intent').write_bytes(b'')
elif '${fault}' == 'unknown': (directory / 'unknown').write_bytes(b'')
elif '${fault}' == 'orphan': (pairs[0] / DATABASE_RETIREMENT_MARKER).write_bytes(database_marker(plan['snapshots'][0], p, 'retiring'))
else:
    rec.write_bytes(database_record(plan, p, 'intent'))
    if '${fault}' == 'missing-unmarked': (pairs[0] / 'state.sqlite').unlink()
    else: selection.pop()
before = tree(); value = retire(p)
assert value['status'] in ('refused', 'uncertain') and value['reason'], value
assert tree() == before
`),
  );
}

for (const drift of ["pair", "settings", "target", "live", "live-identity"]) {
  NodeTest.test(`DBR T6: ${drift} mutation after first unlink stops the next target`, () =>
    databasePython(String.raw`
p = ready(); original_unlink = database_unlink
called = False
def unlink(pair, identity, target):
    global called
    original_unlink(pair, identity, target)
    if called: return
    called = True
    if '${drift}' == 'pair':
        path = pair / 'pair.json'; path.write_bytes(path.read_bytes() + b' ')
    elif '${drift}' == 'settings':
        path = pair / 'settings.json'; path.rename(root / 'saved'); path.write_bytes(b'{}')
    elif '${drift}' == 'target':
        path = pair / 'state.sqlite-shm'; path.rename(root / 'saved'); path.write_bytes(b'{}')
    elif '${drift}' == 'live': durable(manifest_path, dict(active, databasePath=str(pair / 'state.sqlite-shm')))
    else:
        path = live / 'state.sqlite'; path.unlink(); path.symlink_to(pair / 'state.sqlite-shm')
database_unlink = unlink
value = retire(p)
assert value['status'] == 'uncertain' and value['reason'] in ('plan-changed', 'live-identity-collision'), value
assert not (pairs[0] / 'state.sqlite').exists() and (pairs[0] / 'state.sqlite-shm').exists() and (pairs[0] / 'state.sqlite-wal').exists()
assert read(pairs[0] / DATABASE_RETIREMENT_MARKER)['status'] == 'retiring'
try: install_retention_plan()
except RuntimeError as error: assert 'Unsettled database retirement' in str(error)
else: raise AssertionError('generic retention accepted uncertain state')
`),
  );
}

for (const store of ["manual", "warm"]) {
  for (const state of ["retiring", "scratch", "present", "hash", "retired"]) {
    NodeTest.test(`DBR T7: ${store} reader qualification for ${state} evidence`, () =>
      databasePython(String.raw`
p = ready(); plan = database_fresh_plan(active, database_selection(selection))
snap = plan['snapshots'][0 if '${store}' == 'manual' else 2]
pair = pairs[0 if '${store}' == 'manual' else 2]
marker = pair / DATABASE_RETIREMENT_MARKER
status = 'retiring' if '${state}' == 'retiring' else 'retired'
raw = database_marker(snap, p, status)
if '${state}' == 'scratch': database_scratch(marker, p, 'retiring').write_bytes(b'')
else:
    if '${state}' == 'hash':
        value = json.loads(raw); value['pairSha256'] = '0' * 64; raw = database_canon(value)
    marker.write_bytes(raw)
    if '${state}' not in ('retiring', 'present'):
        for target in snap['targets']: (pair / target['name']).unlink()
def readers():
    warm_retention_inventory()
    install_retention_plan()
    retirement_context(active, 'c' * 64)
if '${state}' == 'retired':
    readers()
    _, records, _ = warm_retention_inventory()
    assert all('database-retirement' not in key for key in records['c' * 64]), records
else:
    for reader in ([warm_retention_inventory] if '${store}' == 'warm' else []) + [install_retention_plan, lambda: retirement_context(active, 'c' * 64)]:
        try: reader()
        except RuntimeError as error: assert 'Unsettled database retirement' in str(error), str(error)
        else: raise AssertionError('generic reader accepted unsettled marker')
`),
    );
  }
}

NodeTest.test("DBR T7: pending records hold generic readers and missing generations still fail closure", () =>
  databasePython(String.raw`
p = ready(); plan = database_fresh_plan(active, database_selection(selection))
directory = manifest_path.parent / DATABASE_RETIREMENT_STORE; directory.mkdir()
rec = directory / (p + '.json'); rec.write_bytes(database_record(plan, p, 'intent'))
try: install_retention_plan()
except RuntimeError as error: assert 'Unsettled database retirement' in str(error)
else: raise AssertionError('pending record accepted')
rec.unlink(); directory.rmdir()
make_backup('warm', 'e' * 64, dict(active, generation='f' * 64, transactionId='f' * 64))
try: install_retention_plan()
except RuntimeError as error: assert str(error) == 'Missing warm recovery generation; preserved.', str(error)
else: raise AssertionError('closure was weakened')
`),
);

NodeTest.test("DBR T8: released inventory guard still holds with the retirement guard disabled", () =>
  databasePython(String.raw`
require_unretired_pair = lambda directory: None
prove_quiescence = lambda *args: (_ for _ in ()).throw(AssertionError('quiescence reached'))
for status in ('retiring', 'retired', 'scratch'):
    pair = pairs[0]
    marker = pair / (DATABASE_RETIREMENT_MARKER if status != 'scratch' else '.database-retirement.json.retiring-' + 'f' * 64 + '.pending')
    marker.write_bytes(b'{}')
    before = tree()
    try: restore_pair(read(pair / 'pair.json')['expected'], pair, pair.parent / 'advanced-state', {})
    except RuntimeError as error: assert str(error) == 'Recovery file inventory changed; restore held before state mutation.', str(error)
    else: raise AssertionError('released guard bypassed')
    assert tree() == before
    marker.unlink()
`),
);

NodeTest.test("DBR T2/T9: actual CLI uses existing read-only lock, exact flags and resume", () =>
  fixture(async (root) => {
    const helper = NodePath.join(root, "helper.py");
    const platformGuard =
      "\nif sys.platform != 'darwin': raise SystemExit('The Jones native helper requires macOS.')\n";
    NodeAssert.strict.equal(jonesNativeHelperSource.split(platformGuard).length, 2);
    await NodeFSP.writeFile(helper, jonesNativeHelperSource.replace(platformGuard, "\n"));
    NodeChildProcess.execFileSync("python3", ["-B", "-", root, helper], {
      input: `${definitions}\n${databaseSeed}\n` + String.raw`
helper = sys.argv[2]
base = [sys.executable, '-B', helper, '--manifest', str(manifest_path)]
args = [item for snapshot in selection for item in ('--snapshot', snapshot)]
def cli(action, extra=None):
    value = subprocess.run(base + [action] + args + (extra or []), capture_output=True, text=True, timeout=5)
    assert value.returncode == 0, value.stderr
    return json.loads(value.stdout)
before = tree()
assert cli('--inspect-database-retirement')['reason'] == 'busy'
assert tree() == before
lock.close(); lock_path.unlink()
for action, extra in (('--inspect-database-retirement', []), ('--retire-database-snapshots', ['--plan-sha256', '0' * 64])):
    before = tree()
    value = cli(action, extra); assert value['status'] == 'refused' and value['reason'] == 'activation-lock-absent', value
    assert tree() == before and not lock_path.exists()
for misuse in (['--inspect-database-retirement'], ['--inspect-database-retirement', '--snapshot', 'warm:bad'],
               ['--inspect-database-retirement'] + args + ['--plan-sha256', '0' * 64],
               ['--retire-database-snapshots'] + args,
               ['--inspect-database-retirement'] + args + ['--quiescent'],
               ['--inspect-install-retention', '--snapshot', selection[0]],
               ['--inspect-database-retirement'] + args + ['--snapshot', selection[0]]):
    value = subprocess.run(base + misuse, capture_output=True, text=True, timeout=5)
    assert value.returncode == 2, (misuse, value)
lock_path.symlink_to(manifest_path)
before = tree()
assert cli('--inspect-database-retirement')['reason'] == 'activation-lock-unknown'
assert tree() == before
lock_path.unlink()
lock_path.write_bytes(b'jones-activation-lock-v1\n')
before = tree(); value = cli('--inspect-database-retirement')
assert value['status'] == 'ready' and tree() == before, value
p = value['planSha256']
assert cli('--retire-database-snapshots', ['--plan-sha256', p])['status'] == 'retired'
before = tree()
assert cli('--retire-database-snapshots', ['--plan-sha256', p])['status'] == 'retired'
assert cli('--inspect-database-retirement')['status'] == 'retired'
assert tree() == before
`,
      encoding: "utf8",
      timeout: 20000,
    });
  }),
);
