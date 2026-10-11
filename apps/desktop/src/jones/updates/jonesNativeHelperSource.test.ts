// @effect-diagnostics nodeBuiltinImport:off - Synthetic helper filesystem fixture; no live native app is launched.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { afterEach, describe, expect, it } from "vite-plus/test";
import { jonesNativeHelperSource } from "./jonesNativeHelperSource.ts";
import { hashMacApp } from "./jonesMacStaging.ts";

const fixtures = new Set<string>();
async function cleanupFixture(root: string): Promise<void> {
  await NodeFSP.rm(root, { recursive: true, force: true });
  fixtures.delete(root);
}
afterEach(async () => {
  for (const root of fixtures) await cleanupFixture(root);
});

async function fixture() {
  const parent = NodePath.join(NodeOS.homedir(), ".cache", "jones-updater-test-fixtures");
  await NodeFSP.mkdir(parent, { recursive: true, mode: 0o700 });
  const directory = await NodeFSP.mkdtemp(NodePath.join(parent, "native-helper-"));
  fixtures.add(directory);
  return { directory, cleanup: () => cleanupFixture(directory) };
}

const activationScenario = String.raw`
root, fault = pathlib.Path(sys.argv[1]), sys.argv[2]
# Exercise the real allocation/release protocol with small fixture-only capacity.
RECOVERY_HEADROOM = 16384
home, profile = root / 'userdata', root / 'profile'
home.mkdir(); profile.mkdir()
database = home / 'state.sqlite'
with sqlite3.connect(database) as db:
    db.execute('CREATE TABLE identity (value TEXT)')
    db.execute("INSERT INTO identity VALUES ('previous-state')")
(home / 'settings.json').write_text('previous-settings')
(profile / 'opaque').write_text('same-host-profile')
previous_app, candidate_app = root / 'previous.app', root / 'candidate.app'
for app in (previous_app, candidate_app):
    (app / 'Contents' / 'Resources').mkdir(parents=True)
    (app / 'Contents' / 'MacOS').mkdir()
    (app / 'Contents' / 'Resources' / 'app.asar').write_text('synthetic-asar')
    (app / 'Contents' / 'MacOS' / 'Jones').write_text('previous-executable' if app == previous_app else 'candidate-executable')
expected = {'protocol': 1, 'owner': 'desktop', 'generation': 'previous', 'transactionId': 'bootstrap',
    'home': str(root), 'databasePath': str(database), 'profile': str(profile), 'environmentId': 'same-environment',
    'appPath': str(previous_app), 'executablePath': str(previous_app / 'Contents/MacOS/Jones'),
    'version': 'same-preview-version', 'sourceSha': 'a' * 40, 'sourceTree': 'b' * 40, 'appDigest': app_digest(previous_app)}
manifest_path = root / 'runtime' / 'jones-active-install.json'
durable(manifest_path, expected, True)
transaction_id = 'e' * 64
tx = root / 'runtime' / 'jones-updates' / 'transactions' / transaction_id
tx.mkdir(parents=True)
payload = root / 'native.dmg'; payload.write_text('synthetic-qualified-dmg')
(root / 'github-artifact.zip').write_text('synthetic-qualified-zip')
candidate = {'repository': 'Jones-Systems/Jones-Code', 'source': 'c' * 40, 'tree': 'd' * 40,
    'installedSource': expected['sourceSha'], 'artifactDigest': digest(root / 'github-artifact.zip')}
staged = {'handle': 'f' * 64, 'receiptPath': str(root / 'mac-app-receipt.json'),
    'appPath': str(candidate_app), 'executablePath': str(candidate_app / 'Contents/MacOS/Jones'),
    'version': expected['version'], 'sourceSha': candidate['source'], 'sourceTree': candidate['tree'],
    'appDigest': app_digest(candidate_app), 'asarDigest': digest(candidate_app / 'Contents/Resources/app.asar'),
    'executableDigest': digest(candidate_app / 'Contents/MacOS/Jones'), 'startupGateProtocol': 1}
if fault == 'candidate-gate-missing': staged.pop('startupGateProtocol')
if fault == 'candidate-gate-wrong': staged['startupGateProtocol'] = 2
if fault == 'candidate-gate-boolean': staged['startupGateProtocol'] = True
durable(staged['receiptPath'], {'app': staged, 'candidate': candidate,
    'artifact': {'candidate': candidate, 'payloadPath': str(payload), 'receipt': {'sha256': digest(payload)}}}, True)
staging = manifest_path.parent / 'jones-updates' / 'staging'; staging.mkdir()
selection_path = staging / (expected['sourceSha'] + '-' + 'e' * 64 + '.json')
selection = {'schema': 1, 'source': 'jones-actions', 'home': expected['home'], 'profile': expected['profile'],
             'currentVersion': expected['version'], 'installedSource': expected['sourceSha'], 'active': expected,
             'artifactDirectory': str(root / 'artifact'), 'app': staged}
durable(selection_path, selection, True)
selection_digest = digest(selection_path)
claim = preparation_claim(expected, staged['handle'], transaction_id, selection_path, selection_digest)
attempt_key = hashlib.sha256((str(selection_path) + '\n' + selection_digest).encode()).hexdigest()
attempt_path = manifest_path.parent / 'jones-updates' / 'attempt-selections' / (attempt_key + '.json')
durable(attempt_path, {'protocol': 1, 'transactionId': transaction_id, 'stagedHandle': staged['handle'], 'selectionPath': str(selection_path), 'selectionSha256': selection_digest, 'expected': expected}, True)
durable(tx / 'prepare-intent.json', claim, True)
durable(tx / 'prepare-dispatched.json', claim, True)
continuation = dict(claim, prepared=True)
durable(tx / 'continuation.json', continuation, True)
intent = {'protocol': 1, 'transactionId': transaction_id, 'expected': expected, 'staged': staged,
    'continuationReceipt': str(tx / 'continuation.json'), 'processes': [{'pid': 123, 'identity': 'owned-native-process'}],
    'listener': 'http://127.0.0.1:3777'}
durable(tx / 'intent.json', intent, True)
events = []
native_storage_preflight = storage_preflight
def synthetic_storage_preflight(active, candidate, transaction):
    events.append('storage-preflight')
    if fault == 'storage-refused': raise RuntimeError('Insufficient recovery capacity.')
    native_space = os.statvfs
    os.statvfs = lambda path: type('Space', (), {'f_bavail': 1 << 50, 'f_frsize': 1})()
    try: return native_storage_preflight(active, candidate, transaction)
    finally: os.statvfs = native_space
storage_preflight = synthetic_storage_preflight
if fault == 'reserve-denied':
    def reserve_recovery_capacity(*args): raise OSError('Synthetic ENOSPC before shutdown')
if fault == 'reserve-release-failed':
    def release_recovery_reserve(*args): raise OSError('Synthetic uncertain reserve release')
native_exclusion = acquire_writer_exclusion
def synthetic_exclusion(active):
    if fault == 'recovery-lease-busy' and 'candidate-launch' in events: raise RuntimeError('Synthetic competing writer')
    connections = native_exclusion(active)
    events.append('exclusive-writers')
    return connections
acquire_writer_exclusion = synthetic_exclusion
native_capacity = recovery_capacity_before_restore
def synthetic_capacity(active, pair):
    events.append('recovery-capacity')
    if fault != 'recovery-space-missing': return native_capacity(active, pair)
    native_space = os.statvfs
    os.statvfs = lambda path: type('Space', (), {'f_bavail': 0, 'f_frsize': 1})()
    try: return native_capacity(active, pair)
    finally: os.statvfs = native_space
recovery_capacity_before_restore = synthetic_capacity
if fault in ('bundle-denied', 'bundle-second-rename-denied'):
    native_rename = os.rename
    def denied_rename(source, target):
        source = pathlib.Path(source)
        if (fault == 'bundle-denied' and source == previous_app) or (fault == 'bundle-second-rename-denied' and '.jones-incoming-' in source.name):
            raise PermissionError('App Management denied bundle replacement')
        return native_rename(source, target)
    os.rename = denied_rename
native_stop = lambda proofs: events.append('stop:' + str(len(proofs)))
def synthetic_stop(proofs):
    native_stop(proofs)
    if 'candidate-launch' in events:
        assert (tx / 'recovery-reserve.bin').exists(), 'Bulk reserve released before candidate shutdown'
    if fault == 'state-grew': (profile / 'growth').write_bytes(bytes(RECOVERY_HEADROOM))
    for proof in proofs:
        if proof.get('pid') == 654: alive(proof)
    if fault == 'candidate-stop' and proofs and proofs[0]['pid'] == 456:
        raise RuntimeError('Writer quiescence could not be proved.')
stop_exact = synthetic_stop
prove_quiescence = lambda active: events.append('quiescence')
if fault == 'old-quiescence':
    def fail_quiescence(active): raise RuntimeError('Old native writers remain.')
    prove_quiescence = fail_quiescence
native_pair = pair_state
def synthetic_pair(active, directory):
    events.append('pair')
    if fault == 'snapshot-failed': raise RuntimeError('Profile snapshot did not complete.')
    return native_pair(active, directory)
pair_state = synthetic_pair
def synthetic_launch(active, descriptor=None):
    if descriptor is None:
        events.append('restart-previous')
        return {'pid': 789, 'identity': 'restored-native-app'}
    events.append('candidate-launch')
    assert (tx / 'previous-pair' / 'pair.json').exists()
    with sqlite3.connect(database) as db: db.execute("UPDATE identity SET value='advanced-state'")
    (home / 'settings.json').write_text('candidate-settings')
    (profile / 'opaque').write_text('advanced-profile')
    if fault == 'untracked-launch': raise RuntimeError('Candidate identity was not captured.')
    wanted = read(descriptor)
    receipt = {key: wanted[key] for key in ('protocol', 'startupGateProtocol', 'transactionId', 'stagedHandle', 'home', 'databasePath', 'profile', 'environmentId', 'version', 'sourceSha', 'sourceTree', 'listener')}
    receipt['resumeHeld'] = True
    receipt['backendProcess'] = {'pid': 654, 'identity': 'tracked-native-backend'}
    if fault == 'legacy-receipt':
        for key in ('protocol', 'startupGateProtocol', 'sourceTree'): receipt.pop(key)
    if fault == 'gate-mismatch': receipt['startupGateProtocol'] = 2
    if fault == 'gate-boolean': receipt['startupGateProtocol'] = True
    if fault == 'protocol-mismatch': receipt['protocol'] = 2
    if fault == 'tree-mismatch': receipt['sourceTree'] = 'wrong-tree'
    if fault == 'version-mismatch': receipt['version'] = 'wrong-version'
    if fault == 'listener-mismatch': receipt['listener'] = 'http://127.0.0.1:4777'
    if fault == 'invalid-backend': receipt['backendProcess'] = {'pid': True, 'identity': 'unqualified-process'}
    if fault.startswith('missing-'): receipt.pop(fault.removeprefix('missing-'))
    if fault in ('trial-mismatch', 'candidate-stop', 'resume-dispatched', 'reserve-release-failed', 'recovery-space-missing', 'recovery-lease-busy'): receipt['sourceSha'] = 'wrong-source'
    if fault == 'resume-dispatched': durable(tx / 'resume-dispatched.json', {'uncertain': True}, True)
    durable(tx / 'trial-receipt.json', receipt, True)
    return {'pid': 456, 'identity': 'tracked-native-app'}
launch = synthetic_launch
def synthetic_identity(pid):
    events.append('identity:' + str(pid))
    if pid == 654:
        if fault == 'dead-backend': return None
        if fault == 'changed-backend': return 'reused-backend-pid'
        return 'tracked-native-backend'
    if pid == 456: return 'tracked-native-app'
    return None
process_identity = synthetic_identity
process_start = lambda pid: ('S', '')
native_durable = durable
def synthetic_durable(destination, value, exclusive=False):
    destination = pathlib.Path(destination)
    if fault == 'resume-grant' and destination.name == 'commit-grant.json': raise RuntimeError('Dispatch outcome uncertain.')
    if destination == manifest_path and value['sourceSha'] == staged['sourceSha']:
        assert events[-1] == 'identity:654'
        events.append('manifest-commit')
    native_durable(destination, value, exclusive)
    if fault == 'commit-uncertain' and destination == manifest_path and value['sourceSha'] == staged['sourceSha']:
        raise RuntimeError('Commit readback interrupted.')
durable = synthetic_durable
if fault in ('interrupted-trial', 'interrupted-rollback', 'interrupted-resume'):
    phase = {'interrupted-trial': 'trial', 'interrupted-rollback': 'rollback-intent', 'interrupted-resume': 'resume-intent'}[fault]
    durable(tx / 'journal.json', {'intent': intent, 'phase': phase}, True)
if fault in ('stale-home', 'candidate-gate-missing', 'candidate-gate-wrong', 'candidate-gate-boolean'):
    if fault == 'stale-home':
        changed = dict(expected, home='wrong-home')
        durable(manifest_path, changed)
    try: activate(tx / 'intent.json')
    except RuntimeError: pass
    else: raise RuntimeError('Unqualified candidate or home was accepted.')
    assert events == []
    assert not (tx / 'previous-pair').exists()
    assert not (tx / 'trial-descriptor.json').exists()
    assert not (tx / 'commit-grant.json').exists()
    if fault != 'stale-home': assert read(manifest_path) == expected
    print(json.dumps({'phase': 'refused', 'events': events}))
else:
    activate(tx / 'intent.json')
    journal = read(tx / 'journal.json')
    with sqlite3.connect(database) as db: state = db.execute('SELECT value FROM identity').fetchone()[0]
    if fault in ('storage-refused', 'reserve-denied'):
        assert events == ['storage-preflight']
        assert state == 'previous-state'
        assert read(manifest_path) == expected
        assert not (tx / 'previous-pair').exists()
        assert not (tx / 'trial-descriptor.json').exists()
        assert previous_app.joinpath('Contents/MacOS/Jones').read_text() == 'previous-executable'
    if fault == 'success':
        activate(tx / 'intent.json')
        assert events.count('candidate-launch') == 1
        descriptor = read(tx / 'trial-descriptor.json')
        grant = read(tx / 'commit-grant.json')
        fields = ('protocol', 'startupGateProtocol', 'transactionId', 'stagedHandle', 'home', 'databasePath', 'profile', 'environmentId', 'version', 'sourceSha', 'sourceTree', 'listener')
        assert descriptor['startupGateProtocol'] == 1
        assert grant == dict(generation=descriptor['transactionId'], **{key: descriptor[key] for key in fields})
        assert read(manifest_path)['sourceSha'] == staged['sourceSha']
        assert read(manifest_path)['appPath'] == expected['appPath']
        assert pathlib.Path(read(manifest_path)['executablePath']).read_text() == 'candidate-executable'
        assert pathlib.Path(journal['previousBundle']).joinpath('Contents/MacOS/Jones').read_text() == 'previous-executable'
        reserve = journal['recoveryReserves'][0]
        assert pathlib.Path(reserve['path']).stat().st_blocks * 512 >= reserve['bytes']
    if fault.startswith('missing-') or fault in ('legacy-receipt', 'gate-mismatch', 'gate-boolean', 'protocol-mismatch', 'tree-mismatch', 'version-mismatch', 'listener-mismatch', 'invalid-backend', 'dead-backend', 'changed-backend'):
        assert read(manifest_path) == expected
        assert not (tx / 'commit-grant.json').exists()
        assert 'manifest-commit' not in events
    if fault == 'dead-backend':
        assert 'stop:2' in events
        assert events.count('quiescence') >= 3
    if fault in ('invalid-backend', 'missing-backendProcess'):
        assert 'identity:654' not in events
        assert 'identity:True' not in events
        assert 'stop:2' not in events
    if journal['phase'] == 'rolled-back':
        assert state == 'previous-state'
        assert (home / 'settings.json').read_text() == 'previous-settings'
        assert (profile / 'opaque').read_text() == 'same-host-profile'
        assert previous_app.joinpath('Contents/MacOS/Jones').read_text() == 'previous-executable'
        if fault not in ('snapshot-failed', 'state-grew'):
            with sqlite3.connect(tx / 'advanced-state' / 'state.sqlite') as db:
                assert db.execute('SELECT value FROM identity').fetchone()[0] == ('previous-state' if fault.startswith('bundle-') else 'advanced-state')
        else:
            assert 'candidate-launch' not in events
    if journal['phase'] == 'blocked' and 'candidate-launch' in events:
        assert state == 'advanced-state'
        assert (tx / 'previous-pair' / 'state.sqlite').exists()
        assert 'restart-previous' not in events
        if fault != 'recovery-space-missing': assert (tx / 'recovery-reserve.bin').exists()
    if fault == 'recovery-space-missing':
        assert not (tx / 'advanced-state').exists()
        assert 'available 0 bytes' in journal['message'] and 'needed ' in journal['message']
        assert events[-1] == 'recovery-capacity'
        assert events.index('stop:1', events.index('candidate-launch') + 1) > events.index('candidate-launch')
        assert events[-2] == 'exclusive-writers'
    if journal['phase'] == 'rolled-back': assert not (tx / 'recovery-reserve.bin').exists()
    print(json.dumps({'phase': journal['phase'], 'events': events, 'state': state}))
`;

describe("Jones native helper", () => {
  it.each([
    "success", "internal-symlink", "wrong-plan", "active", "parent", "pending", "stage", "unknown-journal",
    "symlink-target", "changed-inode", "foreign-hardlink", "reference", "writer-busy",
    "partial", "partial-new-file", "partial-replacement", "lost-reply", "reserve", "reserve-owner",
  ])("retires only the inspected obsolete payload for %s", async (fault) => {
    const f = await fixture();
    try {
      const nativeFunctions = jonesNativeHelperSource.split("\nparser = argparse.ArgumentParser()")[0];
      const runtime = activationScenario.split("\nif fault in ('interrupted-trial'")[0];
      const scenario = String.raw`
retirement_fault = sys.argv[3]
activate(tx / 'intent.json')
assert read(tx / 'journal.json')['phase'] == 'rolled-back'
acquire_writer_exclusion = native_exclusion
if retirement_fault != 'stage': selection_path.unlink()
journal = read(tx / 'journal.json')
if retirement_fault in ('active', 'parent'):
    active = dict(expected, generation=transaction_id, transactionId=transaction_id)
    if retirement_fault == 'parent':
        next_id = '9' * 64
        next_tx = tx.parent / next_id
        next_intent = dict(intent, transactionId=next_id, expected=active)
        durable(next_tx / 'intent.json', next_intent, True)
        durable(next_tx / 'journal.json', {'intent': next_intent, 'phase': 'resumed'}, True)
        active = dict(expected, generation=next_id, transactionId=next_id)
    durable(manifest_path, active)
else: active = expected
if retirement_fault in ('pending', 'unknown-journal'):
    durable(tx / 'journal.json', dict(journal, phase='trial' if retirement_fault == 'pending' else 'unknown'))
if retirement_fault == 'symlink-target':
    payload = tx / 'previous-pair' / 'profile'
    payload.rename(payload.with_name('profile-retained'))
    payload.symlink_to(profile)
if retirement_fault == 'foreign-hardlink': os.link(tx / 'previous-pair' / 'state.sqlite', root / 'foreign-link')
if retirement_fault == 'internal-symlink':
    (root / 'unrelated-payload').write_text('must survive')
    (tx / 'previous-pair' / 'profile' / 'external-link').symlink_to(root / 'unrelated-payload')
if retirement_fault == 'reference':
    shared = pathlib.Path(journal['previousBundle'])
    shutil.copytree(previous_app, shared)
    other_tx = tx.parent / ('9' * 64)
    other_intent = dict(intent, transactionId=other_tx.name)
    durable(other_tx / 'intent.json', other_intent, True)
    durable(other_tx / 'journal.json', {'intent': other_intent, 'phase': 'rolled-back', 'previousBundle': str(shared)}, True)
if retirement_fault in ('reserve', 'reserve-owner'):
    reserve = allocate_recovery_file(tx / 'recovery-reserve.bin', 16384)
    journal['recoveryReserves'] = [reserve]
    durable(tx / 'journal.json', journal)
    if retirement_fault == 'reserve-owner':
        pathlib.Path(reserve['path']).rename(root / 'original-reserve')
        pathlib.Path(reserve['path']).write_bytes(bytes(16384))
metadata = [path for path in tx.rglob('*.json') if path.name not in SETTINGS]
before = {str(path): path.read_bytes() for path in metadata}
inspect = retirement_command('inspect-retirement', active, transaction_id)
refusals = ('active', 'parent', 'pending', 'stage', 'unknown-journal', 'symlink-target', 'foreign-hardlink', 'reference', 'reserve-owner')
if retirement_fault in refusals:
    assert inspect['status'] == 'refused', inspect
    assert not (tx / 'retirement-intent.json').exists()
else:
    assert inspect['status'] == 'ready' and inspect['targets'], inspect
    plan_sha = inspect['planSha256']
    if retirement_fault == 'wrong-plan': plan_sha = '0' * 64
    if retirement_fault == 'changed-inode':
        payload = pathlib.Path(inspect['targets'][0]['path'])
        payload.rename(root / 'replaced-payload')
        payload.write_bytes(b'changed')
    reader = None
    if retirement_fault == 'writer-busy':
        reader = sqlite3.connect(native_writer_lease_paths(active)[0][0], isolation_level=None)
        reader.execute('BEGIN'); reader.execute('SELECT scope FROM jones_native_writer_lease').fetchall()
    native_remove = remove_retirement_payload
    calls = []
    def observed_remove(target):
        calls.append(target['path'])
        native_remove(target)
        if retirement_fault.startswith('partial') and len(calls) == 1: raise OSError('Synthetic interruption after one approved payload')
    remove_retirement_payload = observed_remove
    original_durable = durable
    if retirement_fault == 'lost-reply':
        def durable(path, value, exclusive=False):
            original_durable(path, value, exclusive)
            if pathlib.Path(path).name == 'retirement-receipt.json': raise OSError('Synthetic completion response lost')
    try: result = retirement_command('retire-transaction', active, transaction_id, plan_sha)
    finally:
        if reader is not None: reader.close()
    if retirement_fault in ('wrong-plan', 'changed-inode', 'writer-busy'):
        assert result['status'] == 'refused' and calls == [], result
        assert not (tx / 'retirement-intent.json').exists()
    elif retirement_fault.startswith('partial'):
        assert result['status'] == 'uncertain', result
        try: inspect_selection_transactions(active)
        except SelectionRefused as error: assert error.reason == 'activation-pending'
        else: raise AssertionError('Unreconciled retirement admitted activation')
        if retirement_fault == 'partial-new-file': (tx / 'previous-pair' / 'profile' / 'new-file').write_text('unapproved')
        if retirement_fault == 'partial-replacement':
            payload = tx / 'previous-pair' / 'profile' / 'opaque'
            payload.rename(root / 'original-profile-payload'); payload.write_text('replacement')
        resume = retirement_command('inspect-retirement', active, transaction_id)
        if retirement_fault == 'partial':
            assert resume['status'] == 'ready' and resume['reconciliation'] is True, resume
            assert resume['planSha256'] == plan_sha
            result = retirement_command('retire-transaction', active, transaction_id, plan_sha)
            assert result['status'] == 'retired', result
        else:
            assert resume['status'] == 'uncertain', resume
            prior_calls = len(calls)
            assert retirement_command('retire-transaction', active, transaction_id, plan_sha)['status'] == 'uncertain'
            assert len(calls) == prior_calls
    elif retirement_fault == 'lost-reply':
        assert result['status'] == 'uncertain', result
        durable = original_durable
        count = len(calls)
        result = retirement_command('retire-transaction', active, transaction_id, plan_sha)
        assert result['status'] == 'retired' and len(calls) == count, result
    else:
        assert result['status'] == 'retired', result
        count = len(calls)
        assert retirement_command('retire-transaction', active, transaction_id, plan_sha)['status'] == 'retired'
        assert len(calls) == count
    if result['status'] == 'retired':
        assert all(not pathlib.Path(target['path']).exists() for target in inspect['targets'])
        assert retirement_command('inspect-retirement', active, transaction_id)['status'] == 'retired'
assert {str(path): path.read_bytes() for path in metadata} == before
assert database.exists() and (profile / 'opaque').read_text() == 'same-host-profile'
assert previous_app.joinpath('Contents/MacOS/Jones').read_text() == 'previous-executable'
assert read(staged['receiptPath'])['app'] == staged and candidate_app.exists()
if retirement_fault == 'internal-symlink': assert (root / 'unrelated-payload').read_text() == 'must survive'
`;
      NodeChildProcess.execFileSync("python3", ["-c", `${nativeFunctions}\n${runtime}\n${scenario}`, f.directory, "trial-mismatch", fault], {
        encoding: "utf8", timeout: 10000, maxBuffer: 1024 * 1024,
      });
    } finally {
      await f.cleanup();
    }
  });

  it("takes a fresh SQLite and profile snapshot on a second attempt while preserving the rolled-back attempt", async () => {
    const f = await fixture();
    try {
      const nativeFunctions = jonesNativeHelperSource.split("\nparser = argparse.ArgumentParser()")[0];
      const runtime = activationScenario.split("\nif fault in ('interrupted-trial'")[0];
      const scenario = String.raw`
activate(tx / 'intent.json')
assert read(tx / 'journal.json')['phase'] == 'rolled-back'
first = tx
retained = {str(path): digest(path) for path in first.rglob('*') if path.is_file()}
with sqlite3.connect(database) as db: db.execute("UPDATE identity SET value='intervening-state'")
(home / 'settings.json').write_text('intervening-settings')
(profile / 'opaque').write_text('intervening-profile')
transaction_id = 'c' * 64
tx = first.parent / transaction_id
selection_path = selection_path.with_name(expected['sourceSha'] + '-' + transaction_id + '.json')
durable(selection_path, selection, True)
selection_digest = digest(selection_path)
attempt_key = hashlib.sha256((str(selection_path) + '\n' + selection_digest).encode()).hexdigest()
durable(attempt_path.parent / (attempt_key + '.json'), {'protocol': 1, 'transactionId': transaction_id, 'stagedHandle': staged['handle'], 'selectionPath': str(selection_path), 'selectionSha256': selection_digest, 'expected': expected}, True)
result = selection_command('claim-preparation', expected, staged['handle'], str(selection_path), selection_digest, transaction_id=transaction_id)
assert result['status'] == 'preparation-claimed', result
claim = read(tx / 'prepare-intent.json')
durable(tx / 'prepare-dispatched.json', claim, True)
durable(tx / 'continuation.json', dict(claim, prepared=True), True)
intent = dict(intent, transactionId=transaction_id, continuationReceipt=str(tx / 'continuation.json'))
durable(tx / 'activation-request.json', intent, True)
result = selection_command('claim-activation', expected, staged['handle'], str(selection_path), selection_digest, str(tx / 'activation-request.json'), transaction_id)
assert result['status'] == 'claimed', result
events.clear()
fault = 'success'
activate(tx / 'intent.json')
assert read(tx / 'journal.json')['phase'] == 'resumed'
with sqlite3.connect(tx / 'previous-pair' / 'state.sqlite') as db:
    assert db.execute('SELECT value FROM identity').fetchone()[0] == 'intervening-state'
assert (tx / 'previous-pair' / 'settings.json').read_text() == 'intervening-settings'
assert (tx / 'previous-pair' / 'profile' / 'opaque').read_text() == 'intervening-profile'
assert {str(path): digest(path) for path in first.rglob('*') if path.is_file()} == retained
assert read(staged['receiptPath'])['app'] == staged
`;
      NodeChildProcess.execFileSync("python3", ["-c", `${nativeFunctions}\n${runtime}\n${scenario}`, f.directory, "trial-mismatch"], {
        encoding: "utf8", timeout: 10000, maxBuffer: 1024 * 1024,
      });
    } finally {
      await f.cleanup();
    }
  });

  it.each(["rolled-back", "resumed", "blocked", "wrong-active", "occupied-attempt", "lost-reply"])(
    "binds a fresh transaction to the immutable stage after %s",
    async (fault) => {
      const f = await fixture();
      try {
        const nativeFunctions = jonesNativeHelperSource.split(
          "\nparser = argparse.ArgumentParser()",
        )[0];
        const setup = activationScenario.split("\nevents = []")[0];
        const scenario = String.raw`
prior_intent = intent if fault != 'wrong-active' else dict(intent, expected=dict(expected, sourceSha='0' * 40))
durable(tx / 'intent.json', prior_intent)
prior_journal = {'intent': prior_intent, 'phase': fault if fault in ('resumed', 'blocked') else 'rolled-back'}
durable(tx / 'journal.json', prior_journal, True)
(tx / 'retained-evidence').write_text('previous attempt retained')
new_transaction = transaction_id if fault == 'occupied-attempt' else 'c' * 64
new_selection = selection_path.with_name(expected['sourceSha'] + '-' + 'd' * 64 + '.json')
durable(new_selection, selection, True)
new_digest = digest(new_selection)
key = hashlib.sha256((str(new_selection) + '\n' + new_digest).encode()).hexdigest()
attempt_record = {'protocol': 1, 'transactionId': new_transaction, 'stagedHandle': staged['handle'], 'selectionPath': str(new_selection), 'selectionSha256': new_digest, 'expected': expected}
durable(attempt_path.parent / (key + '.json'), attempt_record, True)
def retry(): return selection_command('claim-preparation', expected, staged['handle'], str(new_selection), new_digest, transaction_id=new_transaction)
result = retry()
if fault in ('rolled-back', 'lost-reply'):
    assert result['status'] == 'preparation-claimed' and result['transactionId'] != staged['handle'], result
    new_claim = read(tx.parent / new_transaction / 'prepare-intent.json')
    assert new_claim['transactionId'] == new_transaction and new_claim['stagedHandle'] == staged['handle']
    if fault == 'lost-reply':
        assert retry()['reason'] == 'activation-pending'
        assert read(attempt_path.parent / (key + '.json')) == attempt_record
else:
    assert result['status'] == 'refused', result
    if new_transaction != transaction_id: assert not (tx.parent / new_transaction).exists()
assert read(tx / 'journal.json') == prior_journal
assert (tx / 'retained-evidence').read_text() == 'previous attempt retained'
assert read(staged['receiptPath'])['app']['handle'] == staged['handle']
`;
        NodeChildProcess.execFileSync(
          "python3",
          ["-c", `${nativeFunctions}\n${setup}\n${scenario}`, f.directory, fault],
          {
            encoding: "utf8",
            timeout: 10000,
            maxBuffer: 1024 * 1024,
          },
        );
      } finally {
        await f.cleanup();
      }
    },
  );

  it("physically reserves rollback capacity, preserves changed ownership, and removes only its failed allocation", async () => {
    const f = await fixture();
    try {
      const nativeFunctions = jonesNativeHelperSource.split(
        "\nparser = argparse.ArgumentParser()",
      )[0];
      const scenario = String.raw`
root = pathlib.Path(sys.argv[1])
path = root / 'reserve.bin'
reserve = allocate_recovery_file(path, 16384)
assert path.stat().st_size == 16384 and path.stat().st_blocks * 512 >= 16384
assert reserve['inode'] == str(path.stat().st_ino)
try: allocate_recovery_file(path, 16384)
except FileExistsError: pass
else: raise AssertionError('Occupied reserve overwritten')
retained = root / 'retained.bin'; path.rename(retained); path.write_bytes(b'foreign')
try: release_recovery_reserve([reserve])
except RuntimeError as error: assert 'ownership changed' in str(error)
else: raise AssertionError('Foreign reserve deleted')
assert path.read_bytes() == b'foreign' and retained.stat().st_size == 16384
path.unlink(); retained.rename(path)
reserves = [reserve]; release_recovery_reserve(reserves)
assert reserves == [] and not path.exists()
original_fcntl = fcntl.fcntl
original_platform = sys.platform
sys.platform = 'darwin'
def fail_allocation(descriptor, operation, argument):
    os.write(descriptor, bytes(4096))
    raise OSError(28, 'Synthetic exhausted fixture volume')
fcntl.fcntl = fail_allocation
sibling = root / 'unrelated'; sibling.write_text('keep')
try:
    try: allocate_recovery_file(path, 16384)
    except OSError as error: assert error.errno == 28
    else: raise AssertionError('Partial allocation accepted')
finally:
    fcntl.fcntl = original_fcntl
    sys.platform = original_platform
assert not path.exists() and sibling.read_text() == 'keep'
`;
      NodeChildProcess.execFileSync(
        "python3",
        ["-c", `${nativeFunctions}\n${scenario}`, f.directory],
        {
          encoding: "utf8",
          timeout: 10000,
          maxBuffer: 1024 * 1024,
        },
      );
    } finally {
      await f.cleanup();
    }
  });
  it.each([
    "claim-first",
    "discard-first",
    "existing-claim-mismatch",
    "legacy-backend",
    "claim-sync-failed",
  ])("serializes preparation admission for %s", async (fault) => {
    const f = await fixture();
    try {
      const nativeFunctions = jonesNativeHelperSource.split(
        "\nparser = argparse.ArgumentParser()",
      )[0];
      const setup = activationScenario.split("\nevents = []")[0];
      const scenario = String.raw`

request = tx / 'activation-request.json'; os.rename(tx / 'intent.json', request)
for name in ('prepare-intent.json', 'prepare-dispatched.json', 'continuation.json'): (tx / name).unlink()
def run(operation, selection_hash=selection_digest):
    return selection_command(operation, expected, staged['handle'], str(selection_path), selection_hash, str(request), transaction_id if operation != 'discard-staged' else None)
if fault == 'discard-first':
    assert run('discard-staged')['status'] == 'discarded'
    assert run('claim-preparation') == {'protocol': 1, 'operation': 'claim-preparation', 'handle': staged['handle'], 'transactionId': transaction_id, 'status': 'refused', 'reason': 'selection-mismatch'}
    assert not (tx / 'prepare-intent.json').exists()
else:
    if fault == 'claim-sync-failed':
        original_durable = durable
        def durable(path, value, exclusive=False):
            original_durable(path, value, exclusive)
            if pathlib.Path(path).name == 'prepare-intent.json': raise OSError('Claim publication sync outcome unknown')
    result = run('claim-preparation')
    assert result['status'] == ('uncertain' if fault == 'claim-sync-failed' else 'preparation-claimed'), result
    assert read(tx / 'prepare-intent.json') == claim
    assert run('discard-staged')['reason'] == 'activation-pending'
    assert run('claim-preparation')['reason'] == 'activation-pending'
    if fault == 'existing-claim-mismatch':
        assert run('claim-preparation', '0' * 64)['reason'] == 'activation-pending'
    if fault == 'legacy-backend':
        legacy = {key: continuation[key] for key in ('protocol', 'transactionId', 'home', 'databasePath', 'profile', 'environmentId', 'prepared')}
        durable(tx / 'continuation.json', legacy, True)
        assert run('claim-activation')['status'] == 'refused'
        durable(tx / 'intent.json', intent, True)
        try: activate(tx / 'intent.json')
        except RuntimeError as error: assert 'not prepared' in str(error)
        else: raise AssertionError('Legacy backend preparation qualified')
        assert not (tx / 'journal.json').exists()
assert (profile / 'opaque').read_text() == 'same-host-profile'
`;
      NodeChildProcess.execFileSync(
        "python3",
        ["-c", `${nativeFunctions}\n${setup}\n${scenario}`, f.directory, fault],
        {
          encoding: "utf8",
          timeout: 10000,
          maxBuffer: 1024 * 1024,
        },
      );
    } finally {
      await f.cleanup();
    }
  });

  it("holds the actual activation flock across the preparation scan and durable claim", async () => {
    const f = await fixture();
    try {
      const [nativeFunctions, parser] = jonesNativeHelperSource.split(
        "\nparser = argparse.ArgumentParser()",
      );
      const pause = String.raw`
sys.platform = 'darwin'
bundle_identity = lambda active: None
original_durable = durable
def durable(path, value, exclusive=False):
    if pathlib.Path(path).name == 'prepare-intent.json':
        root = pathlib.Path(__file__).parent
        (root / 'claim-entered').write_text('entered')
        deadline = time.monotonic() + 5
        while not (root / 'claim-release').exists():
            if time.monotonic() >= deadline: raise RuntimeError('Fixture release timeout')
            time.sleep(0.01)
    return original_durable(path, value, exclusive)
`;
      await NodeFSP.writeFile(
        NodePath.join(f.directory, "interleaved-helper.py"),
        `${nativeFunctions}\n${pause}\nparser = argparse.ArgumentParser()${parser}`,
      );
      const setup = activationScenario.split("\nevents = []")[0];
      const scenario = String.raw`
for name in ('intent.json', 'prepare-intent.json', 'prepare-dispatched.json', 'continuation.json'): (tx / name).unlink()
command = ['python3', str(root / 'interleaved-helper.py'), '--manifest', str(manifest_path),
           '--selection', str(selection_path), '--selection-sha256', selection_digest]
child = subprocess.Popen(command + ['--claim-preparation', staged['handle'], '--transaction', transaction_id], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
try:
    deadline = time.monotonic() + 5
    while not (root / 'claim-entered').exists():
        if child.poll() is not None: raise AssertionError(child.communicate())
        if time.monotonic() >= deadline: raise AssertionError('Claim did not enter fixture barrier')
        time.sleep(0.01)
    competing = subprocess.run(command + ['--discard-staged', staged['handle']], capture_output=True, text=True, timeout=5)
    assert competing.returncode == 0, competing.stderr
    assert json.loads(competing.stdout)['reason'] == 'busy'
    assert selection_path.exists() and not (tx / 'prepare-intent.json').exists()
    (root / 'claim-release').write_text('released')
    output, error = child.communicate(timeout=5)
    assert child.returncode == 0, error
    assert json.loads(output)['status'] == 'preparation-claimed'
    after = subprocess.run(command + ['--discard-staged', staged['handle']], capture_output=True, text=True, timeout=5)
    assert json.loads(after.stdout)['reason'] == 'activation-pending'
    assert read(tx / 'prepare-intent.json') == claim and selection_path.exists()
finally:
    if child.poll() is None: child.kill()
    child.wait(timeout=5)
`;
      NodeChildProcess.execFileSync(
        "python3",
        ["-c", `${nativeFunctions}\n${setup}\n${scenario}`, f.directory, "interleaved"],
        {
          encoding: "utf8",
          timeout: 15000,
          maxBuffer: 1024 * 1024,
        },
      );
    } finally {
      await f.cleanup();
    }
  });

  it("holds exclusive state/profile leases and refuses readers, changed inodes, or WAL mode", async () => {
    const f = await fixture();
    try {
      const nativeFunctions = jonesNativeHelperSource.split(
        "\nparser = argparse.ArgumentParser()",
      )[0];
      const scenario = String.raw`
root = pathlib.Path(sys.argv[1])
profile = root / 'profile'; profile.mkdir()
expected = {'home': str(root), 'profile': str(profile)}
connections = acquire_writer_exclusion(expected)
paths = native_writer_lease_paths(expected)
try:
    for path, scope in paths:
        outsider = sqlite3.connect(path, timeout=0, isolation_level=None)
        try:
            try: outsider.execute('SELECT scope FROM jones_native_writer_lease').fetchall()
            except sqlite3.OperationalError as error: assert 'locked' in str(error)
            else: raise AssertionError('An exclusive native copy admitted a reader')
        finally: outsider.close()
finally: release_writer_exclusion(connections)
path, scope = paths[0]
reader = sqlite3.connect(path, isolation_level=None)
reader.execute('BEGIN'); reader.execute('SELECT scope FROM jones_native_writer_lease').fetchall()
try:
    try: acquire_writer_exclusion(expected)
    except sqlite3.OperationalError as error: assert 'locked' in str(error)
    else: raise AssertionError('A participating writer was ignored')
finally: reader.close()
connections = acquire_writer_exclusion(expected); release_writer_exclusion(connections)
connection = sqlite3.connect(path, isolation_level=None)
connection.execute('PRAGMA journal_mode=WAL'); connection.close()
try: acquire_writer_exclusion(expected)
except RuntimeError as error: assert 'rollback journal mode' in str(error)
else: raise AssertionError('WAL lease was accepted')
connection = sqlite3.connect(path, isolation_level=None)
connection.execute('PRAGMA journal_mode=DELETE'); connection.close()
old = path.with_name(path.name + '.retained')
path.rename(old); shutil.copy2(old, path)
try: acquire_writer_exclusion(expected)
except RuntimeError as error: assert 'inode changed' in str(error)
else: raise AssertionError('Replaced lease inode was accepted')
assert old.exists()
`;
      NodeChildProcess.execFileSync(
        "python3",
        ["-c", `${nativeFunctions}\n${scenario}`, f.directory],
        {
          encoding: "utf8",
          timeout: 10000,
          maxBuffer: 1024 * 1024,
        },
      );
    } finally {
      await f.cleanup();
    }
  });

  it.each([
    "discard",
    "claim",
    "discard-before-claim",
    "wrong-selection",
    "wrong-request",
    "wrong-candidate",
    "wrong-continuation",
    "associated-preparation",
    "other-pending",
    "other-terminal",
    "unknown-journal",
    "discard-sync-failed",
    "claim-sync-failed",
  ])("serializes the %s selection outcome without deleting retained artifacts", async (fault) => {
    const f = await fixture();
    try {
      const nativeFunctions = jonesNativeHelperSource.split(
        "\nparser = argparse.ArgumentParser()",
      )[0];
      const setup = activationScenario.split("\nevents = []")[0];
      const scenario = String.raw`
request = tx / 'activation-request.json'
os.rename(tx / 'intent.json', request)
def run(operation):
    return selection_command(operation, expected, staged['handle'], str(selection_path), selection_digest, str(request), transaction_id if operation != 'discard-staged' else None)
claiming = fault in ('claim', 'discard-before-claim', 'wrong-request', 'wrong-candidate', 'wrong-continuation', 'claim-sync-failed')
if not claiming or fault == 'discard-before-claim':
    for name in ('prepare-intent.json', 'prepare-dispatched.json', 'continuation.json'): (tx / name).unlink()
if fault == 'wrong-selection': selection_digest = '0' * 64
if fault == 'wrong-request':
    request = tx / 'other-request.json'; durable(request, intent, True)
if fault == 'wrong-candidate': durable(request, dict(intent, staged=dict(staged, sourceSha='0' * 40)))
if fault == 'wrong-continuation': durable(tx / 'continuation.json', dict(continuation, environmentId='wrong'))
if fault == 'associated-preparation': durable(tx / 'prepare-intent.json', claim, True)
if fault in ('other-pending', 'other-terminal', 'unknown-journal'):
    other = tx.parent / ('d' * 64); other.mkdir()
    prior = dict(intent, transactionId=other.name)
    durable(other / 'intent.json', prior, True)
    if fault == 'other-terminal': durable(other / 'journal.json', {'intent': prior, 'phase': 'rolled-back'}, True)
    if fault == 'unknown-journal': durable(other / 'journal.json', {'intent': prior, 'phase': 'unknown'}, True)
if fault == 'discard-before-claim':
    assert run('discard-staged')['status'] == 'discarded'
    durable(tx / 'prepare-intent.json', claim, True)
if fault == 'discard-sync-failed':
    def sync_parent(path): raise OSError('Synthetic parent sync failure after unlink')
if fault == 'claim-sync-failed':
    original_durable = durable
    def durable(path, value, exclusive=False):
        original_durable(path, value, exclusive)
        if pathlib.Path(path).name == 'intent.json': raise OSError('Synthetic claim sync failure after link')
outcome = run('claim-activation' if claiming else 'discard-staged')
if fault in ('discard', 'other-terminal'):
    assert outcome['status'] == 'discarded', outcome
    assert not selection_path.exists()
elif fault == 'claim':
    assert outcome == {'protocol': 1, 'operation': 'claim-activation', 'handle': staged['handle'], 'transactionId': transaction_id, 'status': 'claimed', 'intentPath': str(tx / 'intent.json')}
    assert read(tx / 'intent.json') == intent
    assert run('claim-activation')['reason'] == 'activation-pending'
    assert run('discard-staged')['reason'] == 'activation-pending'
    assert selection_path.exists()
elif fault in ('discard-sync-failed', 'claim-sync-failed'):
    assert outcome['status'] == 'uncertain', outcome
    assert (tx / 'intent.json').exists() == (fault == 'claim-sync-failed')
    assert selection_path.exists() == (fault == 'claim-sync-failed')
else:
    assert outcome['status'] == 'refused', outcome
    assert not (tx / 'intent.json').exists()
    assert selection_path.exists() == (fault != 'discard-before-claim')
assert (candidate_app / 'Contents/MacOS/Jones').read_text() == 'candidate-executable'
assert (previous_app / 'Contents/MacOS/Jones').read_text() == 'previous-executable'
assert payload.read_text() == 'synthetic-qualified-dmg'
assert (profile / 'opaque').read_text() == 'same-host-profile'
with sqlite3.connect(database) as db: assert db.execute('SELECT value FROM identity').fetchone()[0] == 'previous-state'
`;
      NodeChildProcess.execFileSync(
        "python3",
        ["-c", `${nativeFunctions}\n${setup}\n${scenario}`, f.directory, fault],
        { encoding: "utf8", maxBuffer: 1024 * 1024, timeout: 10000 },
      );
    } finally {
      await f.cleanup();
    }
  });

  it("refuses a selection command while the actual activation lock is owned", async () => {
    const f = await fixture();
    try {
      const [nativeFunctions, parser] = jonesNativeHelperSource.split(
        "\nparser = argparse.ArgumentParser()",
      );
      const helper = NodePath.join(f.directory, "synthetic-helper.py");
      await NodeFSP.writeFile(
        helper,
        `${nativeFunctions}\nsys.platform = 'darwin'\nbundle_identity = lambda active: None\nparser = argparse.ArgumentParser()${parser}`,
      );
      const scenario = String.raw`
root = pathlib.Path(sys.argv[1])
manifest = root / 'manifest.json'; manifest.write_text(json.dumps({'protocol': 1, 'owner': 'desktop'}))
lock_path = root / 'jones-activation.lock'
lock_path.write_text('jones-activation-lock-v1\n')
with open(lock_path, 'r+') as lock:
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    result = subprocess.run(['python3', str(root / 'synthetic-helper.py'), '--manifest', str(manifest), '--discard-staged', 'f' * 64,
                             '--selection', str(root / 'selection.json'), '--selection-sha256', 'e' * 64], capture_output=True, text=True, timeout=5)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {'protocol': 1, 'operation': 'discard-staged', 'handle': 'f' * 64, 'status': 'refused', 'reason': 'busy'}
    for operation in ('inspect-retirement', 'retire-transaction'):
        result = subprocess.run(['python3', str(root / 'synthetic-helper.py'), '--manifest', str(manifest), '--' + operation, 'e' * 64,
                                 '--plan-sha256', 'd' * 64], capture_output=True, text=True, timeout=5)
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) == {'protocol': 1, 'operation': operation, 'transactionId': 'e' * 64, 'status': 'refused', 'reason': 'busy'}
assert sorted(path.name for path in root.iterdir()) == ['jones-activation.lock', 'manifest.json', 'synthetic-helper.py']
`;
      NodeChildProcess.execFileSync(
        "python3",
        ["-c", `${nativeFunctions}\n${scenario}`, f.directory],
        {
          encoding: "utf8",
          maxBuffer: 1024 * 1024,
          timeout: 10000,
        },
      );
    } finally {
      await f.cleanup();
    }
  });

  it.each(["success", "failure", "cancel"])(
    "removes only its captured synthetic fixture after %s",
    async (outcome) => {
      const sibling = await fixture();
      let owned: string | undefined;
      const failure = new Error("synthetic fixture failed");
      const signal = new AbortController();
      try {
        const run = async () => {
          const f = await fixture();
          owned = f.directory;
          try {
            await NodeFSP.writeFile(NodePath.join(f.directory, "synthetic-state"), "fixture");
            if (outcome === "failure") throw failure;
            if (outcome === "cancel") {
              signal.abort(failure);
              signal.signal.throwIfAborted();
            }
          } finally {
            await f.cleanup();
          }
        };
        if (outcome === "success") await run();
        else await expect(run()).rejects.toBe(failure);
        expect(owned).not.toBe(sibling.directory);
        await expect(NodeFSP.lstat(owned!)).rejects.toMatchObject({ code: "ENOENT" });
        expect((await NodeFSP.lstat(sibling.directory)).isDirectory()).toBe(true);
      } finally {
        await sibling.cleanup();
      }
    },
  );
  it.each([
    ["success", "resumed"],
    ["trial-mismatch", "rolled-back"],
    ["bundle-denied", "rolled-back"],
    ["bundle-second-rename-denied", "rolled-back"],
    ["legacy-receipt", "blocked"],
    ["gate-mismatch", "blocked"],
    ["gate-boolean", "blocked"],
    ["protocol-mismatch", "rolled-back"],
    ["tree-mismatch", "rolled-back"],
    ["version-mismatch", "rolled-back"],
    ["listener-mismatch", "rolled-back"],
    ["invalid-backend", "rolled-back"],
    ["dead-backend", "rolled-back"],
    ["changed-backend", "blocked"],
    ["missing-protocol", "rolled-back"],
    ["missing-startupGateProtocol", "blocked"],
    ["missing-transactionId", "rolled-back"],
    ["missing-stagedHandle", "rolled-back"],
    ["missing-home", "rolled-back"],
    ["missing-databasePath", "rolled-back"],
    ["missing-profile", "rolled-back"],
    ["missing-environmentId", "rolled-back"],
    ["missing-version", "rolled-back"],
    ["missing-sourceSha", "rolled-back"],
    ["missing-sourceTree", "rolled-back"],
    ["missing-listener", "rolled-back"],
    ["missing-resumeHeld", "rolled-back"],
    ["missing-backendProcess", "rolled-back"],
    ["candidate-stop", "blocked"],
    ["snapshot-failed", "rolled-back"],
    ["old-quiescence", "blocked"],
    ["storage-refused", "blocked"],
    ["reserve-denied", "blocked"],
    ["reserve-release-failed", "blocked"],
    ["recovery-space-missing", "blocked"],
    ["recovery-lease-busy", "blocked"],
    ["state-grew", "rolled-back"],
    ["resume-grant", "blocked"],
    ["commit-uncertain", "blocked"],
    ["untracked-launch", "blocked"],
    ["resume-dispatched", "blocked"],
    ["interrupted-trial", "blocked"],
    ["interrupted-rollback", "blocked"],
    ["interrupted-resume", "blocked"],
    ["stale-home", "refused"],
    ["candidate-gate-missing", "refused"],
    ["candidate-gate-wrong", "refused"],
    ["candidate-gate-boolean", "refused"],
  ])(
    "runs the actual helper's %s transaction with synthetic native process effects",
    async (fault, phase) => {
      const f = await fixture();
      try {
        const nativeFunctions = jonesNativeHelperSource.split(
          "\nparser = argparse.ArgumentParser()",
        )[0];
        const result = JSON.parse(
          NodeChildProcess.execFileSync(
            "python3",
            ["-c", `${nativeFunctions}\n${activationScenario}`, f.directory, fault!],
            { encoding: "utf8", maxBuffer: 1024 * 1024, timeout: 10000 },
          ),
        ) as { phase: string };
        expect(result.phase).toBe(phase);
      } finally {
        await f.cleanup();
      }
    },
  );
  it("pairs and restores without scanning database or profile contents", async () => {
    const f = await fixture();
    try {
      const nativeFunctions = jonesNativeHelperSource.split(
        "\nparser = argparse.ArgumentParser()",
      )[0];
      const scenario = String.raw`
root = pathlib.Path(sys.argv[1])
home, profile = root / 'userdata', root / 'profile'
home.mkdir(); profile.mkdir()
database = home / 'state.sqlite'
with sqlite3.connect(database) as db:
    db.execute('CREATE TABLE identity (value TEXT)')
    db.execute("INSERT INTO identity VALUES ('previous')")
(profile / 'opaque').write_text('profile')
expected = {'databasePath': str(database), 'profile': str(profile)}
def forbidden(*args): raise RuntimeError('Exhaustive content scan is forbidden')
digest = forbidden
app_digest = forbidden
pair_state(expected, root / 'pair')
assert read(root / 'pair' / 'pair.json')['recovery']['method'] in ('clone', 'copy')
prove_quiescence = lambda active: None
restore_pair(expected, root / 'pair', root / 'advanced')
with sqlite3.connect(database) as db:
    assert db.execute('SELECT value FROM identity').fetchone()[0] == 'previous'
`;
      NodeChildProcess.execFileSync(
        "python3",
        ["-c", `${nativeFunctions}\n${scenario}`, f.directory],
        { encoding: "utf8", maxBuffer: 1024 * 1024, timeout: 10000 },
      );
    } finally {
      await f.cleanup();
    }
  });

  it("budgets full-copy recovery and refuses cross-volume or recursive roots before shutdown", async () => {
    const f = await fixture();
    try {
      const nativeFunctions = jonesNativeHelperSource.split(
        "\nparser = argparse.ArgumentParser()",
      )[0];
      const scenario = String.raw`
root = pathlib.Path(sys.argv[1])
state, profile, tx, candidate, app = [root / name for name in ('state', 'profile', 'tx', 'candidate', 'installed.app')]
for path in (state, profile, tx, candidate, app): path.mkdir()
database = state / 'state.sqlite'
database.write_bytes(b'database')
pathlib.Path(str(database) + '-wal').write_bytes(b'wal')
pathlib.Path(str(database) + '-shm').write_bytes(b'shm')
(state / 'settings.json').write_bytes(b'settings')
(profile / 'opaque').write_bytes(b'profile')
(candidate / 'binary').write_bytes(b'candidate')
expected = {'databasePath': str(database), 'profile': str(profile), 'appPath': str(app)}
staged = {'appPath': str(candidate)}
space = type('Space', (), {'f_bavail': 1 << 50, 'f_frsize': 1})()
os.statvfs = lambda path: space
plan = storage_preflight(expected, staged, tx)
assert plan['stateBytes'] == 29
assert plan['candidateBytes'] == 9
assert plan['requiredBytes'] == 2 * 29 + 9 + RECOVERY_HEADROOM
space.f_bavail = plan['requiredBytes'] - 1
try: storage_preflight(expected, staged, tx)
except RuntimeError as error: assert 'Insufficient free space' in str(error)
else: raise AssertionError('Low space was accepted because cloning might work')
space.f_bavail += 1
storage_preflight(expected, staged, tx)
original_stat = pathlib.Path.stat
def other_volume(path, *args, **kwargs):
    info = original_stat(path, *args, **kwargs)
    if path == profile:
        values = list(info); values[2] += 1
        return os.stat_result(values)
    return info
pathlib.Path.stat = other_volume
try:
    try: storage_preflight(expected, staged, tx)
    except RuntimeError as error: assert 'same-volume recovery' in str(error)
    else: raise AssertionError('Cross-volume rollback was accepted')
finally: pathlib.Path.stat = original_stat
for unsafe in (dict(expected, profile=str(state)), dict(expected, profile=str(root))):
    try: storage_preflight(unsafe, staged, tx)
    except RuntimeError as error: assert 'overlap' in str(error)
    else: raise AssertionError('Overlapping recovery roots were accepted')
recursive = profile / 'transaction'; recursive.mkdir()
try: storage_preflight(expected, staged, recursive)
except RuntimeError as error: assert 'own destination' in str(error)
else: raise AssertionError('Recursive profile snapshot was accepted')
`;
      NodeChildProcess.execFileSync(
        "python3",
        ["-c", `${nativeFunctions}\n${scenario}`, f.directory],
        {
          encoding: "utf8",
          maxBuffer: 1024 * 1024,
          timeout: 10000,
        },
      );
    } finally {
      await f.cleanup();
    }
  });

  it("syncs restored sidecars and both rename parents before declaring the advanced pair retained", async () => {
    const f = await fixture();
    try {
      const nativeFunctions = jonesNativeHelperSource.split(
        "\nparser = argparse.ArgumentParser()",
      )[0];
      const scenario = String.raw`
root = pathlib.Path(sys.argv[1])
state, profile, tx = [root / name for name in ('state', 'profile', 'tx')]
for path in (state, profile, tx): path.mkdir()
database = state / 'state.sqlite'
for suffix in ('', '-wal', '-shm'): pathlib.Path(str(database) + suffix).write_bytes(('old' + suffix).encode())
(profile / 'opaque').write_text('old profile')
expected = {'databasePath': str(database), 'profile': str(profile)}
pair_state(expected, tx / 'pair')
for suffix in ('', '-wal', '-shm'): pathlib.Path(str(database) + suffix).write_bytes(('advanced' + suffix).encode())
events, descriptors = [], {}
native_open, native_sync, native_durable = os.open, os.fsync, durable
def opened(path, flags, *args, **kwargs):
    fd = native_open(path, flags, *args, **kwargs)
    descriptors[fd] = str(path)
    return fd
def synced(fd):
    events.append(('sync', descriptors.get(fd)))
    return native_sync(fd)
def recorded(path, value, exclusive=False):
    if pathlib.Path(path).name == 'retained.json': events.append(('retained', str(path)))
    return native_durable(path, value, exclusive)
os.open, os.fsync, durable = opened, synced, recorded
prove_quiescence = lambda active: None
restore_pair(expected, tx / 'pair', tx / 'advanced')
retained = events.index(('retained', str(tx / 'advanced' / 'retained.json')))
for path in (database, pathlib.Path(str(database) + '-wal'), pathlib.Path(str(database) + '-shm'), state, profile.parent, tx, tx / 'advanced'):
    assert ('sync', str(path)) in events[:retained], (str(path), events)
for suffix in ('', '-wal', '-shm'):
    assert pathlib.Path(str(database) + suffix).read_bytes() == ('old' + suffix).encode()
    assert (tx / 'advanced' / (database.name + suffix)).read_bytes() == ('advanced' + suffix).encode()
`;
      NodeChildProcess.execFileSync(
        "python3",
        ["-c", `${nativeFunctions}\n${scenario}`, f.directory],
        {
          encoding: "utf8",
          maxBuffer: 1024 * 1024,
          timeout: 10000,
        },
      );
    } finally {
      await f.cleanup();
    }
  });

  it("copies the stopped DB and sidecars after a partial clone without opening SQLite", async () => {
    const f = await fixture();
    try {
      const nativeFunctions = jonesNativeHelperSource.split(
        "\nparser = argparse.ArgumentParser()",
      )[0];
      const scenario = String.raw`
root = pathlib.Path(sys.argv[1])
home, profile = root / 'copy-home', root / 'copy-profile'
home.mkdir(); profile.mkdir()
database = home / 'state.sqlite'
contents = {'': b'stopped database bytes', '-wal': b'committed WAL bytes', '-shm': b'shared memory bytes'}
for suffix, content in contents.items():
    pathlib.Path(str(database) + suffix).write_bytes(content)
(profile / 'opaque').write_text('profile')
expected = {'databasePath': str(database), 'profile': str(profile)}
def partial_clone(source, target):
    target.write_bytes(b'partial clone')
    return False
def forbidden(*args, **kwargs): raise RuntimeError('Recovery must not open SQLite')
clone_file = partial_clone
sqlite3.connect = forbidden
pair_state(expected, root / 'copy-pair')
recovery = read(root / 'copy-pair' / 'pair.json')['recovery']
assert recovery['method'] == 'copy'
assert recovery['bytes'] == sum(map(len, contents.values()))
for suffix, content in contents.items():
    assert (root / 'copy-pair' / ('state.sqlite' + suffix)).read_bytes() == content
    pathlib.Path(str(database) + suffix).write_bytes(b'advanced state')
prove_quiescence = lambda active: None
restore_pair(expected, root / 'copy-pair', root / 'copy-advanced')
for suffix, content in contents.items():
    assert pathlib.Path(str(database) + suffix).read_bytes() == content
    assert (root / 'copy-advanced' / ('state.sqlite' + suffix)).read_bytes() == b'advanced state'
`;
      NodeChildProcess.execFileSync(
        "python3",
        ["-c", `${nativeFunctions}\n${scenario}`, f.directory],
        { encoding: "utf8", maxBuffer: 1024 * 1024, timeout: 10000 },
      );
    } finally {
      await f.cleanup();
    }
  });

  it("stops its owned child through the unreaped zombie window and refuses a live mismatch", () => {
    const nativeFunctions = jonesNativeHelperSource.split(
      "\nparser = argparse.ArgumentParser()",
    )[0];
    const scenario = String.raw`
def cancel(signum, frame): raise SystemExit(128 + signum)
signal.signal(signal.SIGTERM, cancel)
signal.signal(signal.SIGINT, cancel)
child = subprocess.Popen(['/bin/sleep', '30'])
native_kill = os.kill
signals = []
def captured_kill(pid, sig):
    assert pid == child.pid
    signals.append((pid, sig))
    native_kill(pid, sig)
os.kill = captured_kill
try:
    proof = {'pid': child.pid, 'identity': process_identity(child.pid)}
    assert proof['identity'] is not None
    try: stop_exact([dict(proof, identity=proof['identity'] + ' altered')])
    except RuntimeError as error: assert str(error) == 'Process identity changed; signal withheld.'
    else: raise AssertionError('A live mismatch was accepted')
    assert signals == []
    assert process_identity(child.pid) == proof['identity']
    stop_exact([proof])
    assert signals == [(child.pid, signal.SIGTERM)]
    assert process_identity(child.pid) != proof['identity']
    assert process_start(child.pid)[0].startswith('Z')
    assert alive(proof) is False
    assert child.wait(timeout=2) == -signal.SIGTERM
    assert alive(proof) is False
finally:
    os.kill = native_kill
    if child.returncode is None: child.kill()
    child.wait(timeout=2)
`;
    NodeChildProcess.execFileSync("python3", ["-c", `${nativeFunctions}\n${scenario}`], {
      encoding: "utf8",
      maxBuffer: 1024 * 1024,
      timeout: 10000,
    });
  });

  it("withholds signals on reused identities and failed observations, and retains the stop deadline", () => {
    const nativeFunctions = jonesNativeHelperSource.split(
      "\nparser = argparse.ArgumentParser()",
    )[0];
    const scenario = String.raw`
birth = 'Sat Oct 10 08:20:14 2026'
proof = {'pid': 123, 'identity': birth + ' /owned/process'}
signals = []
calls = []
os.kill = lambda pid, sig: signals.append((pid, sig))
def result(output='', code=0, error=''):
    return type('Result', (), {'returncode': code, 'stdout': output, 'stderr': error})()
def observe(results):
    pending = iter(results)
    def run(command, **kwargs):
        calls.append(command)
        assert command[:4] == ['/bin/ps', '-p', '123', '-o']
        return next(pending)
    subprocess.run = run
    signals.clear()
def blocked(results, message='Process inspection failed; signal withheld.'):
    observe(results)
    try: stop_exact([proof])
    except RuntimeError as error: assert str(error) == message
    else: raise AssertionError('Unsafe process observation was accepted')
    assert signals == []
for observation in (result(code=2), result(),
                    result(proof['identity'] + '\n' + proof['identity']),
                    result(proof['identity'], error='inspection warning'), result(code=1, error='inspection failed')):
    blocked([observation])
mismatch = result(birth + ' <defunct>')
for status in (result(code=2), result(), result('Z'), result(birth + ' Z\n' + birth + ' Z'),
               result(birth + ' Z', error='inspection warning'), result(birth + ' Zgarbage')):
    blocked([mismatch, status])
blocked([mismatch, result('malformed Z')], 'Process identity changed; signal withheld.')
blocked([mismatch, result('Sat Oct 10 08:20:15 2026 Z')], 'Process identity changed; signal withheld.')
blocked([mismatch, result(birth + ' S')], 'Process identity changed; signal withheld.')
blocked([result('malformed command'), result(birth + ' Z')], 'Process identity changed; signal withheld.')
observe([result(code=1), result(code=1)])
stop_exact([proof]); assert signals == []
observe([mismatch, result(code=1), result(code=1)])
stop_exact([proof]); assert signals == []
observe([mismatch, result(birth + ' Z'), mismatch, result(birth + ' Z')])
stop_exact([proof]); assert signals == []
observe([result(proof['identity']), mismatch, result(birth + ' Z')])
stop_exact([proof]); assert signals == [(123, signal.SIGTERM)]
assert calls[0] == ['/bin/ps', '-p', '123', '-o', 'lstart=', '-o', 'command=']
observe([result(proof['identity']), result(proof['identity'])])
clock = iter((0, 91))
time.monotonic = lambda: next(clock)
time.sleep = lambda delay: (_ for _ in ()).throw(AssertionError('Unexpected wait'))
try: stop_exact([proof])
except RuntimeError as error: assert str(error) == 'Owned native writers did not stop; recovery held.'
else: raise AssertionError('Live process bypassed deadline')
assert signals == [(123, signal.SIGTERM)]
observe([result(proof['identity']), result(proof['identity']), result(proof['identity']), result(code=1)])
clock = iter((0, 31, 76))
time.monotonic = lambda: next(clock)
waits = []
time.sleep = waits.append
stop_exact([proof])
assert signals == [(123, signal.SIGTERM)] and waits == [0.1, 0.1]
time.monotonic = lambda: 0
for birth in ('Sa 10 Okt 08:20:14 2026', '2026年10月10日 08:20:14'):
    proof = {'pid': 123, 'identity': birth + ' /owned/process'}
    observe([result(proof['identity']), result(code=1)])
    stop_exact([proof]); assert signals == [(123, signal.SIGTERM)]
    zombie = result(birth + ' <defunct>')
    observe([result(proof['identity']), zombie, result(birth + ' Z')])
    stop_exact([proof]); assert signals == [(123, signal.SIGTERM)]
    observe([zombie, result(birth + ' Z'), zombie, result(birth + ' Z')])
    stop_exact([proof]); assert signals == []
    blocked([result(birth + ' /different/process'), result(birth + ' S')], 'Process identity changed; signal withheld.')
    blocked([zombie, result(birth.replace('08:20:14', '08:20:15') + ' Z')], 'Process identity changed; signal withheld.')
`;
    NodeChildProcess.execFileSync("python3", ["-c", `${nativeFunctions}\n${scenario}`], {
      encoding: "utf8",
      maxBuffer: 1024 * 1024,
      timeout: 10000,
    });
  });

  it("checks native files and the whole profile tree, refusing writers or incomplete access", () => {
    const nativeFunctions = jonesNativeHelperSource.split(
      "\nparser = argparse.ArgumentParser()",
    )[0];
    const scenario = String.raw`
expected = {'databasePath': '/synthetic/state.sqlite', 'profile': '/synthetic/profile'}
pathlib.Path.exists = lambda path: True
calls = []
def inspect(output, code=0, error=''):
    def run(command, **kwargs):
        calls.append(command)
        return type('Result', (), {'returncode': code, 'stdout': output, 'stderr': error})()
    subprocess.run = run
    prove_quiescence(expected)
inspect('p12\nf3\nar\n')
assert calls[0][calls[0].index('+D') + 1] == '/synthetic/profile'
assert '/synthetic/state.sqlite' in calls[0]
assert '/synthetic/state.sqlite-wal' in calls[0]
assert '/synthetic/state.sqlite-shm' in calls[0]
for output in ('p12\nf3\naw\n', 'p12\nf3\nau\n', 'p12\nf3\n', 'unparsed'):
    try: inspect(output)
    except RuntimeError: pass
    else: raise AssertionError('A writer or unknown access was accepted')
inspect('', 1)
try: inspect('', 1, "lsof: WARNING: can't stat() /synthetic/profile/Local Storage/leveldb\nOutput information may be incomplete.")
except RuntimeError: pass
else: raise AssertionError('An incomplete profile inspection was accepted')
`;
    NodeChildProcess.execFileSync("python3", ["-c", `${nativeFunctions}\n${scenario}`], {
      encoding: "utf8",
      maxBuffer: 1024 * 1024,
      timeout: 10000,
    });
  });

  it("checks normal native launch identity from bounded plist and ASAR metadata", async () => {
    const f = await fixture();
    try {
      const nativeFunctions = jonesNativeHelperSource.split(
        "\nparser = argparse.ArgumentParser()",
      )[0];
      const scenario = String.raw`
root = pathlib.Path(sys.argv[1])
app = root / 'Jones.app'
(app / 'Contents/MacOS').mkdir(parents=True)
(app / 'Contents/Resources').mkdir()
active = {'appPath': str(app), 'executablePath': str(app / 'Contents/MacOS/Jones'),
    'version': 'preview-version', 'bundleIdentifier': 'com.jones.code', 'sourceSha': 'a' * 40, 'sourceTree': 'b' * 40}
with open(app / 'Contents/Info.plist', 'wb') as stream:
    plistlib.dump({'CFBundleIdentifier': active['bundleIdentifier'], 'CFBundleExecutable': 'Jones',
        'CFBundleShortVersionString': active['version']}, stream)
metadata = {'version': active['version'], 'jonesSource': {'repository': 'Jones-Systems/Jones-Code',
    'sha': active['sourceSha'], 'tree': active['sourceTree']}}
payload = json.dumps(metadata).encode()
header = json.dumps({'files': {'package.json': {'offset': '0', 'size': len(payload)}}}).encode()
(app / 'Contents/Resources/app.asar').write_bytes(struct.pack('<IIII', 4, len(header) + 8, len(header) + 4, len(header)) + header + payload)
def forbidden(*args): raise RuntimeError('Normal launches must not hash the bundle')
app_digest = forbidden
digest = forbidden
assert bundle_identity(active) == 'com.jones.code'
for key, bad in (('sourceSha', 'c' * 40), ('sourceTree', 'd' * 40), ('version', 'wrong-version'), ('bundleIdentifier', 'wrong.bundle')):
    try: bundle_identity(dict(active, **{key: bad}))
    except RuntimeError: pass
    else: raise AssertionError('A changed bundle identity was accepted')
`;
      NodeChildProcess.execFileSync(
        "python3",
        ["-c", `${nativeFunctions}\n${scenario}`, f.directory],
        { encoding: "utf8", maxBuffer: 1024 * 1024, timeout: 10000 },
      );
    } finally {
      await f.cleanup();
    }
  });

  it("has valid standalone Python syntax", () => {
    expect(
      NodeChildProcess.execFileSync(
        "python3",
        ["-c", "import sys; compile(sys.stdin.read(), '<jones-native-helper>', 'exec')"],
        {
          input: jonesNativeHelperSource,
          encoding: "utf8",
          maxBuffer: 1024 * 1024,
          timeout: 10000,
        },
      ),
    ).toBe("");
  });

  it("retains advanced SQLite, exact startup settings, and same-host profile before paired restore", async () => {
    const f = await fixture();
    try {
      const nativeFunctions = jonesNativeHelperSource.split(
        "\nparser = argparse.ArgumentParser()",
      )[0];
      const scenario = String.raw`
root = pathlib.Path(sys.argv[1])
home, profile = root / 'userdata', root / 'profile'
home.mkdir(); profile.mkdir()
database = home / 'state.sqlite'
with sqlite3.connect(database) as db:
    db.execute('CREATE TABLE identity (value TEXT)')
    db.execute("INSERT INTO identity VALUES ('previous-native-state')")
(home / 'settings.json').write_text('previous-settings')
(profile / 'opaque-profile').write_text('same-host-profile')
expected = {'databasePath': str(database), 'profile': str(profile), 'appPath': 'previous.app', 'environmentId': 'original-environment'}
pair = root / 'previous-pair'
pair_state(expected, pair)
with sqlite3.connect(database) as db:
    db.execute("UPDATE identity SET value='advanced-state'")
(home / 'settings.json').write_text('candidate-startup-settings')
(home / 'client-settings.json').write_text('new-setting-created-by-startup')
(profile / 'opaque-profile').write_text('advanced-profile')
prove_quiescence = lambda expected: None
restore_pair(expected, pair, root / 'advanced-state')
with sqlite3.connect(database) as db:
    assert db.execute('SELECT value FROM identity').fetchone()[0] == 'previous-native-state'
with sqlite3.connect(root / 'advanced-state' / 'state.sqlite') as db:
    assert db.execute('SELECT value FROM identity').fetchone()[0] == 'advanced-state'
assert (home / 'settings.json').read_text() == 'previous-settings'
assert not (home / 'client-settings.json').exists()
assert (root / 'advanced-state' / 'client-settings.json').read_text() == 'new-setting-created-by-startup'
assert (profile / 'opaque-profile').read_text() == 'same-host-profile'
assert (root / 'advanced-state' / 'profile' / 'opaque-profile').read_text() == 'advanced-profile'
assert (pair / 'state.sqlite').exists()
`;
      NodeChildProcess.execFileSync(
        "python3",
        ["-c", `${nativeFunctions}\n${scenario}`, f.directory],
        { encoding: "utf8", maxBuffer: 1024 * 1024, timeout: 10000 },
      );
    } finally {
      await f.cleanup();
    }
  });

  it("uses the same app-layout digest in the native helper and staged receipt", async () => {
    const f = await fixture();
    try {
      const app = NodePath.join(f.directory, "Jones.app");
      await NodeFSP.mkdir(NodePath.join(app, "Contents", "Versions", "A"), { recursive: true });
      await NodeFSP.writeFile(
        NodePath.join(app, "Contents", "Versions", "A", "payload-ü"),
        "native-data",
      );
      await NodeFSP.symlink("A", NodePath.join(app, "Contents", "Versions", "Current"));
      const nativeFunctions = jonesNativeHelperSource.split(
        "\nparser = argparse.ArgumentParser()",
      )[0];
      const digest = NodeChildProcess.execFileSync(
        "python3",
        ["-c", `${nativeFunctions}\nprint(app_digest(sys.argv[1]))`, app],
        { encoding: "utf8", maxBuffer: 1024 * 1024, timeout: 10000 },
      ).trim();
      expect(await hashMacApp(app)).toBe(digest);
      await NodeFSP.symlink(f.directory, NodePath.join(app, "escape"));
      await expect(hashMacApp(app)).rejects.toThrow("escapes");
    } finally {
      await f.cleanup();
    }
  });
});
