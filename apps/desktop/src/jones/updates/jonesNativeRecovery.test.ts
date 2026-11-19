// @effect-diagnostics nodeBuiltinImport:off - Synthetic Python definitions only; clonefile and process effects are stubbed.
import * as NodeChildProcess from "node:child_process";
import { describe, expect, it } from "vite-plus/test";
import { jonesNativeHelperSource } from "./jonesNativeHelperSource.ts";

const definitions = jonesNativeHelperSource.split("\nparser = argparse.ArgumentParser()")[0]!;
const scenario = String.raw`
import tempfile, ctypes, errno
from types import SimpleNamespace
with tempfile.TemporaryDirectory(prefix='jones-recovery-policy-') as scratch:
    root = pathlib.Path(scratch).resolve()
    state, recovery, apps = root / 'userdata', root / 'runtime/jones-updates/transactions', root / 'apps'
    for path in (state, recovery, apps): path.mkdir(parents=True, mode=0o700)
    state.chmod(0o700); recovery.chmod(0o700)
    database = state / 'state.sqlite'
    profile = root / 'profile'; profile.mkdir(); (profile / 'cache').write_bytes(bytes(20000))
    candidate = root / 'candidate.app'; candidate.mkdir(); (candidate / 'payload').write_bytes(bytes(100))
    manifest_path = root / 'runtime/jones-active-install.json'
    active = {'protocol': 1, 'owner': 'desktop', 'sourceSha': 'a' * 40, 'version': 'old', 'generation': 'generation',
              'home': str(root), 'profile': str(profile), 'databasePath': str(database), 'appPath': str(apps / 'Jones.app')}
    durable(manifest_path, active)
    handle = 'b' * 64
    staging = recovery.parent / 'staging'; staging.mkdir()
    selection_path = staging / (active['sourceSha'] + '-selection.json')
    selected = {'schema': 1, 'source': 'jones-actions', 'home': str(root), 'profile': str(profile), 'currentVersion': 'old',
                'installedSource': active['sourceSha'], 'active': active, 'app': {'handle': handle, 'appPath': str(candidate)}}
    durable(selection_path, selected)
    verify_app = lambda *args: None
    stop_exact = lambda *args: (_ for _ in ()).throw(AssertionError('Inspection stopped a process'))
    pair_state_original = pair_state
    if sys.argv[1] != 'first-install':
        for name, size in (('state.sqlite', 200), ('state.sqlite-wal', 100), ('state.sqlite-shm', 50), ('settings.json', 25)):
            (state / name).write_bytes(bytes(size))
    def fake_clone(source, target, flags):
        assert flags == 1
        source, target = pathlib.Path(os.fsdecode(source)), pathlib.Path(os.fsdecode(target))
        assert source.name == 'source' and target.name == 'clone', 'Probe touched existing state'
        if sys.argv[1] in ('unsupported', 'unknown'):
            ctypes.set_errno(errno.ENOTSUP if sys.argv[1] == 'unsupported' else errno.EIO)
            return -1
        target.write_bytes(source.read_bytes())
        return 0
    darwin_clonefile = lambda: fake_clone
    os.statvfs = lambda path: SimpleNamespace(f_bavail=RECOVERY_HEADROOM + 200, f_frsize=1)
    if sys.argv[1] == 'distinct-volumes':
        original_stat = pathlib.Path.stat
        def virtual_stat(path, *args, **kwargs):
            info = original_stat(path, *args, **kwargs)
            if path == apps:
                values = {name: getattr(info, name) for name in dir(info) if name.startswith('st_')}
                values['st_dev'] += 1
                return SimpleNamespace(**values)
            return info
        pathlib.Path.stat = virtual_stat
    if sys.argv[1] == 'symlink':
        (state / 'settings.json').unlink(); (state / 'settings.json').symlink_to(profile / 'cache')
        try: inspect_recovery(active, handle, selection_path, digest(selection_path))
        except (OSError, RuntimeError): pass
        else: raise AssertionError('Symlink recovery input admitted')
    else:
        result = inspect_recovery(active, handle, selection_path, digest(selection_path))
        assert result['stateBytes'] == (0 if sys.argv[1] == 'first-install' else 375)
        assert result['candidateBytes'] == 100
        assert result['firstInstall'] == (sys.argv[1] == 'first-install')
        assert result['clone']['status'] == ('unsupported' if sys.argv[1] in ('first-install', 'unsupported') else 'unknown' if sys.argv[1] == 'unknown' else 'proved')
        assert len(result['volumes']) == (2 if sys.argv[1] == 'distinct-volumes' else 1)
        assert sum(v['copyRequiredBytes'] for v in result['volumes']) == len(result['volumes']) * RECOVERY_HEADROOM + result['stateBytes'] + 100
        assert sum(v['cloneRequiredBytes'] for v in result['volumes']) == len(result['volumes']) * RECOVERY_HEADROOM + 100
    assert not list(root.rglob('.jones-recovery-probe-*'))
    assert not (manifest_path.parent / 'jones-activation.lock').exists()
    assert not list(recovery.iterdir()), 'Preflight copied state or allocated a transaction'
    assert (profile / 'cache').stat().st_size == 20000
print('ok')
`;

describe("native recovery inspection", () => {
  it("reads the fixed Laptop stage under the exact activation lock or proves it has not been created", () => {
    const source = String.raw`
import tempfile
with tempfile.TemporaryDirectory(prefix='jones-laptop-startup-') as scratch:
    home = pathlib.Path(scratch).resolve()
    manifest_path = home / 'runtime/jones-active-install.json'
    lock_path = manifest_path.with_name('jones-activation.lock')
    updates = manifest_path.parent / 'jones-updates'
    transaction = updates / 'transactions' / ('c' * 64)
    transaction.mkdir(parents=True)
    active = {'protocol': 1, 'owner': 'desktop', 'home': str(home),
              'sourceSha': 'a' * 40, 'version': 'old', 'generation': 'previous',
              'profile': str(home / 'profile'), 'databasePath': str(home / 'userdata/state.sqlite'),
              'environmentId': 'native-environment'}
    durable(manifest_path, active)
    handle = 'b' * 64
    selection = updates / 'staging' / (active['sourceSha'] + '-reviewed.json')
    durable(selection, {'schema': 1, 'source': 'jones-actions', 'home': str(home),
                        'profile': active['profile'], 'currentVersion': active['version'],
                        'installedSource': active['sourceSha'], 'active': active, 'app': {'handle': handle}})
    manifest_sha, selection_sha = digest(manifest_path), digest(selection)
    def inspect(manifest_digest=manifest_sha, selection_digest=selection_sha):
        command = [sys.executable, '-', '--manifest', str(manifest_path), '--inspect-laptop-startup', handle,
                   '--manifest-sha256', manifest_digest, '--selection', str(selection), '--selection-sha256', selection_digest]
        output = subprocess.run(command, input="import sys; sys.platform = 'darwin'\n" + helper,
                                text=True, capture_output=True, check=True, timeout=5)
        return json.loads(output.stdout)
    proof = {'protocol': 1, 'operation': 'inspect-laptop-startup', 'status': 'settled',
             'manifestSha256': manifest_sha, 'selectionSha256': selection_sha}
    assert inspect() == proof
    assert not lock_path.exists() and not list(transaction.iterdir())
    assert inspect(manifest_digest='d' * 64)['status'] == 'refused'
    assert inspect(selection_digest='d' * 64)['status'] == 'refused'

    lock_path.write_text('jones-activation-lock-v1\n')
    with open(lock_path, 'r+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert inspect()['reason'] == 'busy'
    assert inspect() == proof

    for name in ('prepare-intent.json', 'prepare-request.json', 'continuation.json', 'intent.json'):
        record = transaction / name
        durable(record, {})
        assert inspect()['status'] == 'refused', name
        record.unlink()
    maintenance = manifest_path.with_name('jones-maintenance')
    maintenance.symlink_to(home / 'missing-maintenance')
    assert inspect()['reason'] == 'maintenance-pending'
    maintenance.unlink()
    manual = manifest_path.with_name('jones-manual-installs') / ('d' * 64) / 'receipt.json'
    durable(manual, {'protocol': 1, 'operation': 'manual-install', 'phase': 'held', 'transactionId': 'd' * 64})
    assert inspect()['reason'] == 'manual-install-pending'
    shutil.rmtree(manual.parent.parent)

    intent = {'protocol': 1, 'transactionId': transaction.name, 'expected': active,
              'staged': {'handle': handle}, 'continuationReceipt': str(transaction / 'continuation.json'),
              'processes': [], 'listener': 'local'}
    durable(transaction / 'intent.json', intent)
    journal_path = transaction / 'journal.json'
    durable(journal_path, {'phase': 'preparing', 'intent': intent})
    assert inspect()['reason'] == 'activation-pending'
    durable(journal_path, {'phase': 'rolled-back', 'intent': dict(intent, listener='unrelated')})
    assert inspect()['reason'] == 'unknown-state'
    durable(journal_path, {'phase': 'rolled-back', 'intent': intent})
    assert inspect() == proof
    assert digest(manifest_path) == manifest_sha and digest(selection) == selection_sha
    assert lock_path.read_text() == 'jones-activation-lock-v1\n'
    lock_path.write_text('unknown-lock')
    assert inspect()['reason'] == 'activation-lock-unknown'
print('ok')
`;
    const output = NodeChildProcess.execFileSync("python3", ["-"], {
      input: `${definitions}\nhelper = ${JSON.stringify(jonesNativeHelperSource)}\n${source}`,
      encoding: "utf8",
      timeout: 15_000,
    });
    expect(output.trim()).toBe("ok");
  });

  it.each([
    "first-install",
    "same-volume",
    "distinct-volumes",
    "unsupported",
    "unknown",
    "symlink",
  ])("measures %s with owned ephemeral probes only", (mode) => {
    const output = NodeChildProcess.execFileSync("python3", ["-", mode], {
      input: `${definitions}\n${scenario}`,
      encoding: "utf8",
      timeout: 15_000,
    });
    expect(output.trim()).toBe("ok");
  });

  it("rejects an unrecognized activation policy before reading bindings, journaling or stopping", () => {
    const source = String.raw`
import tempfile
with tempfile.TemporaryDirectory(prefix='jones-policy-refusal-') as scratch:
    path = pathlib.Path(scratch) / 'intent.json'
    for value in ('copy', 'clone-or-copy', None, False, {}):
        path.write_text(json.dumps({'stateRecovery': value}))
        try: activate(path)
        except RuntimeError as error: assert 'policy' in str(error)
        else: raise AssertionError('Unknown policy reached activation')
        assert {p.name for p in path.parent.iterdir()} == {'intent.json'}
print('ok')
`;
    expect(
      NodeChildProcess.execFileSync("python3", ["-"], {
        input: `${definitions}\n${source}`,
        encoding: "utf8",
        timeout: 15_000,
      }).trim(),
    ).toBe("ok");
  });

  it.each([
    "strict-failure",
    "strict-success",
    "ordinary-cp-success",
    "ordinary-copy-fallback",
    "empty-state",
  ])("keeps policy and observed method truthful: %s", (mode) => {
    const source = String.raw`
import tempfile, ctypes, errno
from types import SimpleNamespace
with tempfile.TemporaryDirectory(prefix='jones-pair-policy-') as scratch:
    root = pathlib.Path(scratch).resolve()
    state, tx = root / 'state', root / 'tx'; state.mkdir(); tx.mkdir()
    database = state / 'state.sqlite'
    if sys.argv[1] != 'empty-state': database.write_bytes(b'database')
    expected = {'databasePath': str(database), 'profile': str(root / 'profile')}
    calls = []
    def strict_failure(*args):
        ctypes.set_errno(errno.ENOTSUP); return -1
    original_copy = shutil.copy2
    def strict_success(source, target, flags):
        assert flags == 1; calls.append('strict')
        original_copy(os.fsdecode(source), os.fsdecode(target)); return 0
    darwin_clonefile = lambda: strict_success if sys.argv[1] == 'strict-success' else strict_failure
    def ordinary(command, *args, **kwargs):
        calls.append('cp')
        if sys.argv[1] in ('strict-failure', 'strict-success'): raise AssertionError('Required clone fell back to cp')
        if sys.argv[1] == 'ordinary-cp-success':
            original_copy(command[-2], command[-1]); return SimpleNamespace(returncode=0)
        return SimpleNamespace(returncode=1)
    def fallback(*args, **kwargs):
        calls.append('copy')
        if sys.argv[1] in ('strict-failure', 'strict-success'): raise AssertionError('Required clone fell back to copy')
        return original_copy(*args, **kwargs)
    subprocess.run, shutil.copy2 = ordinary, fallback
    if sys.argv[1] == 'strict-failure':
        try: pair_state(expected, tx / 'pair', True)
        except OSError: pass
        else: raise AssertionError('Strict failure was accepted')
        assert calls == [] and database.read_bytes() == b'database'
    else:
        pair_state(expected, tx / 'pair', sys.argv[1] == 'strict-success')
        snapshot = read(tx / 'pair/pair.json')
        if sys.argv[1] == 'empty-state': assert 'recovery' not in snapshot
        elif sys.argv[1] == 'strict-success':
            assert snapshot['recovery']['method'] == 'clone' and snapshot['recovery']['strictClone'] is True
            assert calls == ['strict']
        else:
            assert snapshot['recovery']['method'] == 'copy'
            assert snapshot['recovery']['strictClone'] is False
            assert snapshot['recovery']['copyFallback'] == (sys.argv[1] == 'ordinary-copy-fallback')
print('ok')
`;
    expect(
      NodeChildProcess.execFileSync("python3", ["-", mode], {
        input: `${definitions}\n${source}`,
        encoding: "utf8",
        timeout: 15_000,
      }).trim(),
    ).toBe("ok");
  });
});

it("rechecks actual strict capability before shutdown and holds when earlier proof no longer applies", () => {
  const source = String.raw`
import tempfile
with tempfile.TemporaryDirectory(prefix='jones-activation-recheck-') as scratch:
    root = pathlib.Path(scratch).resolve()
    txid = 'a' * 64
    manifest_path = root / 'runtime/jones-active-install.json'
    tx = manifest_path.parent / 'jones-updates/transactions' / txid
    tx.mkdir(parents=True)
    state = root / 'userdata'; state.mkdir(); (state / 'state.sqlite').write_bytes(b'previous-state')
    apps = root / 'apps'; apps.mkdir()
    candidate = root / 'candidate.app'; candidate.mkdir(); (candidate / 'payload').write_bytes(b'candidate')
    expected = {'protocol': 1, 'owner': 'desktop', 'generation': 'previous', 'home': str(root), 'databasePath': str(state / 'state.sqlite'),
                'profile': str(root / 'profile'), 'environmentId': 'laptop', 'appPath': str(apps / 'Jones.app')}
    staged = {'handle': 'b' * 64, 'appPath': str(candidate)}
    durable(manifest_path, expected)
    claim = dict(expected, transactionId=txid, stagedHandle=staged['handle'], preparationClaimProtocol=3, selectionPath='synthetic', selectionSha256='c' * 64)
    continuation = dict(claim, prepared=True)
    for name in ('prepare-intent.json', 'prepare-dispatched.json'): durable(tx / name, claim)
    durable(tx / 'continuation.json', continuation)
    intent = {'protocol': 1, 'transactionId': txid, 'expected': expected, 'staged': staged, 'stateRecovery': 'clone-required',
              'continuationReceipt': str(tx / 'continuation.json'), 'processes': [], 'listener': 'synthetic'}
    durable(tx / 'intent.json', intent)
    reject_withdrawn_preparation = lambda *args: None
    verify_app = lambda *args: None
    preparation_claim = lambda *args: claim
    selection_binding = lambda *args: (None, {'app': staged}, None, None)
    attempt_selection_binding = lambda *args: None
    original_activation_proofs = lambda *args: []
    capture_activation_processes = lambda *args: []
    pre_effect_unchanged = lambda *args: True
    recorded_processes_exited = lambda *args: False
    release_writer_exclusion = lambda *args: None
    calls = []
    original_storage = storage_preflight
    def storage(active, selected, transaction, required=False):
        assert required is True; calls.append('storage')
        return original_storage(active, selected, transaction, required)
    storage_preflight = storage
    strict_recovery_probe = lambda *args: {'status': 'unknown'}
    def forbidden(*args): raise AssertionError('Changed clone capability reached native effects')
    reserve_recovery_capacity = stop_exact = pair_state = prepare_bundle = forbidden
    activate(tx / 'intent.json')
    journal = read(tx / 'journal.json')
    assert calls == ['storage'] and journal['phase'] == 'rolled-back'
    assert journal['preEffect']['step'] == 'storage'
    assert journal['intent']['stateRecovery'] == 'clone-required'
    assert (state / 'state.sqlite').read_bytes() == b'previous-state'
    assert not (tx / 'previous-pair').exists()
print('ok')
`;
  expect(
    NodeChildProcess.execFileSync("python3", ["-"], {
      input: `${definitions}\n${source}`,
      encoding: "utf8",
      timeout: 15_000,
    }).trim(),
  ).toBe("ok");
});
