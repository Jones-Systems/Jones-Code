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
    (app / 'Contents' / 'MacOS' / 'Jones').write_text('synthetic-executable')
expected = {'protocol': 1, 'owner': 'desktop', 'generation': 'previous', 'transactionId': 'bootstrap',
    'home': str(root), 'databasePath': str(database), 'profile': str(profile), 'environmentId': 'same-environment',
    'appPath': str(previous_app), 'executablePath': str(previous_app / 'Contents/MacOS/Jones'),
    'version': 'same-preview-version', 'sourceSha': 'a' * 40, 'sourceTree': 'b' * 40, 'appDigest': app_digest(previous_app)}
manifest_path = root / 'runtime' / 'jones-active-install.json'
durable(manifest_path, expected, True)
tx = root / 'runtime' / 'jones-updates' / 'transactions' / ('f' * 64)
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
continuation = dict(protocol=1, transactionId=staged['handle'], prepared=True,
    **{key: expected[key] for key in ('home', 'databasePath', 'profile', 'environmentId')})
durable(tx / 'continuation.json', continuation, True)
intent = {'protocol': 1, 'transactionId': staged['handle'], 'expected': expected, 'staged': staged,
    'continuationReceipt': str(tx / 'continuation.json'), 'processes': [{'pid': 123, 'identity': 'owned-native-process'}],
    'listener': 'http://127.0.0.1:3777'}
durable(tx / 'intent.json', intent, True)
events = []
native_stop = lambda proofs: events.append('stop:' + str(len(proofs)))
def synthetic_stop(proofs):
    native_stop(proofs)
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
    receipt = {key: wanted[key] for key in ('protocol', 'startupGateProtocol', 'transactionId', 'home', 'databasePath', 'profile', 'environmentId', 'version', 'sourceSha', 'sourceTree', 'listener')}
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
    if fault in ('trial-mismatch', 'candidate-stop', 'resume-dispatched'): receipt['sourceSha'] = 'wrong-source'
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
    if fault == 'success':
        activate(tx / 'intent.json')
        assert events.count('candidate-launch') == 1
        descriptor = read(tx / 'trial-descriptor.json')
        grant = read(tx / 'commit-grant.json')
        fields = ('protocol', 'startupGateProtocol', 'transactionId', 'home', 'databasePath', 'profile', 'environmentId', 'version', 'sourceSha', 'sourceTree', 'listener')
        assert descriptor['startupGateProtocol'] == 1
        assert grant == dict(generation=descriptor['transactionId'], **{key: descriptor[key] for key in fields})
        assert read(manifest_path)['sourceSha'] == staged['sourceSha']
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
        if fault != 'snapshot-failed':
            with sqlite3.connect(tx / 'advanced-state' / 'state.sqlite') as db:
                assert db.execute('SELECT value FROM identity').fetchone()[0] == 'advanced-state'
        else:
            assert 'candidate-launch' not in events
    if journal['phase'] == 'blocked' and 'candidate-launch' in events:
        assert state == 'advanced-state'
        assert (tx / 'previous-pair' / 'state.sqlite').exists()
        assert 'restart-previous' not in events
    print(json.dumps({'phase': journal['phase'], 'events': events, 'state': state}))
`;

describe("Jones native helper", () => {
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
