// @effect-diagnostics-next-line nodeBuiltinImport:off -- Executes only synthetic paired-state fixtures.
import * as NodeChildProcess from "node:child_process";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The raw-source consumer must read the same tracked literals as the TypeScript composer.
import * as NodeFSP from "node:fs/promises";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { describe, expect, it } from "vite-plus/test";
// @ts-expect-error -- This standalone Node fixture has no declaration file; its output is checked against the typed source exports below.
import * as PairedExecutorFixture from "../../../../../scripts/jones/updates/paired-executor-fixture.mjs";
import { jonesNativeHelperSource } from "./jonesNativeHelperSource.ts";
import { jonesPairedStateSource } from "./jonesPairedStateSource.ts";

const sourceTexts = () =>
  Promise.all([
    NodeFSP.readFile(new URL("./jonesNativeHelperSource.ts", import.meta.url), "utf8"),
    NodeFSP.readFile(new URL("./jonesPairedStateSource.ts", import.meta.url), "utf8"),
  ]);

const identityScenario = String.raw`
import tempfile
def cancel_fixture(signum, frame):
    raise SystemExit(128 + signum)
signal.signal(signal.SIGTERM, cancel_fixture)
with tempfile.TemporaryDirectory(prefix='paired-identity-') as scratch:
    root = pathlib.Path(scratch)
    home, profile, transaction = root / 'userdata', root / 'profile', root / 'transaction'
    for path in (home, profile, transaction): path.mkdir()
    database = home / 'state.sqlite'
    database.write_bytes(b'previous-database')
    (profile / 'opaque').write_bytes(b'previous-profile')
    identity = home / 'environment-id'
    previous_id = b'11111111-1111-4111-8111-111111111111\n'
    changed_id = b'22222222-2222-4222-8222-222222222222\n'
    fault = sys.argv[1]
    if fault != 'absent': identity.write_bytes(previous_id)
    authority = root / 'native-store-authority'
    authority.mkdir()
    authority_record = authority / 'native-store-authority-v1.json'
    authority_record.write_bytes(b'authority-generation-7')
    expected = {'home': str(root), 'databasePath': str(database), 'profile': str(profile)}
    pair, advanced = transaction / 'previous-pair', transaction / 'advanced-state'
    baseline_bytes = len(b'previous-database')
    if fault in ('symlink', 'directory'):
        identity.unlink()
        if fault == 'symlink': identity.symlink_to(authority_record)
        else: identity.mkdir()
        for action in (lambda: recovery_state_bytes(expected), lambda: pair_state(expected, pair)):
            try: action()
            except RuntimeError: pass
            else: raise AssertionError('Unsafe identity file accepted')
        assert database.read_bytes() == b'previous-database'
        assert authority_record.read_bytes() == b'authority-generation-7'
    else:
        assert recovery_state_bytes(expected) == baseline_bytes + (0 if fault == 'absent' else len(previous_id))
        pair_state(expected, pair)
        snapshot = read(pair / 'pair.json')
        assert snapshot['settings']['environment-id'] == (fault != 'absent')
        assert not (pair / 'native-store-authority').exists()
        assert (pair / 'environment-id').exists() == (fault != 'absent')
        if fault != 'absent': assert (pair / 'environment-id').read_bytes() == previous_id
        database.write_bytes(b'candidate-database')
        identity.write_bytes(changed_id)
        (profile / 'opaque').write_bytes(b'candidate-profile')
        authority_record.write_bytes(b'authority-generation-9')
        transitions = []
        guard = {'syntheticGeneration': 9}
        def transition(active, candidate, operation, supplied_guard=None):
            assert active == expected
            assert authority_record.read_bytes() == b'authority-generation-9'
            transitions.append(operation)
            if operation == 'fence':
                assert identity.read_bytes() == changed_id
                if fault == 'fence-failed': raise RuntimeError('Synthetic fence failure')
                return guard
            assert supplied_guard == guard
            assert identity.exists() == (fault != 'absent')
            if fault != 'absent': assert identity.read_bytes() == previous_id
            authority_record.write_bytes(b'authority-generation-10')
        native_authority_transition = transition
        prove_quiescence = lambda active: None
        if fault == 'manifest-missing':
            del snapshot['settings']['environment-id']
            durable(pair / 'pair.json', snapshot)
        if fault in ('manifest-missing', 'fence-failed'):
            try: restore_pair(expected, pair, advanced, {})
            except RuntimeError: pass
            else: raise AssertionError('Unsafe restore continued')
            assert database.read_bytes() == b'candidate-database'
            assert identity.read_bytes() == changed_id
            assert (profile / 'opaque').read_bytes() == b'candidate-profile'
            assert authority_record.read_bytes() == b'authority-generation-9'
            assert transitions == ([] if fault == 'manifest-missing' else ['fence'])
        else:
            restore_pair(expected, pair, advanced, {})
            assert database.read_bytes() == b'previous-database'
            assert (profile / 'opaque').read_bytes() == b'candidate-profile'
            assert (advanced / 'environment-id').read_bytes() == changed_id
            assert not (advanced / 'native-store-authority').exists()
            assert authority_record.read_bytes() == b'authority-generation-10'
            assert transitions == ['fence', 'advance']
assert not root.exists()
`;

const appleCloneScenario = String.raw`
import tempfile, errno
with tempfile.TemporaryDirectory(prefix='paired-apple-clone-') as scratch:
    root = pathlib.Path(scratch)
    home, transaction = root / 'userdata', root / 'transaction'
    home.mkdir(); transaction.mkdir()
    database = home / 'state.sqlite'
    originals = {'state.sqlite': b'previous-database', 'state.sqlite-wal': b'previous-wal',
                 'state.sqlite-shm': b'previous-shm', 'settings.json': b'previous-settings'}
    for name, content in originals.items():
        path = home / name
        path.write_bytes(content)
        path.chmod(0o640)
        os.utime(path, ns=(1700000000123456789, 1700000000123456789))
    expected = {'databasePath': str(database), 'profile': str(root / 'profile')}
    pair = transaction / 'previous-pair'
    pair_state(expected, pair, clone_required=True)
    assert read(pair / 'pair.json')['recovery']['method'] == 'clone'
    for name, content in originals.items():
        source, snapshot = home / name, pair / name
        assert snapshot.read_bytes() == content
        assert stat.S_IMODE(snapshot.stat().st_mode) == stat.S_IMODE(source.stat().st_mode)
        assert snapshot.stat().st_mtime_ns == source.stat().st_mtime_ns
    database.write_bytes(b'advanced-database')
    (home / 'state.sqlite-wal').write_bytes(b'advanced-wal')
    assert (pair / 'state.sqlite').read_bytes() == originals['state.sqlite']
    assert (pair / 'state.sqlite-wal').read_bytes() == originals['state.sqlite-wal']
    probe = transaction / 'probe'
    clone_file(database, probe, required=True)
    probe.write_bytes(b'changed-probe')
    assert database.read_bytes() == b'advanced-database'
    database.write_bytes(b'advanced-again')
    assert probe.read_bytes() == b'changed-probe'
    try: clone_file(database, probe, required=True)
    except OSError as error: assert error.errno == errno.EEXIST
    else: raise AssertionError('Existing clone target was replaced')
    assert probe.read_bytes() == b'changed-probe'
    link = transaction / 'link'
    link.symlink_to(database)
    try: clone_file(link, transaction / 'link-copy', required=True)
    except RuntimeError: pass
    else: raise AssertionError('Symlink recovery source was accepted')
    assert not (transaction / 'link-copy').exists()
    prove_quiescence = lambda active: None
    guard = {'syntheticGeneration': 1}
    native_authority_transition = lambda active, candidate, operation, supplied=None: guard
    advanced = transaction / 'advanced-state'
    restore_pair(expected, pair, advanced, {})
    for name, content in originals.items(): assert (home / name).read_bytes() == content
    assert (advanced / 'state.sqlite').read_bytes() == b'advanced-again'
    assert (advanced / 'state.sqlite-wal').read_bytes() == b'advanced-wal'
assert not root.exists()
`;

describe("shared paired-state Python source", () => {
  it.runIf(HostProcess.Platform.defaultValue() === "darwin")(
    "clones the complete pair with real APFS isolation and restores it by rename",
    () => {
      const definitions = jonesNativeHelperSource.split("\nparser = argparse.ArgumentParser()")[0];
      NodeChildProcess.execFileSync("python3", ["-I", "-"], {
        input: definitions + "\n" + appleCloneScenario,
        encoding: "utf8",
        timeout: 10_000,
      });
    },
  );

  it.each(["present", "absent", "manifest-missing", "fence-failed", "symlink", "directory"])(
    "preserves environment identity and authority boundaries: %s",
    (fault) => {
      const definitions = jonesNativeHelperSource.split("\nparser = argparse.ArgumentParser()")[0];
      NodeChildProcess.execFileSync("python3", ["-", fault], {
        input: definitions + "\n" + identityScenario,
        encoding: "utf8",
        timeout: 10_000,
      });
    },
  );

  it("includes each paired-restore fragment exactly once", () => {
    for (const fragment of Object.values(jonesPairedStateSource)) {
      expect(jonesNativeHelperSource.split(fragment)).toHaveLength(2);
    }
  });

  it("materializes the same executor for native qualification and TypeScript packaging", async () => {
    const [executorText, pairedText] = await sourceTexts();
    expect(PairedExecutorFixture.materializePairedExecutor(executorText, pairedText)).toBe(
      jonesNativeHelperSource,
    );
    expect(await PairedExecutorFixture.readPairedExecutorSource()).toBe(jonesNativeHelperSource);
  });

  it("refuses missing, duplicate, unknown, and nonliteral substitutions", async () => {
    const [executorText, pairedText] = await sourceTexts();
    const reference = "${jonesPairedStateSource.syncTree}";
    for (const changed of [
      executorText.replace(reference, ""),
      executorText.replace(reference, () => reference + reference),
      executorText.replace(reference, "${jonesPairedStateSource.unknown}"),
      executorText.replace(reference, () => reference + "${unbound}"),
    ])
      expect(() => PairedExecutorFixture.materializePairedExecutor(changed, pairedText)).toThrow();
    for (const changed of [
      pairedText.replace("syncTree: String.raw`", "syncTree: String.raw`${unbound}"),
      pairedText.replace("copyAndTopology: String.raw`", "unknown: String.raw`"),
      pairedText.replace("} as const);", "extra: String.raw`unbound`,\n} as const);"),
      pairedText.replace("} as const);", "} as const);\nunbound();"),
    ])
      expect(() =>
        PairedExecutorFixture.materializePairedExecutor(executorText, changed),
      ).toThrow();
  });
});
