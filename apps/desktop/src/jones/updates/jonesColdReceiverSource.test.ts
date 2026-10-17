// @effect-diagnostics-next-line nodeBuiltinImport:off -- Owned Python fixtures use the existing captured-child scratch lifecycle.
import * as NodeFSP from "node:fs/promises";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Paths stay under the captured fixture root.
import * as NodePath from "node:path";
import { describe, expect, it } from "vite-plus/test";
// @ts-expect-error -- The existing owned fixture has no declarations.
import * as IdentityFixture from "../../../../../scripts/jones/updates/legacy-identity-native-fixture.mjs";
import { jonesColdReceiverSource } from "./jonesColdReceiverSource.ts";
import { jonesLegacyMaintenanceSource } from "./jonesLegacyMaintenanceSource.ts";

// Native epochs and settled preparation are modeled here. The issuer, lock,
// durable transfer, receiver validation and capability expiry are production code.
const driver = String.raw`
import sys, json, os, pathlib, stat, types
payload = json.loads(pathlib.Path(sys.argv[1]).read_text())
root = pathlib.Path(sys.argv[2]).resolve(strict=True)
scenario = payload['case']
scope = {'__name__': 'owned_cold_receiver_fixture'}
exec(compile(payload['source'], 'maintenance.py', 'exec'), scope)
encode, sha, require = scope['encoded'], scope['sha'], scope['require']
Maintenance, Refused = scope['Maintenance'], scope['Refused']
epoch_a, epoch_b = '00000000-0000-4000-8000-000000000001', '00000000-0000-4000-8000-000000000002'
generation = '2' * 64
manifest = root / 'runtime/jones-active-install.json'
directory = manifest.parent / 'jones-maintenance' / generation
home = root
profile, database, gate = root / 'profile', root / 'userdata/state.sqlite', root / 'owned-gate'
for path in (directory / 'tools', database.parent, profile): path.mkdir(mode=0o700, parents=True, exist_ok=True)
def write(path, raw, mode=0o600):
    path.write_bytes(raw)
    path.chmod(mode)
write(database, b'preserved-database')
write(profile / 'Preferences', b'preserved-profile')
write(gate, b'preserved-gated-executable', 0o400)
original = {'owner': 'desktop', 'generation': '1' * 64, 'sourceSha': '3' * 40,
            'home': str(home), 'databasePath': str(database), 'profile': str(profile),
            'appPath': str(root / 'Old.app'), 'executablePath': str(root / 'Old.app/Contents/MacOS/Jones')}
candidate = {'handle': '4' * 64, 'sourceSha': '5' * 40, 'sourceTree': '6' * 40, 'appDigest': '7' * 64,
             'version': 'fixture', 'appPath': str(root / 'New.app'), 'executablePath': str(root / 'New.app/Contents/MacOS/Jones')}
bundle = {'maintenanceSha256': sha(b'owned-maintenance'), 'identityNativeSha256': sha(b'owned-native'), 'version': 'fixture'}
binding = {'stagedHandle': candidate['handle'], 'sourceSha': candidate['sourceSha'], 'sourceTree': candidate['sourceTree'],
           'appDigest': candidate['appDigest'], 'executorSha256': '8' * 64, 'artifactReceiptSha256': '9' * 64,
           'macAppReceiptSha256': 'a' * 64, 'bundleSha256': 'b' * 64}
plan = {'maintenanceGeneration': generation, 'originalManifest': encode(original).decode(),
        'originalManifestSha256': sha(encode(original)), 'bundle': bundle, 'candidateBinding': binding,
        'selectionPath': str(root / 'selection.json'), 'selectionSha256': 'c' * 64, 'lockIdentity': {'fixture': True}}
plan_raw = encode(plan) + b'\n'
journal = {'schema': 1, 'maintenanceGeneration': generation, 'planSha256': sha(plan_raw), 'phase': 'armed',
           'done': [str(gate)], 'intent': None, 'armedBootSessionUuid': epoch_a, 'cancel': None}
write(directory / 'plan.json', plan_raw, 0o400)
write(directory / 'journal.json', encode(journal) + b'\n')
write(directory.parent / 'current.json', encode({'schema': 1, 'generation': generation, 'planSha256': sha(plan_raw)}) + b'\n', 0o400)
write(directory / 'tools/maintenance.py', b'owned-maintenance', 0o500)
write(directory / 'tools/identity-native', b'owned-native', 0o500)
owner_manifest = dict(original, owner='maintenance', generation=generation, maintenance={'schema': 1, 'planSha256': sha(plan_raw)})
manifest_raw = encode(owner_manifest) + b'\n'
write(manifest, manifest_raw)
def load(self):
    self.directory, self.native = directory, directory / 'tools/identity-native'
    self.plan, self.original, self.bundle = json.loads(plan_raw), dict(original), dict(bundle)
    self.journal_raw = (directory / 'journal.json').read_bytes()
    self.journal = json.loads(self.journal_raw)
    self.gates = [{'path': str(gate), 'app': original['appPath']}]
    self.validate_cursor()
def verify(self):
    self.current_lock()
    self.validate_cursor()
    require(stat.S_IMODE(gate.stat().st_mode) == 0o400)
    require(manifest.read_bytes() == manifest_raw)
    scope['_cold_receiver_files'](self)
    return epoch_b
Maintenance.load, Maintenance.verify = load, verify
def prepare_fixture(session):
    owner, record = session.owner, session.record
    state = {'transaction': {'schema': 1, 'transactionId': 'd' * 64, 'intentSha256': 'e' * 64},
             'pairSha256': 'f' * 64, 'reserve': {'identity': [1], 'bytes': 4096}, 'reserveState': 'held',
             'planSha256': 'a' * 64, 'envelopeSha256': 'b' * 64, 'evidenceSha256': 'c' * 64,
             'registeredEvidenceSha256': 'c' * 64, 'custodySha256': 'd' * 64}
    tx = 'e' * 64
    receiving = dict(original, owner='desktop', generation=tx, transactionId=tx,
                     **{key: candidate[key] for key in ('version', 'sourceSha', 'sourceTree', 'appDigest')})
    receiving['executablePath'] = str(root / 'Old.app/Contents/MacOS/Jones')
    receiving_raw = encode(receiving) + b'\n'
    handoff = {'schema': 1, 'phase': 'intent', 'maintenanceGeneration': generation, 'maintenancePlanSha256': sha(plan_raw),
               'sessionId': record['sessionId'], 'bootSessionUuid': record['bootSessionUuid'],
               'lock': {'native': plan['lockIdentity'], 'descriptor': list(scope['identity'](os.fstat(session.held['fd'])))},
               'source': {'originalManifestSha256': plan['originalManifestSha256'], 'selectionPath': plan['selectionPath'],
                          'selectionSha256': plan['selectionSha256'], 'candidateBinding': binding},
               'state': state, 'desktop': {'protocol': 1, 'startupGateProtocol': 1, 'transactionId': tx,
                                          'candidate': candidate, 'listener': 'http://127.0.0.1:4000'},
               'maintenanceOwner': manifest_raw.decode(), 'maintenanceOwnerSha256': sha(manifest_raw),
               'receivingOwner': receiving_raw.decode(), 'receivingOwnerSha256': sha(receiving_raw)}
    owner.save(stateTransaction=state['transaction'], desktopHandoff=handoff)
    target = root / 'runtime/jones-updates/transactions' / tx
    target.mkdir(mode=0o700, parents=True)
    receiver = scope['_cold_receiver_value'](owner, 'f' * 64,
                  list(scope['identity'](target.parent.stat(), True)), list(scope['identity'](target.stat(), True)))
    for name in ('intent', 'journal'): write(target / (name + '.json'), receiver[name].encode())
    owner.save(desktopReceiver=dict(receiver, phase='prepared'))
    # This fixture begins at an already-settled preparation boundary. It does not claim child or SQLite qualification.
    process = types.SimpleNamespace(spawn_unknown=False, reaped=True, closed=True)
    session.worker = types.SimpleNamespace(settled=True, failed=False, process=process, observers=[], drain=lambda: None)
    session._handoff_value = lambda listener, transaction: handoff
    session.boundary()
    return target
def refused(fn):
    try: fn()
    except (Refused, KeyError, TypeError, OSError): return
    raise AssertionError('Expected held refusal')
real_save = Maintenance.save
session = receiver = None
prepared = fault_fired = False
def fail_save(self, **updates):
    global fault_fired
    transfer = updates.get('desktopReceiver', {}).get('transfer')
    if transfer is not None:
        assert session.accepting is False
        if scenario == 'before-intent':
            fault_fired = True
            raise OSError('before transfer publication')
        if scenario == 'after-intent' and transfer['phase'] == 'transfer-intent':
            real_save(self, **updates)
            fault_fired = True
            raise OSError('lost transfer publication response')
        if scenario == 'after-advance' and transfer['phase'] == 'transferred-held':
            real_save(self, **updates)
            fault_fired = True
            raise OSError('lost transfer completion response')
    return real_save(self, **updates)
Maintenance.save = fail_save
expected_refusal = scenario in ('double-transfer', 'cold-reuse', 'unsettled-worker', 'unknown-spawn', 'unreaped-worker',
                                'before-intent', 'after-intent', 'after-advance', 'owner-drift', 'journal-drift', 'unknown-field')
try:
    with scope['fixed_lock'](manifest):
        with scope['_cold_owner_session']() as session:
            target = prepare_fixture(session)
            prepared = True
            if scenario == 'unsettled-worker': session.worker.settled = False
            if scenario == 'unknown-spawn': session.worker.process.spawn_unknown = True
            if scenario == 'unreaped-worker': session.worker.process.reaped = False
            receiver = session.transfer_receiver()
            assert not session.accepting
            assert session.owner.journal['desktopReceiver']['transfer']['phase'] == 'transfer-intent'
            if scenario == 'binding-matrix':
                current = session.owner.journal['desktopReceiver']['transfer']
                session.owner.journal['desktopReceiver']['transfer'] = None
                refused(lambda: scope['_validate_cold_receiver_transfer'](session.owner))
                for field, changed in [('schema', True), ('phase', 'activated'), ('extra', True)] + [
                    (name, '0' * len(current[name])) for name in ('transferId', 'sessionId', 'maintenanceGeneration',
                    'planSha256', 'handoffSha256', 'receiverIntentSha256', 'receiverJournalSha256', 'trialOwnerSha256',
                    'finalOwnerSha256', 'pairSha256', 'preparationEvidenceSha256', 'custodySha256', 'transactionId',
                    'stagedHandle', 'sourceSha', 'sourceTree', 'appDigest', 'artifactReceiptSha256', 'macAppReceiptSha256',
                    'bundleSha256', 'executorSha256') if name != 'transferId']:
                    session.owner.journal['desktopReceiver']['transfer'] = dict(current, **{field: changed})
                    refused(lambda: scope['_validate_cold_receiver_transfer'](session.owner))
                session.owner.journal['desktopReceiver']['transfer'] = current
            if scenario == 'double-transfer': session.transfer_receiver()
            if scenario == 'cold-reuse': session.snapshot()
            if scenario == 'owner-drift': write(manifest, encode(dict(owner_manifest, generation='0' * 64)))
            if scenario == 'journal-drift': write(directory / 'journal.json', b'{}')
            if scenario == 'unknown-field':
                previous = session.owner.journal['desktopReceiver']
                session.owner.save(desktopReceiver=dict(previous, transfer=dict(previous['transfer'], activationAllowed=True)))
            if scenario != 'intent-only':
                report = receiver.advance()
                assert report['status'] == 'receiver-transferred-held'
                before = (directory / 'journal.json').read_bytes()
                assert receiver.advance() == report
                assert (directory / 'journal.json').read_bytes() == before
            report = receiver.reconcile()
            assert report['activationAllowed'] is False and report['stateAdmissionAllowed'] is False
            assert report['receiverEffectsAllowed'] is False and report['candidateCustody'] == report['backendCustody'] == 'unqualified'
except (Refused, OSError, KeyError, TypeError):
    assert expected_refusal and prepared, scenario
else:
    assert not expected_refusal, scenario
if scenario in ('before-intent', 'after-intent', 'after-advance'): assert fault_fired
if receiver is not None: refused(receiver.reconcile)
if scenario in ('success', 'intent-only', 'after-intent', 'after-advance'):
    retained = (directory / 'journal.json').read_bytes()
    with scope['fixed_lock'](manifest):
        recovered = scope['_cold_receiver_reconcile']()
        assert recovered['status'] in ('receiver-transfer-intent', 'receiver-transferred-held')
        assert recovered['receiverEffectsAllowed'] is False
    assert (directory / 'journal.json').read_bytes() == retained
    try:
        with scope['fixed_lock'](manifest):
            with scope['_cold_owner_session'](): raise AssertionError('Cold owner re-entered after handoff')
    except Refused: pass
    else: raise AssertionError('Cold owner re-entry was not held')
if scenario == 'corrupt-recovery':
    retained = json.loads((directory / 'journal.json').read_bytes())
    retained['desktopReceiver']['transfer']['sourceSha'] = '0' * 40
    write(directory / 'journal.json', encode(retained) + b'\n')
    try:
        with scope['fixed_lock'](manifest): scope['_cold_receiver_reconcile']()
    except Refused: pass
    else: raise AssertionError('Corrupt receiver was accepted')
assert database.read_bytes() == b'preserved-database'
assert (profile / 'Preferences').read_bytes() == b'preserved-profile'
assert gate.read_bytes() == b'preserved-gated-executable' and stat.S_IMODE(gate.stat().st_mode) == 0o400
if scenario != 'owner-drift': assert manifest.read_bytes() == manifest_raw
print(json.dumps({'case': scenario, 'passed': True, 'qualification': 'owned-mechanical-held-transfer',
                  'stateAdmissionAllowed': False, 'activationAllowed': False, 'nativeCustodyQualified': False}))
`;

describe("held cold receiver transfer", () => {
  it("is composed as definitions without changing the public maintenance operations", () => {
    expect(jonesLegacyMaintenanceSource).toContain(jonesColdReceiverSource);
    expect(jonesLegacyMaintenanceSource).toContain(
      "for operation in ('arm', 'reconcile', 'cancel'):",
    );
  });

  it.for([
    "success",
    "intent-only",
    "double-transfer",
    "cold-reuse",
    "unsettled-worker",
    "unknown-spawn",
    "unreaped-worker",
    "before-intent",
    "after-intent",
    "after-advance",
    "owner-drift",
    "journal-drift",
    "unknown-field",
    "corrupt-recovery",
    "binding-matrix",
  ])("preserves the held boundary for %s", { timeout: 30000 }, async (scenario, { signal }) => {
    await IdentityFixture.withLegacyIdentityScratch(async (root: string) => {
      const entry = NodePath.join(root, "receiver.py");
      const payload = NodePath.join(root, "receiver.json");
      await NodeFSP.writeFile(entry, driver);
      await NodeFSP.writeFile(
        payload,
        JSON.stringify({ source: jonesLegacyMaintenanceSource, case: scenario }),
      );
      const result = await IdentityFixture.runLegacyIdentityFixtureCommand(
        "/usr/bin/python3",
        ["-I", "-B", entry, payload, root],
        root,
        { signal },
      );
      expect(result.code, result.stderr).toBe(0);
      expect(result.stderr).toBe("");
      expect(JSON.parse(result.stdout.trim())).toEqual({
        case: scenario,
        passed: true,
        qualification: "owned-mechanical-held-transfer",
        stateAdmissionAllowed: false,
        activationAllowed: false,
        nativeCustodyQualified: false,
      });
    });
  });
});
