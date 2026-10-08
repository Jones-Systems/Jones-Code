/** Kept outside the app bundle before handoff so quitting Electron cannot stop activation. */
export const jonesNativeHelperSource = String.raw`#!/usr/bin/env python3
import argparse, fcntl, hashlib, json, os, pathlib, signal, sqlite3, subprocess, sys, time, uuid, shutil, stat

PROTOCOL = 1
STARTUP_GATE_PROTOCOL = 1
SETTINGS = ('settings.json', 'desktop-settings.json', 'client-settings.json', 'saved-environments.json')
TRANSIENT_PROFILE = {'SingletonLock', 'SingletonCookie', 'SingletonSocket'}

def read(path):
    with open(path, 'r', encoding='utf8') as stream:
        return json.load(stream)

def durable(path, value, exclusive=False):
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    scratch = path.with_name(path.name + '.' + uuid.uuid4().hex)
    try:
        with open(scratch, 'x', encoding='utf8') as stream:
            os.chmod(scratch, 0o600)
            json.dump(value, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        if exclusive:
            os.link(scratch, path)
            scratch.unlink()
        else:
            os.replace(scratch, path)
        descriptor = os.open(path.parent, os.O_RDONLY)
        try: os.fsync(descriptor)
        finally: os.close(descriptor)
    finally:
        if scratch.exists(): scratch.unlink()

def digest(path):
    result = hashlib.sha256()
    with open(path, 'rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''): result.update(block)
    return result.hexdigest()

def app_digest(root, opaque_profile=False):
    root = pathlib.Path(root).resolve()
    result = hashlib.sha256()
    entries = sorted(root.rglob('*'), key=lambda value: value.relative_to(root).as_posix().encode('utf8'))
    if len(entries) > 200000: raise RuntimeError('Native app exceeds its file-count bound.')
    total_bytes = 0
    for entry in entries:
        relative = entry.relative_to(root).as_posix()
        info = entry.lstat()
        if entry.is_symlink():
            target = os.readlink(entry)
            if not opaque_profile and not entry.resolve().is_relative_to(root): raise RuntimeError('App symlink escapes the staged app.')
            record = ['link', relative, target]
        elif entry.is_file():
            total_bytes += info.st_size
            if total_bytes > 8 * 1024 * 1024 * 1024: raise RuntimeError('Native app exceeds its payload-size bound.')
            record = ['file', relative, info.st_mode & 0o777, digest(entry)]
        elif entry.is_dir(): record = ['directory', relative, info.st_mode & 0o777]
        else: raise RuntimeError('Unexpected app payload file type.')
        result.update((json.dumps(record, separators=(',', ':'), ensure_ascii=False) + '\n').encode())
    return result.hexdigest()

def sync_tree(directory):
    directory = pathlib.Path(directory)
    directories = [directory]
    for entry in directory.rglob('*'):
        if entry.is_symlink(): continue
        if entry.is_dir(): directories.append(entry)
        elif entry.is_file():
            descriptor = os.open(entry, os.O_RDONLY | os.O_NOFOLLOW)
            try: os.fsync(descriptor)
            finally: os.close(descriptor)
    for entry in reversed(directories):
        descriptor = os.open(entry, os.O_RDONLY)
        try: os.fsync(descriptor)
        finally: os.close(descriptor)

def same_binding(left, right):
    return left == right and left.get('protocol') == PROTOCOL and left.get('owner') == 'desktop'

def process_identity(pid):
    result = subprocess.run(['/bin/ps', '-p', str(pid), '-o', 'lstart=', '-o', 'command='], capture_output=True, text=True)
    if result.returncode != 0 or not result.stdout.strip(): return None
    return result.stdout.strip()

def alive(proof):
    identity = process_identity(proof['pid'])
    if identity is None: return False
    if identity != proof['identity']: raise RuntimeError('Process identity changed; signal withheld.')
    return True

def stop_exact(proofs):
    for proof in proofs:
        if alive(proof): os.kill(proof['pid'], signal.SIGTERM)
    deadline = time.monotonic() + 30
    while any(alive(proof) for proof in proofs):
        if time.monotonic() >= deadline: raise RuntimeError('Owned native writers did not stop; recovery held.')
        time.sleep(0.1)

def prove_quiescence(expected):
    # Profile and the whole userdata directory include SQLite WAL and settings writers.
    for directory in (str(pathlib.Path(expected['databasePath']).parent), expected['profile']):
        result = subprocess.run(['/usr/sbin/lsof', '-t', '+D', directory], capture_output=True, text=True)
        if result.returncode not in (0, 1): raise RuntimeError('Native file-writer inspection failed.')
        if result.stdout.strip(): raise RuntimeError('Native state still has open writers; recovery held.')

def preserve_restart_tunnel(expected, transaction_id):
    marker = pathlib.Path(expected['home']) / 'runtime' / 'desktop-update-restart'
    try:
        with open(marker, 'x', encoding='utf8') as stream:
            os.chmod(marker, 0o600)
            stream.write(transaction_id)
            stream.flush(); os.fsync(stream.fileno())
    except FileExistsError:
        pass  # Preserve an occupied marker; the backend consumes its own protocol marker.

def verify_app(staged, expected):
    if type(staged.get('startupGateProtocol')) is not int or staged['startupGateProtocol'] != STARTUP_GATE_PROTOCOL:
        raise RuntimeError('The staged candidate does not prove the required startup gate.')
    if app_digest(staged['appPath']) != staged['appDigest']: raise RuntimeError('Staged app integrity changed.')
    if digest(staged['executablePath']) != staged['executableDigest']: raise RuntimeError('Staged executable changed.')
    if digest(pathlib.Path(staged['appPath']) / 'Contents/Resources/app.asar') != staged['asarDigest']:
        raise RuntimeError('Staged ASAR changed.')
    receipt = read(staged['receiptPath'])
    if receipt['app'] != staged: raise RuntimeError('Staged handle no longer binds its receipt.')
    candidate = receipt['candidate']
    if candidate['repository'] != 'Jones-Systems/Jones-Code' or candidate['source'] != staged['sourceSha'] or candidate['tree'] != staged['sourceTree']:
        raise RuntimeError('Staged app is not source-qualified.')
    artifact = receipt['artifact']
    if artifact['candidate'] != candidate or candidate['installedSource'] != expected['sourceSha']:
        raise RuntimeError('Staged artifact does not descend the active source.')
    if digest(artifact['payloadPath']) != artifact['receipt']['sha256'] or digest(pathlib.Path(artifact['payloadPath']).parent / 'github-artifact.zip') != candidate['artifactDigest']:
        raise RuntimeError('Qualified outer artifact or native payload integrity changed.')
    if app_digest(expected['appPath']) != expected['appDigest']:
        raise RuntimeError('Previous app integrity changed; activation held.')

def pair_state(expected, directory):
    directory.mkdir(mode=0o700)
    database = pathlib.Path(expected['databasePath'])
    uri = database.as_uri() + '?mode=ro'
    with sqlite3.connect(uri, uri=True) as source, sqlite3.connect(directory / 'state.sqlite') as target:
        source.backup(target)
        if target.execute('PRAGMA integrity_check').fetchone()[0] != 'ok': raise RuntimeError('SQLite backup failed integrity check.')
    os.chmod(directory / 'state.sqlite', database.stat().st_mode & 0o777)
    presence = {}
    for name in SETTINGS:
        source = database.parent / name
        presence[name] = source.exists()
        if source.exists():
            if source.is_symlink() or not source.is_file(): raise RuntimeError('Unexpected settings file type.')
            shutil.copy2(source, directory / name)
    shutil.copytree(expected['profile'], directory / 'profile', symlinks=True,
                    ignore=lambda root, names: [name for name in names if name in TRANSIENT_PROFILE])
    sync_tree(directory)
    durable(directory / 'pair.json', {'expected': expected, 'settings': presence,
            'settingsDigests': {name: digest(directory / name) for name, exists in presence.items() if exists},
            'profileDigest': app_digest(directory / 'profile', True), 'databaseDigest': digest(directory / 'state.sqlite')}, True)

def restore_pair(expected, directory, advanced):
    pair = read(directory / 'pair.json')
    if pair['expected'] != expected or digest(directory / 'state.sqlite') != pair['databaseDigest']:
        raise RuntimeError('Recovery pair no longer binds the previous app and state.')
    if app_digest(directory / 'profile', True) != pair['profileDigest']:
        raise RuntimeError('Recovery profile integrity changed.')
    for name, expected_digest in pair['settingsDigests'].items():
        if name not in SETTINGS or digest(directory / name) != expected_digest:
            raise RuntimeError('Recovery settings integrity changed.')
    prove_quiescence(expected)
    advanced.mkdir(mode=0o700)
    database = pathlib.Path(expected['databasePath'])
    for name in (database.name, database.name + '-wal', database.name + '-shm') + SETTINGS:
        source = database.parent / name
        if source.exists(): os.rename(source, advanced / name)
    os.rename(expected['profile'], advanced / 'profile')
    shutil.copy2(directory / 'state.sqlite', database)
    for name, exists in pair['settings'].items():
        if exists: shutil.copy2(directory / name, database.parent / name)
    shutil.copytree(directory / 'profile', expected['profile'], symlinks=True)
    sync_tree(expected['profile'])
    for restored in [database] + [database.parent / name for name, exists in pair['settings'].items() if exists]:
        descriptor = os.open(restored, os.O_RDONLY | os.O_NOFOLLOW)
        try: os.fsync(descriptor)
        finally: os.close(descriptor)
    durable(advanced / 'retained.json', {'protocol': PROTOCOL, 'previous': expected}, True)

def launch(active, descriptor=None):
    env = os.environ.copy()
    # Provider stores and credentials stay on this host. No copied auth is consulted.
    env.pop('ELECTRON_RUN_AS_NODE', None)
    env['T3CODE_HOME'] = active['home']
    env['T3CODE_DESKTOP_USER_DATA_DIR'] = active['profile']
    env['T3CODE_JONES_ACTIVE_MANIFEST'] = str(manifest_path)
    env['T3CODE_JONES_ACTIVE_GENERATION'] = active['generation']
    if descriptor:
        env['T3CODE_JONES_TRIAL_DESCRIPTOR'] = str(descriptor)
        env['T3CODE_PORT'] = str(read(descriptor)['listener'].rsplit(':', 1)[1])
    else: env.pop('T3CODE_JONES_TRIAL_DESCRIPTOR', None)
    child = subprocess.Popen([active['executablePath']], env=env, stdin=subprocess.DEVNULL,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    identity = process_identity(child.pid)
    if identity is None: raise RuntimeError('Native app exited before its identity was captured; OS consent may be required.')
    return {'pid': child.pid, 'identity': identity}

def activate(intent_file):
    intent = read(intent_file)
    expected, staged = intent['expected'], intent['staged']
    tx = pathlib.Path(intent_file).parent
    journal_path = tx / 'journal.json'
    if journal_path.exists():
        journal = read(journal_path)
        if journal.get('intent') != intent: raise RuntimeError('Occupied journal belongs to a different install intent.')
        if journal['phase'] in ('resumed', 'rolled-back', 'blocked'): return
        journal.update(phase='blocked', message='Interrupted native activation requires reconciliation; automatic replay held.')
        durable(journal_path, journal)
        return
    if intent['protocol'] != PROTOCOL or not same_binding(read(manifest_path), expected):
        raise RuntimeError('Active binding changed; install refused.')
    verify_app(staged, expected)
    continuation = read(intent['continuationReceipt'])
    for field in ('transactionId', 'home', 'databasePath', 'profile', 'environmentId'):
        wanted = intent['transactionId'] if field == 'transactionId' else expected[field]
        if continuation.get(field) != wanted: raise RuntimeError('Continuation receipt has a stale native binding.')
    if continuation.get('protocol') != PROTOCOL or continuation.get('prepared') is not True:
        raise RuntimeError('Native continuations were not prepared.')
    journal = {'intent': intent, 'phase': 'intent'}
    def record(phase, **fields):
        journal.update(fields, phase=phase)
        durable(journal_path, journal)
    record('intent')
    candidate = None
    backend_process = None
    startup_unknown = False
    try:
        record('preparing')
        preserve_restart_tunnel(expected, intent['transactionId'])
        stop_exact(intent['processes'])
        prove_quiescence(expected)
        record('quiescent')
        pair_state(expected, tx / 'previous-pair')
        record('paired', pairedState=str(tx / 'previous-pair'))
        descriptor = {'protocol': PROTOCOL, 'startupGateProtocol': STARTUP_GATE_PROTOCOL, 'transactionId': intent['transactionId'],
                      'home': expected['home'], 'databasePath': expected['databasePath'],
                      'profile': expected['profile'], 'environmentId': expected['environmentId'],
                      'sourceSha': staged['sourceSha'], 'sourceTree': staged['sourceTree'],
                      'version': staged['version'], 'listener': intent['listener'],
                      'trialReceiptPath': str(tx / 'trial-receipt.json'), 'commitGrantPath': str(tx / 'commit-grant.json')}
        durable(tx / 'trial-descriptor.json', descriptor, True)
        next_active = dict(expected, generation=intent['transactionId'], transactionId=intent['transactionId'],
                           **{key: staged[key] for key in ('appPath', 'executablePath', 'version', 'sourceSha', 'sourceTree', 'appDigest')})
        record('trial')
        candidate = launch(next_active, tx / 'trial-descriptor.json')
        record('trial', candidateWriter=candidate)
        deadline = time.monotonic() + 90
        while not (tx / 'trial-receipt.json').exists():
            if not alive(candidate): raise RuntimeError('Candidate app stopped; native OS consent may be required.')
            if time.monotonic() >= deadline: raise RuntimeError('Candidate did not prove native readiness.')
            time.sleep(0.1)
        startup_unknown = True
        receipt = read(tx / 'trial-receipt.json')
        if not isinstance(receipt, dict) or type(receipt.get('startupGateProtocol')) is not int or receipt['startupGateProtocol'] != STARTUP_GATE_PROTOCOL:
            raise RuntimeError('Candidate startup dispatch has unknown effects; the required gate was not proved.')
        startup_unknown = False
        for key in ('protocol', 'startupGateProtocol', 'transactionId', 'home', 'databasePath', 'profile', 'environmentId', 'version', 'sourceSha', 'sourceTree', 'listener'):
            if receipt.get(key) != descriptor[key]: raise RuntimeError('Candidate trial identity does not match install intent.')
        if type(receipt.get('protocol')) is not int or type(receipt.get('startupGateProtocol')) is not int:
            raise RuntimeError('Candidate did not prove the required startup gate protocol.')
        if receipt.get('resumeHeld') is not True: raise RuntimeError('Candidate did not hold native continuations.')
        proof = receipt.get('backendProcess')
        if not isinstance(proof, dict) or set(proof) != {'pid', 'identity'} or type(proof.get('pid')) is not int or proof['pid'] <= 0 or not isinstance(proof.get('identity'), str) or not proof['identity'].strip():
            raise RuntimeError('Candidate did not prove the exact backend process.')
        backend_process = proof
        record('validated', trialReceipt=receipt)
        if not alive(backend_process): raise RuntimeError('Candidate backend stopped before commit.')
        if not same_binding(read(manifest_path), expected): raise RuntimeError('Active manifest changed before commit.')
        durable(manifest_path, next_active)
        record('committed')
        # Resume intent is durable before dispatch; a second helper cannot replay it.
        record('resume-intent')
        durable(tx / 'commit-grant.json', dict(generation=intent['transactionId'],
                    **{key: descriptor[key] for key in ('protocol', 'startupGateProtocol', 'transactionId', 'home', 'databasePath', 'profile', 'environmentId', 'version', 'sourceSha', 'sourceTree', 'listener')}), True)
        record('resumed')
    except Exception:
        active = read(manifest_path)
        if startup_unknown or (tx / 'resume-dispatched.json').exists() or journal['phase'] in ('committed', 'resume-intent', 'resumed') or active != expected or (journal['phase'] == 'trial' and candidate is None):
            record('blocked', message='Unknown activation, startup, or resume effect; binaries and advanced state retained.')
            return
        if not (tx / 'previous-pair' / 'pair.json').exists():
            if journal['phase'] == 'quiescent':
                # Snapshotting only reads native state. With no trial launched,
                # its unchanged prior app/state pair remains compatible.
                try:
                    prove_quiescence(expected)
                    launch(expected)
                    record('rolled-back', message='Snapshot preparation failed before any candidate launch; prior state was unchanged.')
                    return
                except Exception:
                    pass
            record('blocked', message='Preparation did not prove a complete recovery pair; previous binary and state retained.')
            return
        try:
            record('rollback-intent')
            proofs = [candidate] if candidate else []
            # Only a validated startup-gate receipt supplies the backend proof, independently of app ownership.
            if backend_process is not None: proofs.append(backend_process)
            preserve_restart_tunnel(expected, intent['transactionId'])
            stop_exact(proofs)
            prove_quiescence(expected)
            restore_pair(expected, tx / 'previous-pair', tx / 'advanced-state')
            launch(expected)
            record('rolled-back')
        except Exception:
            record('blocked', message='Paired recovery could not be proved; retained state requires reconciliation.')

parser = argparse.ArgumentParser()
parser.add_argument('--manifest', required=True)
parser.add_argument('--activate')
parser.add_argument('--launch', action='store_true')
parser.add_argument('--cli', action='store_true')
parser.add_argument('arguments', nargs=argparse.REMAINDER)
args = parser.parse_args()
if args.arguments and args.arguments[0] == '--': args.arguments = args.arguments[1:]
manifest_path = pathlib.Path(args.manifest).resolve(strict=True)
if sys.platform != 'darwin': raise SystemExit('The Jones native helper requires macOS.')
lock_path = manifest_path.with_name('jones-activation.lock')
try:
    descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    os.write(descriptor, b'jones-activation-lock-v1\n')
    os.fsync(descriptor)
except FileExistsError:
    descriptor = os.open(lock_path, os.O_RDWR | os.O_NOFOLLOW)
    info = os.fstat(descriptor)
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or os.read(descriptor, 256) != b'jones-activation-lock-v1\n':
        os.close(descriptor)
        raise SystemExit('An occupied native activation lock is unknown and was preserved.')
with os.fdopen(descriptor, 'r+', encoding='utf8') as lock:
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    active = read(manifest_path)
    if active.get('protocol') != PROTOCOL or active.get('owner') != 'desktop':
        raise SystemExit('Native launcher bootstrap is required.')
    if app_digest(active['appPath']) != active['appDigest']:
        raise SystemExit('The active app does not match its validated install manifest.')
    if args.activate: activate(args.activate)
    elif args.launch:
        transactions = manifest_path.parent / 'jones-updates' / 'transactions'
        if transactions.exists():
            for transaction in transactions.iterdir():
                journal = transaction / 'journal.json'
                if journal.exists():
                    if read(journal).get('phase') not in ('resumed', 'rolled-back'):
                        raise SystemExit('Native activation requires reconciliation before launch.')
                elif (transaction / 'intent.json').exists():
                    raise SystemExit('Unknown activation effects require reconciliation before launch.')
        prove_quiescence(active)
        launch(active)
    elif args.cli:
        if args.arguments not in (['--version'], ['--help']):
            raise SystemExit('This home is owned by desktop; the CLI refuses a parallel server writer.')
        env = dict(os.environ, ELECTRON_RUN_AS_NODE='1', T3CODE_HOME=active['home'])
        child = subprocess.run([active['executablePath'], str(pathlib.Path(active['appPath']) / 'Contents/Resources/app.asar/apps/server/dist/bin.mjs')] + args.arguments, env=env)
        raise SystemExit(child.returncode)
    else: raise SystemExit('Choose --activate, --launch, or --cli.')
`;
