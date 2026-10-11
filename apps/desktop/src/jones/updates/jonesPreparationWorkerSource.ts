/** Fixed captured-worker mechanics; only the live cold-owner closure supplies a session. */
export const jonesPreparationWorkerSource = String.raw`
import selectors, signal, time

_PREPARATION_LIMITS = {'header': 16384, 'plan': 8 * 1024 * 1024, 'stderr': 8192,
                       'observer': 262144, 'seconds': 120, 'drainSeconds': 5}
_PREPARATION_NAME = 'jones-offline-preparation'
_PREPARATION_SOURCE_INPUTS = {'pnpm-lock.yaml', 'pnpm-workspace.yaml', 'package.json',
    'apps/server/package.json', 'apps/desktop/package.json', 'apps/web/package.json',
    'packages/contracts/package.json', 'apps/server/vite.config.ts', 'vite.config.ts',
    'scripts/jones/updates/offline-preparation-build.ts',
    'apps/server/src/jones/updates/offlinePreparationEntry.ts',
    'apps/server/src/jones/updates/offlinePreparation.ts',
    'apps/server/src/jones/updates/offlinePreparationCore.ts',
    'apps/server/src/jones/updates/offlinePreparationPlan.ts'}

def _preparation_encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), sort_keys=True, allow_nan=False).encode('utf8')

def _preparation_bundle(bundle):
    require(set(bundle) == BUNDLE_V2_KEYS | {'preparation'} and type(bundle['schema']) is int and bundle['schema'] == 3)
    require(bundle['repository'] == 'Jones-Systems/Jones-Code' and hex_value(bundle['sourceSha'], 40) and hex_value(bundle['sourceTree'], 40))
    require(isinstance(bundle['version'], str) and len(bundle['version']) <= 128 and re.fullmatch('[0-9]+\.[0-9]+\.[0-9]+-preview\.[0-9]{8}\.[0-9]+(?:\.[0-9]+)?', bundle['version']) is not None)
    require(all(hex_value(bundle[key], 64) for key in ('bootstrapSha256', 'executorSha256', 'maintenanceSha256', 'identityNativeSha256', 'identityNativeSourceSha256')))
    value = bundle['preparation']
    require(isinstance(value, dict) and set(value) == {'schema', 'protocol', 'provenanceSha256', 'sourceInputHashes', 'entry', 'seaConfig', 'executable', 'node', 'sqlite'})
    require(type(value['schema']) is int and value['schema'] == 1 and type(value['protocol']) is int and value['protocol'] == 1)
    require(hex_value(value['provenanceSha256'], 64))
    inputs = value['sourceInputHashes']
    require(isinstance(inputs, dict) and set(inputs) == _PREPARATION_SOURCE_INPUTS)
    require(all(hex_value(digest, 64) for digest in inputs.values()))
    for key, maximum in (('entry', 32 * 1024 * 1024), ('seaConfig', 16384), ('executable', 256 * 1024 * 1024)):
        item = value[key]
        require(isinstance(item, dict) and set(item) == ({'sha256', 'size', 'name'} if key == 'executable' else {'sha256', 'size'}))
        require(hex_value(item['sha256'], 64) and integer(item['size']) and item['size'] <= maximum)
    require(value['executable']['name'] == _PREPARATION_NAME)
    config = b'{"main":"offlinePreparationEntry.mjs","mainFormat":"module","executable":"node","output":"jones-offline-preparation","disableExperimentalSEAWarning":true,"useSnapshot":false,"useCodeCache":false,"execArgv":[],"execArgvExtension":"none"}\n'
    require(value['seaConfig'] == {'sha256': sha(config), 'size': len(config)})
    node = value['node']
    require(isinstance(node, dict) and set(node) == {'version', 'platform', 'architecture', 'archiveSha256', 'archiveBytes', 'checksumsSha256', 'runtimeSha256', 'runtimeBytes'})
    require(node['version'] == '26.8.2' and node['platform'] == 'darwin' and node['architecture'] == bundle['nativeArchitecture'] == 'arm64')
    require(all(hex_value(node[key], 64) for key in ('archiveSha256', 'checksumsSha256', 'runtimeSha256')))
    require(integer(node['archiveBytes']) and node['archiveBytes'] <= 128 * 1024 * 1024 and integer(node['runtimeBytes']) and node['runtimeBytes'] <= 256 * 1024 * 1024)
    sqlite = value['sqlite']
    require(isinstance(sqlite, dict) and set(sqlite) == {'version', 'sourceId'})
    require(isinstance(sqlite['version'], str) and len(sqlite['version']) <= 11 and re.fullmatch('(?:0|[1-9][0-9]{0,2})\.(?:0|[1-9][0-9]{0,2})\.(?:0|[1-9][0-9]{0,2})', sqlite['version']) is not None)
    require(isinstance(sqlite['sourceId'], str) and len(sqlite['sourceId']) == 84 and re.fullmatch('[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} [a-f0-9]{64}', sqlite['sourceId']) is not None)
    parts = [int(part) for part in re.split('[- :]', sqlite['sourceId'][:19])]
    # ECMAScript's canonical source timestamp also admits astronomical year zero.
    try: datetime.datetime(parts[0] or 400, *parts[1:])
    except ValueError: raise Refused('Preparation SQLite source timestamp is invalid.')
    return value

def _preparation_worker_runtime():
    raise Refused('A captured preparation worker requires the actual live cold owner.')

class _PreparationProcess:
    # Every process retains its real Popen handle. PID values in reports never authorize adoption.
    def __init__(self, args, descriptors, output_limit, child_input=True):
        self.child, self.selector, self.closed = None, selectors.DefaultSelector(), False
        self.output, self.error, self.pending = bytearray(), bytearray(), bytearray()
        self.eof, self.signals, self.reaped = set(), [], False
        self.output_limit, self.input_open = output_limit, child_input
        self.args, self.descriptors = args, tuple(descriptors)

    def start(self):
        # The owning registry must capture this object before even pipe setup can fail.
        require(self.child is None and not self.closed)
        self.child = subprocess.Popen(self.args, stdin=subprocess.PIPE if self.input_open else subprocess.DEVNULL,
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0,
                                      shell=False, close_fds=True, pass_fds=self.descriptors,
                                      env={'PATH': '/usr/bin:/bin', 'LANG': 'C', 'LC_ALL': 'C'})
        for label, stream in (('out', self.child.stdout), ('err', self.child.stderr)):
            os.set_blocking(stream.fileno(), False)
            self.selector.register(stream, selectors.EVENT_READ, label)
        if self.input_open: os.set_blocking(self.child.stdin.fileno(), False)

    def pump(self, deadline):
        require(time.monotonic() < deadline)
        for key, events in self.selector.select(min(0.1, max(0, deadline - time.monotonic()))):
            if key.data == 'in':
                try: count = os.write(key.fd, self.pending)
                except BlockingIOError: continue
                require(count > 0)
                del self.pending[:count]
                if not self.pending: self.selector.unregister(key.fileobj)
                continue
            try: chunk = os.read(key.fd, 65536)
            except BlockingIOError: continue
            if not chunk:
                self.eof.add(key.data)
                self.selector.unregister(key.fileobj)
                continue
            target = self.output if key.data == 'out' else self.error
            require(len(target) + len(chunk) <= (self.output_limit if key.data == 'out' else _PREPARATION_LIMITS['stderr']))
            target.extend(chunk)

    def send(self, raw, deadline):
        require(self.child is not None and self.input_open and not self.pending)
        self.pending.extend(raw)
        self.selector.register(self.child.stdin, selectors.EVENT_WRITE, 'in')
        while self.pending: self.pump(deadline)

    def read(self, count, deadline):
        require(0 <= count <= self.output_limit)
        while len(self.output) < count:
            require('out' not in self.eof)
            self.pump(deadline)
        result = bytes(self.output[:count])
        del self.output[:count]
        return result

    def finish(self, deadline):
        if self.input_open:
            self.child.stdin.close()
            self.input_open = False
        while self.eof != {'out', 'err'}: self.pump(deadline)
        code = self.child.wait(timeout=max(0.001, deadline - time.monotonic()))
        self.reaped = True
        return code

    def stop(self):
        if self.child is None or self.reaped: return
        # wait(), including after a failed signal, is the only reaping proof.
        for label, action in (('terminate', self.child.terminate), ('kill', self.child.kill)):
            try:
                if self.child.poll() is None: action()
                self.signals.append({'operation': label, 'outcome': 'returned'})
            except (OSError, ProcessLookupError) as failure:
                self.signals.append({'operation': label, 'outcome': 'failed', 'errno': failure.errno})
            try:
                self.child.wait(timeout=_PREPARATION_LIMITS['drainSeconds'])
                self.reaped = True
                return
            except subprocess.TimeoutExpired: pass
        raise Refused('Captured preparation process has unresolved custody.')

    def close(self):
        failures = []
        for stream in (() if self.child is None else (self.child.stdin, self.child.stdout, self.child.stderr)):
            if stream is not None:
                try: stream.close()
                except OSError as failure: failures.append(failure)
        try: self.selector.close()
        except OSError as failure: failures.append(failure)
        self.closed = not failures
        if failures: raise failures[0]

class _PreparationWorker:
    def __init__(self):
        self.session = _preparation_worker_runtime()
        self.session.check(self.session.owner)
        require(self.session.worker is None)
        self.session.worker = self
        self.owner, self.pair = self.session.owner, self.session.pair
        self.process, self.observers, self.tokens = None, [], None
        self.sequence, self.settled, self.failed = 0, False, False
        self.receipt_raw, self.work = None, None
        self.session_id = self.session.record['sessionId']
        self.preparation = _preparation_bundle(self.owner.bundle)
        self.runtime = self.owner.directory / 'tools' / _PREPARATION_NAME
        self.state = self.pair.root
        self.receipt_path = self.state / ('worker-custody-' + self.session_id + '.json')
        self.receipt = {'schema': 1, 'sessionId': self.session_id, 'pid': None, 'processes': None,
                        'phase': 'unspawned', 'reaped': False, 'streamsClosed': False,
                        'signals': [], 'failure': None, 'pidSignalAuthority': False}

    def save_receipt(self, phase):
        self.receipt['phase'] = phase
        if self.process is not None and self.process.child is not None:
            self.receipt.update(pid=self.process.child.pid, reaped=self.process.reaped,
                                streamsClosed=self.process.closed, signals=self.process.signals)
        raw = _preparation_encoded(self.receipt) + b'\n'
        durable_file(self.receipt_path, raw, 0o600, self.receipt_raw)
        self.receipt_raw = raw

    def check(self):
        require(not self.failed)
        self.session.check(self.owner)
        self.pair.check_roots()

    def frame(self, kind, **fields):
        return dict(schema=1, type=kind, sessionId=self.session_id, sequence=self.sequence, payloadBytes=0, **fields)

    def send(self, kind, **fields):
        self.check()
        raw = _preparation_encoded(self.frame(kind, **fields))
        require(len(raw) <= _PREPARATION_LIMITS['header'])
        self.process.send(struct.pack('>I', len(raw)) + raw, time.monotonic() + _PREPARATION_LIMITS['seconds'])

    def receive(self, kinds):
        deadline = time.monotonic() + _PREPARATION_LIMITS['seconds']
        count = struct.unpack('>I', self.process.read(4, deadline))[0]
        require(1 <= count <= _PREPARATION_LIMITS['header'])
        raw = self.process.read(count, deadline)
        value = decode(raw)
        require(_preparation_encoded(value) == raw)
        common = {'schema', 'type', 'sessionId', 'sequence', 'payloadBytes'}
        fields = {'READY': {'runtimeSha256', 'entrySha256'}, 'SQL_SCOPE_OPENED': {'scopeId', 'purpose', 'access'},
                  'PLAN_READY': {'planSha256'}, 'CLOSED_RESULT': {'scopeId', 'planSha256', 'outcome', 'errorCode'},
                  'EXIT': set(), 'ABORT': set()}
        require(value.get('type') in kinds and set(value) == common | fields[value['type']])
        require(type(value['schema']) is int and value['schema'] == 1 and value['sessionId'] == self.session_id)
        require(type(value['sequence']) is int and value['sequence'] == self.sequence)
        require(type(value['payloadBytes']) is int and 0 <= value['payloadBytes'] <= _PREPARATION_LIMITS['plan'])
        require(value['payloadBytes'] == 0 or value['type'] == 'PLAN_READY')
        payload = self.process.read(value['payloadBytes'], deadline)
        self.check()
        return value, payload

    def observe(self, database, capture=False, access=None):
        self.check()
        self.session.boundary()
        observed = Observation()
        require(observed.digest(self.owner.native, 16 * 1024 * 1024) == self.owner.bundle['identityNativeSha256'])
        with contextlib.ExitStack() as stack:
            args = [str(self.owner.native), 'capture-worker' if capture else 'observe-worker']
            if capture: args.append(str(self.process.child.pid))
            else:
                owner, worker = self.tokens
                args += [str(owner[key]) for key in ('uniqueId', 'pidVersion', 'parentUniqueId', 'uid')]
                args += [str(worker[key]) for key in ('pid', 'uniqueId', 'pidVersion', 'parentUniqueId', 'uid')]
            for fd in self.inherited: args += ['--exclude-worker-fd', str(fd)]
            descriptors, requested = [], []
            for role, suffix in (('database', ''), ('wal', '-wal'), ('shm', '-shm'), ('journal', '-journal')):
                path = pathlib.Path(str(database) + suffix)
                with observed.directory(path.parent) as parent:
                    info = self.pair.stat_at(parent, path.name)
                if info is None:
                    require(role != 'database')
                    continue
                require(role != 'journal')
                stream = stack.enter_context(observed.file(path, _COLD_PAIR_BOUNDS['bytes']))
                descriptors.append(stream.fileno())
                requested.append((role, path))
                args += ['--' + role, str(stream.fileno()), str(path)]
            command = _PreparationProcess(args, descriptors, _PREPARATION_LIMITS['observer'], False)
            self.observers.append(command)
            try:
                command.start()
                code = command.finish(time.monotonic() + 15)
                require(code == 0 and not command.error)
                result = decode(bytes(command.output))
            finally:
                try: command.stop()
                finally: command.close()
            observed.current()
        require(result.get('schema') == 1 and result.get('status') == 'observed' and result.get('operation') == args[1])
        require(result.get('bootSessionUuid') == self.session.record['bootSessionUuid'])
        for key in ('sqliteOpenProved', 'databaseSyncCloseProved', 'workerReaped', 'stateAdmissionAllowed', 'launchMembershipComplete', 'activationAllowed', 'familyMembershipComplete'):
            require(result.get(key) is False)
        require(result.get('observationOnly') is True and result.get('suppliedFamilyOnly') is True and result.get('processCustody') == 'unobserved')
        processes = result.get('processes')
        require(isinstance(processes, list) and len(processes) == 2)
        owner, worker = processes
        require(owner.get('role') == 'owner' and owner.get('pid') == os.getpid() and worker.get('role') == 'worker' and worker.get('pid') == self.process.child.pid)
        require(worker.get('parentPid') == os.getpid() and worker.get('parentUniqueId') == owner.get('uniqueId'))
        require(owner.get('uid') == worker.get('uid') == os.getuid())
        if capture:
            self.tokens = processes
            self.receipt['processes'] = processes
        else:
            for old, new in zip(self.tokens, processes):
                require(all(old[key] == new[key] for key in ('role', 'pid', 'parentPid', 'uniqueId', 'parentUniqueId', 'pidVersion', 'uid')))
        require(result.get('excludedWorkerFds') == sorted(self.inherited))
        require([(item.get('role'), item.get('object', {}).get('path')) for item in result.get('family', [])] == [(role, str(path)) for role, path in requested])
        matches = result.get('matches')
        require(isinstance(matches, list) and len(matches) <= 512)
        eligible = [item for item in matches if item.get('process') == 'worker' and item.get('eligibleOpenObservation') is True]
        if capture or access is None: require(not eligible)
        else:
            databases = [item for item in eligible if item.get('familyRole') == 'database']
            require(len(databases) == 1 and databases[0].get('access') == access)
        self.save_receipt('captured' if capture else 'sql-observed')
        self.session.boundary()

    def planning_family(self, pair):
        parent = self.state / 'preparation-work'
        if not self.pair.present(parent): self.pair.mkdir(parent)
        self.work = parent / self.session_id
        self.pair.mkdir(self.work)
        for role, suffix in (('database', ''), ('wal', '-wal'), ('shm', '-shm')):
            if pair['copied'][role] is not None:
                self.pair.copy(self.state / 'previous-pair' / role, self.work / ('state.sqlite' + suffix), False)
        self.pair.sync_directory(self.work)

    def clean_work(self):
        if self.work is None: return
        require(self.settled and self.process.reaped and self.process.closed)
        with Observation().directory(self.work) as parent:
            names = self.pair.names(parent)
            require(set(names) <= {'state.sqlite', 'state.sqlite-wal', 'state.sqlite-shm'})
            for name in names:
                info = self.pair.stat_at(parent, name)
                require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and info.st_nlink == 1)
                os.unlink(name, dir_fd=parent)
            sync_fd(parent)
        with Observation().directory(self.work.parent) as parent:
            os.rmdir(self.work.name, dir_fd=parent)
            sync_fd(parent)
        self.work = None

    def scope(self, purpose, database):
        opened, payload = self.receive({'SQL_SCOPE_OPENED'})
        access = 'read-only' if purpose == 'reconcile' else 'read-write'
        require(not payload and opened['purpose'] == purpose and opened['access'] == access and hex_value(opened['scopeId'], 64))
        self.observe(database, access=access)
        self.send('CONTINUE', scopeId=opened['scopeId'])
        return opened['scopeId']

    def finish(self):
        self.sequence += 1
        self.send('EXIT')
        _, payload = self.receive({'EXIT'})
        require(not payload)
        require(self.process.finish(time.monotonic() + _PREPARATION_LIMITS['drainSeconds']) == 0)
        require(not self.process.output and not self.process.error)
        self.process.close()
        self.settled = True
        self.save_receipt('exit-observed')
        self.session.boundary()

    def drain(self):
        failures = []
        for command in self.observers + ([] if self.process is None else [self.process]):
            try: command.stop()
            except BaseException as failure: failures.append(failure)
            try: command.close()
            except BaseException as failure: failures.append(failure)
        if failures: raise failures[0]

    def run(self, recovery):
        try:
            return self.perform(recovery)
        except BaseException as failure:
            self.failed = True
            self.session.poison(failure)
            self.receipt['failure'] = {'type': type(failure).__name__, 'errno': getattr(failure, 'errno', None)}
            try: self.drain()
            except BaseException as cleanup:
                self.receipt['cleanupFailure'] = {'type': type(cleanup).__name__, 'errno': getattr(cleanup, 'errno', None)}
            try: self.save_receipt('custody-held')
            except BaseException: pass
            raise

    def perform(self, recovery):
        self.check()
        proof = self.preparation['executable']
        require(Observation().digest(self.runtime, 256 * 1024 * 1024) == proof['sha256'])
        with Observation().file(self.runtime, 256 * 1024 * 1024) as stream:
            info = os.fstat(stream.fileno())
            require(info.st_size == proof['size'] and stat.S_IMODE(info.st_mode) == 0o500)
        pair = self.pair.pair()
        require(self.pair.journal['phase'] in ('preparation-intent', 'prepared') if recovery else self.pair.journal['phase'] == 'paired')
        if not recovery: self.planning_family(pair)
        dispatch = {'schema': 1, 'sessionId': self.session_id, 'bootSessionUuid': self.session.record['bootSessionUuid'],
                    'parentPid': os.getpid(), 'maintenanceGeneration': self.owner.plan['maintenanceGeneration'],
                    'maintenancePlanSha256': self.owner.journal['planSha256'], 'transactionId': self.pair.intent['transactionId'],
                    'intentSha256': self.pair.journal['intentSha256'], 'pairSha256': self.pair.journal['pairSha256'],
                    'runtimeSha256': proof['sha256'], 'entrySha256': self.preparation['entry']['sha256'],
                    'purpose': 'recover' if recovery else 'prepare'}
        durable_file(self.state / ('worker-' + self.session_id + '.json'), _preparation_encoded(dispatch) + b'\n', 0o400)
        self.save_receipt('spawn-intent')
        with Observation().directory(self.owner.directory) as directory_fd:
            self.inherited = sorted((self.session.held['fd'], directory_fd))
            self.session.boundary()
            self.process = _PreparationProcess([str(self.runtime), '--jones-preparation-worker-v1', '--lock-fd', str(self.session.held['fd']),
                                                '--owner-directory-fd', str(directory_fd)], self.inherited,
                                               _PREPARATION_LIMITS['plan'] + _PREPARATION_LIMITS['header'] + 4)
            self.process.start()
            self.save_receipt('spawned')
            ready, _ = self.receive({'READY'})
            require(ready['runtimeSha256'] == proof['sha256'] and ready['entrySha256'] == self.preparation['entry']['sha256'])
            self.observe(self.pair.database, capture=True)
            if self.work is not None: self.observe(self.work / 'state.sqlite')
            if not recovery:
                self.sequence += 1
                snapshot = pair['copied']['database']['content'][0][4]
                settings = pair['copied']['settings.json']
                settings_sha = sha(b'') if settings is None else settings['content'][0][4]
                stamp = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
                self.send('PLAN', pairSha256=self.pair.journal['pairSha256'], snapshotSha256=snapshot,
                          settingsSha256=settings_sha, planId=str(uuid.uuid4()), timestamp=stamp)
                scope_id = self.scope('plan', self.work / 'state.sqlite')
                result, plan_raw = self.receive({'PLAN_READY', 'CLOSED_RESULT'})
                if result['type'] == 'CLOSED_RESULT':
                    require(result['scopeId'] == scope_id and result['planSha256'] is None and result['outcome'] == 'query-failed' and result['errorCode'] == 'query-rejected')
                    self.finish()
                    self.clean_work()
                    return {'status': 'query-failed', 'workerReaped': True, 'activationAllowed': False}
                require(plan_raw and sha(plan_raw) == result['planSha256'])
                plan = decode(plan_raw)
                require(plan.get('snapshotSha256') == snapshot and plan.get('settingsSha256') == settings_sha and plan.get('timestamp') == stamp)
                durable_file(self.state / 'preparation-plan.json', plan_raw, 0o400)
                envelope = dict(schema=1, maintenanceGeneration=dispatch['maintenanceGeneration'], maintenancePlanSha256=dispatch['maintenancePlanSha256'],
                                transactionId=dispatch['transactionId'], intentSha256=dispatch['intentSha256'], pairSha256=dispatch['pairSha256'],
                                runtimeSha256=dispatch['runtimeSha256'], entrySha256=dispatch['entrySha256'], originSessionId=self.session_id,
                                originBootSessionUuid=dispatch['bootSessionUuid'], planSha256=sha(plan_raw), planBytes=len(plan_raw),
                                snapshotSha256=snapshot, settingsSha256=settings_sha)
                durable_file(self.state / 'preparation-envelope.json', _preparation_encoded(envelope) + b'\n', 0o400)
                self.pair.preparation_intent(envelope['planSha256'])
                require(self.pair.observe_members() == pair['source'])
            plan_raw = read_owned(self.state / 'preparation-plan.json', _PREPARATION_LIMITS['plan'])
            envelope_raw = read_owned(self.state / 'preparation-envelope.json', 16384)
            envelope = decode(envelope_raw)
            require(_preparation_encoded(envelope) + b'\n' == envelope_raw)
            require(set(envelope) == {'schema', 'maintenanceGeneration', 'maintenancePlanSha256', 'transactionId', 'intentSha256', 'pairSha256', 'runtimeSha256', 'entrySha256', 'originSessionId', 'originBootSessionUuid', 'planSha256', 'planBytes', 'snapshotSha256', 'settingsSha256'})
            require(envelope['schema'] == 1 and envelope['planBytes'] == len(plan_raw) and envelope['planSha256'] == sha(plan_raw) == self.pair.journal['planSha256'])
            for key in ('maintenanceGeneration', 'maintenancePlanSha256', 'transactionId', 'intentSha256', 'pairSha256', 'runtimeSha256', 'entrySha256'): require(envelope[key] == dispatch[key])
            require(hex_value(envelope['originSessionId'], 64) and _cold_owner_boot(envelope['originBootSessionUuid']))
            self.sequence += 1
            self.session.boundary()
            self.send('RECONCILE' if recovery else 'APPLY', planSha256=envelope['planSha256'], envelopeSha256=sha(envelope_raw))
            scope_id = self.scope('reconcile' if recovery else 'apply', self.pair.database)
            result, _ = self.receive({'CLOSED_RESULT'})
            require(result['scopeId'] == scope_id and result['planSha256'] == envelope['planSha256'])
            require(result['outcome'] in ('applied', 'not-applied', 'held', 'query-failed'))
            require(result['errorCode'] == ('query-rejected' if result['outcome'] == 'query-failed' else None))
            self.save_receipt('sql-closed')
            self.finish()
            self.clean_work()
            if result['outcome'] == 'applied':
                evidence = dict(schema=1, sessionId=self.session_id, bootSessionUuid=dispatch['bootSessionUuid'],
                                planSha256=envelope['planSha256'], envelopeSha256=sha(envelope_raw), result=result,
                                custodySha256=sha(self.receipt_raw))
                raw = _preparation_encoded(evidence) + b'\n'
                path = self.state / ('preparation-evidence-' + self.session_id + '.json')
                durable_file(path, raw, 0o400)
                if self.pair.journal['phase'] == 'preparation-intent': self.pair.preparation_observed(sha(raw))
            return {'status': result['outcome'], 'workerReaped': True, 'activationAllowed': False}
`;
