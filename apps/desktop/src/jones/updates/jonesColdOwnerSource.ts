/** Internal cold-owner custody; the public maintenance entry cannot issue a state session. */
export const jonesColdOwnerSource = String.raw`
def _cold_owner_boot(value):
    require(isinstance(value, str))
    try: canonical = str(uuid.UUID(value))
    except (ValueError, AttributeError): raise Refused('Invalid native boot identity.')
    require(value.lower() == canonical)
    return canonical

def _validate_cold_owner_cursor(journal):
    if 'coldOwner' not in journal: return
    value = journal['coldOwner']
    require(journal['phase'] == 'armed' and isinstance(value, dict))
    require(set(value) == {'schema', 'sessionId', 'bootSessionUuid', 'phase'})
    require(type(value['schema']) is int and value['schema'] == 1)
    require(hex_value(value['sessionId'], 64) and value['phase'] in ('entered', 'finalized'))
    require(_cold_owner_boot(value['bootSessionUuid']) != _cold_owner_boot(journal['armedBootSessionUuid']))

def _cold_handoff_listener(value):
    import ipaddress
    require(isinstance(value, str))
    match = re.fullmatch(r'http://(127(?:\.[0-9]{1,3}){3}|\[::1\])(?::([0-9]{1,5}))?/?', value)
    require(match is not None)
    host, port = match.groups()
    try: address = ipaddress.ip_address(host.strip('[]'))
    except ValueError: raise Refused('Invalid trial loopback address.')
    require(address.is_loopback and 1 <= int(port or '80') <= 65535)
    # The future entry must bind this socket before trial; an intent proves no listener is live.
    return value

def _validate_cold_handoff_cursor(owner, proposed_intent=None):
    value = owner.journal.get('desktopHandoff') if proposed_intent is None else proposed_intent
    if value is None:
        require('desktopHandoff' not in owner.journal)
        return
    require(owner.journal['phase'] == 'armed' and 'coldOwner' in owner.journal and 'stateTransaction' in owner.journal)
    require(isinstance(value, dict) and set(value) == {'schema', 'phase', 'maintenanceGeneration', 'maintenancePlanSha256',
            'sessionId', 'bootSessionUuid', 'lock', 'source', 'state', 'desktop', 'maintenanceOwner', 'maintenanceOwnerSha256',
            'receivingOwner', 'receivingOwnerSha256'})
    require(type(value['schema']) is int and value['schema'] == 1 and value['phase'] == 'intent')
    require(value['maintenanceGeneration'] == owner.plan['maintenanceGeneration'] and value['maintenancePlanSha256'] == owner.journal['planSha256'])
    require(value['sessionId'] == owner.journal['coldOwner']['sessionId'] and value['bootSessionUuid'] == owner.journal['coldOwner']['bootSessionUuid'])
    lock, source, state, desktop = (value[key] for key in ('lock', 'source', 'state', 'desktop'))
    require(isinstance(lock, dict) and set(lock) == {'native', 'descriptor'} and lock['native'] == owner.plan['lockIdentity'])
    require(isinstance(lock['descriptor'], list) and len(lock['descriptor']) == 8 and all(type(part) is int and part >= 0 for part in lock['descriptor']))
    require(source == {'originalManifestSha256': owner.plan['originalManifestSha256'], 'selectionPath': owner.plan['selectionPath'],
                       'selectionSha256': owner.plan['selectionSha256'], 'candidateBinding': owner.plan['candidateBinding']})
    require(isinstance(state, dict) and set(state) == {'transaction', 'pairSha256', 'reserve', 'reserveState', 'planSha256', 'envelopeSha256',
            'evidenceSha256', 'registeredEvidenceSha256', 'custodySha256'})
    require(state['transaction'] == owner.journal['stateTransaction'] and state['reserveState'] == 'held')
    require(all(hex_value(state[key], 64) for key in ('pairSha256', 'planSha256', 'envelopeSha256', 'evidenceSha256', 'registeredEvidenceSha256', 'custodySha256')))
    require(isinstance(state['reserve'], dict) and set(state['reserve']) == {'identity', 'bytes'} and type(state['reserve']['bytes']) is int and state['reserve']['bytes'] > 0)
    require(isinstance(desktop, dict) and set(desktop) == {'protocol', 'startupGateProtocol', 'transactionId', 'candidate', 'listener'})
    require(type(desktop['protocol']) is int and desktop['protocol'] == 1 and type(desktop['startupGateProtocol']) is int and desktop['startupGateProtocol'] == 1)
    require(hex_value(desktop['transactionId'], 64) and desktop['transactionId'] not in (owner.original['generation'], owner.original.get('transactionId'), owner.plan['maintenanceGeneration'], state['transaction']['transactionId']))
    _cold_handoff_listener(desktop['listener'])
    candidate = desktop['candidate']
    require(isinstance(candidate, dict) and all(candidate.get(key) == owner.plan['candidateBinding'][binding] for key, binding in
            (('handle', 'stagedHandle'), ('sourceSha', 'sourceSha'), ('sourceTree', 'sourceTree'), ('appDigest', 'appDigest'))))
    require(candidate.get('version') == owner.bundle['version'])
    maintenance_raw, receiving_raw = value['maintenanceOwner'].encode(), value['receivingOwner'].encode()
    require(sha(maintenance_raw) == value['maintenanceOwnerSha256'] and decode(maintenance_raw) == owner.owner())
    proposed = dict(owner.original, owner='desktop', generation=desktop['transactionId'], transactionId=desktop['transactionId'],
                    **{key: candidate[key] for key in ('version', 'sourceSha', 'sourceTree', 'appDigest')})
    proposed['executablePath'] = str(absolute(owner.original['appPath']) / absolute(candidate['executablePath']).relative_to(absolute(candidate['appPath'])))
    require(sha(receiving_raw) == value['receivingOwnerSha256'] and receiving_raw == encoded(proposed) + b'\n')

def _cold_owner_bindings():
    # The issuer captures the real lock implementation, not a caller's assertion
    # of custody. No executable, callback, boot observation or receipt grants entry.
    underlying_lock = _maintenance_fixed_lock
    maintenance_type = Maintenance
    active_lock = active_session = None

    @contextlib.contextmanager
    def fixed_lock(manifest):
        nonlocal active_lock
        require(active_lock is None)
        with underlying_lock(manifest) as borrowed:
            held = {'manifest': manifest, 'pid': os.getpid(), 'live': True, 'failure': None, 'fd': borrowed['fd']}
            def current():
                try:
                    require(active_lock is held and held['live'] and os.getpid() == held['pid'])
                    if held['failure'] is not None:
                        raise Refused('Fixed-lock scope has an unresolved prior effect.') from held['failure']
                    borrowed['current']()
                except BaseException as failure:
                    if held['failure'] is None: held['failure'] = failure
                    raise
            held['current'] = current
            active_lock = held
            try:
                current()
                yield current
                current()
            except BaseException as failure:
                if held['failure'] is None: held['failure'] = failure
                raise
            finally:
                # Expire capabilities before the last parent close; never explicitly unlock a child's shared flock.
                if active_session is not None: active_session.live = False
                held['live'] = False
                active_lock = None

    class Session:
        def __init__(self, owner, held, record):
            self.owner, self.held, self.record = owner, held, record
            self.live, self.failure, self.operating, self.pair = True, None, False, None
            self.accepting, self.worker = True, None
            self.directory, self.native = owner.directory, owner.native
            self.plan_raw = read_owned(owner.directory / 'plan.json')
            self.owner_raw = read_owned(owner.manifest)
            self.fence = owner.journal.get('stateTransaction')
            self.fence = None if self.fence is None else encoded(self.fence)

        def poison(self, failure):
            if self.failure is None: self.failure = failure
            if self.held['failure'] is None: self.held['failure'] = failure

        def check(self, owner):
            try:
                require(self.live and active_session is self and active_lock is self.held)
                require(owner is self.owner and owner.current_lock is self.held['current'])
                require(owner.manifest == self.held['manifest'] and owner.directory == self.directory and owner.native == self.native)
                if self.failure is not None:
                    raise Refused('Cold-owner scope has an unresolved prior effect.') from self.failure
                owner.current_lock()
                owner.validate_cursor()
                require(owner.journal['phase'] == 'armed' and owner.journal.get('coldOwner') == self.record)
                require(read_owned(owner.directory / 'journal.json') == owner.journal_raw)
                require(decode(owner.journal_raw) == owner.journal)
                require(decode(read_owned(owner.manifest)) == owner.owner())
                require(read_owned(owner.manifest) == self.owner_raw)
                fence = owner.journal.get('stateTransaction')
                if self.fence is not None: require(fence is not None and encoded(fence) == self.fence)
                elif fence is not None: self.fence = encoded(fence)
                if self.pair is not None and self.pair.failure is not None:
                    raise Refused('Paired state has an unresolved prior effect.') from self.pair.failure
            except BaseException as failure:
                self.poison(failure)
                raise

        def boundary(self):
            try:
                owner = self.owner
                self.check(owner)
                require(read_owned(self.directory / 'plan.json') == self.plan_raw)
                require(sha(self.plan_raw) == owner.journal['planSha256'] and decode(self.plan_raw) == owner.plan)
                require(owner.original == decode(owner.plan['originalManifest'].encode()) and owner.bundle == owner.plan['bundle'])
                require(decode(read_owned(owner.root / 'current.json', 8192)) ==
                        {'schema': 1, 'generation': owner.plan['maintenanceGeneration'], 'planSha256': owner.journal['planSha256']})
                require(sha(read_owned(self.directory / 'tools/maintenance.py')) == owner.bundle['maintenanceSha256'])
                require(_cold_owner_boot(owner.verify()) == self.record['bootSessionUuid'])
                self.check(owner)
            except BaseException as failure:
                self.poison(failure)
                raise

        def operation(self, name):
            try:
                require(self.accepting and name in ('snapshot', 'begin_restore', 'reconcile_restore', 'prepare', 'recover_preparation') and not self.operating)
                self.boundary()
                require('desktopHandoff' not in self.owner.journal)
                if self.worker is not None: self.require_worker_settled()
                if name in ('prepare', 'recover_preparation'):
                    require(self.worker is None)
                    _preparation_bundle(self.owner.bundle)
                self.operating = True
                if self.pair is None: self.pair = _ColdPairTransaction(self.owner)
                if name in ('prepare', 'recover_preparation'):
                    if name == 'prepare': self.pair.snapshot()
                    result = _PreparationWorker().run(name == 'recover_preparation')
                else: result = getattr(self.pair, name)()
                self.boundary()
                return result
            except BaseException as failure:
                self.poison(failure)
                raise
            finally: self.operating = False

        def snapshot(self): return self.operation('snapshot')
        def begin_restore(self): return self.operation('begin_restore')
        def reconcile_restore(self): return self.operation('reconcile_restore')
        def prepare(self): return self.operation('prepare')
        def recover_preparation(self): return self.operation('recover_preparation')

        def _handoff_value(self, listener, transaction_id):
            self.boundary()
            require(self.worker is not None and self.pair is not None)
            self.require_worker_settled()
            pair, worker, owner = self.pair, self.worker, self.owner
            require(worker.work is None and pair.journal['phase'] == 'prepared' and pair.journal['reserveState'] == 'held')
            pair.check_roots()
            require(read_owned(pair.root / 'journal.json') == pair.journal_raw and decode(pair.journal_raw) == pair.journal)
            require(sha(read_owned(pair.root / 'intent.json')) == owner.journal['stateTransaction']['intentSha256'])
            pair.pair()
            plan_raw = read_owned(pair.root / 'preparation-plan.json', _PREPARATION_LIMITS['plan'])
            envelope_raw = read_owned(pair.root / 'preparation-envelope.json', 16384)
            envelope = decode(envelope_raw)
            require(_preparation_encoded(envelope) + b'\n' == envelope_raw)
            require(envelope['planSha256'] == sha(plan_raw) == pair.journal['planSha256'] and envelope['planBytes'] == len(plan_raw))
            require(envelope['maintenanceGeneration'] == owner.plan['maintenanceGeneration'] and envelope['maintenancePlanSha256'] == owner.journal['planSha256'])
            require(envelope['transactionId'] == pair.intent['transactionId'] and envelope['intentSha256'] == pair.journal['intentSha256'] and envelope['pairSha256'] == pair.journal['pairSha256'])
            require(envelope['runtimeSha256'] == worker.preparation['executable']['sha256'] and envelope['entrySha256'] == worker.preparation['entry']['sha256'])
            custody_raw = read_owned(worker.receipt_path)
            require(custody_raw == worker.receipt_raw and decode(custody_raw) == worker.receipt)
            require(worker.receipt['phase'] == 'exit-observed' and worker.receipt['sessionId'] == self.record['sessionId'])
            require(all(worker.receipt[key] == expected for key, expected in worker.process.evidence().items()))
            require(worker.receipt['observers'] == [command.evidence() for command in worker.observers])
            evidence_raw = read_owned(pair.root / ('preparation-evidence-' + self.record['sessionId'] + '.json'))
            evidence = decode(evidence_raw)
            require(set(evidence) == {'schema', 'sessionId', 'bootSessionUuid', 'planSha256', 'envelopeSha256', 'result', 'custodySha256'})
            require(type(evidence['schema']) is int and evidence['schema'] == 1 and _preparation_encoded(evidence) + b'\n' == evidence_raw)
            require(evidence['sessionId'] == self.record['sessionId'] and evidence['bootSessionUuid'] == self.record['bootSessionUuid'])
            require(evidence['planSha256'] == sha(plan_raw) and evidence['envelopeSha256'] == sha(envelope_raw) and evidence['custodySha256'] == sha(custody_raw))
            require(evidence['result']['outcome'] == 'applied' and evidence['result']['errorCode'] is None and evidence['result']['type'] == 'CLOSED_RESULT')
            require(evidence['result']['sessionId'] == self.record['sessionId'] and evidence['result']['planSha256'] == sha(plan_raw))
            if sha(evidence_raw) != pair.journal['evidenceSha256']:
                require(hex_value(envelope['originSessionId'], 64))
                registered = read_owned(pair.root / ('preparation-evidence-' + envelope['originSessionId'] + '.json'))
                require(sha(registered) == pair.journal['evidenceSha256'])
                require(decode(registered)['planSha256'] == sha(plan_raw) and decode(registered)['envelopeSha256'] == sha(envelope_raw))
            selection_raw = read_owned(absolute(owner.plan['selectionPath']))
            require(sha(selection_raw) == owner.plan['selectionSha256'])
            candidate = decode(selection_raw)['app']
            proposed = dict(owner.original, owner='desktop', generation=transaction_id, transactionId=transaction_id,
                            **{key: candidate[key] for key in ('version', 'sourceSha', 'sourceTree', 'appDigest')})
            proposed['executablePath'] = str(absolute(owner.original['appPath']) / absolute(candidate['executablePath']).relative_to(absolute(candidate['appPath'])))
            receiving_raw = encoded(proposed) + b'\n'
            return {'schema': 1, 'phase': 'intent', 'maintenanceGeneration': owner.plan['maintenanceGeneration'],
                    'maintenancePlanSha256': owner.journal['planSha256'], 'sessionId': self.record['sessionId'], 'bootSessionUuid': self.record['bootSessionUuid'],
                    'lock': {'native': owner.plan['lockIdentity'], 'descriptor': list(identity(os.fstat(self.held['fd'])))},
                    'source': {'originalManifestSha256': owner.plan['originalManifestSha256'], 'selectionPath': owner.plan['selectionPath'],
                               'selectionSha256': owner.plan['selectionSha256'], 'candidateBinding': owner.plan['candidateBinding']},
                    'state': {'transaction': owner.journal['stateTransaction'], 'pairSha256': pair.journal['pairSha256'], 'reserve': pair.reserve(), 'reserveState': 'held',
                              'planSha256': sha(plan_raw), 'envelopeSha256': sha(envelope_raw), 'evidenceSha256': sha(evidence_raw),
                              'registeredEvidenceSha256': pair.journal['evidenceSha256'], 'custodySha256': sha(custody_raw)},
                    'desktop': {'protocol': 1, 'startupGateProtocol': 1, 'transactionId': transaction_id, 'candidate': candidate, 'listener': listener},
                    'maintenanceOwner': self.owner_raw.decode(), 'maintenanceOwnerSha256': sha(self.owner_raw),
                    'receivingOwner': receiving_raw.decode(), 'receivingOwnerSha256': sha(receiving_raw)}

        def prepare_handoff(self, listener):
            try:
                require(self.accepting and not self.operating)
                self.boundary()
                listener = _cold_handoff_listener(listener)
                self.operating = True
                existing = self.owner.journal.get('desktopHandoff')
                transaction_id = existing['desktop']['transactionId'] if existing is not None else uuid.uuid4().hex + uuid.uuid4().hex
                value = self._handoff_value(listener, transaction_id)
                _validate_cold_handoff_cursor(self.owner, value)
                if existing is None:
                    self.owner.save(desktopHandoff=value)
                else: require(existing == value)
                self.owner.sync_evidence()
                self.boundary()
                require(self.owner.journal['desktopHandoff'] == self._handoff_value(listener, transaction_id))
                return maintenance_report('desktop-handoff-intent-held', transactionId=transaction_id, intentSha256=sha(encoded(value)))
            except BaseException as failure:
                self.poison(failure)
                raise
            finally: self.operating = False

        def require_worker_settled(self):
            if self.worker is None: return
            if self.failure is not None:
                raise Refused('Preparation custody cannot finalize an unresolved session.') from self.failure
            require(self.worker.settled and not self.worker.failed and self.worker.process is not None)
            require(all(not process.spawn_unknown and process.reaped and process.closed for process in
                        self.worker.observers + [self.worker.process]))

    @contextlib.contextmanager
    def enter():
        nonlocal active_session
        held, session = active_lock, None
        require(held is not None and active_session is None)
        owner = maintenance_type(held['manifest'], held['current'])
        try:
            owner.current_lock()
            # Construct and load only the owner named by this very lock. The
            # issuer accepts no caller-populated owner or admission callback.
            owner.load()
            require(owner.journal['phase'] == 'armed')
            # A published handoff belongs to receiving-side recovery, never a fresh cold transaction.
            require('desktopHandoff' not in owner.journal)
            boot = _cold_owner_boot(owner.verify())
            require(boot != _cold_owner_boot(owner.journal['armedBootSessionUuid']))
            previous = owner.journal.get('coldOwner')
            require(previous is None or boot != _cold_owner_boot(previous['bootSessionUuid']))
            require(previous is not None or 'stateTransaction' not in owner.journal)
            record = {'schema': 1, 'sessionId': uuid.uuid4().hex + uuid.uuid4().hex,
                      'bootSessionUuid': boot, 'phase': 'entered'}
            require(previous is None or record['sessionId'] != previous['sessionId'])
            # save includes atomic replacement, file/parent sync and exact readback.
            # Even an error after its write leaves this boot consumed on restart.
            owner.save(coldOwner=record)
            owner.sync_evidence()
            session = Session(owner, held, record)
            active_session = session
            session.boundary()
            yield session
            session.accepting = False
            session.require_worker_settled()
            session.boundary()
            owner.sync_evidence()
            # Finalization requires any admitted worker to be closed and reaped;
            # it never grants same-boot reacquisition or executable release.
            session.record = dict(record, phase='finalized')
            owner.save(coldOwner=session.record)
            owner.sync_evidence()
            session.boundary()
        except BaseException as failure:
            if session is not None:
                session.accepting = False
                session.poison(failure)
                if session.worker is not None:
                    try: session.worker.drain()
                    except BaseException as cleanup:
                        raise _PreparationFailure([failure, cleanup], str(session.worker.receipt_path)) from failure
            elif held['failure'] is None: held['failure'] = failure
            raise
        finally:
            if session is not None: session.accepting, session.live = False, False
            active_session = None

    class PairRuntime:
        def __init__(self, session): self.session = session
        def check(self, owner):
            require(self.session.operating)
            self.session.check(owner)

    def paired_runtime():
        require(active_session is not None and active_session.operating)
        active_session.check(active_session.owner)
        return PairRuntime(active_session)

    def preparation_runtime():
        require(active_session is not None and active_session.accepting and active_session.operating)
        active_session.check(active_session.owner)
        return active_session

    return fixed_lock, enter, paired_runtime, preparation_runtime
`;
