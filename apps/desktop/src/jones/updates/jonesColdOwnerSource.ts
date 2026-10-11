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
