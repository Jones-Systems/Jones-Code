/** Private candidate custody definitions; the production cold receiver has no issuer yet. */
export const jonesColdTrialCustodySource = String.raw`
import json, os, selectors, signal, socket, subprocess, time

_COLD_TYPES = frozenset(('READY', 'DISPATCH_INTENT', 'BACKEND_CAPTURED', 'BACKEND_SETTLED', 'STOP_REQUEST', 'STOP_RESULT', 'TERMINAL_CLOSED'))
_COLD_BINDING_KEYS = frozenset(('transactionId', 'transferId', 'handoffSha256', 'sourceSha', 'sourceTree', 'artifactSha256', 'bootSessionUuid'))
_COLD_FRAME_LIMIT = 16384
_COLD_OUTPUT_LIMIT = 1048576

def _cold_custody_require(value):
    if not value: raise RuntimeError('Cold custody binding or lifetime requires reconciliation.')

def _cold_custody_binding(value):
    import re
    _cold_custody_require(isinstance(value, dict) and set(value) == _COLD_BINDING_KEYS)
    for key, item in value.items():
        pattern = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' if key == 'bootSessionUuid' else r'[0-9a-f]{%d}' % (40 if key in ('sourceSha', 'sourceTree') else 64)
        _cold_custody_require(isinstance(item, str) and re.fullmatch(pattern, item) is not None)
    _cold_custody_require(value['transactionId'] != value['transferId'])
    return dict(value)

def _cold_custody_identity(value):
    _cold_custody_require(isinstance(value, dict) and set(value) == {'pid', 'parentPid', 'uniqueId', 'parentUniqueId', 'pidVersion', 'uid'})
    for key in ('pid', 'parentPid', 'pidVersion', 'uid'):
        _cold_custody_require(type(value[key]) is int and (0 if key == 'uid' else 1) <= value[key] <= 9007199254740991)
    for key in ('uniqueId', 'parentUniqueId'):
        item = value[key]
        _cold_custody_require(isinstance(item, str) and item.isascii() and item.isdigit() and not item.startswith('0') and len(item) <= 20 and int(item) <= 18446744073709551615)
    return dict(value)

def _cold_trial_runtime():
    # A future private receiver issuer must bind source, native identity, generation and current fixed-lock ownership.
    raise RuntimeError('Cold candidate launch is not qualified.')

class _CapturedColdCandidate:
    def __init__(self):
        self.runtime = _cold_trial_runtime()
        self.runtime.check()
        self.binding = _cold_custody_binding(self.runtime.binding)
        self.child, self.channel, self.selector = None, None, None
        self.sent, self.received, self.output_bytes = 0, 0, 0
        self.buffer, self.frames = b'', []
        self.failure, self.started, self.spawn_unknown, self.reaped = None, False, False, False
        self.channel_eof, self.closed = False, set()
        self.backend_settled, self.terminal_closed = False, False
        self.ready_started, self.stop_requested = False, False
        self.events, self.streams = [], {}

    def poison(self, failure):
        if self.failure is None: self.failure = failure

    def start(self):
        _cold_custody_require(not self.started and self.child is None and self.failure is None)
        self.started = True
        parent = child = None
        try:
            self.runtime.check()
            parent, child = socket.socketpair()
            parent.setblocking(False)
            self.selector = selectors.DefaultSelector()
            self.channel = parent
            environment = dict(self.runtime.environment, T3CODE_JONES_COLD_CHANNEL_FD=str(child.fileno()))
            self.events.append('DISPATCH_INTENT')
            self.spawn_unknown = True
            # The executable is fixed by the private receiver runtime, never an argv or spawner supplied by an entry caller.
            self.child = subprocess.Popen([str(self.runtime.executable)], cwd=str(self.runtime.directory), env=environment,
                                          stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                          pass_fds=(child.fileno(), self.runtime.lock_fd), start_new_session=False)
            self.spawn_unknown = False
            for name, stream in (('channel', parent), ('stdout', self.child.stdout), ('stderr', self.child.stderr)):
                _cold_custody_require(stream is not None)
                os.set_blocking(stream.fileno(), False)
                self.streams[name] = stream
                self.selector.register(stream, selectors.EVENT_READ, name)
            self.events.append('BACKEND_CAPTURED')
            self.runtime.check()
        except BaseException as failure:
            self.poison(failure)
            raise
        finally:
            if child is not None: child.close()
            if self.child is None:
                if parent is not None: parent.close()
                if self.selector is not None: self.selector.close()

    def pump(self, seconds):
        _cold_custody_require(self.selector is not None)
        for key, _ in self.selector.select(max(0, seconds)):
            name, stream = key.data, key.fileobj
            try: chunk = os.read(stream.fileno(), _COLD_FRAME_LIMIT)
            except BlockingIOError: continue
            if not chunk:
                self.selector.unregister(stream)
                self.closed.add(name)
                stream.close()
                if name == 'channel':
                    self.channel_eof = True
                    if self.failure is None: _cold_custody_require(not self.buffer)
                continue
            if name != 'channel':
                self.output_bytes += len(chunk)
                _cold_custody_require(self.output_bytes <= _COLD_OUTPUT_LIMIT)
                continue
            if self.failure is not None: continue
            self.buffer += chunk
            _cold_custody_require(len(self.buffer) <= _COLD_FRAME_LIMIT)
            while b'\n' in self.buffer:
                line, self.buffer = self.buffer.split(b'\n', 1)
                value = json.loads(line)
                _cold_custody_require(isinstance(value, dict) and set(value) == {'protocol', 'binding', 'sequence', 'type', 'identity'})
                _cold_custody_require(type(value['protocol']) is int and value['protocol'] == 1 and type(value['sequence']) is int and value['sequence'] == self.received + 1)
                _cold_custody_require(value['type'] in _COLD_TYPES and _cold_custody_binding(value['binding']) == self.binding)
                if value['type'] in ('READY', 'BACKEND_CAPTURED'): _cold_custody_identity(value['identity'])
                else: _cold_custody_require(value['identity'] is None)
                _cold_custody_require(len(self.frames) < 16)
                self.received = value['sequence']
                self.frames.append(value)

    def receive(self, expected, timeout=5):
        try:
            _cold_custody_require(isinstance(expected, tuple) and expected and all(value in _COLD_TYPES for value in expected))
            _cold_custody_require(type(timeout) in (int, float) and 0 < timeout <= 30)
            deadline = time.monotonic() + timeout
            while not self.frames:
                _cold_custody_require(self.failure is None and not self.channel_eof and time.monotonic() < deadline)
                self.pump(min(.05, deadline - time.monotonic()))
            _cold_custody_require(self.failure is None)
            value = self.frames.pop(0)
            _cold_custody_require(value['type'] in expected)
            if value['type'] == 'BACKEND_SETTLED':
                _cold_custody_require(not self.backend_settled and not self.terminal_closed)
                self.backend_settled = True
            if value['type'] == 'TERMINAL_CLOSED':
                _cold_custody_require(self.backend_settled and not self.terminal_closed)
                self.terminal_closed = True
            return value
        except BaseException as failure:
            self.poison(failure)
            raise

    def send(self, kind, identity=None, timeout=5):
        try:
            _cold_custody_require(self.failure is None and not self.channel_eof and kind in _COLD_TYPES)
            _cold_custody_require(type(timeout) in (int, float) and 0 < timeout <= 30)
            if kind in ('READY', 'BACKEND_CAPTURED'): _cold_custody_identity(identity)
            else: _cold_custody_require(identity is None)
            _cold_custody_require(self.sent < 9007199254740991)
            self.sent += 1
            raw = json.dumps(dict(protocol=1, binding=self.binding, sequence=self.sent, type=kind, identity=identity), separators=(',', ':')).encode() + b'\n'
            _cold_custody_require(len(raw) <= _COLD_FRAME_LIMIT)
            deadline = time.monotonic() + timeout
            while raw:
                _cold_custody_require(time.monotonic() < deadline)
                try:
                    count = self.channel.send(raw)
                    _cold_custody_require(count > 0)
                    raw = raw[count:]
                except BlockingIOError: self.pump(min(.01, deadline - time.monotonic()))
        except BaseException as failure:
            self.poison(failure)
            raise

    def ready(self):
        try:
            _cold_custody_require(not self.ready_started)
            self.ready_started = True
            value = self.receive(('READY',))
            _cold_custody_require(self.child is not None and value['identity']['pid'] == self.child.pid and value['identity']['parentPid'] == os.getpid())
            self.runtime.check()
            return value
        except BaseException as failure:
            self.poison(failure)
            raise

    def stop(self):
        self.events.append('STOP_REQUEST')
        self.stop_requested = True
        if self.child is not None and self.child.poll() is None:
            try: self.child.terminate()
            except BaseException as failure: self.poison(failure)
        return self.finish(timeout=.1)

    def finish(self, timeout=3):
        _cold_custody_require(type(timeout) in (int, float) and 0 < timeout <= 30)
        deadline = time.monotonic() + timeout
        while self.child is not None and time.monotonic() < deadline:
            try:
                if self.child.poll() is not None:
                    self.child.wait()
                    self.reaped = True
                if self.reaped and self.closed == {'channel', 'stdout', 'stderr'}: break
                self.pump(min(.05, deadline - time.monotonic()))
            except BaseException as failure: self.poison(failure)
        if self.child is not None and self.child.poll() is None:
            self.poison(RuntimeError('Captured candidate closure deadline expired.'))
            try:
                self.child.terminate()
                try: self.child.wait(timeout=.1)
                except subprocess.TimeoutExpired:
                    self.child.kill()
                    self.child.wait(timeout=1)
                self.reaped = True
            except BaseException as failure: self.poison(failure)
            drain_deadline = time.monotonic() + 1
            while self.closed != {'channel', 'stdout', 'stderr'} and time.monotonic() < drain_deadline:
                try: self.pump(.01)
                except BaseException as failure: self.poison(failure)
        returncode = self.child.returncode if self.child is not None else None
        exit_status = None if returncode is None else {'code': returncode if returncode >= 0 else None,
                                                     'signal': signal.Signals(-returncode).name if returncode < 0 else None}
        abnormal_exit = returncode != 0
        settled = self.child is not None and not self.spawn_unknown and self.reaped and self.closed == {'channel', 'stdout', 'stderr'}
        if settled:
            self.selector.close()
            if 'TERMINAL_CLOSED' not in self.events: self.events += ['BACKEND_SETTLED', 'TERMINAL_CLOSED']
        else: self.poison(RuntimeError('Captured candidate closure is incomplete.'))
        return {'exitStatus': exit_status, 'abnormalExit': abnormal_exit, 'stopRequested': self.stop_requested,
                'spawnUnknown': self.spawn_unknown, 'reaped': self.reaped, 'pipesClosed': self.closed == {'channel', 'stdout', 'stderr'},
                'settled': settled, 'held': True, 'lifecycleFailed': not settled or abnormal_exit or self.failure is not None or not (self.backend_settled and self.terminal_closed),
                'backendClosureReported': self.backend_settled and self.terminal_closed, 'events': list(self.events),
                'nativeIdentityQualified': False, 'chromiumWritersQualified': False, 'stateAdmissionAllowed': False,
                'activationAllowed': False, 'restorationAllowed': False}
`;
