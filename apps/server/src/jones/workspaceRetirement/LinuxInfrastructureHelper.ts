// Fixed source is embedded so server bundling includes the helper without a runtime asset lookup.
// It reads only metadata and the normal numeric-UID D-Bus AUTH response; it never inspects unit bodies.
export const linuxInfrastructureHelper = String.raw`
import os, sys, stat, socket, struct, json, hashlib, time, subprocess, selectors, posixpath

POLICY = 'linux-systemd-user-manager-main/v1'
SOCKET = '/run/dbus/system_bus_socket'
BUS = 'org.freedesktop.DBus'
DBUS_PATH = '/org/freedesktop/DBus'
MANAGER = 'org.freedesktop.systemd1'
MANAGER_PATH = '/org/freedesktop/systemd1'
UNIT = MANAGER + '.Unit'
SERVICE = MANAGER + '.Service'

class Unknown(Exception):
    pass

def require(ok, reason='identity'):
    if not ok:
        raise Unknown(reason)

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)

def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()

def read_at(fd, name, cap=65536):
    child = os.open(name, os.O_RDONLY | os.O_CLOEXEC, dir_fd=fd)
    try:
        out = b''
        while len(out) <= cap:
            part = os.read(child, min(8192, cap + 1 - len(out)))
            if not part:
                return out.decode('utf-8', 'strict')
            out += part
        raise Unknown('bounds')
    finally:
        os.close(child)

def proc(fd, pid):
    fields = {}
    for line in read_at(fd, 'status').splitlines():
        if ':' in line:
            k, v = line.split(':', 1)
            if k in ('Pid', 'NSpid', 'Uid'):
                fields[k] = v.split()
    tail = read_at(fd, 'stat', 4096).rsplit(')', 1)[1].split()
    require(fields.get('Pid') == [str(pid)])
    require(fields.get('NSpid') == [str(pid)])
    uids = fields.get('Uid', [])
    require(len(uids) == 4 and all(x.isdecimal() for x in uids))
    require(len(tail) >= 20 and tail[1].isdecimal() and tail[19].isdecimal())
    meta = os.fstat(fd)
    return [str(pid), tail[19], tail[1], uids, fields['NSpid'], read_at(fd, 'cgroup'), str(meta.st_dev), str(meta.st_ino)]

def trust(path, socket_leaf=False):
    require(isinstance(path, str) and path.startswith('/') and '\x00' not in path, 'provenance')
    require(posixpath.normpath(path) == path, 'provenance')
    pending = path[1:].split('/')
    current = '/'
    result = []
    steps = 0
    while True:
        meta = os.lstat(current)
        require(meta.st_uid == 0, 'provenance')
        link = os.readlink(current) if stat.S_ISLNK(meta.st_mode) else None
        is_socket = not pending and socket_leaf
        require(link is not None or is_socket or meta.st_mode & 0o022 == 0, 'provenance')
        result.append([current, meta.st_dev, meta.st_ino, meta.st_uid, meta.st_mode, meta.st_ctime_ns, meta.st_mtime_ns, meta.st_size, link])
        if link is not None:
            steps += 1
            require(steps <= 32, 'bounds')
            resolved = posixpath.normpath(link if link.startswith('/') else posixpath.join(posixpath.dirname(current), link))
            pending = resolved[1:].split('/') + pending
            current = '/'
            continue
        if not pending:
            require(stat.S_ISSOCK(meta.st_mode) if socket_leaf else stat.S_ISREG(meta.st_mode), 'provenance')
            return result
        require(stat.S_ISDIR(meta.st_mode), 'provenance')
        component = pending.pop(0)
        steps += 1
        require(steps <= 32 and component not in ('', '.', '..'), 'bounds')
        current = posixpath.join(current, component)

class Observation:
    def __init__(self, milliseconds):
        self.deadline = time.monotonic() + milliseconds / 1000
        self.handles = []
        self.sockets = []
    def remaining(self):
        remaining = min(1.5, self.deadline - time.monotonic())
        require(remaining > 0, 'timeout')
        return remaining
    def close(self):
        for connection in self.sockets:
            connection.close()
        for fd in self.handles:
            os.close(fd)
    def anchor(self, pid):
        fd = os.open('/proc/' + str(pid), os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        self.handles.append(fd)
        return fd
    def bus(self, args):
        # No shell, PATH lookup, activation, polkit interaction, remote address or caller-selected method.
        timeout = self.remaining()
        child = subprocess.Popen(['/usr/bin/busctl', '--address=unix:path=' + SOCKET, '--auto-start=no', '--allow-interactive-authorization=no', '--timeout=' + str(timeout), '--json=short'] + args,
                                 stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env={'LC_ALL': 'C', 'PATH': '/usr/bin:/bin'})
        try:
            output = b''
            with selectors.DefaultSelector() as selector:
                selector.register(child.stdout, selectors.EVENT_READ)
                while True:
                    require(selector.select(self.remaining()), 'timeout')
                    part = os.read(child.stdout.fileno(), 8192)
                    if not part:
                        break
                    output += part
                    require(len(output) <= 65536, 'bounds')
            require(child.wait(timeout=self.remaining()) == 0, 'unavailable')
            rows = [json.loads(line) for line in output.decode('utf-8', 'strict').splitlines()]
            require(rows and all(isinstance(x, dict) and set(x) == {'type', 'data'} for x in rows), 'unavailable')
            return rows
        finally:
            if child.poll() is None:
                child.kill()
            child.wait()
            child.stdout.close()
    def broker_value(self, method, name, signature):
        rows = self.bus(['call', BUS, DBUS_PATH, BUS, method, 's', name])
        require(len(rows) == 1 and rows[0]['type'] == signature, 'identity')
        data = rows[0]['data']
        require(isinstance(data, list) and len(data) == 1, 'identity')
        return data[0]
    def owner(self):
        owner = self.broker_value('GetNameOwner', MANAGER, 's')
        require(isinstance(owner, str) and owner.startswith(':') and len(owner) <= 128)
        require(self.broker_value('GetConnectionUnixUser', owner, 'u') == 0)
        require(self.broker_value('GetConnectionUnixProcessID', owner, 'u') == 1)
        return owner
    def projection(self, owner, pid, uid):
        rows = self.bus(['call', owner, MANAGER_PATH, MANAGER + '.Manager', 'GetUnitByPID', 'u', str(pid)])
        require(len(rows) == 1 and rows[0]['type'] == 'o' and len(rows[0]['data']) == 1)
        path = rows[0]['data'][0]
        # Exact native object identity; no returned value can select another endpoint or method.
        expected = '/org/freedesktop/systemd1/unit/user_40' + str(uid) + '_2eservice'
        require(path == expected)
        unit_names = ['Id', 'LoadState', 'ActiveState', 'SubState', 'InvocationID', 'FragmentPath', 'DropInPaths', 'SourcePath', 'Transient', 'NeedDaemonReload']
        service_names = ['MainPID', 'ExecMainPID', 'User', 'ControlGroup']
        units = self.bus(['get-property', owner, path, UNIT] + unit_names)
        services = self.bus(['get-property', owner, path, SERVICE] + service_names)
        require(len(units) == len(unit_names) and len(services) == len(service_names), 'unavailable')
        values = {}
        types = ['s', 's', 's', 's', 'ay', 's', 'as', 's', 'b', 'b', 'u', 'u', 's', 's']
        for name, row, typ in zip(unit_names + service_names, units + services, types):
            require(row['type'] == typ, 'unavailable')
            values[name] = row['data']
        require(values['Id'] == 'user@' + str(uid) + '.service')
        require([values[x] for x in ('LoadState', 'ActiveState', 'SubState')] == ['loaded', 'active', 'running'])
        require(values['MainPID'] == pid and values['ExecMainPID'] == pid and values['User'] == str(uid))
        require(values['SourcePath'] == '' and values['Transient'] is False and values['NeedDaemonReload'] is False, 'provenance')
        invocation = values['InvocationID']
        require(isinstance(invocation, list) and len(invocation) == 16 and all(type(x) is int and 0 <= x <= 255 for x in invocation) and any(invocation))
        require(values['FragmentPath'] in ('/usr/lib/systemd/system/user@.service', '/lib/systemd/system/user@.service'), 'provenance')
        drops = values['DropInPaths']
        require(isinstance(drops, list) and len(drops) + 1 <= 32, 'bounds')
        require(len(set(drops)) == len(drops), 'provenance')
        for path in drops:
            require(isinstance(path, str) and path.endswith('.conf') and any(path.startswith(root) for root in ('/etc/systemd/system/', '/run/systemd/system/', '/usr/lib/systemd/system/', '/lib/systemd/system/')), 'provenance')
        require(isinstance(values['ControlGroup'], str) and values['ControlGroup'].startswith('/') and values['ControlGroup'] != '/')
        return values
    def authentication(self):
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sockets.append(connection)
        connection.settimeout(self.remaining())
        connection.setsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED, 1)
        connection.connect(SOCKET)
        connection.sendall(b'\0AUTH EXTERNAL ' + str(os.getuid()).encode().hex().encode() + b'\r\n')
        response = b''
        sender = None
        while not response.endswith(b'\r\n'):
            connection.settimeout(self.remaining())
            data, controls, flags, _ = connection.recvmsg(4096, socket.CMSG_SPACE(struct.calcsize('3i')))
            require(data and not flags & (socket.MSG_TRUNC | socket.MSG_CTRUNC))
            credentials = [struct.unpack('3i', value) for level, kind, value in controls if level == socket.SOL_SOCKET and kind == socket.SCM_CREDENTIALS and len(value) == struct.calcsize('3i')]
            require(len(credentials) == 1 and credentials[0][0] > 0)
            require(sender is None or sender == credentials[0])
            sender = credentials[0]
            response += data
            require(len(response) <= 4096, 'bounds')
        require(response.startswith(b'OK ') and len(response) == 37 and all(c in b'0123456789abcdefABCDEF' for c in response[3:35]))
        return sender

def verify(observation, pid, uid, expected):
    observer = observation.anchor(os.getpid())
    before_observer = proc(observer, os.getpid())
    require(before_observer[3] == [str(os.getuid())] * 4)
    candidate = proc(3, pid)
    require(digest(candidate) == expected and candidate[3] == [str(uid)] * 4)
    proc_root = os.stat('/proc')
    boot_fd = observation.anchor(os.getpid())
    boot = read_at(boot_fd, '../sys/kernel/random/boot_id', 128).strip()
    require(len(boot) == 36)
    tools = [trust('/usr/bin/python3'), trust('/usr/bin/busctl')]
    endpoint = trust(SOCKET, True)
    broker_pid, broker_uid, _ = observation.authentication()
    broker_fd = observation.anchor(broker_pid)
    broker = proc(broker_fd, broker_pid)
    require(broker[3] == [str(broker_uid)] * 4)
    def broker_check():
        require(observation.broker_value('GetConnectionUnixProcessID', BUS, 'u') == broker_pid)
        require(observation.broker_value('GetConnectionUnixUser', BUS, 'u') == broker_uid)
    broker_check()
    owner = observation.owner()
    manager_fd = observation.anchor(1)
    manager = proc(manager_fd, 1)
    require(manager[3] == ['0'] * 4)
    unit = observation.projection(owner, pid, uid)
    groups = candidate[5].splitlines()
    require(len(groups) == 1 and groups[0].startswith('0::'))
    group = groups[0][3:]
    require(group == unit['ControlGroup'] or group.startswith(unit['ControlGroup'] + '/'))
    paths = [unit['FragmentPath']] + unit['DropInPaths']
    provenance = [trust(path) for path in paths]
    require(provenance == [trust(path) for path in paths], 'changed')
    require(unit == observation.projection(owner, pid, uid), 'changed')
    require(owner == observation.owner(), 'changed')
    require(observation.authentication()[:2] == (broker_pid, broker_uid), 'changed')
    broker_check()
    require(endpoint == trust(SOCKET, True) and tools == [trust('/usr/bin/python3'), trust('/usr/bin/busctl')], 'changed')
    require(broker == proc(broker_fd, broker_pid) and manager == proc(manager_fd, 1), 'changed')
    require(candidate == proc(3, pid) and before_observer == proc(observer, os.getpid()), 'changed')
    require(boot == read_at(boot_fd, '../sys/kernel/random/boot_id', 128).strip(), 'changed')
    observation.remaining()
    # Observer PID/birth and connection unique names are ephemeral, never part of the stable basis.
    return digest([POLICY, boot, [proc_root.st_dev, proc_root.st_ino], candidate, broker, manager, unit, provenance, endpoint, tools])

def main():
    observation = None
    try:
        require(len(sys.argv) == 5 and all(x.isdecimal() for x in (sys.argv[1], sys.argv[2], sys.argv[4])))
        pid, uid, timeout = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[4])
        require(pid > 0 and 0 <= uid < 2**32 and 1 <= timeout <= 1500)
        expected = sys.argv[3]
        require(len(expected) == 64 and all(x in '0123456789abcdef' for x in expected))
        observation = Observation(timeout)
        result = {'kind': 'verified', 'basisDigest': verify(observation, pid, uid, expected)}
    except Unknown as error:
        reason = str(error)
        result = {'kind': 'unclassified', 'reason': reason if reason in ('identity', 'provenance', 'changed', 'bounds', 'timeout') else 'unavailable'}
    except (TimeoutError, subprocess.TimeoutExpired):
        result = {'kind': 'unclassified', 'reason': 'timeout'}
    except Exception:
        result = {'kind': 'unclassified', 'reason': 'unavailable'}
    finally:
        if observation is not None:
            observation.close()
    print(canonical(result))

if __name__ == '__main__':
    main()
`;
