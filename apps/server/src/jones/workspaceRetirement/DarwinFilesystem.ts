// @effect-diagnostics nodeBuiltinImport:off -- Node exposes no openat on Darwin. This optional adapter uses the system interpreter, never a shell or installed dependency.
import { execFile } from "node:child_process";

// Keep the complete protection/hash/recheck operation in one descriptor-owning
// process. A missing interpreter is unavailable evidence, never a fallback proof.
const PROGRAM = String.raw`
import os, sys, json, stat, time, hashlib
request = json.loads(sys.argv[1])
def sensitive(name):
    n = name.lower()
    return (n.startswith('.env') and not n.endswith(('.example','.sample','.template'))) or n.endswith(('.pem','.key','.p12')) or n.startswith('id_') or n in ('.netrc','.npmrc')
def snap(s):
    return ':'.join(str(x) for x in (s.st_dev,s.st_ino,s.st_mode,s.st_size,s.st_mtime_ns,s.st_ctime_ns))
def opened(root, relative, directory=False):
    parts = relative.split('/') if relative else []
    parent = os.dup(root)
    try:
        for i, part in enumerate(parts):
            if part in ('','.','..') or '\0' in part: raise ValueError('identity_mismatch')
            flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
            if i < len(parts)-1 or directory: flags |= os.O_DIRECTORY
            child = os.open(part, flags, dir_fd=parent)
            os.close(parent)
            parent = child
        result = parent
        parent = None
        return result
    finally:
        if parent is not None: os.close(parent)
def absolute(path, directory=False):
    if not os.path.isabs(path) or os.path.normpath(path) != path: raise ValueError('identity_mismatch')
    anchor = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
    try: return opened(anchor,path.lstrip('/'),directory)
    finally: os.close(anchor)
def metadata(root):
    entries, snapshots, reasons, nested, secret = [], {}, set(), [], []
    pending, logical, allocated = [''], 0, 0
    device = os.fstat(root).st_dev
    while pending:
        relative = pending.pop()
        directory = opened(root,relative,True)
        try:
            with os.scandir(directory) as children:
                for child in children:
                    if time.monotonic() > deadline or len(entries) >= limits['entries']: raise ValueError('bound_exceeded')
                    name = child.name
                    name.encode('utf-8') # Node cannot faithfully represent surrogateescape names.
                    path = relative+'/'+name if relative else name
                    info = child.stat(follow_symlinks=False)
                    if info.st_dev != device: raise ValueError('mount_boundary')
                    kind = 'file' if stat.S_ISREG(info.st_mode) else 'directory' if stat.S_ISDIR(info.st_mode) else 'symlink' if stat.S_ISLNK(info.st_mode) else 'special'
                    entry = dict(relpath=path,type=kind,size=info.st_size,mode=stat.S_IMODE(info.st_mode),mtime_ns=str(info.st_mtime_ns))
                    if sensitive(name): reasons.add('sensitive_ignored'); secret.append(path)
                    if name == '.git': reasons.add('nested_repository'); nested.append(path)
                    if kind == 'symlink':
                        entry['target'] = os.readlink(name,dir_fd=directory)
                        entry['target'].encode('utf-8')
                    elif kind == 'directory' and name != '.git' and not sensitive(name): pending.append(path)
                    elif kind == 'file': logical += info.st_size
                    elif kind == 'special': reasons.add('special_file')
                    allocated += getattr(info,'st_blocks',0)*512
                    entries.append(entry); snapshots[path] = snap(info)
                    if logical > limits['bytes']: raise ValueError('bound_exceeded')
        finally: os.close(directory)
    directories = {entry['relpath'] for entry in entries if entry['type']=='directory'}
    for entry in entries:
        if entry['type']=='file' and os.path.basename(entry['relpath'])=='HEAD':
            parent = os.path.dirname(entry['relpath']); prefix = parent+'/' if parent else ''
            if prefix+'objects' in directories and prefix+'refs' in directories:
                reasons.add('nested_repository'); nested.append(parent)
    return dict(entries=sorted(entries,key=lambda e:e['relpath']),snapshots=snapshots,reasons=sorted(reasons),logical=logical,allocated=allocated,nested=nested,sensitivePaths=secret)
def canonical(v): return json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=True).encode()
root = None
try:
    path = request['path']
    if request['operation'] == 'read':
        root = absolute(path)
        before = os.fstat(root)
        if not stat.S_ISREG(before.st_mode) or before.st_size > request['limit']: raise ValueError('bound_exceeded')
        chunks, count = [], 0
        while count <= before.st_size:
            chunk = os.read(root,min(1048576,before.st_size-count+1))
            if not chunk: break
            chunks.append(chunk); count += len(chunk)
        if count != before.st_size or snap(before) != snap(os.fstat(root)): raise ValueError('observation_failed')
        import base64
        print(json.dumps(dict(bytes=base64.b64encode(b''.join(chunks)).decode())))
    else:
        limits = request['limits']; deadline = time.monotonic()+limits['seconds']
        root = absolute(path,True)
        initial = snap(os.fstat(root))
        first = metadata(root)
        if request['operation'] == 'metadata': print(json.dumps(first))
        else:
            if any(sensitive(p) for p in path.split('/')): raise ValueError('sensitive_ignored')
            if '.git' in path.split('/'): raise ValueError('nested_repository')
            if not first['reasons']:
                for entry in first['entries']:
                    if entry['type'] != 'file': continue
                    file = opened(root,entry['relpath'])
                    try:
                        if snap(os.fstat(file)) != first['snapshots'][entry['relpath']]: raise ValueError('attestation_mismatch')
                        digest, count = hashlib.sha256(), 0
                        while True:
                            if time.monotonic() > deadline: raise ValueError('bound_exceeded')
                            chunk = os.read(file,min(1048576,entry['size']-count+1))
                            if not chunk: break
                            count += len(chunk)
                            if count > entry['size']: raise ValueError('attestation_mismatch')
                            digest.update(chunk)
                        if count != entry['size'] or snap(os.fstat(file)) != first['snapshots'][entry['relpath']]: raise ValueError('attestation_mismatch')
                        entry['sha256'] = digest.hexdigest()
                    finally: os.close(file)
                second = metadata(root)
                if second['snapshots'] != first['snapshots'] or second['reasons'] or snap(os.fstat(root)) != initial or snap(os.lstat(path)) != initial: raise ValueError('attestation_mismatch')
            print(json.dumps(first))
except Exception as error:
    print(json.dumps(dict(error=str(error) if isinstance(error,ValueError) else 'observation_failed')))
finally:
    if root is not None: os.close(root)
`;

export function darwinFilesystem(request: { operation: "metadata" | "content" | "read"; path: string; limits?: { entries: number; bytes: number; seconds: number }; limit?: number }): Promise<unknown> {
  return new Promise((resolve, reject) => {
    execFile("/usr/bin/python3", ["-B", "-c", PROGRAM, JSON.stringify(request)], {
      timeout: Math.ceil((request.limits?.seconds ?? 10) * 1000) + 1000,
      maxBuffer: 256 * 1024 * 1024,
      encoding: "utf8",
      env: { PATH: "/usr/bin:/bin", PYTHONDONTWRITEBYTECODE: "1" },
    }, (error, stdout) => {
      if (error) return reject(new Error("darwin_descriptor_adapter_unavailable"));
      try {
        const value = JSON.parse(stdout);
        if (value.error) reject(new Error(value.error));
        else resolve(value);
      } catch { reject(new Error("observation_failed")); }
    });
  });
}
