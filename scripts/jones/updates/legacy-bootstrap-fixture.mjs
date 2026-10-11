import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { createJonesLegacyBootstrapBundle } from "./legacy-bootstrap-bundle.ts";
import {
  readPackagedBootstrapProof,
  reconstructBootstrapResources,
  verifyJonesPreparationRuntime,
} from "./legacy-bootstrap-after-pack.mjs";
import { cleanLaunchEnvironment, runOwnedChild, snapshotTree } from "./native-startup-fixture.mjs";
import { protectedMutations } from "./native-startup-observer.mjs";

const sha256 = (contents) => NodeCrypto.createHash("sha256").update(contents).digest("hex");
const json = (value) => JSON.stringify(value);
const writeJson = (filename, value) =>
  NodeFSP.writeFile(filename, json(value), { flag: "wx", mode: 0o600 });

// Closing drains the observer's process output. Validate that final history,
// including events received during close, before starting the separate bootstrap
// audit; its copy/staging activity is outside this FSEvents qualification.
export async function closeStartupObservation(observer, start, protectedPaths, evidence = {}) {
  await observer.close();
  const events = observer.events.slice();
  protectedMutations(events, []);
  const mutations = protectedMutations(events.slice(start), protectedPaths);
  evidence.protectedMutations = mutations.slice(0, 30);
  NodeAssert.deepEqual(
    mutations,
    [],
    "The packaged process changed protected state, including a transient change.",
  );
  return Object.freeze({
    scope: "packaged-normal-startup-refusal",
    historyLoss: false,
    eventCount: events.length,
    closedBeforeBootstrap: true,
  });
}

export async function verifyPackagedBootstrap(
  app,
  metadata,
  nativeProofRaw = process.env.JONES_LEGACY_BOOTSTRAP_NATIVE_PROOF,
) {
  const identity = {
    repository: metadata.jonesSource.repository,
    sourceSha: metadata.jonesSource.sha,
    sourceTree: metadata.jonesSource.tree,
    version: metadata.version,
  };
  let files;
  let runtime;
  if (nativeProofRaw === undefined) {
    files = createJonesLegacyBootstrapBundle(identity);
  } else {
    const proof = readPackagedBootstrapProof(nativeProofRaw, identity);
    const nativeSource = await NodeFSP.readFile(
      new URL(
        "../../../apps/desktop/src/jones/updates/jonesLegacyIdentityNative.c",
        import.meta.url,
      ),
    );
    NodeAssert.equal(sha256(nativeSource), proof.identityNativeSourceSha256);
    const nativePath = NodePath.join(app, "Contents/Resources/jones-bootstrap/identity-native");
    const info = await NodeFSP.lstat(nativePath);
    NodeAssert.ok(info.isFile() && info.nlink === 1 && info.size <= 4194304);
    const binary = Buffer.alloc(info.size);
    const handle = await NodeFSP.open(
      nativePath,
      NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW | NodeFS.constants.O_NONBLOCK,
    );
    try {
      const before = await handle.stat();
      NodeAssert.deepEqual(
        [before.dev, before.ino, before.size, before.mode, before.ctimeMs],
        [info.dev, info.ino, info.size, info.mode, info.ctimeMs],
      );
      let offset = 0;
      while (offset < binary.length) {
        const { bytesRead } = await handle.read(binary, offset, binary.length - offset, offset);
        NodeAssert.ok(bytesRead > 0);
        offset += bytesRead;
      }
      NodeAssert.equal((await handle.read(Buffer.alloc(1), 0, 1, offset)).bytesRead, 0);
      const after = await handle.stat();
      NodeAssert.deepEqual(
        [after.dev, after.ino, after.size, after.mode, after.ctimeMs],
        [info.dev, info.ino, info.size, info.mode, info.ctimeMs],
      );
    } finally {
      await handle.close();
    }
    NodeAssert.equal(sha256(binary), proof.identityNativeSha256);
    const { jonesLegacyMaintenanceSource } =
      await import("../../../apps/desktop/src/jones/updates/jonesLegacyMaintenanceSource.ts");
    const reconstructed = reconstructBootstrapResources(
      identity,
      {
        identityNative: binary,
        identityNativeSourceSha256: sha256(nativeSource),
        maintenanceSource: jonesLegacyMaintenanceSource,
      },
      proof,
    );
    files = reconstructed.files;
    runtime = reconstructed.runtime;
    NodeAssert.deepEqual(JSON.parse(files[2].content), proof);
  }
  const directory = NodePath.join(app, "Contents/Resources/jones-bootstrap");
  for (const file of files) {
    const filename = NodePath.join(app, "Contents", "Resources", file.relativePath);
    const info = await NodeFSP.lstat(filename);
    NodeAssert.ok(info.isFile() && !info.isSymbolicLink());
    NodeAssert.equal(info.mode & 0o777, file.mode);
    if (typeof file.content === "string")
      NodeAssert.equal(await NodeFSP.readFile(filename, "utf8"), file.content);
    else NodeAssert.deepEqual(await NodeFSP.readFile(filename), Buffer.from(file.content));
  }
  if (runtime)
    await verifyJonesPreparationRuntime(
      NodePath.join(app, "Contents/Resources", runtime.relativePath),
      runtime,
    );
  NodeAssert.deepEqual(
    (await NodeFSP.readdir(directory)).sort(),
    [
      ...files.map((file) => NodePath.basename(file.relativePath)),
      ...(runtime ? [NodePath.basename(runtime.relativePath)] : []),
    ].sort(),
  );
  return JSON.parse(files.find((file) => file.relativePath.endsWith("BUNDLE.json")).content);
}

// Encode the public app receipt format from actual fixture bytes. This creates
// fixture input; validation is always performed by the distributed bootstrap.
async function appDigest(root) {
  const entries = [];
  let total = 0;
  const visit = async (directory) => {
    for (const name of await NodeFSP.readdir(directory)) {
      const path = NodePath.join(directory, name);
      const info = await NodeFSP.lstat(path);
      const relative = NodePath.relative(root, path).split(NodePath.sep).join("/");
      NodeAssert.ok(entries.length < 200000);
      if (info.isSymbolicLink()) entries.push(["link", relative, await NodeFSP.readlink(path)]);
      else if (info.isDirectory()) {
        entries.push(["directory", relative, info.mode & 0o777]);
        await visit(path);
      } else {
        NodeAssert.ok(info.isFile());
        total += info.size;
        NodeAssert.ok(total <= 8 * 1024 ** 3);
        entries.push(["file", relative, info.mode & 0o777, await fileDigest(path)]);
      }
    }
  };
  await visit(root);
  entries.sort((left, right) => Buffer.compare(Buffer.from(left[1]), Buffer.from(right[1])));
  return { digest: sha256(entries.map((entry) => json(entry) + "\n").join("")), bytes: total };
}

async function fileDigest(filename) {
  const handle = await NodeFSP.open(filename, "r");
  try {
    const digest = NodeCrypto.createHash("sha256");
    for await (const chunk of handle.createReadStream()) digest.update(chunk);
    return digest.digest("hex");
  } finally {
    await handle.close();
  }
}

// The native fixture needs only a legacy bundle's binding metadata. It never
// copies or launches an installed application or uses real application state.
const makeLegacyAndArchive = String.raw`
import json, pathlib, plistlib, struct, sys, zipfile
config = json.loads(sys.argv[1])
app = pathlib.Path(config['app'])
(app / 'Contents/MacOS').mkdir(parents=True)
(app / 'Contents/Resources').mkdir()
(app / 'Contents/MacOS/legacy').write_bytes(b'synthetic legacy executable; never run\n')
(app / 'Contents/Info.plist').write_bytes(plistlib.dumps({'CFBundleExecutable': 'legacy', 'CFBundleIdentifier': 'com.jones.bootstrap.fixture', 'CFBundleShortVersionString': config['metadata']['version']}))
payload = json.dumps(config['metadata'], separators=(',', ':')).encode()
header = json.dumps({'files': {'package.json': {'offset': '0', 'size': len(payload)}}}, separators=(',', ':')).encode()
padded = (len(header) + 3) & ~3
archive = struct.pack('<IIII', 4, padded + 8, padded + 4, len(header)) + header + b'\0' * (padded - len(header)) + payload
(app / 'Contents/Resources/app.asar').write_bytes(archive)
with zipfile.ZipFile(config['archive'], 'w', compression=zipfile.ZIP_STORED) as output:
    output.writestr('ARTIFACT.json', json.dumps(config['receipt'], separators=(',', ':')))
`;

// Audit ordinary public CLI execution without replacing its validator or
// platform check. The positive control proves the same guard sees protected
// metadata probes as well as opens; process creation is prohibited outright.
// Inventory additionally forbids writes; the staging operations retain their owned writes.
const auditedCli = String.raw`
import json, os, pathlib, runpy, sys
roots = json.loads(sys.argv[1])
script, arguments = sys.argv[2], sys.argv[3:]
read_only = bool(arguments) and arguments[0] == 'inventory-routes'
violations = []
fd_paths = {}
control = False
class ForbiddenFixtureAccess(Exception): pass

def guard(path, dir_fd=None):
    if isinstance(path, int):
        path = fd_paths.get(path)
    if isinstance(path, (str, bytes, os.PathLike)):
        value = os.fsdecode(path)
        if not os.path.isabs(value):
            if dir_fd is not None and dir_fd not in fd_paths:
                violations.append('unresolved-dir-fd')
                raise ForbiddenFixtureAccess('unobserved directory descriptor')
            value = os.path.join(fd_paths[dir_fd] if dir_fd is not None else os.getcwd(), value)
        value = os.path.normpath(value)
        if any(value == root or value.startswith(root + os.sep) for root in roots):
            violations.append('protected-path')
            raise ForbiddenFixtureAccess('bootstrap accessed protected fixture state')
        return value

def audit(event, args):
    if event == 'open' or event in ('os.listdir', 'os.scandir', 'os.chdir'):
        guard(args[0])
    if read_only and (event in ('os.mkdir', 'os.remove', 'os.rmdir', 'os.rename', 'os.link', 'os.symlink', 'os.chmod', 'os.chown', 'os.truncate', 'os.utime', 'os.setxattr', 'os.removexattr') or
                      (event == 'open' and args[2] & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND))):
        violations.append('filesystem-write')
        raise ForbiddenFixtureAccess('inventory attempted filesystem mutation')
    if event in ('subprocess.Popen', 'os.system', 'os.posix_spawn', 'os.exec', 'os.fork', 'os.forkpty', 'os.kill', 'os.killpg'):
        violations.append('child-execution')
        raise ForbiddenFixtureAccess('bootstrap attempted child execution')

sys.addaudithook(audit)
original_open, original_close = os.open, os.close
def tracked_open(path, *args, **kwargs):
    value = guard(path, kwargs.get('dir_fd'))
    fd = original_open(path, *args, **kwargs)
    fd_paths[fd] = value
    return fd

def tracked_close(fd):
    original_close(fd)
    fd_paths.pop(fd, None)

os.open, os.close = tracked_open, tracked_close
original_scandir = os.scandir
class GuardedEntry:
    def __init__(self, entry, parent):
        self.entry, self.parent = entry, parent
    def __fspath__(self): return os.fspath(self.entry)
    def __getattr__(self, name):
        value = getattr(self.entry, name)
        if name in ('stat', 'is_dir', 'is_file', 'is_symlink', 'inode'):
            def checked(*args, **kwargs):
                guard(os.path.join(self.parent, self.entry.name))
                return value(*args, **kwargs)
            return checked
        return value

class GuardedScan:
    def __init__(self, path):
        self.parent = guard(path)
        self.entries = original_scandir(path)
    def __iter__(self): return self
    def __next__(self): return GuardedEntry(next(self.entries), self.parent)
    def __enter__(self): return self
    def __exit__(self, *args): self.close()
    def close(self): self.entries.close()

os.scandir = GuardedScan
for name in ('stat', 'lstat', 'readlink', 'access', 'listdir'):
    original = getattr(os, name)
    def wrapped(path, *args, _original=original, **kwargs):
        guard(path, kwargs.get('dir_fd'))
        return _original(path, *args, **kwargs)
    setattr(os, name, wrapped)
controls = []
parent_fd = os.open(os.path.dirname(roots[0]), os.O_RDONLY | os.O_DIRECTORY)
def entry_probe():
    with os.scandir(parent_fd) as entries:
        next(entry for entry in entries if entry.name == os.path.basename(roots[0])).stat()
try:
    probes = []
    for root in roots:
        probes.extend(((lambda root=root: os.stat(root), 'protected-path'),
                       (lambda root=root: os.open(root, os.O_RDONLY), 'protected-path')))
    probes.extend((
        (lambda: os.stat(os.path.basename(roots[0]), dir_fd=parent_fd), 'protected-path'),
        (entry_probe, 'protected-path'),
        (lambda: sys.audit('subprocess.Popen', 'must-not-launch', [], None, None), 'child-execution'),
    ))
    for probe, expected in probes:
        violations.clear()
        try:
            probe()
        except ForbiddenFixtureAccess:
            controls.append(violations == [expected])
        else:
            controls.append(False)
finally:
    os.close(parent_fd)
control = all(controls) and len(controls) == 2 * len(roots) + 3
write_controls = []
if read_only:
    for event, args in (
        ('open', (script, None, os.O_WRONLY | os.O_CREAT)),
        ('os.chmod', (script, 0o700, -1)),
    ):
        violations.clear()
        try:
            sys.audit(event, *args)
        except ForbiddenFixtureAccess:
            write_controls.append(violations == ['filesystem-write'])
        else:
            write_controls.append(False)
violations.clear()
sys.argv = [script] + arguments
code = 0
try:
    runpy.run_path(script, run_name='__main__')
except SystemExit as error:
    code = error.code
finally:
    report = {'positiveControl': control, 'violations': violations}
    if read_only:
        report.update(readOnly=True, writeControl=all(write_controls) and len(write_controls) == 2)
    print('JONES_BOOTSTRAP_AUDIT=' + json.dumps(report), file=sys.stderr)
sys.exit(code)
`;

export function assertInventoryResponse(result, expected = "partial") {
  NodeAssert.equal(result.signal, null, result.stderr);
  NodeAssert.equal(result.code, { observed: 0, partial: 2, refused: 1 }[expected], result.stderr);
  const response = JSON.parse(result.stdout);
  NodeAssert.equal(response.schema, 1);
  NodeAssert.equal(response.operation, "inventory-routes");
  NodeAssert.equal(response.status, expected);
  NodeAssert.equal(response.observationOnly, true);
  NodeAssert.equal(response.activationAllowed, false);
  NodeAssert.equal(response.launchMembershipComplete, false);
  NodeAssert.equal(response.processCustody, "unobserved");
  NodeAssert.equal(response.blocker, "DESK-FIRST-01");
  NodeAssert.deepEqual(response.coverage, {
    history: "partial",
    orphanSiblingApps: "unobserved",
    manualCopies: "not-enumerated",
    interpreterIdentity: "unobserved",
    payloadDigests: "not-verified",
  });
  NodeAssert.ok(Array.isArray(response.observations) && Array.isArray(response.issues));
  const reports = result.stderr
    .split("\n")
    .filter((line) => line.startsWith("JONES_BOOTSTRAP_AUDIT="));
  NodeAssert.equal(reports.length, 1, result.stderr);
  NodeAssert.deepEqual(JSON.parse(reports[0].slice("JONES_BOOTSTRAP_AUDIT=".length)), {
    positiveControl: true,
    violations: [],
    readOnly: true,
    writeControl: true,
  });
  return response;
}

export function assertBootstrapResponse(result, expected) {
  NodeAssert.equal(result.signal, null, result.stderr);
  NodeAssert.equal(result.code, expected === "refused" ? 1 : 0, result.stderr);
  const response = JSON.parse(result.stdout);
  NodeAssert.equal(response.activationAllowed, false);
  NodeAssert.equal(response.blocker, "DESK-FIRST-01");
  if (expected === "refused") NodeAssert.equal(response.status, "refused");
  else {
    NodeAssert.equal(response.result, expected);
    NodeAssert.equal(
      response.status,
      expected === "inspected" ? "bootstrap-required" : "executor-ready",
    );
    NodeAssert.equal(response.observationOnly, true);
    NodeAssert.equal(response.nextRequirement, "qualified-maintenance-admission");
    NodeAssert.match(response.bindingDigest, /^[a-f0-9]{64}$/);
  }
  const reports = result.stderr
    .split("\n")
    .filter((line) => line.startsWith("JONES_BOOTSTRAP_AUDIT="));
  NodeAssert.equal(reports.length, 1, result.stderr);
  const audit = JSON.parse(reports[0].slice("JONES_BOOTSTRAP_AUDIT=".length));
  NodeAssert.deepEqual(audit, { positiveControl: true, violations: [] });
  return response;
}

export async function createLegacyBootstrapFixture({
  root,
  app,
  metadata,
  executableName,
  ownership,
  signal,
  environment,
}) {
  const fixture = NodePath.join(root, "legacy-bootstrap");
  await ownership.mkdir(fixture);
  const home = NodePath.join(fixture, "home");
  const profile = NodePath.join(fixture, "profile");
  const runtime = NodePath.join(home, "runtime");
  const updater = NodePath.join(runtime, "jones-updates");
  const databaseDirectory = NodePath.join(home, "userdata");
  for (const path of [home, profile, runtime, updater, databaseDirectory])
    await NodeFSP.mkdir(path, { mode: 0o700 });
  const databasePath = NodePath.join(databaseDirectory, "statev2.sqlite");
  await NodeFSP.writeFile(databasePath, "synthetic database; must never be opened", { flag: "wx" });
  await NodeFSP.writeFile(
    NodePath.join(profile, "sentinel"),
    "synthetic profile; must never be opened",
    { flag: "wx" },
  );
  const env = cleanLaunchEnvironment(environment, root);
  const command = async (args) => {
    const result = await runOwnedChild("python3", ["-I", "-B", ...args], {
      env,
      signal,
      timeout: 120000,
    });
    NodeAssert.equal(result.code, 0, result.stderr);
    return result;
  };
  const versionMatch = /\.(\d+)\.(\d+)$/.exec(metadata.version);
  NodeAssert.ok(versionMatch, "Packaged preview must carry the workflow run and attempt.");
  const runId = Number(versionMatch[1]);
  const runAttempt = Number(versionMatch[2]);
  const installedSource =
    metadata.jonesSource.sha === "a".repeat(40) ? "b".repeat(40) : "a".repeat(40);
  const installedApp = NodePath.join(fixture, "Legacy.app");
  const archivePath = NodePath.join(fixture, "github-artifact.zip");
  const payload = Buffer.from(
    "Synthetic artifact payload; actual candidate app is the mounted CI build.\n",
  );
  const receipt = {
    schema: 1,
    repository: metadata.jonesSource.repository,
    source: metadata.jonesSource.sha,
    tree: metadata.jonesSource.tree,
    workflow: ".github/workflows/artifact-desktop-mac.yml",
    version: metadata.version,
    platform: "darwin",
    architecture: "arm64",
    event: "push",
    ref: "refs/heads/main",
    runId: String(runId),
    runAttempt: String(runAttempt),
    artifact: `T3-Code-${metadata.version}-arm64.dmg`,
    sha256: sha256(payload),
  };
  const legacyMetadata = {
    version: "0.0.0-preview.20200101.1.1",
    jonesSource: {
      repository: metadata.jonesSource.repository,
      sha: installedSource,
      tree: "c".repeat(40),
    },
  };
  await command([
    "-c",
    makeLegacyAndArchive,
    json({ app: installedApp, archive: archivePath, receipt, metadata: legacyMetadata }),
  ]);
  const archive = await NodeFSP.readFile(archivePath);
  // Property order is the public Jones Actions candidate-handle tuple.
  const candidate = {
    schema: 1,
    repository: receipt.repository,
    source: receipt.source,
    tree: receipt.tree,
    installedSource,
    workflow: receipt.workflow,
    workflowId: 1,
    runId,
    runAttempt,
    ciRunId: 1,
    artifactId: 1,
    artifactName: `desktop-mac-arm64-${runId}-${runAttempt}--${metadata.version}`,
    artifactDigest: sha256(archive),
    artifactBytes: archive.length,
    expiresAt: "2099-01-01T00:00:00Z",
    version: metadata.version,
    platform: "darwin",
    architecture: "arm64",
  };
  const handle = sha256(json(Object.values(candidate)));
  const appDirectory = NodePath.join(updater, "apps", handle);
  const artifactDirectory = NodePath.join(updater, "artifacts", handle);
  for (const path of [appDirectory, artifactDirectory, NodePath.join(updater, "staging")])
    await NodeFSP.mkdir(path, { recursive: true, mode: 0o700 });
  const candidateApp = NodePath.join(appDirectory, NodePath.basename(app));
  // Retain framework symlinks verbatim; cp's default rewritten absolute links
  // would point back at the mounted image instead of inside this owned copy.
  const sourceApp = await appDigest(app);
  const storage = await NodeFSP.statfs(fixture);
  NodeAssert.ok(
    storage.bavail * storage.bsize >= sourceApp.bytes + 64 * 1024 ** 2,
    "Owned bootstrap fixture has insufficient copy capacity.",
  );
  await NodeFSP.cp(app, candidateApp, {
    recursive: true,
    verbatimSymlinks: true,
    errorOnExist: true,
    force: false,
  });
  const copiedApp = await appDigest(candidateApp);
  NodeAssert.equal(
    copiedApp.digest,
    sourceApp.digest,
    "Packaged app changed while copying the fixture.",
  );
  const bundle = await verifyPackagedBootstrap(candidateApp, metadata);
  const active = {
    protocol: 1,
    owner: "desktop",
    generation: "synthetic-legacy",
    transactionId: "bootstrap",
    home,
    profile,
    databasePath,
    environmentId: "legacy-bootstrap-qualification",
    version: legacyMetadata.version,
    sourceSha: installedSource,
    sourceTree: legacyMetadata.jonesSource.tree,
    appPath: installedApp,
    executablePath: NodePath.join(installedApp, "Contents/MacOS/legacy"),
  };
  const appReceipt = {
    handle,
    appPath: candidateApp,
    executablePath: NodePath.join(candidateApp, "Contents/MacOS", executableName),
    sourceSha: receipt.source,
    sourceTree: receipt.tree,
    version: receipt.version,
    startupGateProtocol: 1,
    appDigest: copiedApp.digest,
    asarDigest: await fileDigest(NodePath.join(candidateApp, "Contents/Resources/app.asar")),
    executableDigest: await fileDigest(
      NodePath.join(candidateApp, "Contents/MacOS", executableName),
    ),
    receiptPath: NodePath.join(appDirectory, "mac-app-receipt.json"),
  };
  const artifact = {
    schema: 1,
    source: "jones-actions",
    channel: "jones-main",
    stagedHandle: handle,
    candidate,
    receipt,
    payloadPath: NodePath.join(artifactDirectory, receipt.artifact),
  };
  await NodeFSP.writeFile(artifact.payloadPath, payload, { flag: "wx", mode: 0o600 });
  await NodeFSP.writeFile(NodePath.join(artifactDirectory, "github-artifact.zip"), archive, {
    flag: "wx",
    mode: 0o600,
  });
  await writeJson(NodePath.join(artifactDirectory, "jones-actions-stage.json"), artifact);
  await writeJson(appReceipt.receiptPath, { app: appReceipt, artifact, candidate });
  const manifest = NodePath.join(runtime, "jones-active-install.json");
  await writeJson(manifest, active);
  const selection = {
    schema: 1,
    source: "jones-actions",
    home,
    profile,
    currentVersion: active.version,
    installedSource,
    active,
    artifactDirectory,
    app: appReceipt,
  };
  const selectionPath = NodePath.join(updater, "staging", `${installedSource}-bootstrap.json`);
  await writeJson(selectionPath, selection);
  const script = NodePath.join(candidateApp, "Contents/Resources/jones-bootstrap/bootstrap.py");
  const protectedPaths = [databaseDirectory, profile];
  const before = await Promise.all([...protectedPaths, manifest].map(snapshotTree));
  const invoke = async (operation, path = selectionPath, denied = protectedPaths) =>
    runOwnedChild(
      "python3",
      [
        "-I",
        "-B",
        "-c",
        auditedCli,
        json(denied),
        script,
        operation,
        "--manifest",
        manifest,
        ...(operation === "inventory-routes"
          ? []
          : ["--selection", path, "--selection-sha256", sha256(await NodeFSP.readFile(path))]),
      ],
      { env, signal, timeout: 120000 },
    );
  return {
    fixture,
    bundle,
    selection,
    selectionPath,
    script,
    manifest,
    protectedPaths,
    before,
    invoke,
  };
}

export async function qualifyLegacyBootstrap(fixture, evidence = { scenarios: [] }) {
  const scenarios = evidence.scenarios;
  evidence.bundle = fixture.bundle;
  evidence.actualPackagedScripts = true;
  const run = async (name, operation, expected, selection) => {
    const response = assertBootstrapResponse(await fixture.invoke(operation, selection), expected);
    scenarios.push({
      name,
      status: response.status,
      result: response.result ?? "refused",
      activationAllowed: false,
    });
    return response;
  };
  const inspected = await run("inspect-distributed-bootstrap", "inspect", "inspected");
  NodeAssert.equal(
    inspected.executorPath,
    NodePath.join(
      NodePath.dirname(fixture.manifest),
      "jones-updates",
      "bootstrap-executors",
      inspected.bindingDigest,
      "executor.py",
    ),
  );
  await NodeAssert.rejects(NodeFSP.lstat(NodePath.dirname(inspected.executorPath)), {
    code: "ENOENT",
  });
  const badSelection = NodePath.join(
    NodePath.dirname(fixture.selectionPath),
    `${fixture.selection.installedSource}-protected.json`,
  );
  await writeJson(badSelection, {
    ...fixture.selection,
    app: { ...fixture.selection.app, appPath: fixture.protectedPaths[0] },
  });
  await run("protected-candidate-path", "stage-executor", "refused", badSelection);
  await NodeAssert.rejects(NodeFSP.lstat(NodePath.dirname(inspected.executorPath)), {
    code: "ENOENT",
  });
  const staged = await run("stage-distributed-executor", "stage-executor", "created");
  NodeAssert.equal(staged.bindingDigest, inspected.bindingDigest);
  NodeAssert.equal(staged.executorPath, inspected.executorPath);
  NodeAssert.equal(
    staged.receiptPath,
    NodePath.join(NodePath.dirname(staged.executorPath), "receipt.json"),
  );
  NodeAssert.equal(await fileDigest(staged.executorPath), fixture.bundle.executorSha256);
  NodeAssert.equal((await NodeFSP.lstat(staged.executorPath)).mode & 0o777, 0o500);
  const retained = JSON.parse(await NodeFSP.readFile(staged.receiptPath, "utf8"));
  NodeAssert.equal(retained.bindingDigest, staged.bindingDigest);
  NodeAssert.equal(retained.activationAllowed, false);
  NodeAssert.deepEqual((await NodeFSP.readdir(NodePath.dirname(staged.executorPath))).sort(), [
    "executor.py",
    "receipt.json",
  ]);
  await run("repeat-stage", "stage-executor", "no-change");
  // Simulate interruption after executor publication but before receipt commit.
  // Only the exact receipt created above is removed from this owned fixture.
  await NodeFSP.unlink(staged.receiptPath);
  const partialBefore = await snapshotTree(NodePath.dirname(staged.executorPath));
  await run("retained-partial-stage", "stage-executor", "refused");
  NodeAssert.deepEqual(await snapshotTree(NodePath.dirname(staged.executorPath)), partialBefore);
  evidence.inventory = await qualifyInventoryRoutes(fixture);
  NodeAssert.deepEqual(
    await Promise.all([...fixture.protectedPaths, fixture.manifest].map(snapshotTree)),
    fixture.before,
  );
  Object.assign(evidence, {
    outcome: "passed",
    protectedStateUnchanged: true,
    protectedAccessAudit: "positive-control-and-zero-accesses",
    childExecutorActivation: false,
    activationAllowed: false,
    blocker: "DESK-FIRST-01",
    limitations: [
      "Artifact receipts and legacy installation are synthetic; supply-chain authentication and maintenance admission are not qualified.",
    ],
  });
  return evidence;
}

// Inventory gets incomplete synthetic remnants only after the original staging
// scenarios finish. A diagnostic report must not become admission evidence.
async function qualifyInventoryRoutes(fixture) {
  const active = fixture.selection.active;
  const updater = NodePath.join(NodePath.dirname(fixture.manifest), "jones-updates");
  const handle = "e".repeat(64);
  const stages = ["stage-PreReceipt", "stage-Interrupted"].map((attempt) =>
    NodePath.join(updater, "apps", "attempts", attempt, handle),
  );
  const apps = [
    NodePath.join(stages[0], "PreReceipt.app"),
    NodePath.join(stages[1], "Partial.app"),
  ];
  for (const stage of stages) await NodeFSP.mkdir(stage, { recursive: true, mode: 0o700 });
  await NodeFSP.cp(active.appPath, apps[0], { recursive: true, errorOnExist: true, force: false });
  const executable = NodePath.join(apps[1], "Contents", "MacOS", "uncommitted");
  await NodeFSP.mkdir(NodePath.dirname(executable), { recursive: true, mode: 0o700 });
  await NodeFSP.writeFile(executable, "synthetic executable; never run\n", {
    flag: "wx",
    mode: 0o500,
  });
  const helper = NodePath.join(updater, `activate-v1-${fixture.bundle.executorSha256}.py`);
  await NodeFSP.writeFile(
    helper,
    await NodeFSP.readFile(NodePath.join(NodePath.dirname(fixture.script), "executor.py")),
    { flag: "wx", mode: 0o700 },
  );
  const transactionId = "d".repeat(64);
  const transactions = [transactionId, "f".repeat(64)].map((id) =>
    NodePath.join(updater, "transactions", id),
  );
  for (const transaction of transactions)
    await NodeFSP.mkdir(transaction, { recursive: true, mode: 0o700 });
  const intent = {
    protocol: 1,
    transactionId,
    expected: active,
    staged: fixture.selection.app,
    continuationReceipt: NodePath.join(transactions[0], "continuation.json"),
  };
  await writeJson(NodePath.join(transactions[0], "intent.json"), intent);
  await writeJson(NodePath.join(transactions[0], "journal.json"), { intent, phase: "resumed" });
  await writeJson(NodePath.join(transactions[1], "intent.json"), {
    protocol: 1,
    transactionId: "f".repeat(64),
  });
  const retainedApps = [
    `${active.appPath}.jones-incoming-${transactionId}`,
    `${active.appPath}.jones-previous-${active.generation}`,
  ];
  for (const app of retainedApps)
    await NodeFSP.cp(active.appPath, app, { recursive: true, errorOnExist: true, force: false });
  const protectedRetained = transactions.flatMap((transaction) =>
    ["previous-pair", "advanced-state"].map((name) => NodePath.join(transaction, name)),
  );
  for (const path of protectedRetained) {
    await NodeFSP.mkdir(path, { mode: 0o700 });
    await NodeFSP.writeFile(
      NodePath.join(path, "sentinel"),
      "synthetic retained state; never read",
      {
        flag: "wx",
      },
    );
  }
  const unchangedPaths = [...stages, helper, ...transactions, ...retainedApps];
  const before = await Promise.all(unchangedPaths.map(snapshotTree));
  const report = assertInventoryResponse(
    await fixture.invoke("inventory-routes", undefined, [
      ...fixture.protectedPaths,
      ...protectedRetained,
    ]),
    "partial",
  );
  NodeAssert.equal(report.manifestPath, fixture.manifest);
  NodeAssert.equal(report.manifestSha256, sha256(await NodeFSP.readFile(fixture.manifest)));
  for (const [path, role] of [
    [active.appPath, "active"],
    [fixture.selection.app.appPath, "staged"],
    ...apps.map((path) => [path, "staged"]),
    [retainedApps[0], "incoming"],
    [retainedApps[1], "previous"],
  ]) {
    NodeAssert.ok(
      report.observations.some(
        (item) => item.kind === "app" && item.path === path && item.role === role,
      ),
      `Packaged inventory omitted ${role} fixture app: ${path}`,
    );
  }
  for (const path of stages) {
    NodeAssert.ok(
      report.observations.some(
        (item) => item.kind === "stage" && item.path === path && item.binding === "unbound",
      ),
    );
    NodeAssert.ok(
      report.issues.some(
        (item) =>
          item.code === "receipt-missing" &&
          typeof item.path === "string" &&
          item.path.startsWith(path),
      ),
    );
  }
  NodeAssert.ok(
    report.observations.some((item) => item.kind === "executable" && item.path === executable),
  );
  NodeAssert.ok(
    report.observations.some(
      (item) =>
        item.kind === "helper" && item.path === helper && item.binding === "content-addressed",
    ),
  );
  NodeAssert.ok(
    report.issues.some(
      (item) => typeof item.path === "string" && item.path.startsWith(transactions[1]),
    ),
  );
  NodeAssert.deepEqual(await Promise.all(unchangedPaths.map(snapshotTree)), before);
  return {
    outcome: "passed",
    scope: "supported-installer-route-diagnostic",
    status: report.status,
    actualPackagedScript: true,
    completedAndPreReceiptAppsObserved: true,
    transactionDerivedAppsObserved: true,
    partialTransactionReported: true,
    helperBinding: "content-addressed",
    protectedAccessAudit: "positive-control-and-zero-accesses",
    writeAudit: "positive-control-and-zero-writes",
    launchMembershipComplete: false,
    processCustody: "unobserved",
    activationAllowed: false,
  };
}
