import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";

const markerName = ".jones-performance-root.json";
const owners = new WeakMap();
const databasePermits = new WeakMap();
const childTokens = new WeakMap();
const closeProofs = new WeakMap();
const resources = new WeakMap();
const defaults = Object.freeze({
  maxFiles: 256,
  maxFileBytes: 64 * 1024 * 1024,
  maxTotalBytes: 256 * 1024 * 1024,
  maxReceiptBytes: 256 * 1024,
});

export class PerformanceStagingError extends Error {
  constructor(code, message) {
    super(message);
    this.name = "PerformanceStagingError";
    this.code = code;
  }
}

function refuse(code, message) {
  throw new PerformanceStagingError(code, message);
}

function freeze(value) {
  if (value && typeof value === "object") {
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  }
  return value;
}

function boundedText(value, name, maxBytes = 512) {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    Buffer.byteLength(value) > maxBytes ||
    value.includes("\0")
  ) {
    refuse("invalid_input", `${name} must be a nonempty bounded string`);
  }
  return value;
}

function absolutePath(value, name) {
  boundedText(value, name, 4096);
  if (!NodePath.isAbsolute(value) || value.split(NodePath.sep).includes("..")) {
    refuse("invalid_path", `${name} must be absolute without parent traversal`);
  }
  return NodePath.resolve(value);
}

function relativePath(value) {
  boundedText(value, "databaseRelativePath", 4096);
  if (
    NodePath.isAbsolute(value) ||
    value.includes("\\") ||
    value.split(NodePath.sep).some((part) => part === "" || part === "." || part === "..") ||
    value === markerName
  ) {
    refuse("invalid_path", "database path must stay below its owned root");
  }
  return value;
}

function within(candidate, boundary) {
  const tail = NodePath.relative(boundary, candidate);
  return (
    tail === "" ||
    (!NodePath.isAbsolute(tail) && tail !== ".." && !tail.startsWith(`..${NodePath.sep}`))
  );
}

function overlaps(first, second) {
  return within(first, second) || within(second, first);
}

function bindingValue(binding) {
  if (!binding || typeof binding !== "object") refuse("invalid_binding", "binding is required");
  const result = {};
  for (const key of ["repository", "sourceRevision", "taskRef", "runId"]) {
    result[key] = boundedText(binding[key], `binding.${key}`);
  }
  if (!/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/.test(result.sourceRevision)) {
    refuse("invalid_binding", "sourceRevision must be an exact source object ID");
  }
  return freeze(result);
}

function policyValue(policy) {
  if (
    !policy ||
    typeof policy !== "object" ||
    !Array.isArray(policy.worktreePaths) ||
    !Array.isArray(policy.protectedPaths)
  ) {
    refuse("invalid_policy", "explicit HOME, worktree and protected boundaries are required");
  }
  const result = {
    homePath: absolutePath(policy.homePath, "policy.homePath"),
    worktreePaths: policy.worktreePaths.map((path) => absolutePath(path, "worktree boundary")),
    protectedPaths: policy.protectedPaths.map((path) => absolutePath(path, "protected boundary")),
  };
  for (const [key, fallback] of Object.entries(defaults)) {
    const value = policy[key] ?? fallback;
    if (!Number.isSafeInteger(value) || value <= 0)
      refuse("invalid_policy", `${key} must be positive`);
    result[key] = value;
  }
  return freeze(result);
}

function checkBoundary(path, policy) {
  // Refuse lexically before lstat: protected-path policy is input, never a discovery request.
  if (policy.protectedPaths.some((protectedPath) => overlaps(path, protectedPath))) {
    refuse("protected_path", "candidate overlaps a protected boundary");
  }
  if (![policy.homePath, ...policy.worktreePaths].some((boundary) => within(path, boundary))) {
    refuse("outside_boundary", "candidate is outside explicit HOME/worktree boundaries");
  }
}

function stat(path) {
  try {
    return NodeFS.lstatSync(path, { bigint: true });
  } catch (error) {
    if (error.code === "ENOENT") return null;
    throw error;
  }
}

function identity(info) {
  return {
    device: info.dev.toString(),
    inode: info.ino.toString(),
    uid: Number(info.uid),
    mode: Number(info.mode & 0o777n),
  };
}

function sameIdentity(info, expected) {
  const actual = identity(info);
  return Object.keys(actual).every((key) => actual[key] === expected[key]);
}

function currentUid() {
  if (!process.getuid) refuse("unsupported_platform", "UID-bound roots require a POSIX runtime");
  return process.getuid();
}

function inspectAncestry(path) {
  const ancestors = [];
  let cursor = path;
  while (true) {
    ancestors.push(cursor);
    const parent = NodePath.resolve(cursor, "..");
    if (parent === cursor) break;
    cursor = parent;
  }
  for (const ancestor of ancestors.toReversed()) {
    const info = stat(ancestor);
    if (!info || info.isSymbolicLink() || !info.isDirectory()) {
      refuse("aliased_path", "parent ancestry must contain only existing directories");
    }
  }
  if (NodeFS.realpathSync(path) !== path) refuse("aliased_path", "parent path is not canonical");
  const info = stat(path);
  if (Number(info.uid) !== currentUid())
    refuse("unowned_parent", "parent is not owned by this UID");
  return info;
}

function requireOwner(owner) {
  const state = owner && owners.get(owner);
  if (!state || state.disposed)
    refuse("invalid_owner", "the original active owner handle is required");
  checkBoundary(state.receipt.canonicalRootPath, state.policy);
  const info = stat(state.receipt.canonicalRootPath);
  if (
    !info ||
    !info.isDirectory() ||
    info.isSymbolicLink() ||
    !sameIdentity(info, state.receipt.identity)
  ) {
    refuse("changed_identity", "owned root identity changed; retain the tree");
  }
  const parent = inspectAncestry(state.receipt.canonicalParentPath);
  if (!sameIdentity(parent, state.parentIdentity)) {
    refuse("changed_identity", "owned parent identity changed; retain the tree");
  }
  return state;
}

function checkEntry(path, info, rootIdentity, policy) {
  if (info.isSymbolicLink()) refuse("symlink", "owned tree contains a symbolic link");
  if (!info.isDirectory() && !info.isFile())
    refuse("nonregular_file", "owned tree contains a nonregular entry");
  if (info.dev.toString() !== rootIdentity.device || Number(info.uid) !== rootIdentity.uid) {
    refuse("changed_identity", "owned entry device or UID differs from its root");
  }
  if (info.isFile() && info.nlink !== 1n)
    refuse("hardlink", "owned files must have exactly one link");
  if (info.isFile() && info.size > BigInt(policy.maxFileBytes))
    refuse("manifest_limit", "file exceeds byte limit");
}

function scanTree(rootPath, rootIdentity, policy) {
  const files = [];
  const pending = [rootPath];
  let entries = 0;
  let totalBytes = 0n;
  while (pending.length) {
    const directory = pending.pop();
    const names = NodeFS.readdirSync(directory).sort();
    for (const name of names) {
      if (++entries > policy.maxFiles) refuse("manifest_limit", "owned tree exceeds entry limit");
      const path = NodePath.join(directory, name);
      const info = stat(path);
      if (!info) refuse("changed_identity", "owned entry disappeared during inspection");
      checkEntry(path, info, rootIdentity, policy);
      if (info.isDirectory()) pending.push(path);
      else {
        totalBytes += info.size;
        if (totalBytes > BigInt(policy.maxTotalBytes))
          refuse("manifest_limit", "owned tree exceeds byte limit");
        files.push({ relativePath: NodePath.relative(rootPath, path), path, info });
      }
    }
  }
  return files.sort((left, right) => left.relativePath.localeCompare(right.relativePath));
}

function markerBytes(state) {
  const path = NodePath.join(state.receipt.canonicalRootPath, markerName);
  const info = stat(path);
  if (
    !info ||
    !info.isFile() ||
    info.isSymbolicLink() ||
    info.nlink !== 1n ||
    !sameIdentity(info, state.markerIdentity) ||
    info.size > BigInt(state.policy.maxReceiptBytes)
  ) {
    refuse("invalid_marker", "creation marker identity changed");
  }
  const bytes = NodeFS.readFileSync(path);
  if (NodeCrypto.createHash("sha256").update(bytes).digest("hex") !== state.receipt.markerSha256) {
    refuse("invalid_marker", "creation marker bytes changed");
  }
  return bytes;
}

export function createOwnedRoot({ parentPath, childName, binding, policy }) {
  const checkedPolicy = policyValue(policy);
  const checkedBinding = bindingValue(binding);
  const parent = absolutePath(parentPath, "parentPath");
  boundedText(childName, "childName", 255);
  if (
    childName === "." ||
    childName === ".." ||
    childName.includes(NodePath.sep) ||
    childName.includes("\\")
  ) {
    refuse("invalid_path", "childName must name one absent direct child");
  }
  const rootPath = NodePath.join(parent, childName);
  checkBoundary(rootPath, checkedPolicy);
  const parentInfo = inspectAncestry(parent);
  if (stat(rootPath)) refuse("existing_destination", "new owned root must not exist");
  NodeFS.mkdirSync(rootPath, { mode: 0o700, recursive: false });
  const rootInfo = stat(rootPath);
  if (
    !rootInfo ||
    !rootInfo.isDirectory() ||
    rootInfo.isSymbolicLink() ||
    Number(rootInfo.uid) !== currentUid() ||
    Number(rootInfo.mode & 0o777n) !== 0o700 ||
    rootInfo.dev !== parentInfo.dev
  ) {
    refuse("changed_identity", "new root identity could not be established; retain it");
  }
  const marker = {
    schema: "jones-performance-root-marker/v1",
    provenance: "synthetic-created",
    binding: checkedBinding,
    rootId: NodeCrypto.randomUUID(),
    canonicalParentPath: parent,
    canonicalRootPath: rootPath,
    identity: identity(rootInfo),
  };
  const bytes = Buffer.from(`${JSON.stringify(marker)}\n`);
  NodeFS.writeFileSync(NodePath.join(rootPath, markerName), bytes, { flag: "wx", mode: 0o600 });
  const receipt = freeze({
    ...marker,
    schema: "jones-performance-root/v1",
    markerSha256: NodeCrypto.createHash("sha256").update(bytes).digest("hex"),
  });
  const owner = Object.freeze({ creationReceipt: receipt });
  owners.set(owner, {
    receipt,
    policy: checkedPolicy,
    parentIdentity: identity(parentInfo),
    markerIdentity: identity(stat(NodePath.join(rootPath, markerName))),
    databases: new Map(),
    children: new Set(),
    disposed: false,
  });
  return owner;
}

export function assertOwnedDatabase(owner, { databaseRelativePath, access }) {
  const state = requireOwner(owner);
  const dbRelativePath = relativePath(databaseRelativePath);
  if (access !== "create" && access !== "readwrite")
    refuse("invalid_access", "access must be create or readwrite");
  markerBytes(state);
  const files = scanTree(state.receipt.canonicalRootPath, state.receipt.identity, state.policy);
  const path = NodePath.join(state.receipt.canonicalRootPath, dbRelativePath);
  checkBoundary(path, state.policy);
  const parent = NodePath.resolve(path, "..");
  const parentInfo = stat(parent);
  if (!parentInfo || !parentInfo.isDirectory())
    refuse("invalid_path", "database parent must exist below its root");
  for (const suffix of ["", "-wal", "-shm", "-journal"]) {
    const info = stat(`${path}${suffix}`);
    if (info) checkEntry(`${path}${suffix}`, info, state.receipt.identity, state.policy);
    if (access === "create" && info)
      refuse("existing_destination", "database and sidecar destinations must be absent");
  }
  let registered = state.databases.get(dbRelativePath);
  if (access === "create") {
    if (registered) refuse("existing_destination", "database path already registered");
    const fd = NodeFS.openSync(
      path,
      NodeFS.constants.O_CREAT |
        NodeFS.constants.O_EXCL |
        NodeFS.constants.O_WRONLY |
        NodeFS.constants.O_NOFOLLOW,
      0o600,
    );
    let info;
    try {
      info = NodeFS.fstatSync(fd, { bigint: true });
    } finally {
      NodeFS.closeSync(fd);
    }
    checkEntry(path, info, state.receipt.identity, state.policy);
    registered = { identity: identity(info), created: true, sealed: false, closeState: "unproved" };
    state.databases.set(dbRelativePath, registered);
  } else {
    const entry = files.find((file) => file.relativePath === dbRelativePath);
    if (!registered || !entry || !sameIdentity(entry.info, registered.identity)) {
      refuse("unregistered_database", "readwrite requires this owner's unchanged created database");
    }
    if (registered.closeState !== "closed")
      refuse("unknown_close", "prior database operation must have an observed successful close");
  }
  const info = stat(path);
  if (!info || !sameIdentity(info, registered.identity))
    refuse("changed_identity", "database identity changed before permit");
  const permit = freeze({
    canonicalPath: path,
    relativePath: dbRelativePath,
    identity: registered.identity,
    access,
  });
  databasePermits.set(permit, { state, registered });
  registered.latestPermit = permit;
  registered.closeState = "unproved";
  registered.currentCloseProof = null;
  registered.sealed = false;
  return permit;
}

function requirePermit(owner, permit) {
  const state = requireOwner(owner);
  const permitState = permit && databasePermits.get(permit);
  if (
    !permitState ||
    permitState.state !== state ||
    permitState.registered.latestPermit !== permit ||
    !permitState.registered.created ||
    permitState.registered.sealed
  ) {
    refuse("invalid_permit", "the original current created database permit is required");
  }
  const info = stat(permit.canonicalPath);
  if (!info || !info.isFile() || !sameIdentity(info, permit.identity)) {
    refuse("changed_identity", "producer database identity changed");
  }
  checkEntry(permit.canonicalPath, info, state.receipt.identity, state.policy);
  markerBytes(state);
  scanTree(state.receipt.canonicalRootPath, state.receipt.identity, state.policy);
  return { state, registered: permitState.registered };
}

function layout(rootPath, databaseRelativePath) {
  return ["", "-wal", "-shm", "-journal"].map((suffix) => {
    const path = `${databaseRelativePath}${suffix}`;
    const info = stat(NodePath.join(rootPath, path));
    return info
      ? {
          relativePath: path,
          present: true,
          identity: identity(info),
          size: Number(info.size),
          nlink: Number(info.nlink),
        }
      : { relativePath: path, present: false };
  });
}

function jsonEqual(left, right) {
  return JSON.stringify(left) === JSON.stringify(right);
}

export async function observeSyntheticClose(owner, { permit, producerStep, resource, close }) {
  boundedText(producerStep, "producerStep");
  if (!resource || typeof resource !== "object" || typeof close !== "function") {
    refuse("invalid_close", "an audited resource and its actual close callback are required");
  }
  const { state, registered } = requirePermit(owner, permit);
  if (resources.has(resource) || registered.closeState !== "unproved") {
    refuse("invalid_close", "resource closure is repeated, concurrent or previously unknown");
  }
  resources.set(resource, "closing");
  registered.closeState = "closing";
  try {
    // The audited caller opens this permit's path and supplies the resource's real close operation.
    await close(resource);
    requirePermit(owner, permit);
    const proof = Object.freeze({});
    closeProofs.set(proof, {
      state,
      permit,
      producerStep,
      layout: layout(state.receipt.canonicalRootPath, permit.relativePath),
      closureId: NodeCrypto.randomUUID(),
      consumed: false,
    });
    resources.set(resource, "closed");
    registered.closeState = "closed";
    registered.currentCloseProof = proof;
    return proof;
  } catch (error) {
    resources.set(resource, "unknown");
    registered.closeState = "unknown";
    throw error;
  }
}

function checkSignal(signal) {
  if (signal !== undefined && !(signal instanceof AbortSignal))
    refuse("invalid_input", "signal must be an AbortSignal");
  if (signal?.aborted) refuse("cancelled", "fixture verification was cancelled");
}

async function fileHash(entry, policy, signal) {
  checkSignal(signal);
  const handle = await NodeFSP.open(
    entry.path,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
  );
  let stream;
  try {
    const before = await handle.stat({ bigint: true });
    if (
      !before.isFile() ||
      before.nlink !== 1n ||
      !sameIdentity(before, identity(entry.info)) ||
      before.size !== entry.info.size ||
      before.mtimeNs !== entry.info.mtimeNs ||
      before.ctimeNs !== entry.info.ctimeNs
    ) {
      refuse("changed_identity", "manifest file changed before hashing");
    }
    const digest = NodeCrypto.createHash("sha256");
    let bytes = 0;
    stream = handle.createReadStream({ autoClose: false, highWaterMark: 64 * 1024, signal });
    for await (const chunk of stream) {
      checkSignal(signal);
      bytes += chunk.length;
      if (bytes > policy.maxFileBytes) refuse("manifest_limit", "stream exceeded file byte limit");
      digest.update(chunk);
    }
    const after = await handle.stat({ bigint: true });
    const current = stat(entry.path);
    if (
      !current ||
      !sameIdentity(current, identity(before)) ||
      bytes !== Number(before.size) ||
      after.size !== before.size ||
      after.mtimeNs !== before.mtimeNs ||
      after.ctimeNs !== before.ctimeNs ||
      current.size !== before.size ||
      current.mtimeNs !== before.mtimeNs ||
      current.ctimeNs !== before.ctimeNs ||
      current.nlink !== 1n
    ) {
      refuse("changed_identity", "manifest file changed while hashing");
    }
    return digest.digest("hex");
  } finally {
    stream?.destroy();
    await handle.close();
  }
}

async function manifestFor(state, signal) {
  const files = scanTree(state.receipt.canonicalRootPath, state.receipt.identity, state.policy);
  const manifest = [];
  for (const entry of files) {
    manifest.push({
      relativePath: entry.relativePath,
      identity: identity(entry.info),
      nlink: Number(entry.info.nlink),
      size: Number(entry.info.size),
      sha256: await fileHash(entry, state.policy, signal),
    });
  }
  return manifest;
}

export function syntheticFixtureReceiptSha256(receipt) {
  return NodeCrypto.createHash("sha256")
    .update(`${JSON.stringify(receipt)}\n`)
    .digest("hex");
}

export async function sealSyntheticFixture(
  owner,
  { databaseRelativePath, producerStep, closedProof },
) {
  const state = requireOwner(owner);
  const dbPath = relativePath(databaseRelativePath);
  boundedText(producerStep, "producerStep");
  const proof =
    closedProof && typeof closedProof === "object" ? closeProofs.get(closedProof) : null;
  if (
    !proof ||
    proof.state !== state ||
    proof.consumed ||
    proof.sealing ||
    proof.producerStep !== producerStep ||
    proof.permit.relativePath !== dbPath ||
    state.databases.get(dbPath)?.currentCloseProof !== closedProof
  ) {
    refuse("invalid_close_proof", "seal requires the original matching unconsumed close proof");
  }
  const { registered } = requirePermit(owner, proof.permit);
  if (
    registered.closeState !== "closed" ||
    !jsonEqual(proof.layout, layout(state.receipt.canonicalRootPath, dbPath))
  ) {
    refuse("changed_layout", "database layout changed after observed closure");
  }
  proof.sealing = true;
  try {
    const manifest = await manifestFor(state);
    requirePermit(owner, proof.permit);
    const closedLayout = layout(state.receipt.canonicalRootPath, dbPath);
    if (registered.closeState !== "closed" || !jsonEqual(proof.layout, closedLayout)) {
      refuse("changed_layout", "database layout changed during sealing");
    }
    const receipt = freeze({
      schema: "jones-performance-fixture/v1",
      provenance: "synthetic-produced",
      creationReceipt: state.receipt,
      producerStep,
      closure: { kind: "observed-resource-close", closureId: proof.closureId, completed: true },
      databaseRelativePath: dbPath,
      layout: closedLayout,
      manifest,
    });
    if (Buffer.byteLength(`${JSON.stringify(receipt)}\n`) > state.policy.maxReceiptBytes) {
      refuse("manifest_limit", "fixture receipt exceeds byte limit");
    }
    proof.consumed = true;
    registered.sealed = true;
    return receipt;
  } finally {
    proof.sealing = false;
  }
}

export async function validateSyntheticFixture({
  receipt,
  expectedReceiptSha256,
  expectedBinding,
  policy,
  signal,
}) {
  checkSignal(signal);
  const checkedPolicy = policyValue(policy);
  const checkedBinding = bindingValue(expectedBinding);
  if (typeof expectedReceiptSha256 !== "string" || !/^[a-f0-9]{64}$/.test(expectedReceiptSha256)) {
    refuse("unpinned_receipt", "an independently trusted producer receipt digest is required");
  }
  const bytes = `${JSON.stringify(receipt)}\n`;
  if (
    Buffer.byteLength(bytes) > checkedPolicy.maxReceiptBytes ||
    syntheticFixtureReceiptSha256(receipt) !== expectedReceiptSha256
  ) {
    refuse("receipt_mismatch", "receipt is oversized or differs from its trusted digest");
  }
  if (
    !receipt ||
    receipt.schema !== "jones-performance-fixture/v1" ||
    receipt.provenance !== "synthetic-produced" ||
    receipt.creationReceipt?.schema !== "jones-performance-root/v1" ||
    receipt.creationReceipt.provenance !== "synthetic-created" ||
    !jsonEqual(receipt.creationReceipt.binding, checkedBinding) ||
    receipt.closure?.kind !== "observed-resource-close" ||
    receipt.closure.completed !== true ||
    typeof receipt.closure.closureId !== "string" ||
    !Array.isArray(receipt.manifest) ||
    !Array.isArray(receipt.layout)
  ) {
    refuse("invalid_receipt", "fixture receipt binding, provenance or closure is invalid");
  }
  boundedText(receipt.producerStep, "producerStep");
  const creation = receipt.creationReceipt;
  const rootPath = absolutePath(creation.canonicalRootPath, "receipt root");
  const parentPath = absolutePath(creation.canonicalParentPath, "receipt parent");
  const dbPath = relativePath(receipt.databaseRelativePath);
  checkBoundary(rootPath, checkedPolicy);
  if (
    rootPath !== creation.canonicalRootPath ||
    parentPath !== creation.canonicalParentPath ||
    NodePath.resolve(rootPath, "..") !== parentPath ||
    creation.identity?.uid !== currentUid() ||
    creation.identity.mode !== 0o700
  ) {
    refuse("invalid_receipt", "creation root location or private identity is invalid");
  }
  inspectAncestry(parentPath);
  const rootInfo = stat(rootPath);
  if (
    !rootInfo ||
    !rootInfo.isDirectory() ||
    rootInfo.isSymbolicLink() ||
    !sameIdentity(rootInfo, creation.identity)
  ) {
    refuse("changed_identity", "fixture root identity differs from producer receipt");
  }
  const files = scanTree(rootPath, creation.identity, checkedPolicy);
  const marker = files.find((entry) => entry.relativePath === markerName);
  if (!marker || marker.info.size > BigInt(checkedPolicy.maxReceiptBytes))
    refuse("invalid_marker", "bounded creation marker is missing");
  const markerData = NodeFS.readFileSync(marker.path);
  if (NodeCrypto.createHash("sha256").update(markerData).digest("hex") !== creation.markerSha256) {
    refuse("invalid_marker", "creation marker digest differs");
  }
  const expectedMarker = {
    schema: "jones-performance-root-marker/v1",
    provenance: creation.provenance,
    binding: creation.binding,
    rootId: creation.rootId,
    canonicalParentPath: parentPath,
    canonicalRootPath: rootPath,
    identity: creation.identity,
  };
  if (!markerData.equals(Buffer.from(`${JSON.stringify(expectedMarker)}\n`))) {
    refuse("invalid_marker", "creation marker does not bind this root");
  }
  if (files.length !== receipt.manifest.length || files.length > checkedPolicy.maxFiles) {
    refuse("manifest_mismatch", "fixture file set differs from sealed manifest");
  }
  const expectedLayout = layout(rootPath, dbPath);
  if (!expectedLayout[0].present || !jsonEqual(expectedLayout, receipt.layout)) {
    refuse("changed_layout", "fixture main/WAL/SHM/journal layout differs");
  }
  let verifiedBytes = 0;
  for (let index = 0; index < files.length; index++) {
    const entry = files[index];
    const expected = receipt.manifest[index];
    if (
      !expected ||
      expected.relativePath !== entry.relativePath ||
      expected.nlink !== 1 ||
      !Number.isSafeInteger(expected.size) ||
      expected.size < 0 ||
      expected.size !== Number(entry.info.size) ||
      !sameIdentity(entry.info, expected.identity ?? {}) ||
      !/^[a-f0-9]{64}$/.test(expected.sha256)
    ) {
      refuse("manifest_mismatch", "fixture manifest metadata differs");
    }
    if ((await fileHash(entry, checkedPolicy, signal)) !== expected.sha256) {
      refuse("hash_mismatch", "fixture file hash differs from sealed manifest");
    }
    verifiedBytes += expected.size;
  }
  checkSignal(signal);
  const afterFiles = scanTree(rootPath, creation.identity, checkedPolicy);
  if (
    afterFiles.length !== files.length ||
    afterFiles.some(
      (entry, index) =>
        entry.relativePath !== files[index].relativePath ||
        !sameIdentity(entry.info, identity(files[index].info)) ||
        entry.info.size !== files[index].info.size ||
        entry.info.mtimeNs !== files[index].info.mtimeNs ||
        entry.info.ctimeNs !== files[index].info.ctimeNs,
    ) ||
    !jsonEqual(expectedLayout, layout(rootPath, dbPath))
  ) {
    refuse("changed_layout", "fixture changed during verification");
  }
  const validatedReceipt = freeze(JSON.parse(bytes));
  return freeze({
    access: "readonly",
    canonicalPath: NodePath.join(rootPath, dbPath),
    receipt: validatedReceipt,
    receiptSha256: expectedReceiptSha256,
    layout: expectedLayout,
    verifiedFiles: files.length,
    verifiedBytes,
  });
}

export function disposeOwnedRoot(owner, { childReceipts = [] } = {}) {
  const state = owner && owners.get(owner);
  const base = { schema: "jones-performance-cleanup/v1", creationReceipt: state?.receipt ?? null };
  if (!state || state.disposed)
    return freeze({
      ...base,
      outcome: "unknown",
      absent: false,
      childReceipts: [],
      reason: "invalid_owner",
    });
  try {
    requireOwner(owner);
    for (const database of state.databases.values()) {
      if (database.closeState !== "closed")
        refuse("unknown_close", "database resource close is unproved; retain the tree");
    }
    if (!Array.isArray(childReceipts) || childReceipts.length !== state.children.size) {
      refuse("unknown_child", "cleanup requires every registered child receipt");
    }
    const supplied = new Set(childReceipts);
    for (const token of state.children) {
      const child = childTokens.get(token);
      if (
        !child.receipt ||
        !supplied.has(child.receipt) ||
        !child.receipt.closed ||
        !child.receipt.reaped ||
        child.receipt.outcome === "unknown"
      ) {
        refuse("unknown_child", "child close/reap is unproved; retain the entire tree");
      }
    }
    markerBytes(state);
    scanTree(state.receipt.canonicalRootPath, state.receipt.identity, state.policy);
    // The private root and cooperative UID are the custody boundary; this is not hostile-race containment.
    NodeFS.rmSync(state.receipt.canonicalRootPath, { recursive: true, force: false });
    const absent = stat(state.receipt.canonicalRootPath) === null;
    if (!absent) refuse("cleanup_unknown", "root removal did not establish absence");
    state.disposed = true;
    return freeze({ ...base, outcome: "complete", absent: true, childReceipts, reason: null });
  } catch (error) {
    return freeze({
      ...base,
      outcome: "retained",
      absent: false,
      childReceipts,
      reason: error.code ?? "cleanup_failed",
    });
  }
}

// Shared only with the audited leaf supervisor. Tokens and final receipts never survive JSON cloning.
export const ownedChildCustody = Object.freeze({
  begin(owner) {
    const state = requireOwner(owner);
    markerBytes(state);
    scanTree(state.receipt.canonicalRootPath, state.receipt.identity, state.policy);
    const token = Object.freeze({});
    childTokens.set(token, { state, receipt: null });
    state.children.add(token);
    return { token, binding: state.receipt.binding, rootPath: state.receipt.canonicalRootPath };
  },
  finish(token, receipt) {
    const child = childTokens.get(token);
    if (!child || child.receipt)
      refuse("unknown_child", "child token is missing or already terminal");
    child.receipt = freeze(receipt);
    return child.receipt;
  },
});
