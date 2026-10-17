// @effect-diagnostics-next-line nodeBuiltinImport:off -- Startup-only private-file reads require no-follow descriptors and POSIX identity checks.
import * as NodeFS from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Resolve the fixed installation-owned activation path without shell expansion.
import * as NodePath from "node:path";

export interface ControlPlaneWorkstreamActivation {
  readonly baseUrl: URL;
  readonly ownerId: string;
  readonly principalId: string;
  readonly authorizationRevision: number;
  readonly keyId: string;
  readonly signingSecret: string;
}

export type WorkstreamActivation =
  | { readonly state: "disabled" }
  | { readonly state: "invalid" }
  | { readonly state: "enabled"; readonly value: ControlPlaneWorkstreamActivation };

// Plain HTTP is accepted only for an IP-literal loopback control plane on the
// same host. Reaching it through the tailnet HTTPS route would pass Tailscale
// Serve, which adds Tailscale identity headers to requests from untagged nodes;
// the control plane refuses those alongside signed requests. HMAC request
// signing is identical on both origins. `localhost` and other hostnames stay
// HTTPS-only because name resolution could leave the host.
function hasCanonicalOrigin(baseUrl: URL): boolean {
  return (
    baseUrl.protocol === "https:" ||
    (baseUrl.protocol === "http:" &&
      (baseUrl.hostname === "127.0.0.1" || baseUrl.hostname === "[::1]"))
  );
}

export function hasCanonicalActivation(config: ControlPlaneWorkstreamActivation): boolean {
  const headerValues = [config.ownerId, config.principalId, config.keyId, config.signingSecret];
  return (
    hasCanonicalOrigin(config.baseUrl) &&
    !config.baseUrl.username &&
    !config.baseUrl.password &&
    !config.baseUrl.hash &&
    !config.baseUrl.search &&
    (config.baseUrl.pathname === "/" || config.baseUrl.pathname === "") &&
    Number.isSafeInteger(config.authorizationRevision) &&
    config.authorizationRevision > 0 &&
    headerValues.every((value) => value.length > 0 && !/[\r\n]/.test(value)) &&
    decodeSigningSecret(config.signingSecret) !== null
  );
}

export function decodeSigningSecret(value: string): Buffer | null {
  if (!/^[A-Za-z0-9+/]{43}=$/.test(value)) return null;
  const key = Buffer.from(value, "base64");
  return key.length === 32 && key.toString("base64") === value ? key : null;
}

const ENVIRONMENT_KEYS = [
  "T3_WORKSTREAM_CONTROL_PLANE_URL",
  "T3_WORKSTREAM_OWNER_ID",
  "T3_WORKSTREAM_PRINCIPAL_ID",
  "T3_WORKSTREAM_KEY_ID",
  "T3_WORKSTREAM_SIGNING_SECRET",
  "T3_WORKSTREAM_AUTHORIZATION_REVISION",
] as const;
const MAX_BYTES = 65_536;
const invalid = (): WorkstreamActivation => ({ state: "invalid" });

export function activationFromEnvironment(
  environment: NodeJS.ProcessEnv = process.env,
): WorkstreamActivation {
  if (ENVIRONMENT_KEYS.every((key) => environment[key] === undefined)) return { state: "disabled" };
  const [raw, ownerId, principalId, keyId, signingSecret, rawRevision] = ENVIRONMENT_KEYS.map(
    (key) => environment[key],
  );
  if (!raw || !ownerId || !principalId || !keyId || !signingSecret || !rawRevision)
    return invalid();
  try {
    const value = {
      baseUrl: new URL(raw),
      ownerId,
      principalId,
      keyId,
      signingSecret,
      authorizationRevision: Number(rawRevision),
    };
    return hasCanonicalActivation(value) ? { state: "enabled", value } : invalid();
  } catch {
    return invalid();
  }
}

function parseActivation(bytes: Buffer): WorkstreamActivation {
  const text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes);
  const lines = text.split("\n");
  if (lines.at(-1) === "") lines.pop();
  if (lines.length !== ENVIRONMENT_KEYS.length) return invalid();
  const environment: NodeJS.ProcessEnv = {};
  for (const line of lines) {
    const equals = line.indexOf("=");
    if (equals < 1) return invalid();
    const key = line.slice(0, equals);
    const value = line.slice(equals + 1);
    if (!ENVIRONMENT_KEYS.some((allowed) => allowed === key) || Object.hasOwn(environment, key))
      return invalid();
    environment[key] = value;
  }
  for (const key of [
    "T3_WORKSTREAM_OWNER_ID",
    "T3_WORKSTREAM_PRINCIPAL_ID",
    "T3_WORKSTREAM_KEY_ID",
  ]) {
    if (!/^[A-Za-z0-9_.:-]{1,256}$/.test(environment[key] ?? "")) return invalid();
  }
  if (!/^[1-9][0-9]*$/.test(environment.T3_WORKSTREAM_AUTHORIZATION_REVISION ?? ""))
    return invalid();
  const activation = activationFromEnvironment(environment);
  // Staging emits a canonical root origin; reject quoting, whitespace and URL normalization in file data.
  return activation.state === "enabled" &&
    activation.value.baseUrl.href === environment.T3_WORKSTREAM_CONTROL_PLANE_URL
    ? activation
    : invalid();
}

type DirectoryIdentity = { readonly path: string; readonly stat: NodeFS.Stats };
const sameIdentity = (left: NodeFS.Stats, right: NodeFS.Stats): boolean =>
  left.dev === right.dev &&
  left.ino === right.ino &&
  left.uid === right.uid &&
  left.mode === right.mode;

function directoryChain(baseDir: string, uid: number): DirectoryIdentity[] {
  if (!NodePath.isAbsolute(baseDir) || NodePath.resolve(baseDir) !== baseDir)
    throw new Error("invalid_path");
  const directories: DirectoryIdentity[] = [];
  let current = NodePath.parse(baseDir).root;
  for (const component of [
    "",
    ...baseDir.slice(current.length).split(NodePath.sep).filter(Boolean),
    "workstreams",
  ]) {
    if (component) current = NodePath.join(current, component);
    const stat = NodeFS.lstatSync(current);
    const privateDirectory = current === NodePath.join(baseDir, "workstreams");
    const installationDirectory = current === baseDir;
    const stickyRootAncestor =
      !privateDirectory && !installationDirectory && stat.uid === 0 && (stat.mode & 0o1000) !== 0;
    if (
      !stat.isDirectory() ||
      (stat.uid !== uid && stat.uid !== 0) ||
      ((privateDirectory || installationDirectory) && stat.uid !== uid) ||
      (!stickyRootAncestor && (stat.mode & 0o022) !== 0) ||
      (privateDirectory && (stat.mode & 0o7777) !== 0o700)
    )
      throw new Error("unsafe_directory");
    directories.push({ path: current, stat });
  }
  return directories;
}

/** A single startup snapshot; never sources shell text or changes the provider-child environment. */
export function resolveWorkstreamActivation(
  baseDir: string,
  environment: NodeJS.ProcessEnv = process.env,
): WorkstreamActivation {
  const explicit = activationFromEnvironment(environment);
  if (explicit.state !== "disabled") return explicit;
  let descriptor: number | undefined;
  try {
    const uid = process.getuid?.();
    if (uid === undefined) return invalid();
    const directories = directoryChain(baseDir, uid);
    const file = NodePath.join(baseDir, "workstreams", "activation.env");
    descriptor = NodeFS.openSync(
      file,
      NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW | NodeFS.constants.O_NONBLOCK,
    );
    try {
      const before = NodeFS.fstatSync(descriptor);
      if (
        !before.isFile() ||
        before.uid !== uid ||
        (before.mode & 0o7777) !== 0o600 ||
        before.size > MAX_BYTES
      )
        return invalid();
      const bytes = Buffer.alloc(MAX_BYTES + 1);
      let length = 0;
      while (length < bytes.length) {
        const read = NodeFS.readSync(descriptor, bytes, length, bytes.length - length, null);
        if (read === 0) break;
        length += read;
      }
      const after = NodeFS.fstatSync(descriptor);
      if (
        length > MAX_BYTES ||
        length !== before.size ||
        !sameIdentity(before, after) ||
        before.size !== after.size ||
        before.mtimeMs !== after.mtimeMs ||
        before.ctimeMs !== after.ctimeMs ||
        !sameIdentity(after, NodeFS.lstatSync(file)) ||
        directories.some(({ path, stat }) => !sameIdentity(stat, NodeFS.lstatSync(path)))
      )
        return invalid();
      return parseActivation(bytes.subarray(0, length));
    } finally {
      NodeFS.closeSync(descriptor);
    }
  } catch (error) {
    // Only absence during discovery is unconfigured; an opened file that changes is invalid.
    return descriptor === undefined && (error as NodeJS.ErrnoException)?.code === "ENOENT"
      ? { state: "disabled" }
      : invalid();
  }
}
