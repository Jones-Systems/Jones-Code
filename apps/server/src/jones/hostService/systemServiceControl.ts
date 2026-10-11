// @effect-diagnostics nodeBuiltinImport:off
// Client and pure renderers for the root-owned system service control worker
// in systemServiceControl.pl. The worker is reached only through `sudo -n`
// after the owner authenticated sudo for a bootstrap, launcher replacement or
// recovery session; no sudoers rule grants it. Normal Download and Install do
// not use it: the qualified launcher supervises its child without root.
import * as NodeCrypto from "node:crypto";
import * as NodePath from "node:path";
import { runCapturedProcess, type CapturedProcessResult } from "../cloud/qualifiedLinuxLease.ts";
import {
  QUALIFIED_WRITER_OBSERVER_PATH,
  QUALIFIED_WRITER_OBSERVER_SHA256,
  renderWriterObserverSocketUnits,
} from "../cloud/qualifiedWriterObserver.ts";

export const SYSTEM_SERVICE_CONTROL_PATH =
  "/usr/local/libexec/jones-code/jones-system-service-control";
export const SYSTEM_SERVICE_POLICY_PATH = "/etc/jones-code/system-service.policy";
/** SHA-256 of systemServiceControl.pl; a root installation reads this digest back. */
export const SYSTEM_SERVICE_CONTROL_SHA256 =
  "93fd38b9a1038725cd8deea90d829a7927618cba616f4f40d5281c76fb808551";
export const OWNED_SYSTEM_DROPIN = "zzzz-jones-qualified-launcher.conf";
const OWNED_MARKER = "# Jones qualified system launcher protocol 1";
const HEADER = "jones-system-service-control 1";
const OUTPUT_LIMIT = 64 * 1024;
// The worker stops itself at 150 seconds, which covers a unit's default stop timeout.
const DEADLINE_MS = 180_000;

const SHA = /^[a-f0-9]{64}$/;
const VERSION = /^\d{1,9}\.\d{1,9}\.\d{1,9}-preview\.\d{8}\.\d{1,20}(?:\.\d{1,20})?$/;
const UNIT = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,200}\.service$/;
const SLICE = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,200}\.slice$/;
const USER = /^[a-z_][a-z0-9_-]{0,31}$/;
const ID = /^[1-9]\d{0,9}$/;
const PATH = /^(?:\/[A-Za-z0-9._@+-]{1,255}){1,64}$/;

class SystemServiceControlError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SystemServiceControlError";
  }
}
const invalid = (message: string): never => {
  throw new SystemServiceControlError(message);
};

export const isCanonicalSystemPath = (path: string): boolean =>
  PATH.test(path) && !path.split("/").some((component) => component === "." || component === "..");

export type SystemControlRequest =
  | { readonly operation: "inspect" }
  | {
      readonly operation: "stop";
      readonly unitDigest: string;
      readonly owned: string;
      readonly mainPid: number;
      readonly mainBirth: string;
    }
  | {
      readonly operation: "install-launcher";
      readonly unitDigest: string;
      readonly owned: string;
      readonly launcherVersion: string;
    }
  | { readonly operation: "start"; readonly unitDigest: string; readonly owned: string }
  | { readonly operation: "archive-launcher"; readonly unitDigest: string; readonly owned: string };

/** The worker's fixed argument order; it refuses any other shape. */
export function systemControlArguments(request: SystemControlRequest): string[] {
  if (request.operation === "inspect") return ["inspect"];
  if (!SHA.test(request.unitDigest) || !(request.owned === "absent" || SHA.test(request.owned)))
    return invalid("System control preimages are malformed.");
  const preimage = ["--expect-unit", request.unitDigest, "--expect-owned", request.owned];
  switch (request.operation) {
    case "stop":
      if (
        !Number.isSafeInteger(request.mainPid) ||
        request.mainPid < 1 ||
        !/^\d{1,20}$/.test(request.mainBirth)
      )
        return invalid("System control process identity is malformed.");
      return [
        "stop",
        ...preimage,
        "--expect-main-pid",
        String(request.mainPid),
        "--expect-main-birth",
        request.mainBirth,
      ];
    case "install-launcher":
      if (!VERSION.test(request.launcherVersion))
        return invalid("System control launcher version is malformed.");
      return ["install-launcher", ...preimage, "--launcher-version", request.launcherVersion];
    case "start":
      return ["start", ...preimage];
    case "archive-launcher":
      if (request.owned === "absent") return invalid("Only an owned launcher drop-in is archived.");
      return ["archive-launcher", ...preimage];
  }
}

export interface SystemUnitInspection {
  readonly policySha256: string;
  readonly unitDigest: string;
  readonly owned: string;
  /** `fragment UID MODE SHA256` and `dropin NAME UID MODE SHA256`; never contents. */
  readonly files: readonly string[];
  readonly activeState: string;
  readonly subState: string;
  readonly mainPid: number;
  readonly mainBirth?: string;
  readonly controlGroup?: string;
  readonly killMode: string;
  /** `retained`, `launcher VERSION` or `other`; never the command line itself. */
  readonly exec: string;
  readonly serve: { readonly port?: number; readonly host?: string; readonly mode?: string };
}

export type SystemControlResult =
  | { readonly status: "complete"; readonly inspection: SystemUnitInspection }
  | { readonly status: "rejected" | "unknown"; readonly reason: string };

function parseInspection(lines: readonly string[]): SystemUnitInspection | undefined {
  const single = new Map<string, string>();
  const files: string[] = [];
  for (const line of lines) {
    const match = /^([a-z-]+) (.+)$/.exec(line);
    if (match === null) return undefined;
    const [, key = "", value = ""] = match;
    if (key === "fragment" || key === "dropin") {
      if (
        !(
          key === "fragment"
            ? /^\d{1,10} [0-7]{4} [a-f0-9]{64}$/
            : /^[A-Za-z0-9][A-Za-z0-9_.@-]{0,200}\.conf \d{1,10} [0-7]{4} [a-f0-9]{64}$/
        ).test(value)
      )
        return undefined;
      files.push(line);
      continue;
    }
    if (single.has(key)) return undefined;
    single.set(key, value);
  }
  const allowed = new Set([
    "policy",
    "unit-digest",
    "owned",
    "active",
    "main-pid",
    "main-birth",
    "control-group",
    "kill-mode",
    "exec",
    "serve-port",
    "serve-host",
    "serve-mode",
  ]);
  if ([...single.keys()].some((key) => !allowed.has(key))) return undefined;
  const policy = single.get("policy") ?? "";
  const digest = single.get("unit-digest") ?? "";
  const owned = single.get("owned") ?? "";
  const active = /^([a-z-]+) ([a-z-]+)$/.exec(single.get("active") ?? "");
  const mainPid = single.get("main-pid") ?? "";
  const birth = single.get("main-birth");
  const exec = single.get("exec") ?? "";
  const port = single.get("serve-port");
  if (
    !SHA.test(policy) ||
    !SHA.test(digest) ||
    !(owned === "absent" || SHA.test(owned)) ||
    active === null ||
    !/^\d{1,10}$/.test(mainPid) ||
    (birth !== undefined && !/^\d{1,20}$/.test(birth)) ||
    !/^[a-z-]+$/.test(single.get("kill-mode") ?? "") ||
    !(exec === "retained" || exec === "other" || /^launcher \S+$/.test(exec)) ||
    files[0]?.startsWith("fragment ") !== true ||
    files.slice(1).some((file) => !file.startsWith("dropin ")) ||
    (port !== undefined && !/^[1-9]\d{0,4}$/.test(port))
  )
    return undefined;
  const controlGroup = single.get("control-group");
  const host = single.get("serve-host");
  const mode = single.get("serve-mode");
  return {
    policySha256: policy,
    unitDigest: digest,
    owned,
    files,
    activeState: active[1]!,
    subState: active[2]!,
    mainPid: Number(mainPid),
    ...(birth === undefined ? {} : { mainBirth: birth }),
    ...(controlGroup === undefined ? {} : { controlGroup }),
    killMode: single.get("kill-mode")!,
    exec,
    serve: {
      ...(port === undefined ? {} : { port: Number(port) }),
      ...(host === undefined ? {} : { host }),
      ...(mode === undefined ? {} : { mode }),
    },
  };
}

/**
 * The header is flushed before the worker does anything, so a run without it
 * (sudo refused, worker missing) had no worker effect. Once the header is
 * present only the worker's own terminal line can report no effect.
 */
export function parseSystemControlResult(result: CapturedProcessResult): SystemControlResult {
  if (result.outcome === "spawn-failed") return { status: "rejected", reason: "sudo-unavailable" };
  if (result.outcome !== "exited") return { status: "unknown", reason: result.outcome };
  const lines = result.stdout.split("\n");
  if (lines[0] !== HEADER) {
    return result.stdout === "" && result.code !== 0
      ? { status: "rejected", reason: "worker-not-started" }
      : { status: "unknown", reason: "malformed" };
  }
  if (lines.pop() !== "") return { status: "unknown", reason: "malformed" };
  const terminal = lines.pop() ?? "";
  const failed = /^(rejected|unknown) ([a-z-]+)$/.exec(terminal);
  if (failed !== null) {
    const [, status = "", reason = ""] = failed;
    if (result.code !== (status === "rejected" ? 2 : 3))
      return { status: "unknown", reason: "malformed" };
    return { status: status as "rejected" | "unknown", reason };
  }
  if (terminal !== "complete" || result.code !== 0)
    return { status: "unknown", reason: "malformed" };
  const inspection = parseInspection(lines.slice(1));
  return inspection === undefined
    ? { status: "unknown", reason: "malformed" }
    : { status: "complete", inspection };
}

/** Runs one worker operation through `sudo -n` with no terminal input and an empty environment. */
export async function runSystemServiceControl(
  request: SystemControlRequest,
  options: {
    readonly sudoPath?: string;
    readonly workerPath?: string;
    readonly deadlineMs?: number;
  } = {},
): Promise<SystemControlResult> {
  const args = systemControlArguments(request);
  const result = await runCapturedProcess({
    command: options.sudoPath ?? "/usr/bin/sudo",
    args: ["-n", "--", options.workerPath ?? SYSTEM_SERVICE_CONTROL_PATH, ...args],
    deadlineMs: options.deadlineMs ?? DEADLINE_MS,
    outputLimit: OUTPUT_LIMIT,
  });
  return parseSystemControlResult(result);
}

export interface SystemServicePolicy {
  readonly unit: string;
  readonly user: string;
  readonly uid: number;
  readonly gid: number;
  readonly base: string;
  readonly slice: string;
  readonly retainedEntry: string;
}

export function renderSystemServicePolicy(policy: SystemServicePolicy): string {
  if (
    !UNIT.test(policy.unit) ||
    !USER.test(policy.user) ||
    policy.user === "root" ||
    !ID.test(String(policy.uid)) ||
    !ID.test(String(policy.gid)) ||
    !isCanonicalSystemPath(policy.base) ||
    !SLICE.test(policy.slice) ||
    !isCanonicalSystemPath(policy.retainedEntry)
  )
    return invalid("System service policy values are outside the closed grammar.");
  return [
    "jones-system-service-policy 1",
    `unit ${policy.unit}`,
    `user ${policy.user}`,
    `uid ${policy.uid}`,
    `gid ${policy.gid}`,
    `base ${policy.base}`,
    `slice ${policy.slice}`,
    `retained-entry ${policy.retainedEntry}`,
    "",
  ].join("\n");
}

export interface RetainedServeCommand {
  readonly port?: number;
  readonly host?: string;
  readonly mode?: "web" | "desktop";
  /**
   * Only for the historical `start` form: the directory it names, equal to the
   * unit's effective WorkingDirectory. The launcher runs the retained version
   * with `serve` from that inherited directory, so `start`'s automatic project
   * bootstrap from it does not continue.
   */
  readonly workingDirectory?: string;
}

/**
 * The closed grammar the worker accepts for the retained executable: `serve`
 * with known flags, or `start` with the same flags, `--mode web` and
 * exactly `--no-browser DIR` last, where DIR is the unit's WorkingDirectory.
 */
export function parseRetainedServeCommand(
  argv: string,
  retainedEntry: string,
  base: string,
  workingDirectory?: string,
): RetainedServeCommand | undefined {
  const words = argv.split(/ +/).filter((word) => word !== "");
  if (words.some((word) => !/^[A-Za-z0-9._@+/-]+$/.test(word))) return undefined;
  if (words.shift() !== retainedEntry) return undefined;
  const verb = words.shift();
  let directory: string | undefined;
  if (verb === "start") {
    if (words.length < 2 || words.at(-2) !== "--no-browser") return undefined;
    directory = words.pop()!;
    words.pop();
    if (!isCanonicalSystemPath(directory) || directory !== workingDirectory) return undefined;
  } else if (verb !== "serve") return undefined;
  if (words.length % 2 !== 0) return undefined;
  const flags = new Map<string, string>();
  for (let index = 0; index < words.length; index += 2) {
    const name = /^--(host|port|base-dir|mode)$/.exec(words[index]!)?.[1];
    if (name === undefined || flags.has(name)) return undefined;
    flags.set(name, words[index + 1]!);
  }
  const port = flags.get("port");
  const host = flags.get("host");
  const mode = flags.get("mode");
  if (
    (flags.has("base-dir") && flags.get("base-dir") !== base) ||
    (port !== undefined && (!/^[1-9]\d{0,4}$/.test(port) || Number(port) > 65535)) ||
    (host !== undefined && !/^(?:(?:\d{1,3}\.){3}\d{1,3}|localhost)$/.test(host)) ||
    (mode !== undefined && mode !== "web" && mode !== "desktop") ||
    (directory !== undefined && mode !== "web")
  )
    return undefined;
  return {
    ...(port === undefined ? {} : { port: Number(port) }),
    ...(host === undefined ? {} : { host }),
    ...(mode === undefined ? {} : { mode: mode as "web" | "desktop" }),
    ...(directory === undefined ? {} : { workingDirectory: directory }),
  };
}

/** The exact bytes the worker writes; adoption compares the worker's reported digest. */
export function renderOwnedSystemDropin(input: {
  readonly base: string;
  readonly launcherVersion: string;
  readonly serve: RetainedServeCommand;
}): string {
  if (!isCanonicalSystemPath(input.base) || !VERSION.test(input.launcherVersion))
    return invalid("Owned launcher drop-in values are outside the closed grammar.");
  return [
    OWNED_MARKER,
    "[Service]",
    "ExecStart=",
    `ExecStart=${NodePath.join(input.base, "runtime/versions", input.launcherVersion, "t3")} __service-launcher`,
    `Environment=T3CODE_HOME=${input.base}`,
    ...(input.serve.port === undefined ? [] : [`Environment=T3CODE_PORT=${input.serve.port}`]),
    ...(input.serve.host === undefined ? [] : [`Environment=T3CODE_HOST=${input.serve.host}`]),
    ...(input.serve.mode === undefined ? [] : [`Environment=T3CODE_MODE=${input.serve.mode}`]),
    "",
  ].join("\n");
}

export const sha256 = (bytes: string | Uint8Array): string =>
  NodeCrypto.createHash("sha256").update(bytes).digest("hex");

/** Same canonical records as the worker: fragment first, then non-owned drop-ins by name. */
export function systemUnitDigest(records: readonly string[]): string {
  return sha256(["jones-system-unit-digest 1", ...records, ""].join("\n"));
}

export const systemUnitRecord = (
  label: string,
  stat: { readonly uid: number; readonly mode: number },
  bytes: Uint8Array,
): string =>
  `${label} ${stat.uid} ${(stat.mode & 0o7777).toString(8).padStart(4, "0")} ${sha256(bytes)}`;

export interface SystemBootstrapFile {
  readonly path: string;
  readonly mode: "0755" | "0644";
  readonly sha256: string;
  /** Source of the bytes: a repository file at the qualified commit, or rendered text. */
  readonly source: string;
  readonly text?: string;
}

/**
 * The one-time root bootstrap for one host, as text and digests only. Nothing
 * here installs, enables or authorizes it: that is a separate approved root
 * operation using the helper and worker bytes from the same qualified source.
 */
export function renderSystemBootstrapManifest(policy: SystemServicePolicy): {
  readonly files: readonly SystemBootstrapFile[];
  readonly enable: readonly string[];
} {
  const policyText = renderSystemServicePolicy(policy);
  const units = renderWriterObserverSocketUnits({ user: policy.user, base: policy.base });
  return {
    files: [
      {
        path: QUALIFIED_WRITER_OBSERVER_PATH,
        mode: "0755",
        sha256: QUALIFIED_WRITER_OBSERVER_SHA256,
        source: "apps/server/src/jones/cloud/qualifiedWriterObserver.pl",
      },
      {
        path: SYSTEM_SERVICE_CONTROL_PATH,
        mode: "0755",
        sha256: SYSTEM_SERVICE_CONTROL_SHA256,
        source: "apps/server/src/jones/hostService/systemServiceControl.pl",
      },
      {
        path: SYSTEM_SERVICE_POLICY_PATH,
        mode: "0644",
        sha256: sha256(policyText),
        source: "rendered",
        text: policyText,
      },
      ...[units.socket, units.service].map((unit) => ({
        path: NodePath.join("/etc/systemd/system", unit.name),
        mode: "0644" as const,
        sha256: sha256(unit.text),
        source: "rendered",
        text: unit.text,
      })),
    ],
    enable: [units.socket.name],
  };
}
