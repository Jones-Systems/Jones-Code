// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalTimers:off
// Closed native adoption for one existing system unit that runs the exact
// pre-publisher private setup build (D3) directly as its service user. The
// unit keeps its owner, user, NoNewPrivileges, KillMode, slice and every
// existing file; one owned drop-in selects a qualified launcher, which then
// supervises the same retained version. Root effects go through the closed
// worker in systemServiceControl.pl, reached only after the owner authenticated
// sudo for this session. Every step records its intent first and nothing is
// retried, reversed or restarted automatically after an uncertain result.
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTimersPromises from "node:timers/promises";
import {
  parseServiceState,
  SERVICE_LAUNCHER_PROTOCOL,
  SERVICE_STATE_FILE,
} from "../../cloud/serviceProtocol.ts";
import {
  readLauncherCapabilityReceipt,
  type LauncherCapabilityProcessGuard,
} from "../cloud/launcherCapability.ts";
import { extractQualifiedLinuxArchive } from "../cloud/qualifiedArchive.ts";
import type { observeLinuxStateLeases } from "../cloud/qualifiedLinuxLease.ts";
import {
  assertLeaseProofFilesystems,
  linuxLeaseQualifiedQuiescenceAdapter,
  proveQualifiedStateQuiescence,
  type FilesystemTypeReader,
} from "../cloud/qualifiedQuiescence.ts";
import {
  enrollQualifiedRuntime,
  qualifiedPayloadDigest,
  readQualifiedRuntimeReceipt,
  withQualifiedRuntimeLock,
} from "../cloud/qualifiedRuntime.ts";
import {
  evaluateWriterObservation,
  parseWriterObserverResult,
  QUALIFIED_WRITER_OBSERVER_FILES,
  QUALIFIED_WRITER_OBSERVER_PATH,
  QUALIFIED_WRITER_OBSERVER_SHA256,
  type QualifiedWriterObservation,
} from "../cloud/qualifiedWriterObserver.ts";
import {
  QUALIFIED_WRITER_OBSERVER_SOCKET,
  readWriterObserverSocket,
  writerObserverSocketInstallation,
  writerObserverSocketResult,
  type SocketPathMetadata,
} from "../cloud/qualifiedWriterObserverSocket.ts";
import {
  ADOPTION_RECEIPT,
  durableReplace,
  hashFile,
  HostAdoptionError,
  readText,
  SYSTEM_ADOPTION_RECEIPT,
  UPDATE_CAPABILITY_RECEIPT,
  verifyAdoptionArtifact,
  type AdoptionRunner,
} from "./adoption.ts";
import * as LegacyBootstrap from "./legacyBootstrap.ts";
import * as PrivateSetup from "./privateSetupCompatibility.ts";
import {
  isCanonicalSystemPath,
  OWNED_SYSTEM_DROPIN,
  parseRetainedServeCommand,
  renderOwnedSystemDropin,
  renderSystemBootstrapManifest,
  renderSystemServicePolicy,
  runSystemServiceControl,
  sha256,
  SYSTEM_SERVICE_CONTROL_PATH,
  SYSTEM_SERVICE_CONTROL_SHA256,
  SYSTEM_SERVICE_POLICY_PATH,
  systemUnitDigest,
  systemUnitRecord,
  type SystemControlRequest,
  type SystemControlResult,
  type SystemUnitInspection,
} from "./systemServiceControl.ts";

const SHA = /^[a-f0-9]{64}$/;
const SOURCE = /^[a-f0-9]{40}$/;
const UNIT = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,200}\.service$/;
const CONTROL_GROUP = /^(?:\/[A-Za-z0-9._@:\\-]+)+$/;
const LAUNCHER_CAPABILITY_RECEIPT = "jones-launcher-capability.json";
const SHOW_PROPERTIES = [
  "FragmentPath",
  "DropInPaths",
  "User",
  "NoNewPrivileges",
  "KillMode",
  "Slice",
  "ControlGroup",
  "ActiveState",
  "MainPID",
  "ExecStart",
  "WorkingDirectory",
] as const;

export interface SystemAdoptInput {
  readonly baseDir: string;
  readonly systemUnit: string;
  readonly port: number;
  /** The retained D3 executable the unit runs directly; its directory is the retained payload. */
  readonly retainedExecutable: string;
  readonly activeArtifactDir: string;
  readonly activeSourceCommit: string;
  readonly launcherArtifactDir: string;
  readonly launcherSourceCommit: string;
  readonly acceptUnattestedChildCapability?: boolean;
  /** Apply runs only when the freshly derived plan has exactly this SHA-256. */
  readonly approvedPlanSha256?: string;
}

export type RootFileObservation =
  | { readonly uid: number; readonly mode: number; readonly bytes: Buffer }
  | "absent"
  | "unreadable";

export interface SystemAdoptionHost {
  readonly platform: "linux" | "darwin";
  readonly architecture: "x64" | "arm64";
  readonly uid: number;
  readonly gid: number;
  readonly user: string;
  readonly run: AdoptionRunner;
  readonly control: (request: SystemControlRequest) => Promise<SystemControlResult>;
  readonly rootFile: (path: string) => Promise<RootFileObservation>;
  readonly observerSocket: () => Promise<"installed" | "absent" | "untrusted">;
  readonly descriptor: (
    port: number,
  ) => Promise<{ readonly serverVersion: string; readonly environmentId: string } | undefined>;
  readonly readiness: (input: {
    readonly baseDir: string;
    readonly allowedProcessIds: readonly number[];
  }) => Promise<QualifiedWriterObservation>;
  /** Kernel-lease proof that no process holds the selected state open for writing. */
  readonly stoppedWriterProof: (baseDir: string) => Promise<void>;
  readonly procRoot?: string;
  readonly systemdDirectory?: string;
  readonly readbackAttempts?: number;
  readonly launcherProcessGuard?: LauncherCapabilityProcessGuard;
}

interface ArtifactSummary {
  readonly version: string;
  readonly source: string;
  readonly tree: string;
  readonly archiveSha256: string;
  readonly runId: number;
  readonly runAttempt: number;
  readonly artifactId: number;
}

export interface SystemAdoptionPlan {
  readonly schema: 1;
  readonly kind: "system-unit";
  readonly baseDir: string;
  readonly systemUnit: string;
  readonly user: string;
  readonly uid: number;
  readonly gid: number;
  readonly port: number;
  readonly environmentId: string;
  readonly databaseIdentity: string;
  readonly active: ArtifactSummary;
  readonly launcher: ArtifactSummary;
  readonly retained: {
    readonly executable: string;
    readonly executableSha256: string;
    readonly payloadSha256: string;
    readonly processId: number;
    readonly processBirth: string;
    readonly controlGroup: string;
  };
  readonly unit: {
    readonly digest: string;
    readonly records: readonly string[];
    readonly killMode: string;
    readonly slice: string;
  };
  readonly runtimeMode: { readonly from: "0775"; readonly to: "0755" } | null;
  readonly ownedDropin: { readonly path: string; readonly sha256: string; readonly text: string };
  readonly bootstrap: {
    readonly workerSha256: string;
    readonly observerSha256: string;
    readonly policySha256: string;
  };
  readonly attestation: "launcher-only";
  readonly effects: readonly string[];
}

export type BootstrapAvailability = "installed" | "absent" | "mismatch" | "untrusted";

export interface SystemAdoptionPlanResult {
  readonly plan: SystemAdoptionPlan;
  readonly planSha256: string;
  /** Observed availability of the root bootstrap; not authorization and not part of the plan digest. */
  readonly bootstrap: {
    readonly worker: BootstrapAvailability;
    readonly policy: BootstrapAvailability;
    readonly observer: BootstrapAvailability;
    readonly socket: BootstrapAvailability;
  };
  readonly manifest: ReturnType<typeof renderSystemBootstrapManifest>;
}

export interface SystemAdoptionResult {
  readonly state: "adopted";
  readonly plan: SystemAdoptionPlan;
  readonly planSha256: string;
  readonly launcherProcessId: number;
  readonly childProcessId: number;
}

const refuse = (reason: string): never => {
  throw new HostAdoptionError(reason);
};

const absent = (path: string) =>
  NodeFSP.lstat(path).then(
    () => false,
    (cause: NodeJS.ErrnoException) => {
      if (cause.code === "ENOENT") return true;
      throw cause;
    },
  );

interface ProcessIdentity {
  readonly birth: string;
  readonly executable: string;
  readonly controlGroups: readonly string[];
  readonly uids: readonly number[];
}

/** Returns undefined only when the process no longer exists. */
async function processIdentity(
  procRoot: string,
  pid: number,
): Promise<ProcessIdentity | undefined> {
  const directory = NodePath.join(procRoot, String(pid));
  try {
    const stat = await NodeFSP.readFile(NodePath.join(directory, "stat"), "utf8");
    const end = stat.lastIndexOf(")");
    const birth = end < 0 ? undefined : stat.slice(end + 2).split(/\s+/)[19];
    const status = await NodeFSP.readFile(NodePath.join(directory, "status"), "utf8");
    const uids = /^Uid:\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)$/m.exec(status);
    const cgroup = await NodeFSP.readFile(NodePath.join(directory, "cgroup"), "utf8");
    const executable = await NodeFSP.readlink(NodePath.join(directory, "exe"));
    if (birth === undefined || !/^\d+$/.test(birth) || uids === null)
      return refuse("Process identity evidence is malformed.");
    return {
      birth,
      executable,
      controlGroups: cgroup.split("\n").filter((line) => line !== ""),
      uids: uids.slice(1).map(Number),
    };
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw cause;
  }
}

function assertProcess(
  identity: ProcessIdentity | undefined,
  expected: { readonly executable: string; readonly uid: number; readonly controlGroup: string },
  label: string,
): ProcessIdentity {
  if (
    identity === undefined ||
    identity.executable !== expected.executable ||
    identity.uids.some((uid) => uid !== expected.uid) ||
    !identity.controlGroups.includes(`0::${expected.controlGroup}`)
  )
    return refuse(`${label} executable, UID or unit cgroup differs from the bound identity.`);
  return identity;
}

async function systemctlShow(host: SystemAdoptionHost, unit: string) {
  const output = await host.run({
    command: "systemctl",
    args: ["show", unit, `--property=${SHOW_PROPERTIES.join(",")}`],
  });
  const values = new Map<string, string>();
  for (const line of output.split("\n")) {
    if (line === "") continue;
    const match = /^([A-Za-z]+)=(.*)$/.exec(line);
    if (match === null || values.has(match[1]!)) return refuse("Unit readback is malformed.");
    values.set(match[1]!, match[2]!);
  }
  if (SHOW_PROPERTIES.some((key) => !values.has(key)))
    return refuse("Unit readback is incomplete.");
  return (key: (typeof SHOW_PROPERTIES)[number]) => values.get(key)!;
}

/**
 * Reads the same records the root worker digests. Every file must be readable
 * by the service user; an unreadable preimage cannot be bound by this plan.
 */
async function unitPreimage(host: SystemAdoptionHost, unit: string) {
  const fragment = NodePath.join(host.systemdDirectory ?? "/etc/systemd/system", unit);
  const directory = `${fragment}.d`;
  const record = async (label: string, path: string) => {
    const stat = await NodeFSP.lstat(path);
    if (!stat.isFile() || stat.isSymbolicLink())
      return refuse("A unit file is not a regular file.");
    const bytes = await NodeFSP.readFile(path).catch((cause: NodeJS.ErrnoException) =>
      refuse(
        cause.code === "EACCES"
          ? `Unit preimage ${NodePath.basename(path)} is unreadable by the service user; this plan cannot bind it.`
          : "A unit file could not be read.",
      ),
    );
    return systemUnitRecord(label, stat, bytes);
  };
  const records = [await record("fragment", fragment)];
  const names = (
    await NodeFSP.readdir(directory).catch((cause: NodeJS.ErrnoException) => {
      if (cause.code !== "ENOENT") throw cause;
      return [] as string[];
    })
  ).sort();
  const dropins: string[] = [];
  let owned = false;
  for (const name of names) {
    if (!/^[A-Za-z0-9][A-Za-z0-9_.@-]{0,200}\.conf$/.test(name))
      return refuse("The unit drop-in directory holds an entry outside the closed grammar.");
    if (name === OWNED_SYSTEM_DROPIN) {
      owned = true;
      continue;
    }
    if (name > OWNED_SYSTEM_DROPIN)
      return refuse("A drop-in sorts after the owned launcher drop-in; precedence is unknown.");
    dropins.push(name);
    records.push(await record(`dropin ${name}`, NodePath.join(directory, name)));
  }
  return { fragment, directory, dropins, owned, records, digest: systemUnitDigest(records) };
}

async function readLegacyRuntime(baseDir: string) {
  const value = JSON.parse(
    await readText(NodePath.join(baseDir, "userdata", "server-runtime.json")),
  ) as unknown;
  const record =
    typeof value === "object" && value !== null && !Array.isArray(value)
      ? (value as Record<string, unknown>)
      : {};
  const { pid, port } = record;
  if (
    !Number.isSafeInteger(pid) ||
    (pid as number) < 1 ||
    !Number.isSafeInteger(port) ||
    (port as number) < 1
  )
    return refuse("The retained server runtime record has no usable PID and port.");
  return { pid: pid as number, port: port as number };
}

async function rootFileStatus(
  host: SystemAdoptionHost,
  path: string,
  expected: (bytes: Buffer) => boolean,
): Promise<BootstrapAvailability> {
  const observed = await host.rootFile(path);
  if (observed === "absent") return "absent";
  if (observed === "unreadable" || observed.uid !== 0 || (observed.mode & 0o022) !== 0)
    return "untrusted";
  return expected(observed.bytes) ? "installed" : "mismatch";
}

const summary = (
  artifact: Awaited<ReturnType<typeof verifyAdoptionArtifact>>,
): ArtifactSummary => ({
  version: artifact.metadata.version,
  source: artifact.metadata.source,
  tree: artifact.metadata.tree,
  archiveSha256: artifact.metadata.sha256,
  runId: artifact.artifact.runId,
  runAttempt: artifact.artifact.runAttempt,
  artifactId: artifact.artifact.artifactId,
});

async function inspectSystemAdoption(input: SystemAdoptInput, host: SystemAdoptionHost) {
  if (host.platform !== "linux") return refuse("System-unit adoption supports Linux only.");
  const procRoot = host.procRoot ?? "/proc";
  if (
    !NodePath.isAbsolute(input.baseDir) ||
    !UNIT.test(input.systemUnit) ||
    !Number.isSafeInteger(input.port) ||
    input.port < 1 ||
    input.port > 65535 ||
    !isCanonicalSystemPath(input.retainedExecutable) ||
    !SOURCE.test(input.activeSourceCommit) ||
    !SOURCE.test(input.launcherSourceCommit) ||
    !Number.isSafeInteger(host.uid) ||
    host.uid < 1
  )
    return refuse("Adoption inputs are outside the closed system-unit grammar.");
  const baseDir = await NodeFSP.realpath(input.baseDir);
  if (baseDir !== input.baseDir || !isCanonicalSystemPath(baseDir))
    return refuse("Base directory resolves through an alias; select its exact path.");
  const home = await NodeFSP.lstat(baseDir);
  if (!home.isDirectory() || home.uid !== host.uid)
    return refuse("The home is not owned by the service user.");
  const runtime = NodePath.join(baseDir, "runtime");
  const runtimeStat = await NodeFSP.lstat(runtime);
  if (!runtimeStat.isDirectory() || runtimeStat.isSymbolicLink() || runtimeStat.uid !== host.uid)
    return refuse("The runtime directory is not a real directory owned by the service user.");
  const runtimePermissions = runtimeStat.mode & 0o7777;
  // The launcher requires a runtime directory nothing else can write; only the
  // observed group-writable 0775 is corrected, by exactly that permission bit.
  const runtimeMode =
    (runtimePermissions & 0o022) === 0
      ? null
      : runtimePermissions === 0o775
        ? ({ from: "0775", to: "0755" } as const)
        : refuse("The runtime directory has unexpected permissions; preserve and reconcile it.");
  if (!(await absent(NodePath.join(runtime, "jones-active-install.json"))))
    return refuse("This home is desktop-owned.");
  for (const name of [
    ADOPTION_RECEIPT,
    SYSTEM_ADOPTION_RECEIPT,
    SERVICE_STATE_FILE,
    LAUNCHER_CAPABILITY_RECEIPT,
    UPDATE_CAPABILITY_RECEIPT,
  ])
    if (!(await absent(NodePath.join(runtime, name))))
      return refuse(
        `Existing ${name} shows an adopted, pending or native home; this closed adoption needs none.`,
      );
  await LegacyBootstrap.assertNoBootstrapHazards(baseDir, host.uid);
  const environmentId = (
    await readText(NodePath.join(baseDir, "userdata", "environment-id"))
  ).trim();
  if (environmentId === "") return refuse("Native environment identity is missing.");
  const database = await NodeFSP.lstat(NodePath.join(baseDir, "userdata", "statev2.sqlite"));
  if (!database.isFile() || database.isSymbolicLink())
    return refuse("Native database is not a regular file.");
  const runtimeRecord = await readLegacyRuntime(baseDir);
  if (runtimeRecord.port !== input.port)
    return refuse("The retained runtime port differs from the bound port.");

  const show = await systemctlShow(host, input.systemUnit);
  const preimage = await unitPreimage(host, input.systemUnit);
  if (preimage.owned)
    return refuse("An owned launcher drop-in already exists; reconcile it first.");
  const dropinPaths = show("DropInPaths")
    .split(" ")
    .filter((path) => path !== "");
  if (
    show("FragmentPath") !== preimage.fragment ||
    JSON.stringify([...dropinPaths].sort()) !==
      JSON.stringify(preimage.dropins.map((name) => NodePath.join(preimage.directory, name)).sort())
  )
    return refuse(
      "Effective unit files differ from the selected unit's own fragment and drop-ins.",
    );
  if (show("User") !== host.user || show("NoNewPrivileges") !== "yes")
    return refuse("The unit does not run as the service user with NoNewPrivileges=yes.");
  if (show("ActiveState") !== "active" || show("MainPID") !== String(runtimeRecord.pid))
    return refuse("The unit's main process is not the retained runtime's process.");
  const controlGroup = show("ControlGroup");
  const slice = show("Slice");
  if (!CONTROL_GROUP.test(controlGroup) || !/^[A-Za-z0-9][A-Za-z0-9_.-]*\.slice$/.test(slice))
    return refuse("The unit cgroup or slice is unknown.");
  const argv = /argv\[\]=([^;]*?) ;/.exec(show("ExecStart"))?.[1];
  const serve =
    argv === undefined
      ? undefined
      : parseRetainedServeCommand(
          argv,
          input.retainedExecutable,
          baseDir,
          show("WorkingDirectory"),
        );
  if (serve === undefined || (serve.port !== undefined && serve.port !== input.port))
    return refuse(
      "The unit does not run the retained executable with the closed serve or start grammar.",
    );

  const retainedProcess = assertProcess(
    await processIdentity(procRoot, runtimeRecord.pid),
    { executable: input.retainedExecutable, uid: host.uid, controlGroup },
    "The retained server",
  );
  const descriptor = await host.descriptor(input.port);
  if (descriptor === undefined || descriptor.environmentId !== environmentId)
    return refuse("The retained server's public descriptor does not match this environment.");

  const active = await verifyAdoptionArtifact(
    input.activeArtifactDir,
    input.activeSourceCommit,
    host,
  );
  const launcher = await verifyAdoptionArtifact(
    input.launcherArtifactDir,
    input.launcherSourceCommit,
    host,
  );
  if (!PrivateSetup.isUnattestedPrivateSetupArtifact(active.metadata))
    return refuse(
      "This closed adoption accepts only the exact authenticated D3 private setup build.",
    );
  if (input.acceptUnattestedChildCapability !== true)
    return refuse(
      "The D3 child predates capability receipts; explicit unattested-child acceptance is required.",
    );
  if (descriptor.serverVersion !== active.metadata.version)
    return refuse("The retained server reports a different version from the authenticated build.");
  if (
    launcher.metadata.version === active.metadata.version ||
    launcher.metadata.source === active.metadata.source
  )
    return refuse("The launcher must be a different qualified build from the retained version.");
  if ((await NodeFSP.realpath(input.retainedExecutable)) !== input.retainedExecutable)
    return refuse("The retained executable resolves through an alias.");
  const executableSha256 = await hashFile(input.retainedExecutable);
  if (executableSha256 !== (await LegacyBootstrap.archiveExecutableSha256(active.archive)))
    return refuse("The retained executable differs from the authenticated archive.");
  const payloadSha256 = await qualifiedPayloadDigest(
    NodePath.dirname(input.retainedExecutable),
    "linux",
    active.metadata.workflow,
  );
  const versions = NodePath.join(runtime, "versions");
  if (!(await absent(NodePath.join(versions, active.metadata.version))))
    return refuse("The retained version's runtime cache is occupied; preserve and reconcile it.");
  if (!(await absent(NodePath.join(versions, launcher.metadata.version)))) {
    const receipt = await readQualifiedRuntimeReceipt(baseDir, launcher.metadata.version, host);
    if (receipt.sourceSha !== launcher.metadata.source)
      return refuse("The launcher version's runtime cache holds a different build.");
  }

  const policyText = renderSystemServicePolicy({
    unit: input.systemUnit,
    user: host.user,
    uid: host.uid,
    gid: host.gid,
    base: baseDir,
    slice,
    retainedEntry: input.retainedExecutable,
  });
  const ownedText = renderOwnedSystemDropin({
    base: baseDir,
    launcherVersion: launcher.metadata.version,
    serve,
  });
  const ownedPath = NodePath.join(preimage.directory, OWNED_SYSTEM_DROPIN);
  const statePath = NodePath.join(runtime, SERVICE_STATE_FILE);
  const plan: SystemAdoptionPlan = {
    schema: 1,
    kind: "system-unit",
    baseDir,
    systemUnit: input.systemUnit,
    user: host.user,
    uid: host.uid,
    gid: host.gid,
    port: input.port,
    environmentId,
    databaseIdentity: `${database.dev}:${database.ino}`,
    active: summary(active),
    launcher: summary(launcher),
    retained: {
      executable: input.retainedExecutable,
      executableSha256,
      payloadSha256,
      processId: runtimeRecord.pid,
      processBirth: retainedProcess.birth,
      controlGroup,
    },
    unit: { digest: preimage.digest, records: preimage.records, killMode: show("KillMode"), slice },
    runtimeMode,
    ownedDropin: { path: ownedPath, sha256: sha256(ownedText), text: ownedText },
    bootstrap: {
      workerSha256: SYSTEM_SERVICE_CONTROL_SHA256,
      observerSha256: QUALIFIED_WRITER_OBSERVER_SHA256,
      policySha256: sha256(policyText),
    },
    attestation: "launcher-only",
    effects: [
      `Require the installed root worker, policy, observer helper and socket to match this plan; adoption installs no root file.`,
      `Write ${NodePath.join(runtime, SYSTEM_ADOPTION_RECEIPT)} as pending intent first; an uncertain step leaves it partial or unknown and blocks retry.`,
      ...(runtimeMode === null
        ? []
        : [
            `Change ${runtime} from 0775 to 0755; versions and the rest of the home are unchanged.`,
          ]),
      `Enroll ${NodePath.join(versions, active.metadata.version)} from Actions run ${active.artifact.runId}/${active.artifact.runAttempt}; its full payload must equal ${NodePath.dirname(input.retainedExecutable)} (${payloadSha256}).`,
      `Enroll ${NodePath.join(versions, launcher.metadata.version)} from Actions run ${launcher.artifact.runId}/${launcher.artifact.runAttempt}, source ${launcher.metadata.source}.`,
      "Read pending migration IDs with the launcher's read-only preflight; no migration, database copy or hash runs.",
      `Prove through the observer socket that only retained PID ${runtimeRecord.pid} writes the selected state.`,
      `Stop ${input.systemUnit} through the root worker after comparing unit digest ${preimage.digest}, no owned drop-in and PID ${runtimeRecord.pid} birth ${retainedProcess.birth}.`,
      "Prove with kernel leases that nothing holds the selected state open for writing; on any doubt the unit stays stopped and nothing restarts automatically.",
      `Create ${statePath} exclusively with protocol ${SERVICE_LAUNCHER_PROTOCOL} and active version ${active.metadata.version}.`,
      `Install only ${ownedPath} (${sha256(ownedText)}) through the root worker and reload systemd; every other unit file stays byte-identical.`,
      ...(serve.workingDirectory === undefined
        ? []
        : [
            `Replace the retained \`start --no-browser ${serve.workingDirectory}\` with the launcher, which runs the retained version as \`serve\` from the unchanged WorkingDirectory ${serve.workingDirectory}: headless, with no automatic project bootstrap from that directory; existing projects and state are not changed.`,
          ]),
      `Start ${input.systemUnit}; read back launcher ${launcher.metadata.version}, retained child ${active.metadata.version}, environment, database inode, unit identity and launcher receipt. Child Install stays unattested.`,
      `Preserve ${NodePath.dirname(input.retainedExecutable)} and the original unit files for separately approved recovery.`,
    ],
  };
  const observerChecks = [
    rootFileStatus(
      host,
      SYSTEM_SERVICE_CONTROL_PATH,
      (bytes) => sha256(bytes) === SYSTEM_SERVICE_CONTROL_SHA256,
    ),
    rootFileStatus(
      host,
      SYSTEM_SERVICE_POLICY_PATH,
      (bytes) => bytes.toString("utf8") === policyText,
    ),
    rootFileStatus(
      host,
      QUALIFIED_WRITER_OBSERVER_PATH,
      (bytes) => sha256(bytes) === QUALIFIED_WRITER_OBSERVER_SHA256,
    ),
    host.observerSocket(),
  ] as const;
  const [worker, policy, observer, socket] = await Promise.all(observerChecks);
  return {
    plan,
    planSha256: sha256(JSON.stringify(plan)),
    bootstrap: { worker, policy, observer, socket },
    manifest: renderSystemBootstrapManifest({
      unit: input.systemUnit,
      user: host.user,
      uid: host.uid,
      gid: host.gid,
      base: baseDir,
      slice,
      retainedEntry: input.retainedExecutable,
    }),
    active,
    launcher,
  };
}

/** Read-only: creates no lock, receipt, scratch or state. */
export async function planSystemAdoption(
  input: SystemAdoptInput,
  host: SystemAdoptionHost,
): Promise<SystemAdoptionPlanResult> {
  const { plan, planSha256, bootstrap, manifest } = await inspectSystemAdoption(input, host);
  return { plan, planSha256, bootstrap, manifest };
}

/**
 * The worker's stopped state as reported: exactly `inactive dead`, or the
 * `failed failed` a retained process leaves when it exits unclean on stop,
 * with no main process and no cgroup. A complete worker `stop` result also
 * carries its private job and original-cgroup checks; `install-launcher`
 * checks its private job. An inspection alone never proves quiescence.
 */
const stoppedUnit = (inspection: SystemUnitInspection) =>
  ((inspection.activeState === "inactive" && inspection.subState === "dead") ||
    (inspection.activeState === "failed" && inspection.subState === "failed")) &&
  inspection.mainPid === 0 &&
  inspection.mainBirth === undefined &&
  inspection.controlGroup === undefined;

function expectInspection(
  result: SystemControlResult,
  check: (inspection: SystemUnitInspection) => boolean,
  label: string,
): SystemUnitInspection {
  if (result.status !== "complete")
    return refuse(`Root worker ${label}: ${result.status} ${result.reason}.`);
  if (!check(result.inspection))
    return refuse(`Root worker ${label} readback differs from the approved plan.`);
  return result.inspection;
}

async function createServiceState(path: string, activeVersion: string): Promise<void> {
  const contents = `${JSON.stringify({ protocol: SERVICE_LAUNCHER_PROTOCOL, activeVersion })}\n`;
  const handle = await NodeFSP.open(path, "wx", 0o600);
  try {
    await handle.writeFile(contents);
    await handle.sync();
  } finally {
    await handle.close();
  }
  const directory = await NodeFSP.open(NodePath.dirname(path), "r");
  try {
    await directory.sync();
  } finally {
    await directory.close();
  }
  const state = parseServiceState(await readText(path));
  if (state?.activeVersion !== activeVersion || state.update !== undefined)
    return refuse("Native service state readback differs.");
}

/**
 * Applies only the approved plan. Each step records its phase first. A failure
 * whose effects are all known records `partial`: the completed setup effects
 * up to its recorded phase remain, so it never means that nothing changed. A
 * failure during an attempt whose result is not settled records `unknown`.
 * Neither is retried, reversed or restarted here.
 */
export async function adoptSystemHost(
  input: SystemAdoptInput,
  host: SystemAdoptionHost,
): Promise<SystemAdoptionResult> {
  if (!SHA.test(input.approvedPlanSha256 ?? ""))
    return refuse("Apply requires the SHA-256 of the reviewed plan.");
  return withQualifiedRuntimeLock(input.baseDir, "qualified-runtime-lock", async () => {
    const inspected = await inspectSystemAdoption(input, host);
    const { plan, planSha256 } = inspected;
    if (planSha256 !== input.approvedPlanSha256)
      return refuse("The current host differs from the approved plan; no effect was attempted.");
    if (Object.values(inspected.bootstrap).some((status) => status !== "installed"))
      return refuse(
        "The one-time root bootstrap is not installed exactly; no effect was attempted.",
      );
    const procRoot = host.procRoot ?? "/proc";
    const runtime = NodePath.join(plan.baseDir, "runtime");
    const receiptPath = NodePath.join(runtime, SYSTEM_ADOPTION_RECEIPT);
    const statePath = NodePath.join(runtime, SERVICE_STATE_FILE);
    const activeEntry = NodePath.join(runtime, "versions", plan.active.version, "t3");
    const launcherEntry = NodePath.join(runtime, "versions", plan.launcher.version, "t3");
    const retainedBinding = {
      executable: plan.retained.executable,
      uid: plan.uid,
      controlGroup: plan.retained.controlGroup,
    };
    // A stopped unit reports no cgroup; every other inspection must report the same one.
    const unchangedUnit = (inspection: SystemUnitInspection) =>
      inspection.unitDigest === plan.unit.digest &&
      inspection.policySha256 === plan.bootstrap.policySha256 &&
      inspection.killMode === plan.unit.killMode &&
      (stoppedUnit(inspection) || inspection.controlGroup === plan.retained.controlGroup);
    let phase = "prepared";
    let outcome: "partial" | "unknown" = "partial";
    let recorded = false;
    // Carried by every record once the plan includes the correction: `applied`
    // is false while it is known unattempted, absent while the change or its
    // readback is unsettled, and true once read back on the same directory.
    let runtimeMode:
      | { readonly from: "0775"; readonly to: "0755"; readonly applied?: boolean }
      | undefined = plan.runtimeMode === null ? undefined : { ...plan.runtimeMode, applied: false };
    const record = async (status: string, extra: Record<string, unknown> = {}) => {
      await durableReplace(
        receiptPath,
        `${JSON.stringify({
          schema: 1,
          kind: "system-unit",
          status,
          phase,
          planSha256,
          plan,
          ...(runtimeMode === undefined ? {} : { runtimeMode }),
          ...extra,
        })}\n`,
      );
      recorded = true;
    };
    const step = async (next: string, uncertain: boolean) => {
      phase = next;
      outcome = uncertain ? "unknown" : "partial";
      await record("pending");
    };
    const assertUnchanged = async () => {
      for (const name of [
        ADOPTION_RECEIPT,
        SERVICE_STATE_FILE,
        LAUNCHER_CAPABILITY_RECEIPT,
        UPDATE_CAPABILITY_RECEIPT,
      ])
        if (!(await absent(NodePath.join(runtime, name))))
          refuse(`${name} appeared during adoption.`);
      if ((await unitPreimage(host, plan.systemUnit)).digest !== plan.unit.digest)
        refuse("Unit files changed during adoption.");
      const identity = assertProcess(
        await processIdentity(procRoot, plan.retained.processId),
        retainedBinding,
        "The retained server",
      );
      if (identity.birth !== plan.retained.processBirth)
        refuse("The retained server process was replaced.");
      if ((await hashFile(plan.retained.executable)) !== plan.retained.executableSha256)
        refuse("The retained executable changed after planning.");
      const descriptor = await host.descriptor(plan.port);
      if (
        descriptor?.environmentId !== plan.environmentId ||
        descriptor.serverVersion !== plan.active.version
      )
        refuse("The retained server descriptor changed.");
      await assertHomeIdentity();
    };
    const assertHomeIdentity = async () => {
      const database = await NodeFSP.lstat(
        NodePath.join(plan.baseDir, "userdata", "statev2.sqlite"),
      );
      if (
        `${database.dev}:${database.ino}` !== plan.databaseIdentity ||
        (await readText(NodePath.join(plan.baseDir, "userdata", "environment-id"))).trim() !==
          plan.environmentId
      )
        refuse("Native database or environment identity changed.");
    };
    /** Settled native evidence after start; read-only, so the caller may retry it. */
    const observeAdopted = async (migrationPlan: unknown) => {
      const inspection = expectInspection(
        await host.control({ operation: "inspect" }),
        (value) =>
          unchangedUnit(value) &&
          value.owned === plan.ownedDropin.sha256 &&
          value.exec === `launcher ${plan.launcher.version}` &&
          value.activeState === "active" &&
          value.mainPid > 0,
        "readback",
      );
      const launcherPid = inspection.mainPid;
      assertProcess(
        await processIdentity(procRoot, launcherPid),
        { ...retainedBinding, executable: launcherEntry },
        "The launcher",
      );
      const runtimeValue = JSON.parse(
        await readText(NodePath.join(plan.baseDir, "userdata", "server-runtime.json")),
      ) as Record<string, unknown>;
      const childPid = runtimeValue.pid;
      if (
        typeof childPid !== "number" ||
        !Number.isSafeInteger(childPid) ||
        childPid === launcherPid ||
        runtimeValue.port !== plan.port ||
        runtimeValue.serviceManaged !== true
      )
        return refuse("The supervised child has not published its runtime record.");
      const child = assertProcess(
        await processIdentity(procRoot, childPid),
        { ...retainedBinding, executable: activeEntry },
        "The supervised child",
      );
      if (
        (await readLauncherCapabilityReceipt(
          plan.baseDir,
          {
            launcherVersion: plan.launcher.version,
            launcherPid,
            childVersion: plan.active.version,
            childPid,
          },
          host.launcherProcessGuard,
        )) === undefined
      )
        refuse("The launcher capability receipt is unavailable or stale.");
      if (!(await absent(NodePath.join(runtime, UPDATE_CAPABILITY_RECEIPT))))
        refuse("The pre-publisher child has an unexpected capability receipt.");
      const descriptor = await host.descriptor(plan.port);
      if (
        descriptor?.environmentId !== plan.environmentId ||
        descriptor.serverVersion !== plan.active.version
      )
        refuse("The supervised child's descriptor differs.");
      const state = parseServiceState(await readText(statePath));
      if (state?.activeVersion !== plan.active.version || state.update !== undefined)
        refuse("Native service state changed after start.");
      await assertHomeIdentity();
      if (JSON.stringify(await migrationPreflight()) !== JSON.stringify(migrationPlan))
        refuse("Pending migration IDs changed; no migration effect was authorized.");
      const ready = await host.readiness({ baseDir: plan.baseDir, allowedProcessIds: [childPid] });
      if (ready.status !== "complete" || ready.writers.length !== 0)
        refuse("The supervised child is not the only writer of the selected state.");
      return { launcherPid, childPid, childBirth: child.birth };
    };
    const migrationPreflight = async () =>
      LegacyBootstrap.readPreflightMigrationPlan(
        await host.run({
          command: launcherEntry,
          args: [
            "__service-preflight",
            "--database-path",
            NodePath.join(plan.baseDir, "userdata", "statev2.sqlite"),
            "--launcher-protocol",
            String(SERVICE_LAUNCHER_PROTOCOL),
          ],
        }),
        plan.launcher.version,
      );
    try {
      expectInspection(
        await host.control({ operation: "inspect" }),
        (inspection) =>
          unchangedUnit(inspection) &&
          inspection.owned === "absent" &&
          inspection.exec === "retained" &&
          inspection.activeState === "active" &&
          inspection.mainPid === plan.retained.processId &&
          inspection.mainBirth === plan.retained.processBirth &&
          (inspection.serve.port === undefined || inspection.serve.port === plan.port),
        "inspection",
      );
      await record("pending");

      if (plan.runtimeMode !== null) {
        const before = await NodeFSP.lstat(runtime);
        if (
          !before.isDirectory() ||
          before.isSymbolicLink() ||
          before.uid !== plan.uid ||
          (before.mode & 0o7777) !== 0o775
        )
          refuse("Runtime directory permissions changed before correction.");
        runtimeMode = { ...plan.runtimeMode };
        await step("runtime-mode-requested", true);
        await NodeFSP.chmod(runtime, 0o755);
        const after = await NodeFSP.lstat(runtime);
        if (
          !after.isDirectory() ||
          after.isSymbolicLink() ||
          after.dev !== before.dev ||
          after.ino !== before.ino ||
          after.uid !== before.uid ||
          (after.mode & 0o7777) !== 0o755
        )
          refuse("Runtime directory permission readback differs.");
        runtimeMode = { ...plan.runtimeMode, applied: true };
        await step("runtime-mode-applied", false);
      }

      await step("enrolling", false);
      const scratch = await NodeFSP.mkdtemp(NodePath.join(runtime, ".jones-system-adoption-"));
      try {
        for (const [index, artifact] of [inspected.active, inspected.launcher].entries()) {
          const payload = NodePath.join(scratch, String(index));
          await NodeFSP.mkdir(payload, { mode: 0o700 });
          await extractQualifiedLinuxArchive(artifact.archive, payload);
          if (
            artifact === inspected.active &&
            (await qualifiedPayloadDigest(payload, "linux", artifact.metadata.workflow)) !==
              plan.retained.payloadSha256
          )
            refuse("The retained payload differs from the authenticated archive.");
          await verifyAdoptionArtifact(artifact.directory, artifact.metadata.source, host);
          await enrollQualifiedRuntime({
            baseDir: plan.baseDir,
            artifact: { ...artifact.artifact, payloadDirectory: payload },
            host: { platform: "linux", architecture: host.architecture },
            validate: async (entry) => {
              const output = await host.run({ command: entry, args: ["--version"] });
              if (/\bv(\S+)\s*$/.exec(output)?.[1] !== artifact.metadata.version)
                refuse("Verified executable reported a different version.");
              const preflight = JSON.parse(
                await host.run({
                  command: entry,
                  args: [
                    "__service-preflight",
                    "--database-path",
                    NodePath.join(plan.baseDir, "userdata", "statev2.sqlite"),
                    "--launcher-protocol",
                    String(SERVICE_LAUNCHER_PROTOCOL),
                  ],
                }),
              ) as Record<string, unknown>;
              if (
                preflight.status !== "ready" ||
                preflight.version !== artifact.metadata.version ||
                preflight.launcherProtocol !== SERVICE_LAUNCHER_PROTOCOL ||
                preflight.startupGateProtocol !== 1
              )
                refuse("The runtime cannot provide the qualified launcher and startup protocols.");
            },
          });
        }
      } finally {
        await NodeFSP.rm(scratch, { recursive: true, force: true });
      }
      const migrationPlan = await migrationPreflight();
      await step("enrolled", false);

      const readiness = await host.readiness({
        baseDir: plan.baseDir,
        allowedProcessIds: [plan.retained.processId],
      });
      if (readiness.status !== "complete" || readiness.writers.length !== 0)
        refuse("The observer socket did not prove that only the retained server writes the state.");
      await assertUnchanged();
      expectInspection(
        await host.control({ operation: "inspect" }),
        (inspection) =>
          unchangedUnit(inspection) &&
          inspection.owned === "absent" &&
          inspection.mainPid === plan.retained.processId &&
          inspection.mainBirth === plan.retained.processBirth,
        "pre-stop inspection",
      );

      await step("stop-requested", true);
      const stopped = await host.control({
        operation: "stop",
        unitDigest: plan.unit.digest,
        owned: "absent",
        mainPid: plan.retained.processId,
        mainBirth: plan.retained.processBirth,
      });
      if (stopped.status === "rejected") outcome = "partial";
      expectInspection(
        stopped,
        (inspection) => unchangedUnit(inspection) && stoppedUnit(inspection),
        "stop",
      );
      await step("stopped", false);
      const old = await processIdentity(procRoot, plan.retained.processId);
      if (old !== undefined && old.birth === plan.retained.processBirth)
        refuse("The retained server process still exists after the unit stopped.");
      try {
        await host.stoppedWriterProof(plan.baseDir);
      } catch (cause) {
        refuse(
          `Stopped-writer proof failed (${cause instanceof Error ? cause.message : "unknown"}); the unit stays stopped and nothing restarts automatically.`,
        );
      }
      await assertHomeIdentity();

      await step("state-requested", true);
      try {
        await createServiceState(statePath, plan.active.version);
      } catch (cause) {
        if ((cause as NodeJS.ErrnoException).code === "EEXIST") {
          outcome = "partial";
          refuse("Native service state appeared before creation; it was preserved.");
        }
        throw cause;
      }
      await step("state-initialized", false);

      await step("launcher-install-requested", true);
      const installed = await host.control({
        operation: "install-launcher",
        unitDigest: plan.unit.digest,
        owned: "absent",
        launcherVersion: plan.launcher.version,
      });
      if (installed.status === "rejected") outcome = "partial";
      expectInspection(
        installed,
        (inspection) =>
          unchangedUnit(inspection) &&
          inspection.owned === plan.ownedDropin.sha256 &&
          inspection.exec === `launcher ${plan.launcher.version}` &&
          stoppedUnit(inspection),
        "launcher installation",
      );
      await step("launcher-installed", false);

      await step("start-requested", true);
      const started = await host.control({
        operation: "start",
        unitDigest: plan.unit.digest,
        owned: plan.ownedDropin.sha256,
      });
      if (started.status === "rejected") outcome = "partial";
      expectInspection(
        started,
        (inspection) =>
          unchangedUnit(inspection) &&
          inspection.owned === plan.ownedDropin.sha256 &&
          inspection.activeState === "active",
        "start",
      );
      await step("started", true);

      const readBack = async () => {
        const attempts = host.readbackAttempts ?? 120;
        let lastFailure = "unavailable";
        for (let attempt = 0; attempt < attempts; attempt++) {
          try {
            return await observeAdopted(migrationPlan);
          } catch (cause) {
            // Read-only observations may be retried; no service effect is replayed.
            lastFailure = cause instanceof Error ? cause.message : "unavailable";
          }
          if (attempt + 1 < attempts) await NodeTimersPromises.setTimeout(250);
        }
        return refuse(`Native readback did not settle: ${lastFailure}`);
      };
      const observed = await readBack();
      // Payload digests walk every runtime file, so they are read once after settling.
      for (const version of [plan.active.version, plan.launcher.version])
        await readQualifiedRuntimeReceipt(plan.baseDir, version, host);
      phase = "applied";
      await record("applied", {
        launcherProcessId: observed.launcherPid,
        childProcessId: observed.childPid,
        childProcessBirth: observed.childBirth,
        attestation: "launcher-only",
        attestationBasis: "pre-publisher-child",
        childCapability: "unattested",
        migrationPlan,
      });
      return {
        state: "adopted" as const,
        plan,
        planSha256,
        launcherProcessId: observed.launcherPid,
        childProcessId: observed.childPid,
      };
    } catch (cause) {
      const reason = cause instanceof Error ? cause.message : "Adoption effect is unknown.";
      if (recorded) await record(outcome, { reason }).catch(() => undefined);
      throw cause instanceof HostAdoptionError ? cause : new HostAdoptionError(reason);
    }
  });
}

/**
 * Readiness through the root socket with the captured process allowed. The
 * service user's own CLI has no NoNewPrivileges, so this composes the socket
 * transport directly rather than the launcher's route selection.
 */
export async function observeSystemReadiness(
  input: { readonly baseDir: string; readonly allowedProcessIds: readonly number[] },
  options: {
    readonly socketPath?: string;
    readonly socketMetadata?: (path: string) => Promise<SocketPathMetadata>;
    readonly procRoot?: string;
    readonly owner?: number;
    readonly deadlineMs?: number;
  } = {},
): Promise<QualifiedWriterObservation> {
  const owner = options.owner ?? process.getuid?.();
  const socketPath = options.socketPath ?? QUALIFIED_WRITER_OBSERVER_SOCKET;
  const procRoot = options.procRoot ?? "/proc";
  const before = await writerObserverSocketInstallation(socketPath, owner, options.socketMetadata);
  if (before.status !== "installed") return { status: "unavailable" };
  const files: { path: string; device: bigint; inode: bigint }[] = [];
  for (const name of QUALIFIED_WRITER_OBSERVER_FILES) {
    const path = NodePath.join(input.baseDir, "userdata", name);
    const stat = await NodeFSP.lstat(path, { bigint: true }).catch(
      (cause: NodeJS.ErrnoException) => {
        if (cause.code === "ENOENT" && name !== "statev2.sqlite") return undefined;
        throw cause;
      },
    );
    if (stat === undefined) continue;
    if (!stat.isFile() || stat.uid !== BigInt(owner ?? -1)) return { status: "unavailable" };
    files.push({ path, device: stat.dev, inode: stat.ino });
  }
  // The helper reports the inodes it observed; the selected files must still
  // be those inodes once the exchange ends, or its answer is about other state.
  const unchangedFiles = async () => {
    for (const file of files) {
      const stat = await NodeFSP.lstat(file.path, { bigint: true }).catch(() => undefined);
      if (stat?.dev !== file.device || stat.ino !== file.inode) return false;
    }
    for (const name of QUALIFIED_WRITER_OBSERVER_FILES) {
      const path = NodePath.join(input.baseDir, "userdata", name);
      if (!files.some((file) => file.path === path) && !(await absent(path).catch(() => false)))
        return false;
    }
    return true;
  };
  const birth = async (pid: number) =>
    (await processIdentity(procRoot, pid).catch(() => undefined))?.birth;
  const allowed = new Map<number, string>();
  for (const pid of input.allowedProcessIds) {
    const value = await birth(pid);
    if (value === undefined) return { status: "unavailable" };
    allowed.set(pid, value);
  }
  const response = await readWriterObserverSocket({
    path: socketPath,
    deadlineMs: options.deadlineMs ?? 15_000,
    outputLimit: 64 * 1024,
  });
  const after = await writerObserverSocketInstallation(socketPath, owner, options.socketMetadata);
  if (
    response.outcome !== "closed" ||
    after.status !== "installed" ||
    after.identity !== before.identity
  )
    return { status: "unavailable" };
  const mapped = writerObserverSocketResult(response.stdout);
  const result = mapped === undefined ? undefined : parseWriterObserverResult(mapped);
  if (result === undefined) return { status: "unavailable" };
  for (const [pid, value] of allowed)
    if ((await birth(pid)) !== value) return { status: "unavailable" };
  const observation = evaluateWriterObservation(result, files, allowed);
  if (observation.status === "complete" && !(await unchangedFiles()))
    return { status: "identity-changed" };
  return observation;
}

/**
 * Stopped-writer proof for system adoption: the selected files must be on a
 * filesystem whose lease grant is a kernel proof before any lease is taken,
 * then the kernel lease scan answers. The shared quiescence proof still owns
 * the file selection and its before-and-after identity check.
 */
export async function proveSystemStoppedWriters(
  baseDir: string,
  options: {
    readonly filesystemType?: FilesystemTypeReader;
    readonly leases?: Parameters<typeof observeLinuxStateLeases>[1];
  } = {},
): Promise<void> {
  const leases = linuxLeaseQualifiedQuiescenceAdapter(options.leases);
  await proveQualifiedStateQuiescence({
    baseDir,
    adapter: {
      async scan(input) {
        await assertLeaseProofFilesystems(input.files, options.filesystemType);
        return leases.scan(input);
      },
    },
  });
}

/** Native host capabilities for the service user's CLI; transports supply the runner and descriptor. */
export function nativeSystemAdoptionHost(input: {
  readonly architecture: "x64" | "arm64";
  readonly run: AdoptionRunner;
  readonly descriptor: SystemAdoptionHost["descriptor"];
}): SystemAdoptionHost {
  const account = NodeOS.userInfo();
  return {
    platform: "linux",
    architecture: input.architecture,
    uid: account.uid,
    gid: account.gid,
    user: account.username,
    run: input.run,
    descriptor: input.descriptor,
    control: (request) => runSystemServiceControl(request),
    rootFile: async (path) => {
      try {
        const stat = await NodeFSP.lstat(path);
        if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 1024 * 1024) return "unreadable";
        const handle = await NodeFSP.open(
          path,
          NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
        );
        try {
          return { uid: stat.uid, mode: stat.mode, bytes: await handle.readFile() };
        } finally {
          await handle.close();
        }
      } catch (cause) {
        return (cause as NodeJS.ErrnoException).code === "ENOENT" ? "absent" : "unreadable";
      }
    },
    observerSocket: async () =>
      (await writerObserverSocketInstallation(QUALIFIED_WRITER_OBSERVER_SOCKET, account.uid))
        .status,
    readiness: (request) => observeSystemReadiness(request),
    stoppedWriterProof: (baseDir) => proveSystemStoppedWriters(baseDir),
  };
}
