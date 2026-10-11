// @effect-diagnostics nodeBuiltinImport:off
// This native boundary shares immutable runtime receipts with the detached launcher.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeTimersPromises from "node:timers/promises";
import type { JonesStagedArtifact } from "@t3tools/shared/jones/jonesActions";
import {
  bootServiceBaseDirOf,
  renderBootServicePlist,
  renderBootServiceUnit,
} from "../../cloud/bootService.ts";
import {
  parseServiceState,
  SERVICE_LAUNCHER_PROTOCOL,
  SERVICE_STATE_FILE,
} from "../../cloud/serviceProtocol.ts";
import { extractQualifiedLinuxArchive } from "../cloud/qualifiedArchive.ts";
import { extractQualifiedDarwinRuntime } from "../cloud/qualifiedDarwinRuntime.ts";
import {
  enrollQualifiedRuntime,
  readQualifiedRuntimeReceipt,
  withQualifiedRuntimeLock,
  type QualifiedRuntimeArtifact,
} from "../cloud/qualifiedRuntime.ts";
import { JONES_BOOT_SERVICE_IDENTITY as identity } from "./identity.ts";
import * as LegacyBootstrap from "./legacyBootstrap.ts";
import { readLauncherCapabilityReceipt } from "../cloud/launcherCapability.ts";

const repository = "Jones-Systems/Jones-Code";
const hash = /^[a-f0-9]{64}$/;
const source = /^[a-f0-9]{40}$/;
const versionPattern = /^\d+\.\d+\.\d+-preview\.\d{8}\.\d+(?:\.\d+)?$/;
const marker = "# Jones qualified host adoption protocol 1";
export const ADOPTION_RECEIPT = "jones-host-adoption.json";
export const UPDATE_CAPABILITY_RECEIPT = "jones-update-capability.json";
export interface AdoptInput extends LegacyBootstrap.LegacyBootstrapBinding {
  readonly baseDir: string;
  readonly activeArtifactDir: string;
  readonly activeSourceCommit: string;
  readonly launcherArtifactDir: string;
  readonly launcherSourceCommit: string;
  readonly supersedeExecstart?: boolean;
  readonly dryRun?: boolean;
}
export interface AdoptionCommand {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
}
export type AdoptionRunner = (input: AdoptionCommand) => Promise<string>;
export interface AdoptionHost {
  readonly platform: "linux" | "darwin";
  readonly architecture: "x64" | "arm64";
  readonly home: string;
  readonly uid: number;
  readonly environmentPath: string;
  readonly run: AdoptionRunner;
  readonly readbackAttempts?: number;
  readonly launcherProcessGuard?: {
    readonly uid: number;
    readonly isOwnedLive: (pid: number) => Promise<boolean>;
  };
  readonly readback: (input: {
    readonly baseDir: string;
    readonly activeVersion: string;
    readonly environmentId: string;
  }) => Promise<{
    readonly processId: number;
    readonly serviceManaged: boolean;
    readonly serverVersion: string;
    readonly environmentId: string;
    readonly port?: number;
  }>;
}
interface ArtifactMetadata {
  readonly schema: 1;
  readonly repository: typeof repository;
  readonly source: string;
  readonly tree: string;
  readonly version: string;
  readonly platform: "linux" | "darwin";
  readonly architecture: "x64" | "arm64";
  readonly workflow: QualifiedRuntimeArtifact["workflow"];
  readonly event: "push" | "workflow_dispatch";
  readonly ref: "refs/heads/main";
  readonly runId: string;
  readonly runAttempt: string;
  readonly artifact: string;
  readonly sha256: string;
}
interface VerifiedArtifact {
  readonly metadata: ArtifactMetadata;
  readonly artifact: Omit<QualifiedRuntimeArtifact, "payloadDirectory">;
  readonly archive: string;
  readonly directory: string;
}
export interface AdoptionPlan {
  readonly baseDir: string;
  readonly environmentId: string;
  readonly activeVersion: string;
  readonly launcherVersion: string;
  readonly servicePath: string;
  readonly serviceContents: string;
  readonly effects: ReadonlyArray<string>;
  readonly commands: ReadonlyArray<AdoptionCommand>;
  readonly serviceUnit?: "jones-code.service" | "t3code.service";
  readonly attestation?: "child" | "launcher-only";
  readonly archiveDirectory?: string;
  readonly recovery?: ReadonlyArray<string>;
}
export class HostAdoptionError extends Error {
  readonly reason: string;
  constructor(reason: string) {
    super(`Jones host adoption blocked: ${reason}`);
    this.name = "HostAdoptionError";
    this.reason = reason;
  }
}
const refuse = (reason: string): never => {
  throw new HostAdoptionError(reason);
};
const object = (value: unknown): Record<string, unknown> => {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    return refuse("Invalid qualification document.");
  return value as Record<string, unknown>;
};
async function regular(file: string, owner?: number): Promise<NodeFSP.FileHandle> {
  const descriptor = await NodeFSP.open(
    file,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
  );
  const stat = await descriptor.stat();
  if (
    !stat.isFile() ||
    stat.size > 2 * 1024 * 1024 * 1024 ||
    (owner !== undefined && (stat.uid !== owner || (stat.mode & 0o077) !== 0 || stat.size > 4096))
  ) {
    await descriptor.close();
    return refuse("Artifact or receipt is not a bounded regular file.");
  }
  return descriptor;
}
async function readText(file: string, owner?: number): Promise<string> {
  const fd = await regular(file, owner);
  try {
    if ((await fd.stat()).size > 128 * 1024)
      return refuse("Qualification document exceeds its bound.");
    const bytes = await fd.readFile();
    const text = bytes.toString("utf8");
    if (!Buffer.from(text, "utf8").equals(bytes))
      return refuse("Qualification preimage is not exact UTF-8 bytes.");
    return text;
  } finally {
    await fd.close();
  }
}
async function hashFile(file: string): Promise<string> {
  const fd = await regular(file);
  try {
    const digest = NodeCrypto.createHash("sha256");
    for await (const bytes of fd.createReadStream({ autoClose: false })) digest.update(bytes);
    return digest.digest("hex");
  } finally {
    await fd.close();
  }
}
async function optionalText(file: string, owner?: number): Promise<string | undefined> {
  try {
    return await readText(file, owner);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw error;
  }
}
function decodeMetadata(value: unknown, host: AdoptionHost, expected: string): ArtifactMetadata {
  const m = object(value);
  const workflow =
    host.platform === "linux" ? ".github/workflows/artifact-cli-linux.yml" : m.workflow;
  if (
    m.schema !== 1 ||
    m.repository !== repository ||
    m.source !== expected ||
    !source.test(String(m.source)) ||
    !source.test(String(m.tree)) ||
    !versionPattern.test(String(m.version)) ||
    m.platform !== host.platform ||
    m.architecture !== host.architecture ||
    m.ref !== "refs/heads/main" ||
    typeof m.runId !== "string" ||
    !/^[1-9]\d*$/.test(m.runId) ||
    typeof m.runAttempt !== "string" ||
    !/^[1-9]\d*$/.test(m.runAttempt) ||
    !Number.isSafeInteger(Number(m.runId)) ||
    !Number.isSafeInteger(Number(m.runAttempt)) ||
    !hash.test(String(m.sha256))
  )
    return refuse("ARTIFACT.json does not bind the exact source and host.");
  const cliMac = workflow === ".github/workflows/artifact-cli-mac.yml";
  if (
    m.workflow !== workflow ||
    (host.platform === "darwin" &&
      ![
        ".github/workflows/artifact-cli-mac.yml",
        ".github/workflows/artifact-desktop-mac.yml",
      ].includes(String(workflow))) ||
    m.event !== (cliMac ? "workflow_dispatch" : "push")
  )
    return refuse("Workflow or event is not an accepted main build.");
  const filename =
    workflow === ".github/workflows/artifact-desktop-mac.yml"
      ? `T3-Code-${m.version}-arm64.dmg`
      : `t3-${m.version}-${host.platform}-${host.architecture}.tar.gz`;
  if (m.artifact !== filename)
    return refuse("Archive filename does not match the workflow and host.");
  return m as unknown as ArtifactMetadata;
}

/** The Actions ZIP digest authenticates embedded metadata; local checksums alone never enroll a runtime. */
export async function verifyAdoptionArtifact(
  directory: string,
  expected: string,
  host: AdoptionHost,
): Promise<VerifiedArtifact> {
  if (!NodePath.isAbsolute(directory) || (await NodeFSP.realpath(directory)) !== directory)
    return refuse("Artifact directory must be a real absolute path.");
  const metadataText = await readText(NodePath.join(directory, "ARTIFACT.json"));
  const m = decodeMetadata(JSON.parse(metadataText) as unknown, host, expected);
  const api = async (endpoint: string) =>
    object(
      JSON.parse(
        await host.run({
          command: "gh",
          args: [
            "api",
            "--hostname",
            "github.com",
            "--method",
            "GET",
            `repos/${repository}/${endpoint}`,
          ],
        }),
      ) as unknown,
    );
  const run = await api(`actions/runs/${m.runId}`);
  const commit = await api(`git/commits/${m.source}`);
  if (
    run.id !== Number(m.runId) ||
    run.run_attempt !== Number(m.runAttempt) ||
    run.path !== m.workflow ||
    run.head_sha !== m.source ||
    run.head_branch !== "main" ||
    run.event !== m.event ||
    run.status !== "completed" ||
    run.conclusion !== "success" ||
    object(run.repository).full_name !== repository ||
    object(run.head_repository).full_name !== repository ||
    commit.sha !== m.source ||
    object(commit.tree).sha !== m.tree
  )
    return refuse("Actions run, generation or source tree is unqualified.");
  const ci = await api(
    `actions/workflows/ci.yml/runs?head_sha=${m.source}&event=push&branch=main&per_page=100&page=1`,
  );
  if (
    !Array.isArray(ci.workflow_runs) ||
    !ci.workflow_runs
      .map(object)
      .some(
        (check) =>
          check.path === ".github/workflows/ci.yml" &&
          check.head_sha === m.source &&
          check.head_branch === "main" &&
          check.event === "push" &&
          check.status === "completed" &&
          check.conclusion === "success" &&
          object(check.repository).full_name === repository &&
          object(check.head_repository).full_name === repository,
      )
  )
    return refuse("The exact source has no successful canonical main CI run.");
  const listing = await api(`actions/runs/${m.runId}/artifacts?per_page=100&page=1`);
  if (
    !Array.isArray(listing.artifacts) ||
    listing.artifacts.length > 100 ||
    listing.total_count !== listing.artifacts.length
  )
    return refuse("Artifact enumeration is incomplete.");
  const prefix =
    m.workflow === ".github/workflows/artifact-desktop-mac.yml"
      ? "desktop-mac-arm64"
      : `jones-code-cli-${host.platform === "darwin" ? "mac" : "linux"}-${host.architecture}`;
  const artifactName = `${prefix}-${m.runId}-${m.runAttempt}--${m.version}`;
  const artifacts = listing.artifacts.map(object).filter((a) => a.name === artifactName);
  if (artifacts.length !== 1) return refuse("Exact artifact generation is missing or ambiguous.");
  const a = artifacts[0]!;
  const digest = String(a.digest).replace(/^sha256:/, "");
  const workflowRun = object(a.workflow_run);
  if (
    !Number.isSafeInteger(a.id) ||
    Number(a.id) < 1 ||
    !hash.test(digest) ||
    a.expired !== false ||
    workflowRun.id !== run.id ||
    workflowRun.head_sha !== m.source ||
    workflowRun.head_branch !== "main" ||
    workflowRun.repository_id !== object(run.repository).id ||
    workflowRun.head_repository_id !== object(run.repository).id
  )
    return refuse("Actions artifact identity is unqualified.");
  const zip = NodePath.join(directory, "github-artifact.zip");
  const zipStat = await NodeFSP.lstat(zip);
  if (zipStat.size !== a.size_in_bytes || (await hashFile(zip)) !== digest)
    return refuse("Actions ZIP digest does not match this artifact generation.");
  const embedded = await host.run({ command: "unzip", args: ["-p", zip, "ARTIFACT.json"] });
  const original = decodeMetadata(JSON.parse(embedded) as unknown, host, expected);
  for (const key of Object.keys(m) as Array<keyof ArtifactMetadata>)
    if (original[key] !== m[key])
      return refuse("Local metadata differs from the authenticated Actions ZIP.");
  const archive = NodePath.join(directory, m.artifact);
  if ((await hashFile(archive)) !== m.sha256)
    return refuse("Runtime archive differs from the authenticated metadata.");
  return {
    metadata: m,
    archive,
    directory,
    artifact: {
      repository,
      channel: "jones-main",
      version: m.version,
      sourceSha: m.source,
      sourceTree: m.tree,
      installedSourceSha: m.source,
      runId: Number(m.runId),
      runAttempt: Number(m.runAttempt),
      artifactId: Number(a.id),
      workflow: m.workflow,
      artifactDigest: `sha256:${digest}`,
      archiveSha256: m.sha256,
      platform: m.platform,
      architecture: m.architecture,
    },
  };
}

async function inspectAdoption(input: AdoptInput, host: AdoptionHost) {
  if (
    !NodePath.isAbsolute(input.baseDir) ||
    !NodePath.isAbsolute(host.home) ||
    !Number.isSafeInteger(host.uid) ||
    host.uid < 1
  )
    return refuse("An absolute home, base and non-root service user are required.");
  const baseDir = await NodeFSP.realpath(input.baseDir);
  if (baseDir !== input.baseDir)
    return refuse("Base directory resolves through an alias; select its exact path.");
  if (
    (await optionalText(NodePath.join(baseDir, "runtime", "jones-active-install.json"))) !==
    undefined
  )
    return refuse("This home is desktop-owned.");
  const previousAdoptionText = await optionalText(
    NodePath.join(baseDir, "runtime", ADOPTION_RECEIPT),
  );
  const previousAdoption =
    previousAdoptionText === undefined
      ? undefined
      : object(JSON.parse(previousAdoptionText) as unknown);
  if (
    previousAdoption !== undefined &&
    (previousAdoption.schema !== 1 || previousAdoption.status !== "applied")
  )
    return refuse("Previous adoption has a pending or unknown effect; reconcile it before retry.");
  const stateText = await readText(NodePath.join(baseDir, "runtime", SERVICE_STATE_FILE));
  const classification = LegacyBootstrap.classifyAdoptionServiceState(stateText, input);
  const legacy = classification.kind === "task-handoff";
  if (legacy && host.platform !== "linux")
    return refuse("Bound legacy direct-serve bootstrap supports a Linux user unit only.");
  const state = {
    activeVersion:
      classification.kind === "native"
        ? classification.state.activeVersion
        : classification.activeVersion,
  };
  const retainedBackups = legacy
    ? await LegacyBootstrap.assertNoBootstrapHazards(baseDir, host.uid)
    : [];
  if (legacy && input.serviceUnit === undefined)
    return refuse("Legacy bootstrap requires explicit service-unit selection.");
  const serviceUnit = input.serviceUnit ?? "jones-code.service";
  const continuation =
    !legacy &&
    previousAdoption?.archiveDigests !== undefined &&
    previousAdoption.baseDir === baseDir &&
    previousAdoption.serviceUnit === serviceUnit &&
    previousAdoption.activeVersion === state.activeVersion;
  if (
    !["jones-code.service", "t3code.service"].includes(serviceUnit) ||
    (!legacy && !continuation && serviceUnit !== "jones-code.service")
  )
    return refuse(
      "Only a bound legacy bootstrap may select the existing t3code.service user unit.",
    );
  const environmentId = (
    await readText(NodePath.join(baseDir, "userdata", "environment-id"))
  ).trim();
  if (environmentId === "") return refuse("Native environment identity is missing.");
  const running = await host.readback({
    baseDir,
    activeVersion: state.activeVersion,
    environmentId,
  });
  if (
    (legacy ? running.serviceManaged : !running.serviceManaged) ||
    running.serverVersion !== state.activeVersion ||
    running.environmentId !== environmentId ||
    !Number.isSafeInteger(running.processId) ||
    running.processId < 1
  )
    return refuse("The running server does not match this native service state.");
  const dbPath = NodePath.join(baseDir, "userdata", "statev2.sqlite");
  const db = await NodeFSP.lstat(dbPath);
  if (!db.isFile() || db.isSymbolicLink()) return refuse("Native database is not a regular file.");
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
  if (active.metadata.version !== state.activeVersion)
    return refuse("Active artifact does not match service-state activeVersion.");
  if (
    launcher.metadata.version === active.metadata.version &&
    launcher.metadata.source !== active.metadata.source
  )
    return refuse("Different artifacts claim the same version.");
  const oldRuntime = NodePath.join(baseDir, "runtime", "versions", state.activeVersion, "t3");
  const launcherPath = NodePath.join(
    baseDir,
    "runtime",
    "versions",
    launcher.metadata.version,
    "t3",
  );
  const unitPath =
    host.platform === "linux"
      ? NodePath.join(host.home, ".config/systemd/user", serviceUnit)
      : NodePath.join(host.home, "Library/LaunchAgents", `${identity.launchdLabel}.plist`);
  if (
    (legacy || continuation) &&
    (!hash.test(input.serviceUnitSha256 ?? "") ||
      (await hashFile(unitPath)) !== input.serviceUnitSha256)
  )
    return refuse("Legacy base unit SHA-256 is not the bound approved preimage.");
  const unitText = await readText(unitPath);
  if (
    (legacy && (unitText.match(/^Environment=T3CODE_HOME=/gm) ?? []).length !== 1) ||
    bootServiceBaseDirOf(unitText) !== baseDir
  )
    return refuse("Existing service belongs to a different or unknown home.");
  const renderedPlan = {
    baseDir,
    program: [launcherPath, "__service-launcher"],
    logPath: NodePath.join(baseDir, "userdata/logs/boot-service.log"),
    unitPath,
  };
  let servicePath = unitPath;
  let serviceContents: string;
  const snapshots = new Map<string, string>([[unitPath, unitText]]);
  const preservedMetadata = new Map<string, string>();
  let task:
    | {
        path: string;
        text: string;
        executable: string;
        environment: Record<string, string>;
        port: number;
        execStart: string;
        executableSha256: string;
      }
    | undefined;
  let archiveDirectory: string | undefined;
  if (legacy || continuation) {
    if (
      continuation &&
      previousAdoption?.attestation === "launcher-only" &&
      input.acceptUnattestedChildCapability !== true
    )
      return refuse("Launcher-only continuation requires explicit unattested-child acceptance.");
    const boundDropin = LegacyBootstrap.taskDropinBinding(input.taskDropin);
    const taskPath = NodePath.join(`${unitPath}.d`, boundDropin.name);
    const taskText = await readText(taskPath);
    if (LegacyBootstrap.bytesSha256(taskText) !== boundDropin.sha256)
      return refuse("Task drop-in hash differs from its approved preimage.");
    const parsed = LegacyBootstrap.parseTaskDropin(taskText, baseDir);
    if (running.port !== parsed.port)
      return refuse("Direct-serve runtime port does not match task drop-in mapping.");
    const mainPid = Number(
      (
        await host.run({
          command: "systemctl",
          args: ["--user", "show", serviceUnit, "--property=MainPID", "--value"],
        })
      ).trim(),
    );
    if (legacy && mainPid !== running.processId)
      return refuse("Direct-serve MainPID differs from native runtime PID.");
    const observedExecutable = legacy
      ? (await host.run({ command: "readlink", args: [`/proc/${mainPid}/exe`] })).trim()
      : parsed.executable;
    if (observedExecutable !== parsed.executable)
      return refuse("Running executable path differs from the bound task direct-serve path.");
    const expectedExecutable = await LegacyBootstrap.archiveExecutableSha256(active.archive);
    if ((await hashFile(parsed.executable)) !== expectedExecutable)
      return refuse("Running executable bytes differ from the authenticated active archive.");
    if (
      (await optionalText(
        NodePath.join(baseDir, "runtime/versions", state.activeVersion, ".jones-provenance.json"),
      )) !== undefined
    )
      return refuse(
        "PR239 active runtime layout rejects private provenance; preserve and reconcile that cache before bootstrap.",
      );
    task = { ...parsed, path: taskPath, text: taskText, executableSha256: expectedExecutable };
    snapshots.set(taskPath, taskText);
    if (continuation && object(previousAdoption!.archiveDigests).taskDropin !== boundDropin.sha256)
      return refuse("Continuation task drop-in is not the previously archived preimage.");
    if (legacy)
      archiveDirectory = NodePath.join(
        baseDir,
        "runtime/jones-adoption",
        `${LegacyBootstrap.bytesSha256(stateText).slice(0, 16)}-${launcher.metadata.version}`,
      );
    if (
      archiveDirectory !== undefined &&
      (await NodeFSP.lstat(archiveDirectory).then(
        () => true,
        (error: NodeJS.ErrnoException) => {
          if (error.code !== "ENOENT") throw error;
          return false;
        },
      ))
    )
      return refuse(
        "Bootstrap recovery archive already exists; reconcile its prior effect before retry.",
      );
  }
  let dropinObservation: { paths: string; names: string[] } | undefined;
  if (host.platform === "linux") {
    const fragment = (
      await host.run({
        command: "systemctl",
        args: ["--user", "show", serviceUnit, "--property=FragmentPath", "--value"],
      })
    ).trim();
    if (fragment !== unitPath)
      return refuse("Effective unit fragment has a different or unknown owner.");
    const observedDropins = (
      await host.run({
        command: "systemctl",
        args: ["--user", "show", serviceUnit, "--property=DropInPaths", "--value"],
      })
    ).trim();
    const dropins = observedDropins === "" ? [] : observedDropins.split(/\s+/);
    const ownedDir = `${unitPath}.d`;
    const local = await NodeFSP.readdir(ownedDir).catch((e: NodeJS.ErrnoException) => {
      if (e.code !== "ENOENT") throw e;
      return [] as string[];
    });
    dropinObservation = {
      paths: observedDropins,
      names: local.filter((n) => n.endsWith(".conf")).sort(),
    };
    const allDropins = Array.from(
      new Set([
        ...dropins,
        ...local.filter((n) => n.endsWith(".conf")).map((n) => NodePath.join(ownedDir, n)),
      ]),
    ).sort();
    servicePath = NodePath.join(ownedDir, "zzzz-jones-qualified-launcher.conf");
    const preserveNames = input.preserveDropin ?? [];
    if (
      new Set(preserveNames).size !== preserveNames.length ||
      preserveNames.some((name) => !/^[A-Za-z0-9_.-]+\.conf$/.test(name) || name.startsWith("."))
    )
      return refuse("Preserved drop-in names must be unique plain filenames.");
    let unownedOverride = false;
    for (const file of allDropins) {
      if (NodePath.dirname(file) !== ownedDir)
        return refuse("Effective drop-in is outside the selected user unit directory.");
      if (preserveNames.includes(NodePath.basename(file))) {
        if (
          (!legacy && !continuation) ||
          file === servicePath ||
          file === task?.path ||
          NodePath.basename(file).localeCompare(NodePath.basename(task!.path), "en") >= 0
        )
          return refuse(
            "Only earlier credential drop-ins may be preserved by metadata in bound legacy mode.",
          );
        preservedMetadata.set(file, await LegacyBootstrap.fileMetadata(file));
        continue;
      }
      if ((legacy || continuation) && file !== task?.path && file !== servicePath)
        return refuse(
          "Legacy bootstrap has an unbound drop-in; declare its metadata-only preservation.",
        );
      const text = await readText(file);
      snapshots.set(file, text);
      if (
        /^\s*ExecStart\s*=/m.test(text) &&
        !(file === servicePath && text.startsWith(`${marker}\n`)) &&
        file !== task?.path
      )
        unownedOverride = true;
      if (
        NodePath.basename(file).localeCompare(NodePath.basename(servicePath), "en") >= 0 &&
        file !== servicePath
      )
        return refuse(
          "A drop-in sorts after the proposed launcher override; precedence is unknown.",
        );
    }
    if (preserveNames.some((name) => !allDropins.includes(NodePath.join(ownedDir, name))))
      return refuse("A declared preserved drop-in is missing.");
    const oldLauncher = previousAdoption?.launcherVersion;
    const ownedEntry = [
      oldRuntime,
      ...(typeof oldLauncher === "string"
        ? [NodePath.join(baseDir, "runtime/versions", oldLauncher, "t3")]
        : []),
    ];
    const currentExec = /^ExecStart=(.*)$/m.exec(unitText)?.[1];
    if (
      !legacy &&
      (currentExec === undefined ||
        !ownedEntry.some(
          (entry) =>
            currentExec === entry ||
            currentExec === `${entry} __service-launcher` ||
            currentExec === `"${entry}" __service-launcher`,
        ))
    )
      unownedOverride = true;
    if (unownedOverride && input.supersedeExecstart !== true)
      return refuse(
        "Unowned ExecStart override; approve --supersede-execstart to add a higher-priority drop-in.",
      );
    const existing = await optionalText(servicePath);
    if (existing !== undefined && !existing.startsWith(`${marker}\n`))
      return refuse("Proposed drop-in path is occupied by an unowned file.");
    serviceContents = `${marker}\n[Service]\nExecStart=\n${renderBootServiceUnit(
      renderedPlan,
      identity,
    )
      .split("\n")
      .find((line) => line.startsWith("ExecStart="))}\n${
      task === undefined
        ? ""
        : Object.entries(task.environment)
            .sort(([a], [b]) => a.localeCompare(b, "en"))
            .map(([key, value]) => `Environment=${key}=${value}\n`)
            .join("")
    }`;
  } else {
    const program = /<key>ProgramArguments<\/key>\s*<array>[\s\S]*?<\/array>/;
    const existingProgram = program.exec(unitText)?.[0];
    if (
      existingProgram === undefined ||
      (!existingProgram.includes(oldRuntime) && input.supersedeExecstart !== true)
    )
      return refuse(
        "Launch agent ProgramArguments has an unknown owner; approve explicit supersession.",
      );
    const rendered = renderBootServicePlist(
      renderedPlan,
      { homeDir: host.home, environmentPath: host.environmentPath },
      identity,
    );
    serviceContents = unitText.replace(program, program.exec(rendered)![0]);
  }
  const commands =
    host.platform === "linux"
      ? [
          { command: "systemctl", args: ["--user", "stop", serviceUnit] },
          { command: "systemctl", args: ["--user", "daemon-reload"] },
          { command: "systemctl", args: ["--user", "restart", serviceUnit] },
        ]
      : [
          {
            command: "launchctl",
            args: ["bootout", "--wait", `gui/${host.uid}/${identity.launchdLabel}`],
          },
          { command: "launchctl", args: ["bootstrap", `gui/${host.uid}`, unitPath] },
        ];
  const plan: AdoptionPlan = {
    baseDir,
    environmentId,
    activeVersion: state.activeVersion,
    launcherVersion: launcher.metadata.version,
    servicePath,
    serviceContents,
    commands,
    serviceUnit,
    attestation: legacy || continuation ? "launcher-only" : "child",
    ...(archiveDirectory === undefined
      ? {}
      : {
          archiveDirectory,
          recovery: [
            `Review a separate recovery approval for ${serviceUnit}, ${baseDir} and the archived preimages.`,
            `Compare the current owned drop-in and native state against this adoption receipt before restoring anything.`,
            `Overwrite only ${servicePath} with [Service], ExecStart= and the archived ${task!.execStart}; preserve all original drop-ins.`,
            `Compare native protocol-4 state, then restore exact bytes from ${NodePath.join(archiveDirectory, "service-state.task-handoff.json")}.`,
            `Reload the user manager and restart ${serviceUnit}; verify the retained direct-serve version, port and environment.`,
          ],
        }),
    effects: [
      ...(legacy
        ? [
            `Preserve credential drop-ins by metadata only: ${(input.preserveDropin ?? []).join(", ")}; contents are never opened.`,
            `Retain database backup directories: ${retainedBackups.join(", ") || "none"}. Compare candidate pending migration IDs before stopping and after restart; do not run migrations.`,
            "Read back qualified launcher evidence; retained server Install is unattested unless its own capability receipt is observed.",
          ]
        : []),
      `Write ${NodePath.join(baseDir, "runtime", ADOPTION_RECEIPT)} before enrollment or stopping; an incomplete result prevents retry.`,
      ...[active, launcher].map(
        (a) =>
          `Enroll ${NodePath.join(baseDir, "runtime/versions", a.metadata.version)} from Actions ${a.metadata.runId}/${a.metadata.runAttempt}, source ${a.metadata.source}, archive SHA-256 ${a.metadata.sha256}; preserve occupied payloads and append a qualified receipt only after exact comparison.`,
      ),
      `Stop ${host.platform === "linux" ? serviceUnit : identity.launchdLabel}; preserve database, environment ID, configuration and service-state activeVersion ${state.activeVersion}.`,
      ...(legacy
        ? [
            `Archive exact handoff and task drop-in bytes exclusively under ${archiveDirectory}, sync and read back their SHA-256 digests, then compare-and-replace only service-state.json with protocol 4 and activeVersion ${state.activeVersion}.`,
          ]
        : []),
      `Write ${servicePath}:\n${serviceContents}`,
      ...commands.slice(1).map((c) => `Run ${c.command} ${c.args.join(" ")}.`),
      legacy || continuation
        ? "Read back launcher protocols, new child PID, retained version/home/port/environment and launcher-only attestation; Install remains unattested unless the child receipt exists."
        : "Read back the new launcher service, unchanged native identity/version, and owner-only capability.install receipt; no deletion or sudo.",
    ],
  };
  return {
    plan,
    stateText,
    active,
    launcher,
    snapshots,
    dbPath,
    databaseIdentity: `${db.dev}:${db.ino}`,
    previousAdoptionText,
    previousProcessId: running.processId,
    dropinObservation,
    preservedMetadata,
    task,
    legacy,
    continuation,
    archiveDirectory,
  };
}
export async function planHostAdoption(
  input: AdoptInput,
  host: AdoptionHost,
): Promise<AdoptionPlan> {
  return (await inspectAdoption(input, host)).plan;
}
async function durableReplace(file: string, contents: string): Promise<void> {
  await NodeFSP.mkdir(NodePath.dirname(file), { recursive: true, mode: 0o700 });
  const temporary = `${file}.write-${NodeCrypto.randomUUID()}`;
  const fd = await NodeFSP.open(temporary, "wx", 0o600);
  try {
    await fd.writeFile(contents);
    await fd.sync();
  } finally {
    await fd.close();
  }
  try {
    await NodeFSP.rename(temporary, file);
    const directory = await NodeFSP.open(NodePath.dirname(file), "r");
    try {
      await directory.sync();
    } finally {
      await directory.close();
    }
  } finally {
    await NodeFSP.rm(temporary, { force: true });
  }
}
async function archiveBootstrapPreimages(
  directory: string,
  files: ReadonlyArray<{ name: string; text: string }>,
): Promise<void> {
  await NodeFSP.mkdir(NodePath.dirname(directory), { recursive: true, mode: 0o700 });
  await NodeFSP.mkdir(directory, { mode: 0o700 });
  for (const file of files) {
    const destination = NodePath.join(directory, file.name);
    const descriptor = await NodeFSP.open(destination, "wx", 0o600);
    try {
      await descriptor.writeFile(file.text);
      await descriptor.sync();
    } finally {
      await descriptor.close();
    }
    if ((await hashFile(destination)) !== LegacyBootstrap.bytesSha256(file.text))
      return refuse("Recovery archive readback differs; reconcile before retry.");
  }
  for (const target of [directory, NodePath.dirname(directory)]) {
    const descriptor = await NodeFSP.open(target, "r");
    try {
      await descriptor.sync();
    } finally {
      await descriptor.close();
    }
  }
}
async function compareAndReplace(
  file: string,
  expected: string,
  replacement: string,
): Promise<void> {
  if ((await readText(file)) !== expected)
    return refuse("Bootstrap preimage changed before native-state replacement.");
  await durableReplace(file, replacement);
  if ((await readText(file)) !== replacement)
    return refuse("Native bootstrap state readback differs; effect requires reconciliation.");
}
export async function adoptHost(
  input: AdoptInput,
  host: AdoptionHost,
): Promise<{ readonly state: "dry-run" | "adopted"; readonly plan: AdoptionPlan }> {
  if (input.dryRun === true) return { state: "dry-run", plan: await planHostAdoption(input, host) };
  return withQualifiedRuntimeLock(input.baseDir, "qualified-runtime-lock", async () => {
    const inspected = await inspectAdoption(input, host);
    const { plan } = inspected;
    const serviceUnit = plan.serviceUnit ?? "jones-code.service";
    const receiptPath = NodePath.join(plan.baseDir, "runtime", ADOPTION_RECEIPT);
    const assertUnchanged = async () => {
      if (
        (await readText(NodePath.join(plan.baseDir, "runtime", SERVICE_STATE_FILE))) !==
          inspected.stateText ||
        (await readText(NodePath.join(plan.baseDir, "userdata/environment-id"))).trim() !==
          plan.environmentId
      )
        return refuse("Native state changed after planning.");
      const db = await NodeFSP.lstat(inspected.dbPath);
      if (`${db.dev}:${db.ino}` !== inspected.databaseIdentity)
        return refuse("Native database identity changed.");
      if (inspected.legacy) {
        await LegacyBootstrap.assertNoBootstrapHazards(plan.baseDir, host.uid);
        if ((await hashFile(inspected.task!.executable)) !== inspected.task!.executableSha256)
          return refuse("Bound running executable changed after planning.");
      }
      for (const [file, metadata] of inspected.preservedMetadata)
        if ((await LegacyBootstrap.fileMetadata(file)) !== metadata)
          return refuse("Preserved credential drop-in metadata changed after planning.");
      for (const [file, text] of inspected.snapshots)
        if ((await readText(file)) !== text)
          return refuse("Service definition changed after planning.");
      if (inspected.dropinObservation !== undefined) {
        const paths = (
          await host.run({
            command: "systemctl",
            args: ["--user", "show", serviceUnit, "--property=DropInPaths", "--value"],
          })
        ).trim();
        const names = await NodeFSP.readdir(
          `${NodePath.join(host.home, ".config/systemd/user", serviceUnit)}.d`,
        ).catch((e: NodeJS.ErrnoException) => {
          if (e.code !== "ENOENT") throw e;
          return [] as string[];
        });
        if (
          paths !== inspected.dropinObservation.paths ||
          JSON.stringify(names.filter((n) => n.endsWith(".conf")).sort()) !==
            JSON.stringify(inspected.dropinObservation.names)
        )
          return refuse("Service drop-in set changed after planning.");
      }
    };
    await assertUnchanged();
    await durableReplace(
      receiptPath,
      `${JSON.stringify({ schema: 1, status: "pending", ...plan })}\n`,
    );
    const scratch = await NodeFSP.mkdtemp(
      NodePath.join(plan.baseDir, "runtime", ".jones-adoption-"),
    );
    try {
      for (const [index, artifact] of [inspected.active, inspected.launcher].entries()) {
        const payload = NodePath.join(scratch, String(index));
        await NodeFSP.mkdir(payload, { mode: 0o700 });
        if (artifact.metadata.workflow === ".github/workflows/artifact-desktop-mac.yml") {
          const staged = {
            candidate: {
              platform: "darwin",
              architecture: "arm64",
              source: artifact.metadata.source,
              tree: artifact.metadata.tree,
            },
            receipt: { version: artifact.metadata.version },
            payloadPath: artifact.archive,
          } as JonesStagedArtifact;
          await extractQualifiedDarwinRuntime({
            artifact: staged,
            destination: payload,
            baseDir: plan.baseDir,
            run: async (c) => ({ code: 0, stdout: await host.run(c) }),
          });
        } else await extractQualifiedLinuxArchive(artifact.archive, payload);
        await verifyAdoptionArtifact(artifact.directory, artifact.metadata.source, host);
        await enrollQualifiedRuntime({
          baseDir: plan.baseDir,
          artifact: { ...artifact.artifact, payloadDirectory: payload },
          host: { platform: host.platform, architecture: host.architecture },
          validate: async (entry) => {
            const output = await host.run({ command: entry, args: ["--version"] });
            if (/\bv(\S+)\s*$/.exec(output)?.[1] !== artifact.metadata.version)
              return refuse("Verified executable reported a different version.");
            const preflight = object(
              JSON.parse(
                await host.run({
                  command: entry,
                  args: [
                    "__service-preflight",
                    "--database-path",
                    inspected.dbPath,
                    "--launcher-protocol",
                    String(SERVICE_LAUNCHER_PROTOCOL),
                  ],
                }),
              ) as unknown,
            );
            if (
              preflight.status !== "ready" ||
              preflight.version !== artifact.metadata.version ||
              preflight.launcherProtocol !== SERVICE_LAUNCHER_PROTOCOL ||
              preflight.startupGateProtocol !== 1
            )
              return refuse("The runtime cannot provide the qualified launcher/startup protocol.");
          },
        });
      }
      const migrationPreflight = async () =>
        LegacyBootstrap.readPreflightMigrationPlan(
          await host.run({
            command: NodePath.join(plan.baseDir, "runtime/versions", plan.launcherVersion, "t3"),
            args: [
              "__service-preflight",
              "--database-path",
              inspected.dbPath,
              "--launcher-protocol",
              String(SERVICE_LAUNCHER_PROTOCOL),
            ],
          }),
          plan.launcherVersion,
        );
      const pendingMigrationPlan = inspected.legacy ? await migrationPreflight() : undefined;
      await assertUnchanged();
      await host.run(plan.commands[0]!);
      await assertUnchanged();
      if (inspected.legacy) {
        await archiveBootstrapPreimages(inspected.archiveDirectory!, [
          { name: "service-state.task-handoff.json", text: inspected.stateText },
          { name: "task-dropin.conf", text: inspected.task!.text },
        ]);
        await compareAndReplace(
          NodePath.join(plan.baseDir, "runtime", SERVICE_STATE_FILE),
          inspected.stateText,
          `${JSON.stringify({ protocol: SERVICE_LAUNCHER_PROTOCOL, activeVersion: plan.activeVersion })}\n`,
        );
      }
      await durableReplace(plan.servicePath, plan.serviceContents);
      for (const command of plan.commands.slice(1)) await host.run(command);
      await readQualifiedRuntimeReceipt(plan.baseDir, plan.activeVersion, {
        platform: host.platform,
        architecture: host.architecture,
      });
      await readQualifiedRuntimeReceipt(plan.baseDir, plan.launcherVersion, {
        platform: host.platform,
        architecture: host.architecture,
      });
      if ((await readText(plan.servicePath)) !== plan.serviceContents)
        return refuse("Service definition readback differs; effect requires reconciliation.");
      if (
        parseServiceState(
          await readText(NodePath.join(plan.baseDir, "runtime", SERVICE_STATE_FILE)),
        )?.activeVersion !== plan.activeVersion
      )
        return refuse("Active version changed during adoption; effect requires reconciliation.");
      const attempts = host.readbackAttempts ?? 120;
      let observed: Awaited<ReturnType<AdoptionHost["readback"]>> | undefined;
      let attestation: "child" | "launcher-only" = "child";
      for (let attempt = 0; attempt < attempts; attempt++) {
        try {
          const live = await host.readback(plan);
          if (
            live.processId === inspected.previousProcessId ||
            !live.serviceManaged ||
            live.serverVersion !== plan.activeVersion ||
            live.environmentId !== plan.environmentId ||
            (inspected.task !== undefined && live.port !== inspected.task.port)
          )
            throw new Error("Native child startup identity has not settled.");
          const capabilityText = await optionalText(
            NodePath.join(plan.baseDir, "runtime", UPDATE_CAPABILITY_RECEIPT),
            host.uid,
          );
          if (capabilityText !== undefined) {
            const capability = object(JSON.parse(capabilityText) as unknown);
            if (
              capability.schema !== 1 ||
              capability.baseDir !== plan.baseDir ||
              capability.environmentId !== plan.environmentId ||
              capability.currentVersion !== plan.activeVersion ||
              capability.processId !== live.processId ||
              capability.qualifiedLauncher !== true ||
              object(capability.capability).install !== true
            )
              throw new Error("Child capability receipt is stale or unqualified.");
            attestation = "child";
          } else {
            if (
              (!inspected.legacy && !inspected.continuation) ||
              input.acceptUnattestedChildCapability !== true
            )
              throw new Error("Child capability receipt is unavailable.");
            const launcherPid = Number(
              (
                await host.run({
                  command: "systemctl",
                  args: ["--user", "show", serviceUnit, "--property=MainPID", "--value"],
                })
              ).trim(),
            );
            const launcher = await readLauncherCapabilityReceipt(
              plan.baseDir,
              {
                launcherVersion: plan.launcherVersion,
                launcherPid,
                childVersion: plan.activeVersion,
                childPid: live.processId,
              },
              host.launcherProcessGuard,
            );
            if (launcher === undefined)
              throw new Error("Launcher capability receipt is unavailable or stale.");
            attestation = "launcher-only";
          }
          observed = live;
          break;
        } catch {
          // Read-only startup observations can be retried; service effects are never replayed.
        }
        if (attempt + 1 < attempts) await NodeTimersPromises.setTimeout(250);
      }
      if (observed === undefined)
        return refuse(
          inspected.legacy || inspected.continuation
            ? "New runtime or required child/launcher capability readback is unavailable or stale; effect requires reconciliation."
            : "New runtime or observed capability.install readback is unavailable or stale; effect requires reconciliation.",
        );
      if (
        pendingMigrationPlan !== undefined &&
        JSON.stringify(await migrationPreflight()) !== JSON.stringify(pendingMigrationPlan)
      )
        return refuse(
          "Pending migration IDs changed during adoption; no migration effects were authorized.",
        );
      for (const [file, metadata] of inspected.preservedMetadata)
        if ((await LegacyBootstrap.fileMetadata(file)) !== metadata)
          return refuse("Preserved credential drop-in metadata changed during activation.");
      for (const [file, text] of inspected.snapshots)
        if (file !== plan.servicePath && (await readText(file)) !== text)
          return refuse("Original service preimage changed during activation.");
      const database = await NodeFSP.lstat(inspected.dbPath);
      if (
        `${database.dev}:${database.ino}` !== inspected.databaseIdentity ||
        (await readText(NodePath.join(plan.baseDir, "userdata/environment-id"))).trim() !==
          plan.environmentId
      )
        return refuse("Native database or environment identity changed during activation.");
      if (host.platform === "linux") {
        await host.run({
          command: "systemctl",
          args: ["--user", "is-active", serviceUnit],
        });
        const executable = await host.run({
          command: "systemctl",
          args: ["--user", "show", serviceUnit, "--property=ExecStart", "--value"],
        });
        if (
          !executable.includes(
            NodePath.join(plan.baseDir, "runtime/versions", plan.launcherVersion, "t3"),
          )
        )
          return refuse("Effective launcher readback differs.");
      } else {
        const executable = await host.run({
          command: "launchctl",
          args: ["print", `gui/${host.uid}/${identity.launchdLabel}`],
        });
        if (
          !executable.includes(
            NodePath.join(plan.baseDir, "runtime/versions", plan.launcherVersion, "t3"),
          )
        )
          return refuse("Effective launcher readback differs.");
      }
      await durableReplace(
        receiptPath,
        `${JSON.stringify({
          schema: 1,
          status: "applied",
          ...plan,
          attestation,
          ...(attestation === "child"
            ? { capability: { install: true } }
            : { childCapability: "unattested" }),
          processId: observed.processId,
          ...(inspected.legacy
            ? {
                archiveDigests: {
                  handoff: LegacyBootstrap.bytesSha256(inspected.stateText),
                  taskDropin: LegacyBootstrap.bytesSha256(inspected.task!.text),
                },
                pendingMigrationPlan,
              }
            : inspected.continuation
              ? {
                  archiveDigests: object(JSON.parse(inspected.previousAdoptionText!) as unknown)
                    .archiveDigests,
                }
              : {}),
        })}\n`,
      );
      return { state: "adopted", plan: { ...plan, attestation } };
    } finally {
      await NodeFSP.rm(scratch, { recursive: true, force: true });
    }
  });
}
