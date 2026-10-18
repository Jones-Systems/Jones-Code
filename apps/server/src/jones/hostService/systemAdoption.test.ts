// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeZlib from "node:zlib";
import { expect, it as vitestIt, vi } from "vite-plus/test";
import { SERVICE_LAUNCHER_PROTOCOL } from "../../cloud/serviceProtocol.ts";
import { readQualifiedRuntimeReceipt } from "../cloud/qualifiedRuntime.ts";
import { ADOPTION_RECEIPT, planHostAdoption, SYSTEM_ADOPTION_RECEIPT } from "./adoption.ts";
import {
  adoptSystemHost,
  planSystemAdoption,
  type SystemAdoptInput,
  type SystemAdoptionHost,
} from "./systemAdoption.ts";
import {
  OWNED_SYSTEM_DROPIN,
  renderOwnedSystemDropin,
  renderSystemServicePolicy,
  SYSTEM_SERVICE_CONTROL_PATH,
  SYSTEM_SERVICE_POLICY_PATH,
  systemUnitDigest,
  systemUnitRecord,
  type SystemControlRequest,
  type SystemControlResult,
} from "./systemServiceControl.ts";

// Passes through to the native call; one test makes the runtime correction's
// change unobservable to exercise its unsettled-effect record.
vi.mock("node:fs/promises", async (importOriginal) => {
  const native = await importOriginal<typeof import("node:fs/promises")>();
  return { ...native, chmod: vi.fn(native.chmod) };
});

// Every fixture plans the runtime correction; once read back it stays in every record.
const verifiedRuntimeMode = { from: "0775", to: "0755", applied: true };
const oldVersion = "0.0.45-preview.20261008.37762212730.1";
const newVersion = "0.0.46-preview.20261011.1";
const privateSetupSource = "d3e6f8e843a0477542380a4ec0d9ae02a1084053";
const privateSetupTree = "250db17dd136cef960e9bb8e054acadb41fba03d";
const unit = "host-tooling-t3.service";
const controlGroup = "/hostdev.slice/host-tooling-t3.service";
const account = NodeOS.userInfo();
const sha = (value: string | Buffer) => NodeCrypto.createHash("sha256").update(value).digest("hex");
const runtimeExecutable = (version: string) => `#!/bin/sh\n# version: ${version}\nexit 0\n`;
// Adoption refuses a root service user by design, so a root runner cannot exercise it.
const it = process.getuid?.() === 0 ? vitestIt.skip : vitestIt;

function tarPayload(version: string) {
  const entries: Buffer[] = [];
  for (const [name, kind, body] of [
    ["payload/", "5", ""],
    ["payload/t3", "0", runtimeExecutable(version)],
    ["payload/client/", "5", ""],
    ["payload/node_modules/", "5", ""],
    ["payload/resource-monitor/", "5", ""],
  ] as const) {
    const header = Buffer.alloc(512);
    header.write(name, 0);
    header.write("0000755\0", 100);
    header.write("0000000\0", 108);
    header.write("0000000\0", 116);
    header.write(Buffer.byteLength(body).toString(8).padStart(11, "0") + "\0", 124);
    header.write("00000000000\0", 136);
    header.fill(32, 148, 156);
    header.write(kind, 156);
    const checksum = header.reduce((a, b) => a + b, 0);
    header.write(checksum.toString(8).padStart(6, "0") + "\0 ", 148);
    entries.push(
      header,
      Buffer.from(body),
      Buffer.alloc((512 - (Buffer.byteLength(body) % 512)) % 512),
    );
  }
  entries.push(Buffer.alloc(1024));
  return NodeZlib.gzipSync(Buffer.concat(entries));
}

/** Flat stored Actions ZIP with real CRC-32; its bytes alone determine the faked GitHub digest. */
function zipEnvelope(entries: ReadonlyArray<readonly [string, string | Buffer]>): Buffer {
  const locals: Buffer[] = [];
  const centrals: Buffer[] = [];
  let offset = 0;
  for (const [entryName, entryContent] of entries) {
    const name = Buffer.from(entryName);
    const content = Buffer.from(entryContent);
    const crc = NodeZlib.crc32(content);
    const local = Buffer.alloc(30);
    local.writeUInt32LE(0x04034b50);
    local.writeUInt16LE(20, 4);
    local.writeUInt32LE(crc, 14);
    local.writeUInt32LE(content.length, 18);
    local.writeUInt32LE(content.length, 22);
    local.writeUInt16LE(name.length, 26);
    const central = Buffer.alloc(46);
    central.writeUInt32LE(0x02014b50);
    central.writeUInt16LE(0x0314, 4);
    central.writeUInt16LE(20, 6);
    central.writeUInt32LE(crc, 16);
    central.writeUInt32LE(content.length, 20);
    central.writeUInt32LE(content.length, 24);
    central.writeUInt16LE(name.length, 28);
    central.writeUInt32LE((0o100644 << 16) >>> 0, 38);
    central.writeUInt32LE(offset, 42);
    locals.push(local, name, content);
    centrals.push(central, name);
    offset += local.length + name.length + content.length;
  }
  const central = Buffer.concat(centrals);
  const end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50);
  end.writeUInt16LE(entries.length, 8);
  end.writeUInt16LE(entries.length, 10);
  end.writeUInt32LE(central.length, 12);
  end.writeUInt32LE(offset, 16);
  return Buffer.concat([...locals, central, end]);
}

async function tree(directory: string): Promise<string[]> {
  const results: string[] = [];
  for (const name of (await NodeFSP.readdir(directory)).sort()) {
    const file = NodePath.join(directory, name);
    const stat = await NodeFSP.lstat(file);
    results.push(
      stat.isDirectory()
        ? `${file}/ ${(stat.mode & 0o777).toString(8)}`
        : stat.isSymbolicLink()
          ? `${file} -> ${await NodeFSP.readlink(file)}`
          : `${file}:${sha(await NodeFSP.readFile(file))}`,
    );
    if (stat.isDirectory()) results.push(...(await tree(file)));
  }
  return results;
}

async function processEntry(
  proc: string,
  pid: number,
  executable: string,
  birth = String(pid * 10),
  group = controlGroup,
) {
  const directory = NodePath.join(proc, String(pid));
  await NodeFSP.mkdir(directory, { recursive: true });
  await NodeFSP.writeFile(
    NodePath.join(directory, "stat"),
    `${pid} (t3) S ${Array(18).fill("0").join(" ")} ${birth} 0`,
  );
  await NodeFSP.writeFile(
    NodePath.join(directory, "status"),
    `Name:\tt3\nUid:\t${account.uid}\t${account.uid}\t${account.uid}\t${account.uid}\n`,
  );
  await NodeFSP.writeFile(NodePath.join(directory, "cgroup"), `0::${group}\n`);
  await NodeFSP.rm(NodePath.join(directory, "exe"), { force: true });
  await NodeFSP.symlink(executable, NodePath.join(directory, "exe"));
}

/** What the worker reports for a unit that is not running. */
interface StoppedReport {
  readonly activeState: string;
  readonly subState: string;
  readonly mainPid?: number;
  readonly controlGroup?: string;
}

interface FixtureOptions {
  readonly ownershipBusy?: boolean;
  readonly stateAppearsDuringOwnership?: boolean;
  readonly stopResult?: SystemControlResult["status"];
  /** The complete stop's report; inactive/dead otherwise. */
  readonly stoppedAs?: StoppedReport;
  readonly installResult?: SystemControlResult["status"];
  /** The complete launcher installation's report; the stop's state otherwise. */
  readonly installedAs?: StoppedReport;
  readonly childReceiptAfterStart?: boolean;
  /** The unit runs the historical `start ... --no-browser HOME` form from WorkingDirectory HOME. */
  readonly startForm?: boolean;
}

async function fixture(
  body: (f: Awaited<ReturnType<typeof createFixture>>) => Promise<void>,
  options: FixtureOptions = {},
) {
  const f = await createFixture(options);
  try {
    await body(f);
  } finally {
    await NodeFSP.rm(f.root, { recursive: true, force: true });
    await expect(NodeFSP.lstat(f.root)).rejects.toMatchObject({ code: "ENOENT" });
  }
}

/** Owns the allocated scratch from mkdtemp, so a failed setup removes it and rethrows. */
async function createFixture(options: FixtureOptions) {
  const allocated = await NodeFSP.mkdtemp(
    NodePath.join(NodeOS.tmpdir(), "jones-system-adoption-test-"),
  );
  try {
    return await populateFixture(await NodeFSP.realpath(allocated), options);
  } catch (failure) {
    try {
      await NodeFSP.rm(allocated, { recursive: true, force: true });
    } catch (cleanup) {
      throw new AggregateError([failure, cleanup], "Fixture setup and cleanup failed.");
    }
    throw failure;
  }
}

async function populateFixture(root: string, options: FixtureOptions) {
  const home = NodePath.join(root, "home");
  const base = NodePath.join(home, ".t3");
  const runtime = NodePath.join(base, "runtime");
  const userdata = NodePath.join(base, "userdata");
  await NodeFSP.mkdir(NodePath.join(runtime, "versions"), { recursive: true });
  await NodeFSP.chmod(NodePath.join(runtime, "versions"), 0o700);
  await NodeFSP.chmod(runtime, 0o775);
  await NodeFSP.mkdir(userdata);
  await NodeFSP.writeFile(NodePath.join(userdata, "environment-id"), "fixture-environment");
  await NodeFSP.writeFile(NodePath.join(userdata, "statev2.sqlite"), "unchanged database bytes");
  await NodeFSP.writeFile(
    NodePath.join(userdata, "server-runtime.json"),
    JSON.stringify({ pid: 4242, port: 3773 }),
  );
  const retainedDirectory = NodePath.join(root, "Jones-Code-Install", `t3-${oldVersion}-linux-x64`);
  const retained = NodePath.join(retainedDirectory, "t3");
  await NodeFSP.mkdir(retainedDirectory, { recursive: true });
  await NodeFSP.writeFile(retained, runtimeExecutable(oldVersion), { mode: 0o755 });
  await NodeFSP.chmod(retained, 0o755);
  for (const name of ["client", "node_modules", "resource-monitor"])
    await NodeFSP.mkdir(NodePath.join(retainedDirectory, name));

  const systemd = NodePath.join(root, "systemd");
  const dropins = NodePath.join(systemd, `${unit}.d`);
  await NodeFSP.mkdir(dropins, { recursive: true });
  const fragment = NodePath.join(systemd, unit);
  await NodeFSP.writeFile(
    fragment,
    `[Service]\nUser=service\nNoNewPrivileges=yes\nKillMode=control-group\n${options.startForm ? `WorkingDirectory=${home}\n` : ""}`,
  );
  const retainedCommand = options.startForm
    ? `${retained} start --mode web --host 127.0.0.1 --port 3773 --base-dir ${base} --no-browser ${home}`
    : `${retained} serve --port 3773 --base-dir ${base}`;
  const conf = NodePath.join(dropins, "jones-code.conf");
  await NodeFSP.writeFile(conf, `[Service]\nExecStart=\nExecStart=${retainedCommand}\n`);

  const proc = NodePath.join(root, "proc");
  await processEntry(proc, 4242, retained, "777");

  const artifacts = new Map<
    number,
    { name: string; id: number; digest: string; bytes: number; source: string }
  >();
  const directories: string[] = [];
  for (const [index, version] of [oldVersion, newVersion].entries()) {
    const dir = NodePath.join(root, `artifact-${index}`);
    await NodeFSP.mkdir(dir);
    directories.push(dir);
    const archive = tarPayload(version);
    const source = index === 0 ? privateSetupSource : "b".repeat(40);
    const m = {
      schema: 1,
      repository: "Jones-Systems/Jones-Code",
      source,
      tree: index === 0 ? privateSetupTree : "c".repeat(40),
      version,
      platform: "linux",
      architecture: "x64",
      workflow: ".github/workflows/artifact-cli-linux.yml",
      event: "push",
      ref: "refs/heads/main",
      runId: String(100 + index),
      runAttempt: "1",
      artifact: `t3-${version}-linux-x64.tar.gz`,
      sha256: sha(archive),
    };
    const zip = zipEnvelope([
      ["ARTIFACT.json", JSON.stringify(m)],
      ["SOURCE_COMMIT", `${source}\n`],
      [m.artifact, archive],
      ["SHA256SUMS", `${m.sha256}  ${m.artifact}\n`],
    ]);
    await NodeFSP.writeFile(NodePath.join(dir, "ARTIFACT.json"), JSON.stringify(m));
    await NodeFSP.writeFile(NodePath.join(dir, m.artifact), archive);
    await NodeFSP.writeFile(NodePath.join(dir, "github-artifact.zip"), zip);
    artifacts.set(100 + index, {
      name: `jones-code-cli-linux-x64-${100 + index}-1--${version}`,
      id: 200 + index,
      digest: sha(zip),
      bytes: zip.length,
      source,
    });
  }

  const launcherEntry = NodePath.join(runtime, "versions", newVersion, "t3");
  const activeEntry = NodePath.join(runtime, "versions", oldVersion, "t3");
  const policyText = renderSystemServicePolicy({
    unit,
    user: account.username,
    uid: account.uid,
    gid: account.gid,
    base,
    slice: "hostdev.slice",
    retainedEntry: retained,
  });
  const ownedText = renderOwnedSystemDropin({
    base,
    launcherVersion: newVersion,
    serve: options.startForm ? { port: 3773, host: "127.0.0.1", mode: "web" } : { port: 3773 },
  });
  const events: string[] = [];
  const unitState: {
    active: boolean;
    stopped: StoppedReport;
    mainPid: number;
    owned: string;
    exec: string;
  } = {
    active: true,
    stopped: { activeState: "inactive", subState: "dead" },
    mainPid: 4242,
    owned: "absent",
    exec: "retained",
  };
  let showExec = retainedCommand;
  let showUser = account.username;
  let showNnp = "yes";
  let showWorkingDirectory = home;
  const digest = async () => {
    const records = [
      systemUnitRecord("fragment", await NodeFSP.lstat(fragment), await NodeFSP.readFile(fragment)),
    ];
    for (const name of (await NodeFSP.readdir(dropins)).sort()) {
      if (name === OWNED_SYSTEM_DROPIN) continue;
      const path = NodePath.join(dropins, name);
      records.push(
        systemUnitRecord(`dropin ${name}`, await NodeFSP.lstat(path), await NodeFSP.readFile(path)),
      );
    }
    return systemUnitDigest(records);
  };
  const inspection = async (): Promise<SystemControlResult> => ({
    status: "complete",
    inspection: {
      policySha256: sha(policyText),
      unitDigest: await digest(),
      owned: unitState.owned,
      files: [],
      activeState: unitState.active ? "active" : unitState.stopped.activeState,
      subState: unitState.active ? "running" : unitState.stopped.subState,
      mainPid: unitState.active ? unitState.mainPid : (unitState.stopped.mainPid ?? 0),
      ...(unitState.active
        ? {
            mainBirth: unitState.mainPid === 4242 ? "777" : String(unitState.mainPid * 10),
            controlGroup,
          }
        : unitState.stopped.controlGroup === undefined
          ? {}
          : { controlGroup: unitState.stopped.controlGroup }),
      killMode: "control-group",
      exec: unitState.exec,
      serve: { port: 3773 },
    },
  });
  const control = async (request: SystemControlRequest): Promise<SystemControlResult> => {
    events.push(`control ${request.operation}`);
    switch (request.operation) {
      case "inspect":
        return inspection();
      case "stop":
        if (options.stopResult === "unknown") return { status: "unknown", reason: "stop" };
        if (options.stopResult === "rejected")
          return { status: "rejected", reason: "main-process" };
        unitState.active = false;
        unitState.stopped = options.stoppedAs ?? unitState.stopped;
        await NodeFSP.rm(NodePath.join(proc, "4242"), { recursive: true });
        return inspection();
      case "install-launcher":
        if (options.installResult === "unknown")
          return { status: "unknown", reason: "daemon-reload" };
        await NodeFSP.writeFile(NodePath.join(dropins, OWNED_SYSTEM_DROPIN), ownedText);
        unitState.owned = sha(ownedText);
        unitState.exec = `launcher ${newVersion}`;
        unitState.stopped = options.installedAs ?? unitState.stopped;
        return inspection();
      case "start":
        unitState.active = true;
        unitState.mainPid = 5000;
        await processEntry(proc, 5000, launcherEntry);
        await processEntry(proc, 5001, activeEntry);
        await NodeFSP.writeFile(
          NodePath.join(userdata, "server-runtime.json"),
          JSON.stringify({
            version: 1,
            pid: 5001,
            port: 3773,
            origin: "http://127.0.0.1:3773",
            startedAt: "now",
            serviceManaged: true,
          }),
        );
        await NodeFSP.writeFile(
          NodePath.join(runtime, "jones-launcher-capability.json"),
          JSON.stringify({
            schema: 1,
            baseDir: base,
            launcherVersion: newVersion,
            launcherPid: 5000,
            launcherProtocol: 4,
            qualifiedUpdatesProtocol: 1,
            startupGateProtocol: 1,
            childPid: 5001,
            childVersion: oldVersion,
          }),
          { mode: 0o600 },
        );
        if (options.childReceiptAfterStart)
          await NodeFSP.writeFile(NodePath.join(runtime, "jones-update-capability.json"), "{}", {
            mode: 0o600,
          });
        return inspection();
      case "archive-launcher":
        return { status: "rejected", reason: "unexpected" };
    }
  };
  const workerBytes = await NodeFSP.readFile(
    NodePath.join(import.meta.dirname, "systemServiceControl.pl"),
  );
  const host: SystemAdoptionHost = {
    platform: "linux",
    architecture: "x64",
    uid: account.uid,
    gid: account.gid,
    user: account.username,
    procRoot: proc,
    systemdDirectory: systemd,
    readbackAttempts: 1,
    launcherProcessGuard: { uid: account.uid, isOwnedLive: async () => true },
    control,
    rootFile: async (path) =>
      path === SYSTEM_SERVICE_CONTROL_PATH
        ? { uid: 0, mode: 0o100755, bytes: workerBytes }
        : path === SYSTEM_SERVICE_POLICY_PATH
          ? { uid: 0, mode: 0o100644, bytes: Buffer.from(policyText) }
          : "absent",
    descriptor: async () => ({ serverVersion: oldVersion, environmentId: "fixture-environment" }),
    acquireStateOwnership: async () => {
      events.push("state-ownership-acquired");
      if (options.stateAppearsDuringOwnership)
        await NodeFSP.writeFile(NodePath.join(runtime, "service-state.json"), "foreign\n");
      if (options.ownershipBusy) throw new Error("state-dir-owned");
      return () => {
        events.push("state-ownership-released");
      };
    },
    run: async (command) => {
      const args = command.args;
      if (command.command === "systemctl")
        return [
          `FragmentPath=${fragment}`,
          `DropInPaths=${(await NodeFSP.readdir(dropins))
            .filter((name) => name !== OWNED_SYSTEM_DROPIN)
            .sort()
            .map((name) => NodePath.join(dropins, name))
            .join(" ")}`,
          `User=${showUser}`,
          `NoNewPrivileges=${showNnp}`,
          "KillMode=control-group",
          "Slice=hostdev.slice",
          `ControlGroup=${controlGroup}`,
          "ActiveState=active",
          "MainPID=4242",
          `ExecStart={ path=${retained} ; argv[]=${showExec} ; ignore_errors=no ; start_time=[n/a] }`,
          `WorkingDirectory=${showWorkingDirectory}`,
          "",
        ].join("\n");
      if (command.command === "gh") {
        const endpoint = args.at(-1)!;
        if (endpoint.includes("/git/commits/")) {
          const source = endpoint.split("/").at(-1);
          return JSON.stringify({
            sha: source,
            tree: { sha: source === privateSetupSource ? privateSetupTree : "c".repeat(40) },
          });
        }
        if (endpoint.includes("actions/workflows/ci.yml/"))
          return JSON.stringify({
            workflow_runs: [
              {
                path: ".github/workflows/ci.yml",
                head_sha: /head_sha=([a-f0-9]+)/.exec(endpoint)?.[1],
                head_branch: "main",
                event: "push",
                status: "completed",
                conclusion: "success",
                repository: { full_name: "Jones-Systems/Jones-Code" },
                head_repository: { full_name: "Jones-Systems/Jones-Code" },
              },
            ],
          });
        const runId = Number(/\/runs\/(\d+)/.exec(endpoint)?.[1]);
        const artifact = artifacts.get(runId)!;
        if (endpoint.includes("/artifacts?"))
          return JSON.stringify({
            total_count: 1,
            artifacts: [
              {
                id: artifact.id,
                name: artifact.name,
                digest: `sha256:${artifact.digest}`,
                expired: false,
                size_in_bytes: artifact.bytes,
                workflow_run: {
                  id: runId,
                  head_sha: artifact.source,
                  head_branch: "main",
                  repository_id: 123,
                  head_repository_id: 123,
                },
              },
            ],
          });
        return JSON.stringify({
          id: runId,
          run_attempt: 1,
          path: ".github/workflows/artifact-cli-linux.yml",
          head_sha: artifact.source,
          head_branch: "main",
          event: "push",
          status: "completed",
          conclusion: "success",
          repository: { id: 123, full_name: "Jones-Systems/Jones-Code" },
          head_repository: { full_name: "Jones-Systems/Jones-Code" },
        });
      }
      // Hosts may lack an external unzip; embedded metadata is read natively.
      if (command.command === "unzip") throw new Error("External unzip must never be invoked.");
      const version = /^# version: (\S+)$/m.exec(
        await NodeFSP.readFile(command.command, "utf8"),
      )?.[1];
      if (args.includes("__service-preflight")) {
        events.push(`preflight ${version}`);
        return JSON.stringify({
          status: "ready",
          version,
          launcherProtocol: SERVICE_LAUNCHER_PROTOCOL,
          startupGateProtocol: 1,
          migrationPlan: { pendingUpstream: [], pendingJones: [130] },
        });
      }
      return `t3 v${version}\n`;
    },
  };
  const input: SystemAdoptInput = {
    baseDir: base,
    systemUnit: unit,
    port: 3773,
    retainedExecutable: retained,
    activeArtifactDir: directories[0]!,
    activeSourceCommit: privateSetupSource,
    launcherArtifactDir: directories[1]!,
    launcherSourceCommit: "b".repeat(40),
    acceptUnattestedChildCapability: true,
  };
  return {
    root,
    home,
    base,
    runtime,
    userdata,
    retained,
    retainedDirectory,
    systemd,
    dropins,
    fragment,
    conf,
    proc,
    host,
    input,
    events,
    ownedText,
    receipt: async () =>
      JSON.parse(
        await NodeFSP.readFile(NodePath.join(runtime, SYSTEM_ADOPTION_RECEIPT), "utf8"),
      ) as Record<string, unknown>,
    retainedCommand,
    setShow: (change: {
      exec?: string;
      user?: string;
      nnp?: string;
      workingDirectory?: string;
    }) => {
      showExec = change.exec ?? showExec;
      showUser = change.user ?? showUser;
      showNnp = change.nnp ?? showNnp;
      showWorkingDirectory = change.workingDirectory ?? showWorkingDirectory;
    },
  };
}

it("plans read-only: no lock, receipt, permission change, cache or root call", () =>
  fixture(async (f) => {
    const before = await tree(f.root);
    const planned = await planSystemAdoption(f.input, f.host);
    expect(await tree(f.root)).toEqual(before);
    expect(f.events.filter((event) => event.startsWith("control"))).toEqual([]);
    expect(planned.plan).toMatchObject({
      systemUnit: unit,
      port: 3773,
      active: { version: oldVersion, source: privateSetupSource },
      launcher: { version: newVersion },
      retained: { executable: f.retained, processId: 4242, processBirth: "777", controlGroup },
      runtimeMode: { from: "0775", to: "0755" },
      attestation: "launcher-only",
      ownedDropin: { path: NodePath.join(f.dropins, OWNED_SYSTEM_DROPIN), text: f.ownedText },
    });
    expect(planned.planSha256).toMatch(/^[a-f0-9]{64}$/);
    expect(planned.plan.effects.some((effect) => effect.includes("--no-browser"))).toBe(false);
    expect((await planSystemAdoption(f.input, f.host)).planSha256).toBe(planned.planSha256);
    expect(planned.manifest.files.map((file) => file.path)).toContain(SYSTEM_SERVICE_POLICY_PATH);
  }));

it("adopts in order and records applied launcher-only evidence without touching preserved files", () =>
  fixture(async (f) => {
    const { planSha256 } = await planSystemAdoption(f.input, f.host);
    const preserved = await tree(f.retainedDirectory);
    const unitFiles = [await NodeFSP.readFile(f.fragment), await NodeFSP.readFile(f.conf)];
    const database = await NodeFSP.readFile(NodePath.join(f.userdata, "statev2.sqlite"));
    const result = await adoptSystemHost({ ...f.input, approvedPlanSha256: planSha256 }, f.host);
    expect(result).toMatchObject({
      state: "adopted",
      launcherProcessId: 5000,
      childProcessId: 5001,
    });
    expect(f.events.filter((event) => !event.startsWith("preflight"))).toEqual([
      "control inspect",
      "control inspect",
      "control stop",
      "state-ownership-acquired",
      "control install-launcher",
      "state-ownership-released",
      "control start",
      "control inspect",
    ]);
    expect(await f.receipt()).toMatchObject({
      status: "applied",
      phase: "applied",
      runtimeMode: verifiedRuntimeMode,
      planSha256,
      attestation: "launcher-only",
      attestationBasis: "pre-publisher-child",
      childCapability: "unattested",
      launcherProcessId: 5000,
      childProcessId: 5001,
    });
    expect(JSON.stringify(await f.receipt())).not.toContain('"install":true');
    expect(
      JSON.parse(await NodeFSP.readFile(NodePath.join(f.runtime, "service-state.json"), "utf8")),
    ).toEqual({
      protocol: SERVICE_LAUNCHER_PROTOCOL,
      activeVersion: oldVersion,
    });
    expect((await NodeFSP.lstat(f.runtime)).mode & 0o777).toBe(0o755);
    expect((await NodeFSP.lstat(NodePath.join(f.runtime, "versions"))).mode & 0o777).toBe(0o700);
    for (const version of [oldVersion, newVersion])
      await expect(
        readQualifiedRuntimeReceipt(f.base, version, { platform: "linux", architecture: "x64" }),
      ).resolves.toMatchObject({ version });
    expect(await tree(f.retainedDirectory)).toEqual(preserved);
    expect([await NodeFSP.readFile(f.fragment), await NodeFSP.readFile(f.conf)]).toEqual(unitFiles);
    expect(await NodeFSP.readFile(NodePath.join(f.userdata, "statev2.sqlite"))).toEqual(database);
  }));

it("adopts the historical start form as headless serve from its unchanged WorkingDirectory", () =>
  fixture(
    async (f) => {
      const planned = await planSystemAdoption(f.input, f.host);
      expect(planned.plan.ownedDropin.text).toBe(f.ownedText);
      expect(f.ownedText).toContain("Environment=T3CODE_MODE=web\n");
      expect(f.ownedText).not.toMatch(/WorkingDirectory|no-browser/);
      expect(planned.plan.effects.filter((effect) => effect.includes("--no-browser"))).toEqual([
        expect.stringContaining(
          `\`start --no-browser ${f.home}\` with the launcher, which runs the retained version as \`serve\` from the unchanged WorkingDirectory ${f.home}: headless, with no automatic project bootstrap`,
        ),
      ]);
      const unitFiles = [await NodeFSP.readFile(f.fragment), await NodeFSP.readFile(f.conf)];
      const databasePath = NodePath.join(f.userdata, "statev2.sqlite");
      const database = await NodeFSP.readFile(databasePath);
      const result = await adoptSystemHost(
        { ...f.input, approvedPlanSha256: planned.planSha256 },
        f.host,
      );
      expect(result).toMatchObject({
        state: "adopted",
        launcherProcessId: 5000,
        childProcessId: 5001,
      });
      expect(await f.receipt()).toMatchObject({ status: "applied", phase: "applied" });
      const unitFilesAfter = [await NodeFSP.readFile(f.fragment), await NodeFSP.readFile(f.conf)];
      expect(unitFilesAfter).toEqual(unitFiles);
      expect(await NodeFSP.readFile(databasePath)).toEqual(database);
    },
    { startForm: true },
  ));

it("refuses the start form unless it names the unit's exact WorkingDirectory with --no-browser", () =>
  fixture(
    async (f) => {
      const before = await tree(f.root);
      const flags = `--mode web --host 127.0.0.1 --port 3773 --base-dir ${f.base}`;
      for (const change of [
        { workingDirectory: "" },
        { workingDirectory: f.base },
        { workingDirectory: `!${f.home}` },
        { exec: `${f.retained} start ${flags} ${f.home}` },
        { exec: `${f.retained} start ${flags} --no-browser` },
        { exec: `${f.retained} start ${flags} --no-browser ${f.home} ${f.base}` },
        { exec: `${f.retained} start ${flags} --no-browser ${f.home}/.` },
        { exec: `${f.retained} start ${flags} --no-browser true ${f.home}` },
        { exec: `${f.retained} start ${flags} --no-browser --no-browser ${f.home}` },
        { exec: `${f.retained} start ${flags} --token x --no-browser ${f.home}` },
        { exec: `${f.retained} start --port 3773 --base-dir ${f.base} --no-browser ${f.home}` },
      ]) {
        f.setShow({ exec: f.retainedCommand, workingDirectory: f.home, ...change });
        await expect(planSystemAdoption(f.input, f.host), JSON.stringify(change)).rejects.toThrow(
          /closed serve or start grammar/,
        );
      }
      f.setShow({ exec: f.retainedCommand, workingDirectory: f.home });
      await expect(planSystemAdoption(f.input, f.host)).resolves.toBeDefined();
      expect(await tree(f.root)).toEqual(before);
    },
    { startForm: true },
  ));

it("applies nothing without the exact reviewed plan or the installed root bootstrap", async () => {
  await fixture(async (f) => {
    const before = await tree(f.root);
    for (const approvedPlanSha256 of [undefined, "0".repeat(64)])
      await expect(
        adoptSystemHost(
          { ...f.input, ...(approvedPlanSha256 === undefined ? {} : { approvedPlanSha256 }) },
          f.host,
        ),
      ).rejects.toThrow();
    expect(f.events.filter((event) => event.startsWith("control"))).toEqual([]);
    expect(
      (await tree(f.root)).filter((entry) => !entry.includes("qualified-runtime-lock")),
    ).toEqual(before);
  });
  await fixture(async (f) => {
    const rootFile = f.host.rootFile;
    const host = {
      ...f.host,
      rootFile: async (path: string) =>
        path === SYSTEM_SERVICE_CONTROL_PATH ? ("absent" as const) : rootFile(path),
    };
    const { planSha256, bootstrap } = await planSystemAdoption(f.input, host);
    expect(bootstrap.worker).toBe("absent");
    await expect(
      adoptSystemHost({ ...f.input, approvedPlanSha256: planSha256 }, host),
    ).rejects.toThrow(/bootstrap/);
    expect(f.events).toEqual([]);
  });
});

it.each([
  [
    "existing native state",
    async (f: Fixture) => NodeFSP.writeFile(NodePath.join(f.runtime, "service-state.json"), "{}"),
  ],
  [
    "user adoption receipt",
    async (f: Fixture) => NodeFSP.writeFile(NodePath.join(f.runtime, ADOPTION_RECEIPT), "{}"),
  ],
  [
    "system adoption receipt",
    async (f: Fixture) =>
      NodeFSP.writeFile(NodePath.join(f.runtime, SYSTEM_ADOPTION_RECEIPT), "{}"),
  ],
  ["unsafe runtime permissions", async (f: Fixture) => NodeFSP.chmod(f.runtime, 0o777)],
  [
    "different runtime port",
    async (f: Fixture) =>
      NodeFSP.writeFile(
        NodePath.join(f.userdata, "server-runtime.json"),
        JSON.stringify({ pid: 4242, port: 3774 }),
      ),
  ],
  [
    "different main process",
    async (f: Fixture) =>
      NodeFSP.writeFile(
        NodePath.join(f.userdata, "server-runtime.json"),
        JSON.stringify({ pid: 4243, port: 3773 }),
      ),
  ],
  [
    "different executable",
    async (f: Fixture) => processEntry(f.proc, 4242, NodePath.join(f.root, "other", "t3"), "777"),
  ],
  [
    "foreign cgroup",
    async (f: Fixture) =>
      processEntry(f.proc, 4242, f.retained, "777", "/user.slice/other.service"),
  ],
  [
    "late drop-in precedence",
    async (f: Fixture) => NodeFSP.writeFile(NodePath.join(f.dropins, "zzzzz.conf"), "[Service]\n"),
  ],
  [
    "owned drop-in already present",
    async (f: Fixture) => NodeFSP.writeFile(NodePath.join(f.dropins, OWNED_SYSTEM_DROPIN), "x"),
  ],
  ["unexpected service user", async (f: Fixture) => f.setShow({ user: "root" })],
  ["NoNewPrivileges disabled", async (f: Fixture) => f.setShow({ nnp: "no" })],
  [
    "unknown serve flag",
    async (f: Fixture) => f.setShow({ exec: `${f.retained} serve --port 3773 --token x` }),
  ],
  [
    "unexpected retained payload entry",
    async (f: Fixture) =>
      NodeFSP.writeFile(NodePath.join(f.retainedDirectory, "ARTIFACT.json"), "{}"),
  ],
  [
    "occupied retained-version cache",
    async (f: Fixture) => NodeFSP.mkdir(NodePath.join(f.runtime, "versions", oldVersion)),
  ],
  [
    "missing unattested acceptance",
    async (f: Fixture) => Object.assign(f.input, { acceptUnattestedChildCapability: false }),
  ],
] as const)("refuses %s while planning and writes nothing", (_name, change) =>
  fixture(async (f) => {
    await change(f);
    const before = await tree(f.root);
    await expect(planSystemAdoption(f.input, f.host)).rejects.toThrow();
    expect(await tree(f.root)).toEqual(before);
  }),
);

it("refuses a drop-in preimage the service user cannot read", () =>
  fixture(async (f) => {
    await NodeFSP.chmod(f.conf, 0o000);
    await expect(planSystemAdoption(f.input, f.host)).rejects.toThrow(/unreadable/);
    await NodeFSP.chmod(f.conf, 0o644);
  }));

type Fixture = Awaited<ReturnType<typeof createFixture>>;

async function applyApproved(f: Fixture) {
  const { planSha256 } = await planSystemAdoption(f.input, f.host);
  return adoptSystemHost({ ...f.input, approvedPlanSha256: planSha256 }, f.host);
}

it("keeps the unit stopped when state ownership is busy and never starts it automatically", () =>
  fixture(
    async (f) => {
      await expect(applyApproved(f)).rejects.toThrow(/nothing restarts automatically/);
      expect(f.events.filter((event) => event.startsWith("control")).slice(-1)).toEqual([
        "control stop",
      ]);
      expect(await f.receipt()).toMatchObject({
        status: "partial",
        phase: "stopped",
        runtimeMode: verifiedRuntimeMode,
      });
      await expect(
        NodeFSP.lstat(NodePath.join(f.runtime, "service-state.json")),
      ).rejects.toMatchObject({ code: "ENOENT" });
    },
    { ownershipBusy: true },
  ));

it("records an uncertain stop as unknown and attempts nothing after it", () =>
  fixture(
    async (f) => {
      await expect(applyApproved(f)).rejects.toThrow(/unknown stop/);
      expect(f.events.at(-1)).toBe("control stop");
      expect(await f.receipt()).toMatchObject({
        status: "unknown",
        phase: "stop-requested",
        runtimeMode: verifiedRuntimeMode,
      });
      for (const path of [
        NodePath.join(f.runtime, "service-state.json"),
        NodePath.join(f.dropins, OWNED_SYSTEM_DROPIN),
      ])
        await expect(NodeFSP.lstat(path), path).rejects.toMatchObject({ code: "ENOENT" });
      // The unknown record blocks a new trial and stays exactly as written.
      const receiptPath = NodePath.join(f.runtime, SYSTEM_ADOPTION_RECEIPT);
      const recorded = await NodeFSP.readFile(receiptPath, "utf8");
      await expect(planSystemAdoption(f.input, f.host)).rejects.toThrow(
        new RegExp(`Existing ${SYSTEM_ADOPTION_RECEIPT}`),
      );
      expect(await NodeFSP.readFile(receiptPath, "utf8")).toBe(recorded);
    },
    { stopResult: "unknown" },
  ));

it("adopts a unit the retained server left failed/failed on stop, as the worker reported it", () =>
  fixture(
    async (f) => {
      const unitFiles = [await NodeFSP.readFile(f.fragment), await NodeFSP.readFile(f.conf)];
      await expect(applyApproved(f)).resolves.toMatchObject({
        state: "adopted",
        launcherProcessId: 5000,
        childProcessId: 5001,
      });
      // State ownership spans installation and is released before the server starts.
      expect(f.events.filter((event) => !event.startsWith("preflight"))).toEqual([
        "control inspect",
        "control inspect",
        "control stop",
        "state-ownership-acquired",
        "control install-launcher",
        "state-ownership-released",
        "control start",
        "control inspect",
      ]);
      expect(await f.receipt()).toMatchObject({ status: "applied", phase: "applied" });
      expect([await NodeFSP.readFile(f.fragment), await NodeFSP.readFile(f.conf)]).toEqual(
        unitFiles,
      );
    },
    { stoppedAs: { activeState: "failed", subState: "failed" } },
  ));

it.each([
  ["inactive/failed", { activeState: "inactive", subState: "failed" }],
  ["failed/dead", { activeState: "failed", subState: "dead" }],
  ["a stopping unit", { activeState: "deactivating", subState: "stop-sigterm" }],
  ["a restarting unit", { activeState: "activating", subState: "auto-restart" }],
  ["a main process", { activeState: "inactive", subState: "dead", mainPid: 4243 }],
  ["a failed main process", { activeState: "failed", subState: "failed", mainPid: 4243 }],
  ["a current cgroup", { activeState: "failed", subState: "failed", controlGroup }],
] as const)(
  "records a stop that reports %s as unknown and attempts nothing after it",
  (_, report) =>
    fixture(
      async (f) => {
        await expect(applyApproved(f)).rejects.toThrow(/stop readback differs/);
        expect(f.events.at(-1)).toBe("control stop");
        expect(await f.receipt()).toMatchObject({ status: "unknown", phase: "stop-requested" });
        await expect(
          NodeFSP.lstat(NodePath.join(f.runtime, "service-state.json")),
        ).rejects.toMatchObject({ code: "ENOENT" });
      },
      { stoppedAs: report },
    ),
);

it.each([
  ["a starting unit", { activeState: "activating", subState: "start" }],
  ["failed/dead", { activeState: "failed", subState: "dead" }],
  ["a main process", { activeState: "failed", subState: "failed", mainPid: 4243 }],
  ["a current cgroup", { activeState: "failed", subState: "failed", controlGroup }],
] as const)("does not start after a launcher installation that reports %s", (_, report) =>
  fixture(
    async (f) => {
      await expect(applyApproved(f)).rejects.toThrow(/launcher installation readback differs/);
      expect(f.events.filter((event) => event.startsWith("control")).at(-1)).toBe(
        "control install-launcher",
      );
      expect(f.events).not.toContain("control start");
      expect(f.events.at(-1)).toBe("state-ownership-released");
      expect(await f.receipt()).toMatchObject({
        status: "unknown",
        phase: "launcher-install-requested",
      });
    },
    { stoppedAs: { activeState: "failed", subState: "failed" }, installedAs: report },
  ),
);

it("records a refused stop as partial with the service untouched", () =>
  fixture(
    async (f) => {
      await expect(applyApproved(f)).rejects.toThrow(/rejected main-process/);
      expect(await f.receipt()).toMatchObject({ status: "partial", phase: "stop-requested" });
    },
    { stopResult: "rejected" },
  ));

it("never overwrites native state that appears before its exclusive creation", () =>
  fixture(
    async (f) => {
      await expect(applyApproved(f)).rejects.toThrow(/preserved/);
      expect(await NodeFSP.readFile(NodePath.join(f.runtime, "service-state.json"), "utf8")).toBe(
        "foreign\n",
      );
      expect(await f.receipt()).toMatchObject({ status: "partial", phase: "state-requested" });
      expect(f.events).not.toContain("control install-launcher");
    },
    { stateAppearsDuringOwnership: true },
  ));

it("records an uncertain launcher installation as unknown and does not start", () =>
  fixture(
    async (f) => {
      await expect(applyApproved(f)).rejects.toThrow(/daemon-reload/);
      expect(await f.receipt()).toMatchObject({
        status: "unknown",
        phase: "launcher-install-requested",
      });
      expect(f.events).not.toContain("control start");
    },
    { installResult: "unknown" },
  ));

it("compares the full retained payload with the authenticated archive before stopping", () =>
  fixture(async (f) => {
    const { planSha256 } = await planSystemAdoption(f.input, f.host);
    await NodeFSP.writeFile(NodePath.join(f.retainedDirectory, "client", "extra.js"), "x");
    await expect(
      adoptSystemHost({ ...f.input, approvedPlanSha256: planSha256 }, f.host),
    ).rejects.toThrow(/differs from the approved plan/);
    expect(f.events).toEqual([]);
    // A plan made from a retained payload the archive does not contain is
    // refused when the authenticated archive is extracted, before any stop.
    const approved = await planSystemAdoption(f.input, f.host);
    await expect(
      adoptSystemHost({ ...f.input, approvedPlanSha256: approved.planSha256 }, f.host),
    ).rejects.toThrow(/retained payload differs/);
    expect(f.events).not.toContain("control stop");
    // A partial record keeps the known setup effects before its phase.
    expect(await f.receipt()).toMatchObject({
      status: "partial",
      phase: "enrolling",
      runtimeMode: verifiedRuntimeMode,
    });
    expect((await NodeFSP.lstat(f.runtime)).mode & 0o777).toBe(0o755);
  }));

it("records an unverified runtime correction as unknown and attempts nothing after it", () =>
  fixture(async (f) => {
    const { planSha256 } = await planSystemAdoption(f.input, f.host);
    // The permission call reports success but the directory never changes.
    vi.mocked(NodeFSP.chmod).mockImplementationOnce(async () => undefined);
    await expect(
      adoptSystemHost({ ...f.input, approvedPlanSha256: planSha256 }, f.host),
    ).rejects.toThrow(/permission readback differs/);
    const receipt = await f.receipt();
    expect(receipt).toMatchObject({
      status: "unknown",
      phase: "runtime-mode-requested",
      runtimeMode: { from: "0775", to: "0755" },
    });
    // Unsettled: neither applied nor known unattempted.
    expect(receipt.runtimeMode).not.toHaveProperty("applied");
    expect(f.events.filter((event) => event.startsWith("control"))).toEqual(["control inspect"]);
    expect(f.events.some((event) => event.startsWith("preflight"))).toBe(false);
    expect(await NodeFSP.readdir(NodePath.join(f.runtime, "versions"))).toEqual([]);
  }));

it("leaves an unexpected child capability receipt unknown instead of claiming Install", () =>
  fixture(
    async (f) => {
      await expect(applyApproved(f)).rejects.toThrow(/did not settle/);
      expect(await f.receipt()).toMatchObject({ status: "unknown", phase: "started" });
    },
    { childReceiptAfterStart: true },
  ));

it("fences user adoption from a home that holds a system adoption record", () =>
  fixture(async (f) => {
    await NodeFSP.chmod(f.runtime, 0o755);
    await NodeFSP.writeFile(NodePath.join(f.runtime, SYSTEM_ADOPTION_RECEIPT), "{}");
    await expect(
      planHostAdoption(
        {
          baseDir: f.base,
          activeArtifactDir: f.input.activeArtifactDir,
          activeSourceCommit: f.input.activeSourceCommit,
          launcherArtifactDir: f.input.launcherArtifactDir,
          launcherSourceCommit: f.input.launcherSourceCommit,
        },
        {
          platform: "linux",
          architecture: "x64",
          home: f.root,
          uid: account.uid,
          environmentPath: "/usr/bin:/bin",
          run: f.host.run,
          readback: async () => ({
            processId: 1,
            serviceManaged: true,
            serverVersion: oldVersion,
            environmentId: "fixture-environment",
          }),
        },
      ),
    ).rejects.toThrow(/system-unit adoption record/);
  }));
