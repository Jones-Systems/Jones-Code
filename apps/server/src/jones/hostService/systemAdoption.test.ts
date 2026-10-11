// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeZlib from "node:zlib";
import { expect, it as vitestIt } from "vite-plus/test";
import { SERVICE_LAUNCHER_PROTOCOL } from "../../cloud/serviceProtocol.ts";
import { readQualifiedRuntimeReceipt } from "../cloud/qualifiedRuntime.ts";
import { ADOPTION_RECEIPT, planHostAdoption, SYSTEM_ADOPTION_RECEIPT } from "./adoption.ts";
import { QualifiedQuiescenceError } from "../cloud/qualifiedQuiescence.ts";
import type { SocketPathMetadata } from "../cloud/qualifiedWriterObserverSocket.ts";
import {
  adoptSystemHost,
  observeSystemReadiness,
  planSystemAdoption,
  proveSystemStoppedWriters,
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
import { QUALIFIED_WRITER_OBSERVER_PATH } from "../cloud/qualifiedWriterObserver.ts";

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

interface FixtureOptions {
  readonly proofFails?: boolean;
  readonly stateAppearsDuringProof?: boolean;
  readonly stopResult?: SystemControlResult["status"];
  readonly installResult?: SystemControlResult["status"];
  readonly childReceiptAfterStart?: boolean;
  readonly writers?: boolean;
  readonly socket?: "installed" | "absent";
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
  }
}

async function createFixture(options: FixtureOptions) {
  const root = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-system-adoption-test-")),
  );
  const base = NodePath.join(root, "home", ".t3");
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
    "[Service]\nUser=service\nNoNewPrivileges=yes\nKillMode=control-group\n",
  );
  const serve = `${retained} serve --port 3773 --base-dir ${base}`;
  const conf = NodePath.join(dropins, "jones-code.conf");
  await NodeFSP.writeFile(conf, `[Service]\nExecStart=\nExecStart=${serve}\n`);

  const proc = NodePath.join(root, "proc");
  await processEntry(proc, 4242, retained, "777");

  const metadata = new Map<string, Record<string, unknown>>();
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
    const zip = Buffer.from(`synthetic zip envelope ${index}`);
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
    await NodeFSP.writeFile(NodePath.join(dir, "ARTIFACT.json"), JSON.stringify(m));
    await NodeFSP.writeFile(NodePath.join(dir, m.artifact), archive);
    await NodeFSP.writeFile(NodePath.join(dir, "github-artifact.zip"), zip);
    metadata.set(dir, m);
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
    serve: { port: 3773 },
  });
  const events: string[] = [];
  const unitState = { active: true, mainPid: 4242, owned: "absent", exec: "retained" };
  let showExec = serve;
  let showUser = account.username;
  let showNnp = "yes";
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
      activeState: unitState.active ? "active" : "inactive",
      subState: unitState.active ? "running" : "dead",
      mainPid: unitState.active ? unitState.mainPid : 0,
      ...(unitState.active
        ? {
            mainBirth: unitState.mainPid === 4242 ? "777" : String(unitState.mainPid * 10),
            controlGroup,
          }
        : {}),
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
        await NodeFSP.rm(NodePath.join(proc, "4242"), { recursive: true });
        return inspection();
      case "install-launcher":
        if (options.installResult === "unknown")
          return { status: "unknown", reason: "daemon-reload" };
        await NodeFSP.writeFile(NodePath.join(dropins, OWNED_SYSTEM_DROPIN), ownedText);
        unitState.owned = sha(ownedText);
        unitState.exec = `launcher ${newVersion}`;
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
  const helperBytes = await NodeFSP.readFile(
    NodePath.join(import.meta.dirname, "..", "cloud", "qualifiedWriterObserver.pl"),
  );
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
        : path === QUALIFIED_WRITER_OBSERVER_PATH
          ? { uid: 0, mode: 0o100755, bytes: helperBytes }
          : path === SYSTEM_SERVICE_POLICY_PATH
            ? { uid: 0, mode: 0o100644, bytes: Buffer.from(policyText) }
            : "absent",
    observerSocket: async () => options.socket ?? "installed",
    descriptor: async () => ({ serverVersion: oldVersion, environmentId: "fixture-environment" }),
    readiness: async (input) => {
      events.push(`readiness ${input.allowedProcessIds.join(",")}`);
      return {
        status: "complete",
        writers: options.writers
          ? [{ pid: 999, path: NodePath.join(userdata, "statev2.sqlite") }]
          : [],
      };
    },
    stoppedWriterProof: async () => {
      events.push("stopped-writer-proof");
      if (options.stateAppearsDuringProof)
        await NodeFSP.writeFile(NodePath.join(runtime, "service-state.json"), "foreign\n");
      if (options.proofFails) throw new Error("writer-active");
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
      if (command.command === "unzip")
        return JSON.stringify(metadata.get(NodePath.dirname(args[1]!)));
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
    setShow: (change: { exec?: string; user?: string; nnp?: string }) => {
      showExec = change.exec ?? showExec;
      showUser = change.user ?? showUser;
      showNnp = change.nnp ?? showNnp;
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
      "readiness 4242",
      "control inspect",
      "control stop",
      "stopped-writer-proof",
      "control install-launcher",
      "control start",
      "control inspect",
      "readiness 5001",
    ]);
    expect(await f.receipt()).toMatchObject({
      status: "applied",
      phase: "applied",
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
  await fixture(
    async (f) => {
      const { planSha256, bootstrap } = await planSystemAdoption(f.input, f.host);
      expect(bootstrap.socket).toBe("absent");
      await expect(
        adoptSystemHost({ ...f.input, approvedPlanSha256: planSha256 }, f.host),
      ).rejects.toThrow(/bootstrap/);
      expect(f.events).toEqual([]);
    },
    { socket: "absent" },
  );
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

it("keeps the unit stopped after a failed writer proof and never starts it automatically", () =>
  fixture(
    async (f) => {
      await expect(applyApproved(f)).rejects.toThrow(/nothing restarts automatically/);
      expect(f.events.filter((event) => event.startsWith("control")).slice(-1)).toEqual([
        "control stop",
      ]);
      expect(await f.receipt()).toMatchObject({ status: "partial", phase: "stopped" });
      await expect(
        NodeFSP.lstat(NodePath.join(f.runtime, "service-state.json")),
      ).rejects.toMatchObject({ code: "ENOENT" });
    },
    { proofFails: true },
  ));

it("records an uncertain stop as unknown and attempts nothing after it", () =>
  fixture(
    async (f) => {
      await expect(applyApproved(f)).rejects.toThrow(/unknown stop/);
      expect(f.events.at(-1)).toBe("control stop");
      expect(await f.receipt()).toMatchObject({ status: "unknown", phase: "stop-requested" });
    },
    { stopResult: "unknown" },
  ));

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
    { stateAppearsDuringProof: true },
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
    expect(await f.receipt()).toMatchObject({ status: "partial", phase: "enrolling" });
  }));

it("does not stop when the observer socket reports another writer", () =>
  fixture(
    async (f) => {
      await expect(applyApproved(f)).rejects.toThrow(/only the retained server/);
      expect(f.events).not.toContain("control stop");
      expect(await f.receipt()).toMatchObject({ status: "partial", phase: "enrolled" });
    },
    { writers: true },
  ));

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

/** Task scratch removed in `finally`, whatever the body leaves behind. */
async function scratch(prefix: string, body: (root: string) => Promise<void>) {
  const root = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), prefix)),
  );
  try {
    await body(root);
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
    await expect(NodeFSP.lstat(root)).rejects.toMatchObject({ code: "ENOENT" });
  }
}

it("checks every selected file's filesystem before the native lease scan proves no writer", () =>
  scratch("jones-system-stopped-proof-", async (root) => {
    const userdata = NodePath.join(root, "home", "userdata");
    await NodeFSP.mkdir(userdata, { recursive: true });
    const database = NodePath.join(userdata, "statev2.sqlite");
    const settings = NodePath.join(userdata, "settings.json");
    await NodeFSP.writeFile(database, "unchanged database bytes");
    await NodeFSP.writeFile(settings, "{}");
    // Stands in for the kernel lease probe at its fixed program boundary; it
    // records each run and grants every lease, or reports the database busy.
    const runs = NodePath.join(root, "lease-runs");
    const granted = NodePath.join(root, "grant-leases");
    const busy = NodePath.join(root, "busy-leases");
    await NodeFSP.writeFile(granted, `#!/bin/sh\necho run >> '${runs}'\necho complete\n`, {
      mode: 0o755,
    });
    await NodeFSP.writeFile(
      busy,
      `#!/bin/sh\necho run >> '${runs}'\nprintf 'busy 0\\ncomplete\\n'\n`,
      {
        mode: 0o755,
      },
    );
    const prove = async (perl: string, filesystemType: (path: string) => Promise<bigint>) => {
      const observed: string[] = [];
      const runsBefore = await NodeFSP.readFile(runs, "utf8").catch(() => "");
      const result = await proveSystemStoppedWriters(NodePath.join(root, "home"), {
        leases: { perl },
        filesystemType: async (path) => {
          observed.push(path);
          return filesystemType(path);
        },
      }).then(
        () => "proved" as const,
        (cause: unknown) =>
          cause instanceof QualifiedQuiescenceError ? cause.reason : "untyped-failure",
      );
      const leaseRan = (await NodeFSP.readFile(runs, "utf8").catch(() => "")) !== runsBefore;
      return { result, observed, leaseRan };
    };
    expect(await prove(granted, async () => 0xef53n)).toEqual({
      result: "proved",
      observed: [database, settings],
      leaseRan: true,
    });
    // An allowed filesystem only admits the lease scan; a write-open holder still refuses.
    expect(await prove(busy, async () => 0x58465342n)).toEqual({
      result: "writer-active",
      observed: [database, settings],
      leaseRan: true,
    });
    for (const filesystemType of [
      async () => 0x6969n,
      async () => 0x65735546n,
      async (path: string) => (path === settings ? 0x794c7630n : 0xef53n),
      async (): Promise<bigint> => {
        throw Object.assign(new Error("synthetic statfs failure"), { code: "EIO" });
      },
    ]) {
      const refused = await prove(granted, filesystemType);
      expect(refused).toMatchObject({ result: "unavailable", leaseRan: false });
    }
    expect(await NodeFSP.readFile(database, "utf8")).toBe("unchanged database bytes");
  }));

const observerHeader = "jones-qualified-writer-observer 1";
const rootOwnedDirectory: SocketPathMetadata = {
  isDirectory: () => true,
  isSocket: () => false,
  isSymbolicLink: () => false,
  uid: 0,
  mode: 0o040755,
  dev: 1,
  ino: 1,
};

/**
 * A stand-in observer socket over task scratch. Each connection records the
 * request bytes it received and whether the client half-closed; the listener
 * replies only after that half-close. Every peer is destroyed and its close
 * awaited before the listener closes and the scratch is removed.
 */
async function readinessFixture(
  body: (r: {
    readonly userdata: string;
    readonly proc: string;
    readonly connections: readonly {
      readonly received: number;
      readonly halfClosed: boolean;
      readonly closed: Promise<void>;
    }[];
    readonly respond: (handler: (socket: NodeNet.Socket) => void) => void;
    readonly fileLine: (index: number, name: string, inodeOffset?: bigint) => Promise<string>;
    readonly observe: (
      options?: Parameters<typeof observeSystemReadiness>[1],
      allowedProcessIds?: readonly number[],
    ) => ReturnType<typeof observeSystemReadiness>;
  }) => Promise<void>,
) {
  await scratch("jones-system-readiness-", async (root) => {
    const connections: {
      received: number;
      halfClosed: boolean;
      closed: Promise<void>;
      socket: NodeNet.Socket;
    }[] = [];
    let handler: (socket: NodeNet.Socket) => void = () => undefined;
    const server = NodeNet.createServer({ allowHalfOpen: true }, (socket) => {
      const entry = {
        received: 0,
        halfClosed: false,
        socket,
        closed: new Promise<void>((resolve) => socket.once("close", () => resolve())),
      };
      connections.push(entry);
      socket.on("error", () => undefined);
      socket.on("data", (bytes: Buffer) => {
        entry.received += bytes.length;
      });
      socket.once("end", () => {
        entry.halfClosed = true;
        handler(socket);
      });
    });
    const socketPath = NodePath.join(root, "o.sock");
    try {
      await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(socketPath, () => resolve());
      });
      await NodeFSP.chmod(socketPath, 0o600);
      const base = NodePath.join(root, "home");
      const userdata = NodePath.join(base, "userdata");
      await NodeFSP.mkdir(userdata, { recursive: true });
      await NodeFSP.writeFile(
        NodePath.join(userdata, "statev2.sqlite"),
        "unchanged database bytes",
      );
      await NodeFSP.writeFile(NodePath.join(userdata, "settings.json"), "{}");
      const proc = NodePath.join(root, "proc");
      await processEntry(proc, 4242, NodePath.join(root, "t3"), "777");
      // The real test-owned socket below synthetic root-owned ancestors.
      const trusted = async (path: string): Promise<SocketPathMetadata> => {
        const stat = await NodeFSP.lstat(path);
        return stat.isDirectory() ? rootOwnedDirectory : stat;
      };
      await body({
        userdata,
        proc,
        connections,
        respond: (next) => {
          handler = next;
        },
        fileLine: async (index, name, inodeOffset = 0n) => {
          const stat = await NodeFSP.lstat(NodePath.join(userdata, name), { bigint: true });
          return `file ${index} ${stat.dev} ${stat.ino + inodeOffset}`;
        },
        observe: (options = {}, allowedProcessIds = [4242]) =>
          observeSystemReadiness(
            { baseDir: base, allowedProcessIds },
            { socketPath, socketMetadata: trusted, procRoot: proc, deadlineMs: 5_000, ...options },
          ),
      });
    } finally {
      for (const connection of connections) connection.socket.destroy();
      await Promise.all(connections.map((connection) => connection.closed));
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });
}

it("observes readiness through the socket bound to the exact inodes and captured birth", () =>
  readinessFixture(async (r) => {
    const files = [await r.fileLine(0, "statev2.sqlite"), await r.fileLine(3, "settings.json")];
    const reply = (...lines: string[]) =>
      r.respond((socket) => socket.end(`${[observerHeader, ...lines].join("\n")}\n`));
    reply(...files, "writer 0 4242 777 read-write", "complete 9");
    expect(await r.observe()).toEqual({ status: "complete", writers: [] });
    // Any other writer is reported, so adoption refuses to stop.
    reply(...files, "writer 0 4242 777 read-write", "writer 3 999 5 write", "complete 9");
    expect(await r.observe()).toEqual({
      status: "complete",
      writers: [{ pid: 999, path: NodePath.join(r.userdata, "settings.json") }],
    });
    // The same PID with a different birth is a different process.
    reply(...files, "writer 0 4242 778 read-write", "complete 9");
    expect(await r.observe()).toMatchObject({ status: "complete", writers: [{ pid: 4242 }] });
    // An answer about another inode than this snapshot.
    reply(files[0]!, await r.fileLine(3, "settings.json", 1n), "complete 9");
    expect(await r.observe()).toEqual({ status: "identity-changed" });
    for (const lines of [
      ["rejected base-owner"],
      [...files, "incomplete deadline"],
      [...files],
      ["garbage"],
      [...files, "writer 0 999 5 write", "writer 0 999 5 write", "complete 9"],
    ]) {
      reply(...lines);
      expect(await r.observe(), lines.join("|")).toEqual({ status: "unavailable" });
    }
    r.respond((socket) => socket.end("x".repeat(64 * 1024 + 1)));
    expect(await r.observe()).toEqual({ status: "unavailable" });
    for (const connection of r.connections)
      expect(connection).toMatchObject({ received: 0, halfClosed: true });
  }));

it("fails closed when the captured birth, socket or selected state changes across the exchange", () =>
  readinessFixture(async (r) => {
    const files = [await r.fileLine(0, "statev2.sqlite"), await r.fileLine(3, "settings.json")];
    const complete = `${[observerHeader, ...files, "complete 9"].join("\n")}\n`;
    // A captured process that is gone before the exchange: nothing is requested.
    expect(await r.observe({}, [4243])).toEqual({ status: "unavailable" });
    expect(r.connections).toHaveLength(0);

    r.respond(
      (socket) =>
        void processEntry(r.proc, 4242, "/synthetic/t3", "778").then(() => socket.end(complete)),
    );
    expect(await r.observe()).toEqual({ status: "unavailable" });
    await processEntry(r.proc, 4242, "/synthetic/t3", "777");

    // A selected file replaced, or a selected file appearing, after the
    // helper observed the snapshot: its answer is about other state.
    r.respond((socket) => {
      const settings = NodePath.join(r.userdata, "settings.json");
      void NodeFSP.writeFile(`${settings}.next`, "{}")
        .then(() => NodeFSP.rename(`${settings}.next`, settings))
        .then(() => socket.end(complete));
    });
    expect(await r.observe()).toEqual({ status: "identity-changed" });
    const current = [await r.fileLine(0, "statev2.sqlite"), await r.fileLine(3, "settings.json")];
    const currentComplete = `${[observerHeader, ...current, "complete 9"].join("\n")}\n`;
    r.respond((socket) => {
      void NodeFSP.writeFile(NodePath.join(r.userdata, "keybindings.json"), "[]").then(() =>
        socket.end(currentComplete),
      );
    });
    expect(await r.observe()).toEqual({ status: "identity-changed" });
    await NodeFSP.rm(NodePath.join(r.userdata, "keybindings.json"));

    // A socket replaced during the exchange voids its answer.
    r.respond((socket) => socket.end(currentComplete));
    let socketReads = 0;
    const drifting = async (path: string): Promise<SocketPathMetadata> => {
      const stat = await NodeFSP.lstat(path);
      if (stat.isDirectory()) return rootOwnedDirectory;
      return socketReads++ === 0
        ? stat
        : {
            ...rootOwnedDirectory,
            isDirectory: () => false,
            isSocket: () => true,
            uid: stat.uid,
            mode: stat.mode,
            ino: stat.ino + 1,
          };
    };
    expect(await r.observe({ socketMetadata: drifting })).toEqual({ status: "unavailable" });

    // A silent observer reaches the bounded deadline; finally owns its half-open peer.
    const before = r.connections.length;
    r.respond(() => undefined);
    expect(await r.observe({ deadlineMs: 200 })).toEqual({ status: "unavailable" });
    expect(r.connections).toHaveLength(before + 1);

    // An untrusted socket path is never contacted.
    const contacted = r.connections.length;
    expect(await r.observe({ socketMetadata: (path) => NodeFSP.lstat(path) })).toEqual({
      status: "unavailable",
    });
    expect(r.connections).toHaveLength(contacted);
  }));
