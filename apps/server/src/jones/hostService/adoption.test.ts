// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import * as NodeChildProcess from "node:child_process";
import * as NodeProcess from "node:process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeZlib from "node:zlib";
import { expect, it, vi } from "vite-plus/test";
import * as LegacyBootstrap from "./legacyBootstrap.ts";
import { renderBootServiceUnit } from "../../cloud/bootService.ts";
import { SERVICE_LAUNCHER_PROTOCOL } from "../../cloud/serviceProtocol.ts";
import { readQualifiedRuntimeReceipt } from "../cloud/qualifiedRuntime.ts";
import { JONES_BOOT_SERVICE_IDENTITY } from "./identity.ts";
import {
  adoptHost,
  planHostAdoption,
  verifyAdoptionArtifact,
  ADOPTION_RECEIPT,
  UPDATE_CAPABILITY_RECEIPT,
  type AdoptionHost,
} from "./adoption.ts";

vi.mock("node:fs/promises", async (importOriginal) => {
  const native = await importOriginal<typeof import("node:fs/promises")>();
  return { ...native, open: vi.fn(native.open), readFile: vi.fn(native.readFile) };
});

const oldVersion = "0.0.0-preview.20261010.100";
const newVersion = "0.0.0-preview.20261010.101";
const privateSetupSource = "d3e6f8e843a0477542380a4ec0d9ae02a1084053";
const privateSetupTree = "250db17dd136cef960e9bb8e054acadb41fba03d";
const runtimeExecutable = (version: string) => `#!/bin/sh\n# version: ${version}\nexit 0\n`;
const sha = (value: string | Buffer) => NodeCrypto.createHash("sha256").update(value).digest("hex");
function tarPayload(version: string, privateSetup = false) {
  const executable = runtimeExecutable(version);
  const entries: Buffer[] = [];
  for (const [name, kind, body] of [
    ["payload/", "5", ""],
    ["payload/t3", "0", executable],
    ...(privateSetup
      ? ([
          ["payload/client/", "5", ""],
          ["payload/node_modules/", "5", ""],
          ["payload/resource-monitor/", "5", ""],
        ] as const)
      : []),
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
    results.push(stat.isDirectory() ? `${file}/` : `${file}:${sha(await NodeFSP.readFile(file))}`);
    if (stat.isDirectory()) results.push(...(await tree(file)));
  }
  return results;
}
async function expectOnlyRuntimeLockCreated(
  f: { readonly root: string; readonly runtime: string },
  before: ReadonlyArray<string>,
) {
  const lock = NodePath.join(f.runtime, "qualified-runtime-lock.sqlite");
  expect((await NodeFSP.lstat(lock)).isFile()).toBe(true);
  expect(await NodeFSP.readFile(lock)).toEqual(Buffer.alloc(0));
  expect(await tree(f.root)).toEqual([...before, `${lock}:${sha(Buffer.alloc(0))}`].sort());
}
async function fixture(
  body: (f: Awaited<ReturnType<typeof createFixture>>) => Promise<void>,
  options: {
    readonly legacy?: boolean;
    readonly migrationChanged?: boolean;
    readonly privateSetup?: boolean;
  } = {},
) {
  const f = await createFixture(options);
  try {
    await body(f);
  } finally {
    await NodeFSP.rm(f.root, { recursive: true, force: true });
  }
}
async function createFixture(
  options: {
    readonly legacy?: boolean;
    readonly migrationChanged?: boolean;
    readonly privateSetup?: boolean;
  } = {},
) {
  const root = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-adoption-test-")),
  );
  const base = NodePath.join(root, "base");
  const home = NodePath.join(root, "home");
  const runtime = NodePath.join(base, "runtime");
  const active = NodePath.join(runtime, "versions", oldVersion);
  await NodeFSP.mkdir(active, { recursive: true });
  await NodeFSP.mkdir(NodePath.join(base, "userdata"));
  await NodeFSP.writeFile(NodePath.join(active, "t3"), runtimeExecutable(oldVersion), {
    mode: 0o755,
  });
  await NodeFSP.writeFile(NodePath.join(active, ".install-complete"), oldVersion);
  if (options.privateSetup) {
    for (const name of ["client", "node_modules", "resource-monitor"])
      await NodeFSP.mkdir(NodePath.join(active, name));
  }
  if (!options.legacy && !options.privateSetup)
    await NodeFSP.writeFile(
      NodePath.join(active, ".jones-provenance.json"),
      "private provenance is not qualification",
    );
  await NodeFSP.writeFile(
    NodePath.join(runtime, "service-state.json"),
    JSON.stringify({ protocol: SERVICE_LAUNCHER_PROTOCOL, activeVersion: oldVersion }),
  );
  await NodeFSP.writeFile(NodePath.join(base, "userdata/environment-id"), "fixture-environment");
  await NodeFSP.writeFile(
    NodePath.join(base, "userdata/statev2.sqlite"),
    "unchanged database bytes",
  );
  const serviceUnit = options.legacy ? "t3code.service" : "jones-code.service";
  const unit = NodePath.join(home, ".config/systemd/user", serviceUnit);
  await NodeFSP.mkdir(NodePath.dirname(unit), { recursive: true });
  await NodeFSP.writeFile(
    unit,
    renderBootServiceUnit(
      {
        baseDir: base,
        program: [NodePath.join(active, "t3"), "__service-launcher"],
        unitPath: unit,
        logPath: NodePath.join(base, "userdata/logs/boot-service.log"),
        environment: { KEEP_THIS: "unchanged" },
      },
      { ...JONES_BOOT_SERVICE_IDENTITY, systemdUnitFile: serviceUnit },
    ),
  );
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
    const archive = tarPayload(version, options.privateSetup && index === 0);
    const zip = Buffer.from(`synthetic zip envelope ${index}`);
    const commit =
      options.privateSetup && index === 0
        ? privateSetupSource
        : (index === 0 ? "a" : "b").repeat(40);
    const m = {
      schema: 1,
      repository: "Jones-Systems/Jones-Code",
      source: commit,
      tree: options.privateSetup && index === 0 ? privateSetupTree : "c".repeat(40),
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
      source: commit,
    });
  }
  if (options.privateSetup) {
    const artifact = metadata.get(directories[0]!)!;
    await NodeFSP.writeFile(
      NodePath.join(active, ".jones-provenance.json"),
      JSON.stringify({
        schema: 1,
        repository: artifact.repository,
        source: artifact.source,
        version: artifact.version,
        platform: artifact.platform,
        architecture: artifact.architecture,
        artifact: artifact.artifact,
        sha256: artifact.sha256,
        entrySha256: sha(runtimeExecutable(oldVersion)),
      }),
      { mode: 0o600 },
    );
  }
  const commands: Array<{ command: string; args: ReadonlyArray<string> }> = [];
  let dropins = "";
  let taskText: string | undefined;
  let stateText: string | undefined;
  let taskPath: string | undefined;
  const preserveDropin = [
    "20-openrouter.conf",
    "30-github-cli-path.conf",
    "40-agent-mail-token.conf",
  ];
  const directExecutable = NodePath.join(root, "Jones-Code-Install", "t3");
  if (options.legacy) {
    await NodeFSP.mkdir(NodePath.dirname(directExecutable));
    await NodeFSP.writeFile(directExecutable, runtimeExecutable(oldVersion), { mode: 0o755 });
    await NodeFSP.mkdir(`${unit}.d`);
    for (const name of preserveDropin)
      await NodeFSP.writeFile(
        NodePath.join(`${unit}.d`, name),
        "synthetic credential directive that must never be opened",
      );
    taskPath = NodePath.join(`${unit}.d`, "50-jones-code-transition.conf");
    taskText = `[Service]\nExecStart=\nExecStart=${directExecutable} serve --host 127.0.0.1 --port 3774 --base-dir ${base}\n`;
    await NodeFSP.writeFile(taskPath, taskText);
    stateText = JSON.stringify({
      nativeProtocol3State: false,
      operationId: "synthetic-completed-installation",
      purpose: "retain existing managed tunnel during operator-owned replacement",
      schema: "jones.task-tunnel-handoff/1",
      source: {
        binarySha256: "a".repeat(64),
        pid: 12000,
        startTicks: "123456",
        uid: 1000,
        version: "0.0.46-nightly.20261008.2801",
      },
      target: { version: oldVersion },
      update: { operationId: "synthetic-completed-installation", status: "pending" },
    });
    await NodeFSP.writeFile(NodePath.join(runtime, "service-state.json"), stateText);
    dropins = [...preserveDropin.map((name) => NodePath.join(`${unit}.d`, name)), taskPath].join(
      " ",
    );
  }
  let generation = 1;
  let restarted = false;
  const host: AdoptionHost = {
    platform: "linux",
    architecture: "x64",
    home,
    uid: NodeOS.userInfo().uid,
    environmentPath: "/usr/bin:/bin",
    readbackAttempts: 1,
    launcherProcessGuard: { uid: NodeOS.userInfo().uid, isOwnedLive: async () => true },
    run: async (command) => {
      commands.push(command);
      const args = command.args;
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
          run_attempt: generation,
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
      if (command.command === "readlink") return directExecutable;
      if (args.includes("--property=MainPID")) return String(restarted ? 12347 : 12345);
      if (args.includes("--property=FragmentPath")) return unit;
      if (args.includes("--property=DropInPaths")) return dropins;
      if (args.includes("--property=ExecStart"))
        return NodePath.join(runtime, "versions", newVersion, "t3");
      if (command.command === "systemctl") {
        if (args.includes("restart")) {
          restarted = true;
          if (options.legacy || options.privateSetup)
            await NodeFSP.writeFile(
              NodePath.join(runtime, "jones-launcher-capability.json"),
              JSON.stringify({
                schema: 1,
                baseDir: base,
                launcherVersion: newVersion,
                launcherPid: 12347,
                launcherProtocol: 4,
                qualifiedUpdatesProtocol: 1,
                startupGateProtocol: 1,
                childPid: 12346,
                childVersion: oldVersion,
              }),
              { mode: 0o600 },
            );
          else
            await NodeFSP.writeFile(
              NodePath.join(runtime, UPDATE_CAPABILITY_RECEIPT),
              JSON.stringify({
                schema: 1,
                baseDir: base,
                environmentId: "fixture-environment",
                currentVersion: oldVersion,
                processId: 12346,
                capability: { install: true },
                qualifiedLauncher: true,
              }),
              { mode: 0o600 },
            );
        }
        return "active";
      }
      const version = /^# version: (\S+)$/m.exec(
        await NodeFSP.readFile(command.command, "utf8"),
      )?.[1];
      if (args.includes("__service-preflight"))
        return JSON.stringify({
          status: "ready",
          version,
          launcherProtocol: SERVICE_LAUNCHER_PROTOCOL,
          startupGateProtocol: 1,
          ...(options.privateSetup && version === oldVersion
            ? {}
            : {
                migrationPlan: {
                  pendingUpstream: [],
                  pendingJones: options.migrationChanged && restarted ? [] : [120, 121],
                },
              }),
        });
      return `t3 v${version}\n`;
    },
    readback: async () => ({
      processId: restarted ? 12346 : 12345,
      serviceManaged: !options.legacy || restarted,
      port: 3774,
      serverVersion: oldVersion,
      environmentId: "fixture-environment",
    }),
  };
  const input = {
    baseDir: base,
    activeArtifactDir: directories[0]!,
    activeSourceCommit: options.privateSetup ? privateSetupSource : "a".repeat(40),
    launcherArtifactDir: directories[1]!,
    launcherSourceCommit: "b".repeat(40),
    ...(options.privateSetup ? { acceptUnattestedChildCapability: true } : {}),
    ...(options.legacy
      ? {
          legacyDirectServe: true,
          serviceUnit: "t3code.service" as const,
          serviceUnitSha256: sha(await NodeFSP.readFile(unit)),
          taskOperationId: "synthetic-completed-installation",
          taskHandoffSha256: sha(stateText!),
          taskDropin: `50-jones-code-transition.conf=${sha(taskText!)}`,
          preserveDropin,
          acceptUnattestedChildCapability: true,
        }
      : {}),
  };
  return {
    root,
    base,
    home,
    runtime,
    unit,
    host,
    input,
    commands,
    metadata,
    taskPath,
    taskText,
    stateText,
    preserveDropin,
    directExecutable,
    setDropins: (value: string) => {
      dropins = value;
    },
    setGeneration: (value: number) => {
      generation = value;
    },
  };
}
it("dry-run qualifies both exact generations without changing any file", () =>
  fixture(async (f) => {
    const before = await tree(f.root);
    const result = await adoptHost({ ...f.input, dryRun: true }, f.host);
    expect(result.state).toBe("dry-run");
    expect(result.plan.activeVersion).toBe(oldVersion);
    expect(result.plan.launcherVersion).toBe(newVersion);
    expect(result.plan.serviceContents).toContain("ExecStart=\n");
    expect(await tree(f.root)).toEqual(before);
    expect(
      f.commands.every(
        (c) => c.command === "gh" || c.command === "unzip" || c.args.includes("show"),
      ),
    ).toBe(true);
  }));
it.each(
  (["group-writable", "world-writable", "symlink"] as const).flatMap((hazard) =>
    (["plan", "dry-run", "apply"] as const).map((entry) => ({ hazard, entry })),
  ),
)("refuses a $hazard runtime directory before $entry effects", ({ hazard, entry }) =>
  fixture(async (f) => {
    const realRuntime = hazard === "symlink" ? NodePath.join(f.root, "runtime-target") : f.runtime;
    if (hazard === "symlink") {
      await NodeFSP.rename(f.runtime, realRuntime);
      await NodeFSP.symlink(realRuntime, f.runtime);
    } else {
      await NodeFSP.chmod(f.runtime, hazard === "group-writable" ? 0o775 : 0o757);
    }
    const before = await NodeFSP.lstat(f.runtime);
    const statePath = NodePath.join(realRuntime, "service-state.json");
    const stateBefore = await NodeFSP.readFile(statePath, "utf8");
    const unitBefore = await NodeFSP.readFile(f.unit, "utf8");
    const versionsBefore = await tree(NodePath.join(realRuntime, "versions"));
    const operation =
      entry === "plan"
        ? planHostAdoption(f.input, f.host)
        : adoptHost({ ...f.input, dryRun: entry === "dry-run" }, f.host);
    await expect(operation).rejects.toThrow("Launcher capability directory has unknown ownership.");
    expect(f.commands).toEqual([]);
    await expect(NodeFSP.lstat(NodePath.join(realRuntime, ADOPTION_RECEIPT))).rejects.toMatchObject(
      {
        code: "ENOENT",
      },
    );
    expect(await tree(NodePath.join(realRuntime, "versions"))).toEqual(versionsBefore);
    expect(await NodeFSP.readFile(statePath, "utf8")).toBe(stateBefore);
    expect(await NodeFSP.readFile(f.unit, "utf8")).toBe(unitBefore);
    const after = await NodeFSP.lstat(f.runtime);
    expect([after.dev, after.ino, after.uid, after.mode]).toEqual([
      before.dev,
      before.ino,
      before.uid,
      before.mode,
    ]);
    if (hazard === "symlink") expect(await NodeFSP.readlink(f.runtime)).toBe(realRuntime);
  }),
);
it("enrolls old and new receipts, preserves native identity and starts only the new launcher", () =>
  fixture(async (f) => {
    const stateBefore = await NodeFSP.readFile(
      NodePath.join(f.runtime, "service-state.json"),
      "utf8",
    );
    const unitBefore = await NodeFSP.readFile(f.unit, "utf8");
    const result = await adoptHost(f.input, f.host);
    expect(result.state).toBe("adopted");
    expect(await NodeFSP.readFile(f.unit, "utf8")).toBe(unitBefore);
    expect(await NodeFSP.readFile(NodePath.join(f.runtime, "service-state.json"), "utf8")).toBe(
      stateBefore,
    );
    expect(await NodeFSP.readFile(NodePath.join(f.base, "userdata/statev2.sqlite"), "utf8")).toBe(
      "unchanged database bytes",
    );
    expect(await NodeFSP.readFile(NodePath.join(f.base, "userdata/environment-id"), "utf8")).toBe(
      "fixture-environment",
    );
    for (const version of [oldVersion, newVersion]) {
      const receipt = await readQualifiedRuntimeReceipt(f.base, version, {
        platform: "linux",
        architecture: "x64",
      });
      expect(receipt.version).toBe(version);
      expect(receipt.runAttempt).toBe(1);
    }
    expect(
      JSON.parse(await NodeFSP.readFile(NodePath.join(f.runtime, ADOPTION_RECEIPT), "utf8")),
    ).toMatchObject({ status: "applied", capability: { install: true } });
  }));
it("archives authenticated private setup evidence before qualification and retains launcher-only attestation", () =>
  fixture(
    async (f) => {
      const provenance = NodePath.join(f.runtime, "versions", oldVersion, ".jones-provenance.json");
      const original = await NodeFSP.readFile(provenance, "utf8");
      const before = await NodeFSP.lstat(provenance);
      const stateBefore = await NodeFSP.readFile(
        NodePath.join(f.runtime, "service-state.json"),
        "utf8",
      );
      const beforeDryRun = await tree(f.root);
      const dry = await adoptHost({ ...f.input, dryRun: true }, f.host);
      expect(dry.plan.attestation).toBe("launcher-only");
      expect(await tree(f.root)).toEqual(beforeDryRun);
      let observedOrdering = false;
      const host: AdoptionHost = {
        ...f.host,
        run: async (command) => {
          if (
            command.args.includes("__service-preflight") &&
            command.command.includes(oldVersion)
          ) {
            const pending = JSON.parse(
              await NodeFSP.readFile(NodePath.join(f.runtime, ADOPTION_RECEIPT), "utf8"),
            );
            expect(pending.status).toBe("pending");
            expect(pending.privateSetupProvenance.preimage.text).toBe(original);
            await expect(NodeFSP.lstat(provenance)).rejects.toMatchObject({ code: "ENOENT" });
            const archived = NodePath.join(
              pending.privateSetupProvenance.archiveDirectory,
              ".jones-provenance.json",
            );
            expect(await NodeFSP.readFile(archived, "utf8")).toBe(original);
            expect((await NodeFSP.lstat(archived)).ino).toBe(before.ino);
            await expect(
              NodeFSP.lstat(
                NodePath.join(f.runtime, "versions", oldVersion, ".jones-runtime-receipt.json"),
              ),
            ).rejects.toMatchObject({ code: "ENOENT" });
            expect(f.commands.some((c) => c.args.includes("stop"))).toBe(false);
            observedOrdering = true;
          }
          return f.host.run(command);
        },
      };
      const result = await adoptHost(f.input, host);
      expect(observedOrdering).toBe(true);
      const receipt = JSON.parse(
        await NodeFSP.readFile(NodePath.join(f.runtime, ADOPTION_RECEIPT), "utf8"),
      );
      expect(receipt).toMatchObject({
        status: "applied",
        attestation: "launcher-only",
        childCapability: "unattested",
        attestationBasis: "pre-publisher-child",
        privateSetupProvenance: { state: "archived" },
      });
      expect(receipt.capability).toBeUndefined();
      const archived = NodePath.join(
        result.plan.privateSetupProvenance!.archiveDirectory,
        ".jones-provenance.json",
      );
      const after = await NodeFSP.lstat(archived);
      expect([after.dev, after.ino, after.uid, after.mode]).toEqual([
        before.dev,
        before.ino,
        before.uid,
        before.mode,
      ]);
      expect(await NodeFSP.readFile(archived, "utf8")).toBe(original);
      await expect(NodeFSP.lstat(provenance)).rejects.toMatchObject({ code: "ENOENT" });
      expect(await NodeFSP.readFile(NodePath.join(f.runtime, "service-state.json"), "utf8")).toBe(
        stateBefore,
      );
      expect(
        (
          await readQualifiedRuntimeReceipt(f.base, oldVersion, {
            platform: "linux",
            architecture: "x64",
          })
        ).sourceSha,
      ).toBe(privateSetupSource);
      expect(f.commands.some((c) => c.args.includes("restart"))).toBe(true);
    },
    { privateSetup: true },
  ));
it(
  "enrolls genuine private modes with authenticated 0755 archive extraction under isolated umask 0077",
  () =>
    fixture(
      async (f) => {
        const active = NodePath.join(f.runtime, "versions", oldVersion);
        for (const directory of [
          f.base,
          f.runtime,
          NodePath.dirname(active),
          active,
          ...["client", "node_modules", "resource-monitor"].map((name) =>
            NodePath.join(active, name),
          ),
        ])
          await NodeFSP.chmod(directory, 0o700);
        await NodeFSP.chmod(NodePath.join(active, "t3"), 0o700);
        for (const name of [".install-complete", ".jones-provenance.json"])
          await NodeFSP.chmod(NodePath.join(active, name), 0o600);
        const verified = await verifyAdoptionArtifact(
          f.input.activeArtifactDir,
          privateSetupSource,
          f.host,
        );
        const scratch = await NodeFSP.mkdtemp(NodePath.join(f.root, "private-mode-proof-"));
        const script = `
        process.umask(0o077);
        const assert = (await import("node:assert/strict")).default;
        const fs = await import("node:fs/promises");
        const path = await import("node:path");
        const { extractQualifiedLinuxArchive } = await import(${JSON.stringify(new URL("../cloud/qualifiedArchive.ts", import.meta.url).href)});
        const { enrollQualifiedRuntime, qualifiedPayloadDigest } = await import(${JSON.stringify(new URL("../cloud/qualifiedRuntime.ts", import.meta.url).href)});
        const { inspectPrivateSetupRuntime, archivePrivateSetupProvenance } = await import(${JSON.stringify(new URL("./privateSetupCompatibility.ts", import.meta.url).href)});
        const input = JSON.parse(process.argv[1]);
        const payload = path.join(input.scratch, "payload");
        await fs.mkdir(payload, { mode: 0o700 });
        await extractQualifiedLinuxArchive(input.archive, payload);
        assert.equal((await fs.stat(path.join(payload, "t3"))).mode & 0o777, 0o700);
        const preimage = await inspectPrivateSetupRuntime(input.privateSetup);
        assert.equal(await qualifiedPayloadDigest(payload), preimage.payloadSha256);
        const archive = path.join(input.scratch, "archive");
        const before = await fs.stat(preimage.path);
        const receipt = await enrollQualifiedRuntime({
          baseDir: input.baseDir,
          artifact: { ...input.artifact, payloadDirectory: payload },
          host: { platform: "linux", architecture: "x64" },
          prepareExistingRuntime: () => archivePrivateSetupProvenance(preimage, archive, input.privateSetup.uid),
          validate: async () => {},
        });
        assert.equal(receipt.payloadSha256, preimage.payloadSha256);
        assert.equal((await fs.stat(path.join(input.privateSetup.directory, "t3"))).mode & 0o777, 0o700);
        const preserved = await fs.stat(path.join(archive, ".jones-provenance.json"));
        assert.equal(preserved.ino, before.ino);
        assert.equal(preserved.dev, before.dev);
        assert.equal(await fs.readFile(path.join(archive, ".jones-provenance.json"), "utf8"), preimage.text);
        process.stdout.write(JSON.stringify({ source: receipt.sourceSha, mode: 0o700 }));
      `;
        const input = JSON.stringify({
          scratch,
          archive: verified.archive,
          baseDir: f.base,
          artifact: verified.artifact,
          privateSetup: {
            directory: active,
            uid: f.host.uid,
            artifact: verified.metadata,
            entrySha256: sha(runtimeExecutable(oldVersion)),
          },
        });
        const stdout = await new Promise<string>((resolve, reject) =>
          NodeChildProcess.execFile(
            NodeProcess.execPath,
            ["--experimental-strip-types", "--input-type=module", "-e", script, input],
            { timeout: 30_000, maxBuffer: 64 * 1024 },
            (error, stdout) => (error === null ? resolve(stdout) : reject(error)),
          ),
        );
        expect(JSON.parse(stdout)).toEqual({ source: privateSetupSource, mode: 0o700 });
        await expect(
          NodeFSP.lstat(NodePath.join(active, ".jones-provenance.json")),
        ).rejects.toMatchObject({ code: "ENOENT" });
      },
      { privateSetup: true },
    ),
  35_000,
);
it.each(["missing-acceptance", "current-child-receipt", "stale-child-receipt"])(
  "refuses private setup %s before pending intent or service stop",
  (kind) =>
    fixture(
      async (f) => {
        if (kind !== "missing-acceptance")
          await NodeFSP.writeFile(
            NodePath.join(f.runtime, UPDATE_CAPABILITY_RECEIPT),
            JSON.stringify({
              schema: 1,
              baseDir: f.base,
              environmentId: "fixture-environment",
              currentVersion: oldVersion,
              processId: kind === "current-child-receipt" ? 12345 : 999,
              qualifiedLauncher: true,
              capability: { install: true },
            }),
            { mode: 0o600 },
          );
        const before = await tree(f.root);
        await expect(
          adoptHost(
            { ...f.input, acceptUnattestedChildCapability: kind !== "missing-acceptance" },
            f.host,
          ),
        ).rejects.toThrow(
          kind === "missing-acceptance"
            ? "explicit unattested-child acceptance"
            : "unexpected capability receipt",
        );
        await expectOnlyRuntimeLockCreated(f, before);
        expect(f.commands.some((c) => c.args.includes("stop") || c.args.includes("restart"))).toBe(
          false,
        );
        await expect(
          NodeFSP.lstat(NodePath.join(f.runtime, ADOPTION_RECEIPT)),
        ).rejects.toMatchObject({ code: "ENOENT" });
      },
      { privateSetup: true },
    ),
);
it("preserves archived provenance and pending intent after pre-stop qualification interruption without retry", () =>
  fixture(
    async (f) => {
      const provenance = NodePath.join(f.runtime, "versions", oldVersion, ".jones-provenance.json");
      const original = await NodeFSP.readFile(provenance, "utf8");
      const before = await NodeFSP.lstat(provenance);
      const host: AdoptionHost = {
        ...f.host,
        run: async (command) => {
          if (command.args.includes("__service-preflight") && command.command.includes(oldVersion))
            throw new Error("Synthetic pre-stop qualification interruption");
          return f.host.run(command);
        },
      };
      await expect(adoptHost(f.input, host)).rejects.toThrow(
        "Synthetic pre-stop qualification interruption",
      );
      const pending = JSON.parse(
        await NodeFSP.readFile(NodePath.join(f.runtime, ADOPTION_RECEIPT), "utf8"),
      );
      expect(pending.status).toBe("pending");
      expect(pending.privateSetupProvenance.preimage.text).toBe(original);
      const archived = NodePath.join(
        pending.privateSetupProvenance.archiveDirectory,
        ".jones-provenance.json",
      );
      expect(await NodeFSP.readFile(archived, "utf8")).toBe(original);
      expect((await NodeFSP.lstat(archived)).ino).toBe(before.ino);
      await expect(NodeFSP.lstat(provenance)).rejects.toMatchObject({ code: "ENOENT" });
      expect(f.commands.some((c) => c.args.includes("stop") || c.args.includes("restart"))).toBe(
        false,
      );
      const commandCount = f.commands.length;
      await expect(adoptHost(f.input, f.host)).rejects.toThrow("Previous adoption");
      expect(f.commands.length).toBe(commandCount);
      expect(await NodeFSP.readFile(archived, "utf8")).toBe(original);
    },
    { privateSetup: true },
  ));
it("compares the occupied payload with the authenticated artifact before moving provenance", () =>
  fixture(
    async (f) => {
      const active = NodePath.join(f.runtime, "versions", oldVersion);
      const provenance = NodePath.join(active, ".jones-provenance.json");
      await NodeFSP.writeFile(
        NodePath.join(active, "client", "unexpected-content"),
        "different payload",
      );
      const original = await NodeFSP.readFile(provenance, "utf8");
      await expect(adoptHost(f.input, f.host)).rejects.toThrow("Existing runtime differs");
      const pending = JSON.parse(
        await NodeFSP.readFile(NodePath.join(f.runtime, ADOPTION_RECEIPT), "utf8"),
      );
      expect(pending.status).toBe("pending");
      expect(await NodeFSP.readFile(provenance, "utf8")).toBe(original);
      await expect(
        NodeFSP.lstat(pending.privateSetupProvenance.archiveDirectory),
      ).rejects.toMatchObject({ code: "ENOENT" });
      expect(f.commands.some((c) => c.args.includes("stop") || c.args.includes("restart"))).toBe(
        false,
      );
    },
    { privateSetup: true },
  ));
it.each(["missing", "stale"])("requires genuine launcher readback for private setup: %s", (kind) =>
  fixture(
    async (f) => {
      const host: AdoptionHost = {
        ...f.host,
        run: async (command) => {
          const output = await f.host.run(command);
          if (command.args.includes("restart")) {
            const path = NodePath.join(f.runtime, "jones-launcher-capability.json");
            if (kind === "missing") await NodeFSP.rm(path);
            else {
              const value = JSON.parse(await NodeFSP.readFile(path, "utf8"));
              await NodeFSP.writeFile(path, JSON.stringify({ ...value, launcherPid: 999 }));
            }
          }
          return output;
        },
      };
      await expect(adoptHost(f.input, host)).rejects.toThrow("readback is unavailable or stale");
      const pending = JSON.parse(
        await NodeFSP.readFile(NodePath.join(f.runtime, ADOPTION_RECEIPT), "utf8"),
      );
      expect(pending.status).toBe("pending");
      expect(
        await NodeFSP.readFile(
          NodePath.join(pending.privateSetupProvenance.archiveDirectory, ".jones-provenance.json"),
          "utf8",
        ),
      ).toBe(pending.privateSetupProvenance.preimage.text);
    },
    { privateSetup: true },
  ),
);
it("refuses ordinary unattested acceptance outside the exact authenticated private source", () =>
  fixture(async (f) => {
    const before = await tree(f.root);
    await expect(
      adoptHost({ ...f.input, acceptUnattestedChildCapability: true }, f.host),
    ).rejects.toThrow("limited to the exact supported private setup artifact");
    await expectOnlyRuntimeLockCreated(f, before);
    expect(f.commands.some((c) => c.args.includes("stop") || c.args.includes("restart"))).toBe(
      false,
    );
    await expect(NodeFSP.lstat(NodePath.join(f.runtime, ADOPTION_RECEIPT))).rejects.toMatchObject({
      code: "ENOENT",
    });
  }));
it("retains ordinary publisher-capable child readback requirements without the compatibility flag", () =>
  fixture(async (f) => {
    const host: AdoptionHost = {
      ...f.host,
      run: async (command) => {
        const output = await f.host.run(command);
        if (command.args.includes("restart"))
          await NodeFSP.rm(NodePath.join(f.runtime, UPDATE_CAPABILITY_RECEIPT));
        return output;
      },
    };
    await expect(adoptHost(f.input, host)).rejects.toThrow("readback is unavailable or stale");
    const pending = JSON.parse(
      await NodeFSP.readFile(NodePath.join(f.runtime, ADOPTION_RECEIPT), "utf8"),
    );
    expect(pending.status).toBe("pending");
    expect(pending.privateSetupProvenance).toBeUndefined();
  }));
it("preserves the exact public service carryover drop-in during private setup adoption", () =>
  fixture(
    async (f) => {
      const path = `${f.unit}.d/40-native-carryover.conf`;
      await NodeFSP.mkdir(NodePath.dirname(path));
      const text =
        "[Service]\nWorkingDirectory=%h\nUMask=0077\nNoNewPrivileges=yes\nPrivateTmp=yes\nLimitNOFILE=65536\nMemoryMax=8G\n";
      await NodeFSP.writeFile(path, text, { mode: 0o600 });
      f.setDropins(path);
      const before = await LegacyBootstrap.fileMetadata(path);
      await adoptHost(f.input, f.host);
      expect(await NodeFSP.readFile(path, "utf8")).toBe(text);
      expect(await LegacyBootstrap.fileMetadata(path)).toBe(before);
    },
    { privateSetup: true },
  ));
it("refuses an unowned ExecStart drop-in, then explicitly supersedes without deleting it", () =>
  fixture(async (f) => {
    const unowned = `${f.unit}.d/90-owner.conf`;
    await NodeFSP.mkdir(NodePath.dirname(unowned));
    await NodeFSP.writeFile(unowned, "[Service]\nExecStart=\nExecStart=/unowned/t3\n");
    f.setDropins(unowned);
    await expect(planHostAdoption(f.input, f.host)).rejects.toThrow("Unowned ExecStart");
    await adoptHost({ ...f.input, supersedeExecstart: true }, f.host);
    expect(await NodeFSP.readFile(unowned, "utf8")).toContain("/unowned/t3");
  }));
it.each(["unknown", "pending"])("refuses %s service state without writes", (state) =>
  fixture(async (f) => {
    const file = NodePath.join(f.runtime, "service-state.json");
    await NodeFSP.writeFile(
      file,
      state === "unknown"
        ? "{}"
        : JSON.stringify({
            protocol: SERVICE_LAUNCHER_PROTOCOL,
            activeVersion: oldVersion,
            update: {
              id: "fixture",
              fromVersion: oldVersion,
              targetVersion: newVersion,
              dbPath: NodePath.join(f.base, "userdata/statev2.sqlite"),
              status: "pending",
              phase: "accepted",
            },
          }),
    );
    const before = await tree(f.root);
    await expect(adoptHost({ ...f.input, dryRun: true }, f.host)).rejects.toThrow(
      "pending or unknown",
    );
    expect(await tree(f.root)).toEqual(before);
  }),
);
it("refuses stale Actions generation and an unauthenticated local ZIP", () =>
  fixture(async (f) => {
    f.setGeneration(2);
    await expect(
      verifyAdoptionArtifact(f.input.activeArtifactDir, f.input.activeSourceCommit, f.host),
    ).rejects.toThrow("generation");
    f.setGeneration(1);
    await NodeFSP.writeFile(
      NodePath.join(f.input.activeArtifactDir, "github-artifact.zip"),
      "tampered",
    );
    await expect(
      verifyAdoptionArtifact(f.input.activeArtifactDir, f.input.activeSourceCommit, f.host),
    ).rejects.toThrow("ZIP digest");
  }));
it("retains a pending receipt after uncertain activation and refuses a blind retry", () =>
  fixture(async (f) => {
    const host = {
      ...f.host,
      readback: async (input: Parameters<AdoptionHost["readback"]>[0]) => {
        if (f.commands.some((c) => c.args.includes("restart"))) throw new Error("not ready");
        return f.host.readback(input);
      },
    };
    await expect(adoptHost(f.input, host)).rejects.toThrow("readback is unavailable");
    expect(
      JSON.parse(await NodeFSP.readFile(NodePath.join(f.runtime, ADOPTION_RECEIPT), "utf8")),
    ).toMatchObject({ status: "pending" });
    const count = f.commands.length;
    await expect(adoptHost(f.input, f.host)).rejects.toThrow("Previous adoption");
    expect(f.commands.length).toBe(count);
  }));

it("preserves an occupied cache that differs from the authenticated payload", () =>
  fixture(async (f) => {
    const oldEntry = NodePath.join(f.runtime, "versions", oldVersion, "t3");
    await NodeFSP.writeFile(oldEntry, "modified runtime", { mode: 0o755 });
    await expect(adoptHost(f.input, f.host)).rejects.toThrow("Existing runtime differs");
    expect(await NodeFSP.readFile(oldEntry, "utf8")).toBe("modified runtime");
    expect(f.commands.some((c) => c.args.includes("stop"))).toBe(false);
  }));
it("refuses a higher-priority drop-in even with explicit supersession", () =>
  fixture(async (f) => {
    const unowned = `${f.unit}.d/zzzzz-owner.conf`;
    await NodeFSP.mkdir(NodePath.dirname(unowned));
    await NodeFSP.writeFile(unowned, "[Service]\nExecStart=/unowned/t3\n");
    f.setDropins(unowned);
    const before = await tree(f.root);
    await expect(
      adoptHost({ ...f.input, dryRun: true, supersedeExecstart: true }, f.host),
    ).rejects.toThrow("sorts after");
    expect(await tree(f.root)).toEqual(before);
  }));
it("holds a stale capability receipt instead of reporting inferred install availability", () =>
  fixture(async (f) => {
    const host = {
      ...f.host,
      run: async (c: Parameters<AdoptionHost["run"]>[0]) => {
        const result = await f.host.run(c);
        if (c.args.includes("restart"))
          await NodeFSP.writeFile(
            NodePath.join(f.runtime, UPDATE_CAPABILITY_RECEIPT),
            JSON.stringify({
              schema: 1,
              baseDir: f.base,
              environmentId: "fixture-environment",
              currentVersion: oldVersion,
              processId: 999,
              capability: { install: true },
              qualifiedLauncher: true,
            }),
          );
        return result;
      },
    };
    await expect(adoptHost(f.input, host)).rejects.toThrow("readback is unavailable or stale");
    expect(
      JSON.parse(await NodeFSP.readFile(NodePath.join(f.runtime, ADOPTION_RECEIPT), "utf8")),
    ).toMatchObject({ status: "pending" });
  }));

it("plans a bound task marker without writes or credential-drop-in reads", () =>
  fixture(
    async (f) => {
      const before = await tree(f.root);
      const preserved = new Set(f.preserveDropin.map((name) => NodePath.join(`${f.unit}.d`, name)));
      const native = await vi.importActual<typeof import("node:fs/promises")>("node:fs/promises");
      const openGuard = vi.mocked(NodeFSP.open).mockImplementation((...args) => {
        if (preserved.has(String(args[0]))) throw new Error("Credential drop-in was opened");
        return native.open(...args);
      });
      const readGuard = vi.mocked(NodeFSP.readFile).mockImplementation((...args) => {
        if (preserved.has(String(args[0]))) throw new Error("Credential drop-in was read");
        return native.readFile(...args);
      });
      try {
        const result = await adoptHost({ ...f.input, dryRun: true }, f.host);
        expect(result.plan.serviceUnit).toBe("t3code.service");
        expect(result.plan.serviceContents).toContain("Environment=T3CODE_PORT=3774");
        expect(result.plan.effects.join("\n")).toContain("Archive exact handoff");
        expect(result.plan.recovery?.join("\n")).toContain("service-state.task-handoff.json");
        expect(openGuard).toHaveBeenCalledWith(
          NodePath.join(f.runtime, "service-state.json"),
          expect.any(Number),
        );
      } finally {
        openGuard.mockImplementation(native.open);
        readGuard.mockImplementation(native.readFile);
      }
      expect(await tree(f.root)).toEqual(before);
    },
    { legacy: true },
  ));
it("archives exact task preimages and reports launcher-only evidence with retained state", () =>
  fixture(
    async (f) => {
      const originalUnit = await NodeFSP.readFile(f.unit, "utf8");
      const originalMetadata = await Promise.all(
        f.preserveDropin.map((name) =>
          LegacyBootstrap.fileMetadata(NodePath.join(`${f.unit}.d`, name)),
        ),
      );
      const result = await adoptHost(f.input, f.host);
      expect(result.state).toBe("adopted");
      const receipt = JSON.parse(
        await NodeFSP.readFile(NodePath.join(f.runtime, ADOPTION_RECEIPT), "utf8"),
      );
      expect(receipt).toMatchObject({
        status: "applied",
        attestation: "launcher-only",
        childCapability: "unattested",
        pendingMigrationPlan: { pendingUpstream: [], pendingJones: [120, 121] },
      });
      expect(receipt.capability).toBeUndefined();
      expect(
        await NodeFSP.readFile(
          NodePath.join(result.plan.archiveDirectory!, "service-state.task-handoff.json"),
          "utf8",
        ),
      ).toBe(f.stateText);
      expect(
        await NodeFSP.readFile(
          NodePath.join(result.plan.archiveDirectory!, "task-dropin.conf"),
          "utf8",
        ),
      ).toBe(f.taskText);
      expect(await NodeFSP.readFile(f.taskPath!, "utf8")).toBe(f.taskText);
      expect(await NodeFSP.readFile(f.unit, "utf8")).toBe(originalUnit);
      expect(
        await Promise.all(
          f.preserveDropin.map((name) =>
            LegacyBootstrap.fileMetadata(NodePath.join(`${f.unit}.d`, name)),
          ),
        ),
      ).toEqual(originalMetadata);
      expect(
        JSON.parse(await NodeFSP.readFile(NodePath.join(f.runtime, "service-state.json"), "utf8")),
      ).toEqual({ protocol: 4, activeVersion: oldVersion });
      expect(await NodeFSP.readFile(NodePath.join(f.base, "userdata/statev2.sqlite"), "utf8")).toBe(
        "unchanged database bytes",
      );
    },
    { legacy: true },
  ));
it.each(["digest", "extra-key", "native-keys", "native-preimage", "purpose", "missing-acceptance"])(
  "refuses invalid task binding %s before mutation",
  (kind) =>
    fixture(
      async (f) => {
        const document = JSON.parse(f.stateText!);
        if (kind === "extra-key") document.protocol = 4;
        if (kind === "native-keys") document.update.phase = "accepted";
        if (kind === "native-preimage")
          document.nativeProtocol3State = {
            protocol: 3,
            activeVersion: oldVersion,
            update: { status: "pending" },
          };
        if (kind === "purpose") document.purpose = "unknown operation";
        const text = JSON.stringify(document);
        await NodeFSP.writeFile(NodePath.join(f.runtime, "service-state.json"), text);
        const before = await tree(f.root);
        await expect(
          adoptHost(
            {
              ...f.input,
              dryRun: true,
              taskHandoffSha256: kind === "digest" ? "f".repeat(64) : sha(text),
              acceptUnattestedChildCapability: kind !== "missing-acceptance",
            },
            f.host,
          ),
        ).rejects.toThrow();
        expect(await tree(f.root)).toEqual(before);
      },
      { legacy: true },
    ),
);
it.each([
  "runtime/.restart-pending",
  "runtime/.service-stopping",
  "runtime/native-store-authority",
  "runtime/jones-updates/selections/staged.json",
  "runtime/db-backup/retained/.restore-pending",
])("refuses bootstrap hazard %s", (marker) =>
  fixture(
    async (f) => {
      const file = NodePath.join(f.base, marker);
      await NodeFSP.mkdir(NodePath.dirname(file), { recursive: true });
      if (marker === "runtime/native-store-authority") await NodeFSP.mkdir(file);
      else await NodeFSP.writeFile(file, "held");
      const before = await tree(f.root);
      await expect(adoptHost({ ...f.input, dryRun: true }, f.host)).rejects.toThrow();
      expect(await tree(f.root)).toEqual(before);
    },
    { legacy: true },
  ),
);
it("preserves an empty owned private authority directory through legacy dry-run and adoption", () =>
  fixture(
    async (f) => {
      const authority = NodePath.join(f.base, "native-store-authority");
      await NodeFSP.mkdir(authority, { mode: 0o700 });
      const before = await NodeFSP.lstat(authority);
      const treeBefore = await tree(f.root);
      expect((await adoptHost({ ...f.input, dryRun: true }, f.host)).state).toBe("dry-run");
      expect(await tree(f.root)).toEqual(treeBefore);
      const afterDryRun = await NodeFSP.lstat(authority);
      expect([afterDryRun.dev, afterDryRun.ino]).toEqual([before.dev, before.ino]);
      expect(await NodeFSP.readdir(authority)).toEqual([]);
      expect((await adoptHost(f.input, f.host)).state).toBe("adopted");
      const after = await NodeFSP.lstat(authority);
      expect([after.dev, after.ino, after.uid, after.mode & 0o777]).toEqual([
        before.dev,
        before.ino,
        f.host.uid,
        0o700,
      ]);
      expect(await NodeFSP.readdir(authority)).toEqual([]);
    },
    { legacy: true },
  ));
it.each(["state.json", "lock", "lock.sqlite", ".state.json.tmp"])(
  "refuses and preserves authority entry %s",
  (entry) =>
    fixture(
      async (f) => {
        const authority = NodePath.join(f.base, "native-store-authority");
        await NodeFSP.mkdir(authority, { mode: 0o700 });
        await NodeFSP.writeFile(NodePath.join(authority, entry), "held native authority");
        const before = await tree(f.root);
        await expect(adoptHost({ ...f.input, dryRun: true }, f.host)).rejects.toThrow();
        expect(await tree(f.root)).toEqual(before);
        expect(f.commands.some((command) => command.args.includes("stop"))).toBe(false);
      },
      { legacy: true },
    ),
);
it.each(["symlink", "file", "public-mode", "wrong-owner"] as const)(
  "refuses an unsafe empty authority placeholder: %s",
  (kind) =>
    fixture(
      async (f) => {
        const authority = NodePath.join(f.base, "native-store-authority");
        if (kind === "symlink") {
          const target = NodePath.join(f.root, "empty-authority-target");
          await NodeFSP.mkdir(target, { mode: 0o700 });
          await NodeFSP.symlink(target, authority);
        } else if (kind === "file") {
          await NodeFSP.writeFile(authority, "", { mode: 0o700 });
        } else {
          await NodeFSP.mkdir(authority, { mode: 0o700 });
          if (kind === "public-mode") await NodeFSP.chmod(authority, 0o755);
        }
        const before = await NodeFSP.lstat(authority);
        await expect(
          LegacyBootstrap.assertNoBootstrapHazards(
            f.base,
            kind === "wrong-owner" ? f.host.uid + 1 : f.host.uid,
          ),
        ).rejects.toThrow();
        const after = await NodeFSP.lstat(authority);
        expect([after.dev, after.ino, after.mode, after.uid]).toEqual([
          before.dev,
          before.ino,
          before.mode,
          before.uid,
        ]);
        if (kind === "symlink")
          expect(await NodeFSP.readlink(authority)).toBe(
            NodePath.join(f.root, "empty-authority-target"),
          );
        else if (kind === "file") expect(await NodeFSP.readFile(authority, "utf8")).toBe("");
        else expect(await NodeFSP.readdir(authority)).toEqual([]);
      },
      { legacy: true },
    ),
);
it("refuses authority state appearing during migration preflight before stopping the service", () =>
  fixture(
    async (f) => {
      const authority = NodePath.join(f.base, "native-store-authority");
      await NodeFSP.mkdir(authority, { mode: 0o700 });
      const stateBefore = await NodeFSP.readFile(
        NodePath.join(f.runtime, "service-state.json"),
        "utf8",
      );
      const unitBefore = await NodeFSP.readFile(f.unit, "utf8");
      let preflights = 0;
      const host: AdoptionHost = {
        ...f.host,
        run: async (command) => {
          const result = await f.host.run(command);
          if (command.args.includes("__service-preflight") && ++preflights === 3)
            await NodeFSP.writeFile(NodePath.join(authority, "state.json"), "late authority state");
          return result;
        },
      };
      await expect(adoptHost(f.input, host)).rejects.toThrow();
      expect(preflights).toBe(3);
      expect(f.commands.some((command) => command.args.includes("stop"))).toBe(false);
      expect(f.commands.some((command) => command.args.includes("restart"))).toBe(false);
      expect(await NodeFSP.readFile(NodePath.join(authority, "state.json"), "utf8")).toBe(
        "late authority state",
      );
      expect(await NodeFSP.readFile(NodePath.join(f.runtime, "service-state.json"), "utf8")).toBe(
        stateBefore,
      );
      expect(await NodeFSP.readFile(f.unit, "utf8")).toBe(unitBefore);
    },
    { legacy: true },
  ));
it.each([
  "ExecStartPre=/unexpected",
  "Environment=PRIVATE_TOKEN=unsupported",
  "[Unit]",
  "ExecStart=/another serve --unknown yes",
])("refuses task directive %s", (directive) =>
  fixture(
    async (f) => {
      const text = `${f.taskText}${directive}\n`;
      await NodeFSP.writeFile(f.taskPath!, text);
      await expect(
        adoptHost(
          { ...f.input, dryRun: true, taskDropin: `50-jones-code-transition.conf=${sha(text)}` },
          f.host,
        ),
      ).rejects.toThrow();
    },
    { legacy: true },
  ),
);
it.each(["pid", "binary", "descriptor", "managed", "fragment", "outside"])(
  "refuses mismatched legacy runtime proof %s",
  (mismatch) =>
    fixture(
      async (f) => {
        const host = {
          ...f.host,
          run: async (c: Parameters<AdoptionHost["run"]>[0]) => {
            if (mismatch === "pid" && c.args.includes("--property=MainPID")) return "999";
            if (mismatch === "fragment" && c.args.includes("--property=FragmentPath"))
              return "/etc/systemd/system/t3code.service";
            if (mismatch === "outside" && c.args.includes("--property=DropInPaths"))
              return "/outside/50.conf";
            return f.host.run(c);
          },
          readback: async (input: Parameters<AdoptionHost["readback"]>[0]) => ({
            ...(await f.host.readback(input)),
            ...(mismatch === "descriptor" ? { environmentId: "wrong-environment" } : {}),
            ...(mismatch === "managed" ? { serviceManaged: true } : {}),
          }),
        };
        if (mismatch === "binary") await NodeFSP.writeFile(f.directExecutable, "wrong executable");
        await expect(adoptHost({ ...f.input, dryRun: true }, host)).rejects.toThrow();
      },
      { legacy: true },
    ),
);
it("holds if preserved drop-in metadata changes before stopping", () =>
  fixture(
    async (f) => {
      const preserved = NodePath.join(`${f.unit}.d`, f.preserveDropin[0]!);
      let changed = false;
      const host = {
        ...f.host,
        run: async (c: Parameters<AdoptionHost["run"]>[0]) => {
          const result = await f.host.run(c);
          if (!changed && c.args.includes("--version")) {
            changed = true;
            await NodeFSP.appendFile(preserved, "changed");
          }
          return result;
        },
      };
      await expect(adoptHost(f.input, host)).rejects.toThrow("metadata changed");
      expect(f.commands.some((c) => c.args.includes("stop"))).toBe(false);
    },
    { legacy: true },
  ));
it("retains preimages and pending intent if migrations change during adoption", () =>
  fixture(
    async (f) => {
      await expect(adoptHost(f.input, f.host)).rejects.toThrow("Pending migration IDs changed");
      const receipt = JSON.parse(
        await NodeFSP.readFile(NodePath.join(f.runtime, ADOPTION_RECEIPT), "utf8"),
      );
      expect(receipt.status).toBe("pending");
      expect(
        await NodeFSP.readFile(
          NodePath.join(receipt.archiveDirectory, "service-state.task-handoff.json"),
          "utf8",
        ),
      ).toBe(f.stateText);
      await expect(adoptHost(f.input, f.host)).rejects.toThrow("Previous adoption");
    },
    { legacy: true, migrationChanged: true },
  ));
it("refuses a PR239-incompatible active cache without deleting its private provenance", () =>
  fixture(
    async (f) => {
      const file = NodePath.join(f.runtime, "versions", oldVersion, ".jones-provenance.json");
      await NodeFSP.writeFile(file, "retained");
      await expect(adoptHost({ ...f.input, dryRun: true }, f.host)).rejects.toThrow(
        "PR239 active runtime layout",
      );
      expect(await NodeFSP.readFile(file, "utf8")).toBe("retained");
    },
    { legacy: true },
  ));
it.each(["missing-launcher", "stale-launcher", "stale-child"])(
  "holds uncertain bootstrap receipt %s without retry",
  (kind) =>
    fixture(
      async (f) => {
        const host = {
          ...f.host,
          run: async (c: Parameters<AdoptionHost["run"]>[0]) => {
            const output = await f.host.run(c);
            if (c.args.includes("restart")) {
              const launcher = NodePath.join(f.runtime, "jones-launcher-capability.json");
              if (kind === "missing-launcher") await NodeFSP.rm(launcher);
              if (kind === "stale-launcher") {
                const receipt = JSON.parse(await NodeFSP.readFile(launcher, "utf8"));
                await NodeFSP.writeFile(launcher, JSON.stringify({ ...receipt, launcherPid: 999 }));
              }
              if (kind === "stale-child")
                await NodeFSP.writeFile(
                  NodePath.join(f.runtime, UPDATE_CAPABILITY_RECEIPT),
                  JSON.stringify({
                    schema: 1,
                    baseDir: f.base,
                    environmentId: "fixture-environment",
                    currentVersion: oldVersion,
                    processId: 12345,
                    qualifiedLauncher: true,
                    capability: { install: true },
                  }),
                  { mode: 0o600 },
                );
            }
            return output;
          },
        };
        await expect(adoptHost(f.input, host)).rejects.toThrow("readback is unavailable or stale");
        expect(
          JSON.parse(await NodeFSP.readFile(NodePath.join(f.runtime, ADOPTION_RECEIPT), "utf8"))
            .status,
        ).toBe("pending");
        await expect(adoptHost(f.input, host)).rejects.toThrow("Previous adoption");
      },
      { legacy: true },
    ),
);
it("reports observed child Install only after the retained child publishes its matching receipt", () =>
  fixture(
    async (f) => {
      const host = {
        ...f.host,
        run: async (c: Parameters<AdoptionHost["run"]>[0]) => {
          const output = await f.host.run(c);
          if (c.args.includes("restart"))
            await NodeFSP.writeFile(
              NodePath.join(f.runtime, UPDATE_CAPABILITY_RECEIPT),
              JSON.stringify({
                schema: 1,
                baseDir: f.base,
                environmentId: "fixture-environment",
                currentVersion: oldVersion,
                processId: 12346,
                qualifiedLauncher: true,
                capability: { install: true },
              }),
              { mode: 0o600 },
            );
          return output;
        },
      };
      await adoptHost(f.input, host);
      expect(
        JSON.parse(await NodeFSP.readFile(NodePath.join(f.runtime, ADOPTION_RECEIPT), "utf8")),
      ).toMatchObject({ attestation: "child", capability: { install: true } });
    },
    { legacy: true },
  ));
