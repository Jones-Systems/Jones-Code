// @effect-diagnostics nodeBuiltinImport:off - Real isolated stage files with injected native mount/copy boundaries; never mounts a DMG or launches an app.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import {
  validateJonesStagedArtifact,
  type JonesStagedArtifact,
} from "@t3tools/shared/jones/jonesActions";
import { afterEach, expect, it, vi } from "vite-plus/test";
import { stageJonesMacApp } from "./jonesMacStaging.ts";

const failures = vi.hoisted(() => ({
  sync: false,
  receipt: false,
  index: false,
  publishedIndex: false,
  cleanup: false,
  foreignDevice: false,
}));

const mountedSource = vi.hoisted(() => ({
  invalid: undefined as undefined | "escaping-link" | "dangling-link" | "type" | "count" | "bytes",
  streams: 0,
  copiedStreams: 0,
}));

const remnantBoundary = vi.hoisted(() => ({
  path: undefined as string | undefined,
  kind: undefined as "device" | "owner" | undefined,
}));

vi.mock("node:child_process", async (original) => ({
  ...(await original<typeof import("node:child_process")>()),
  execFile: vi.fn(),
}));
vi.mock("@t3tools/shared/jones/jonesActions", async (original) => ({
  ...(await original<typeof import("@t3tools/shared/jones/jonesActions")>()),
  // Archive qualification is covered by jonesActions.test; this fixture starts at native staging.
  validateJonesStagedArtifact: vi.fn(async () => undefined),
}));
vi.mock("node:fs", async (original) => {
  const actual = await original<typeof import("node:fs")>();
  return {
    ...actual,
    createReadStream: (...args: Parameters<typeof actual.createReadStream>) => {
      if (typeof args[0] === "string") {
        if (args[0].includes("/mount/")) mountedSource.streams++;
        else if (args[0].includes(".app/")) mountedSource.copiedStreams++;
      }
      return actual.createReadStream(...args);
    },
    promises: {
      ...actual.promises,
      readdir: async (...args: Parameters<typeof actual.promises.readdir>) => {
        const entries = await actual.promises.readdir(...args);
        if (
          mountedSource.invalid === "count" &&
          typeof args[0] === "string" &&
          args[0].endsWith("/mount/Candidate.app/Contents/MacOS")
        ) {
          const entry = entries[0]!;
          // Enumerate past the bound without allocating another large array or writing files.
          entries[Symbol.iterator] = function* () {
            for (let i = 0; i < 200001; i++) yield entry;
            return undefined;
          };
        }
        return entries;
      },
      lstat: async (...args: Parameters<typeof actual.promises.lstat>) => {
        const info = await actual.promises.lstat(...args);
        if (
          typeof args[0] === "string" &&
          args[0].endsWith("/mount/Candidate.app/Contents/MacOS/Jones")
        ) {
          if (mountedSource.invalid === "type") return Object.assign(info, { isFile: () => false });
          // The next ordinary file pushes the aggregate above 8 GiB; no large payload is created.
          if (mountedSource.invalid === "bytes")
            return Object.assign(info, { size: 8 * 1024 * 1024 * 1024 });
        }
        if (failures.foreignDevice && typeof args[0] === "string" && args[0].includes(".app/"))
          return Object.assign(info, { dev: Number(info.dev) + 1 });
        return info;
      },
      rm: async (...args: Parameters<typeof actual.promises.rm>) => {
        if (failures.cleanup) throw new Error("Injected attempt cleanup failure");
        return actual.promises.rm(...args);
      },
      open: async (...args: Parameters<typeof actual.promises.open>) => {
        const handle = await actual.promises.open(...args);
        const sync = handle.sync.bind(handle);
        handle.sync = async () => {
          if (failures.sync && typeof args[0] === "string" && args[0].includes(".app/")) {
            throw new Error("Injected app payload sync failure");
          }
          await sync();
        };
        return handle;
      },
    },
  };
});
vi.mock("node:fs/promises", async (original) => {
  const actual = await original<typeof import("node:fs/promises")>();
  return {
    ...actual,
    lstat: async (...args: Parameters<typeof actual.lstat>) => {
      const info = await actual.lstat(...args);
      if (args[0] === remnantBoundary.path) {
        if (remnantBoundary.kind === "device")
          return Object.assign(info, { dev: Number(info.dev) + 1 });
        if (remnantBoundary.kind === "owner")
          return Object.assign(info, { uid: Number(info.uid) + 1 });
      }
      return info;
    },
    open: async (...args: Parameters<typeof actual.open>) => {
      if (
        failures.receipt &&
        typeof args[0] === "string" &&
        args[0].endsWith("mac-app-receipt.json")
      )
        throw new Error("Injected receipt failure");
      return actual.open(...args);
    },
    link: async (...args: Parameters<typeof actual.link>) => {
      if (failures.index) throw new Error("Injected index failure");
      const result = await actual.link(...args);
      if (failures.publishedIndex) throw new Error("Injected error after index publication");
      return result;
    },
    readFile: (
      path: Parameters<typeof actual.readFile>[0],
      options: Parameters<typeof actual.readFile>[1],
    ) =>
      actual.readFile(
        typeof path === "string" && path.endsWith("/app.asar/package.json")
          ? path.replace(/app\.asar\/package\.json$/, "package.fixture.json")
          : path,
        options,
      ),
  };
});

afterEach(() => {
  vi.mocked(NodeChildProcess.execFile).mockReset();
  for (const key of Object.keys(failures) as Array<keyof typeof failures>) failures[key] = false;
  mountedSource.invalid = undefined;
  mountedSource.streams = 0;
  mountedSource.copiedStreams = 0;
  remnantBoundary.path = undefined;
  remnantBoundary.kind = undefined;
  vi.mocked(validateJonesStagedArtifact).mockClear();
});

type FailurePhase =
  | "attach"
  | "copy"
  | "metadata"
  | "sync"
  | "detach"
  | "mounted"
  | "receipt"
  | "index"
  | "publishedIndex"
  | "cleanup"
  | "foreignDevice"
  | "writer";

async function fixture(
  run: (input: {
    root: string;
    artifact: JonesStagedArtifact;
    fail: (...phases: FailurePhase[]) => void;
  }) => Promise<void>,
) {
  const allocated = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-mac-stage-test-"));
  try {
    const root = await NodeFSP.realpath(allocated);
    const source = "b".repeat(40);
    const tree = "c".repeat(40);
    const version = "1.0.0-preview.20261010.11.1";
    const artifact: JonesStagedArtifact = {
      schema: 1,
      source: "jones-actions",
      channel: "jones-main",
      stagedHandle: "d".repeat(64),
      payloadPath: NodePath.join(root, "qualified.dmg"),
      candidate: {
        schema: 1,
        repository: "Jones-Systems/Jones-Code",
        source,
        tree,
        installedSource: "a".repeat(40),
        workflow: ".github/workflows/artifact-desktop-mac.yml",
        workflowId: 1,
        runId: 11,
        runAttempt: 1,
        ciRunId: 12,
        artifactId: 13,
        artifactName: "fixture",
        artifactDigest: "e".repeat(64),
        artifactBytes: 10,
        expiresAt: "2030-01-01T00:00:00Z",
        version,
        platform: "darwin",
        architecture: "arm64",
      },
      receipt: {
        schema: 1,
        repository: "Jones-Systems/Jones-Code",
        source,
        tree,
        workflow: ".github/workflows/artifact-desktop-mac.yml",
        event: "push",
        ref: "refs/heads/main",
        runId: "11",
        runAttempt: "1",
        version,
        platform: "darwin",
        architecture: "arm64",
        artifact: "qualified.dmg",
        sha256: "f".repeat(64),
      },
    };
    const stageRoot = NodePath.join(root, "apps");
    await NodeFSP.mkdir(stageRoot);
    let fail = new Set<FailurePhase>();
    vi.mocked(NodeChildProcess.execFile).mockImplementation(
      (command, rawArgs, _options, callback) => {
        if (typeof callback !== "function") throw new Error("Missing native callback.");
        const args = rawArgs as string[];
        const child = { pid: 1234, exitCode: null as number | null, signalCode: null };
        void (async () => {
          if (command === "/usr/bin/hdiutil" && args[0] === "attach") {
            if (fail.has("attach")) throw new Error("Injected attach failure");
            const mount = args[args.indexOf("-mountpoint") + 1]!;
            const app = NodePath.join(mount, "Candidate.app");
            await NodeFSP.mkdir(NodePath.join(app, "Contents", "MacOS"), { recursive: true });
            await NodeFSP.mkdir(NodePath.join(app, "Contents", "Resources"));
            await NodeFSP.writeFile(NodePath.join(app, "Contents", "MacOS", "Jones"), "executable");
            await NodeFSP.writeFile(
              NodePath.join(app, "Contents", "Resources", "app.asar"),
              "archive",
            );
            await NodeFSP.writeFile(
              NodePath.join(app, "Contents", "Resources", "package.fixture.json"),
              JSON.stringify({
                version,
                startupGateProtocol: 1,
                jonesSource: { repository: "Jones-Systems/Jones-Code", sha: source, tree },
              }),
            );
            for (const directory of ["Contents", "Contents/MacOS", "Contents/Resources"])
              await NodeFSP.chmod(NodePath.join(app, directory), 0o755);
            for (const file of [
              "Contents/MacOS/Jones",
              "Contents/Resources/app.asar",
              "Contents/Resources/package.fixture.json",
            ])
              await NodeFSP.chmod(NodePath.join(app, file), 0o644);
            if (mountedSource.invalid === "escaping-link")
              await NodeFSP.symlink("..", NodePath.join(app, "invalid-link"));
            if (mountedSource.invalid === "dangling-link")
              await NodeFSP.symlink("missing", NodePath.join(app, "invalid-link"));
            return "";
          }
          if (command === "/usr/bin/ditto") {
            await NodeFSP.cp(args[0]!, args[1]!, { recursive: true });
            if (fail.has("writer")) throw new Error("Injected copier with unknown exit");
            if (fail.has("copy")) throw new Error("Injected interrupted copy");
            return "";
          }
          if (command === "/usr/bin/plutil") {
            if (fail.has("metadata")) throw new Error("Injected validation failure");
            return JSON.stringify({
              CFBundleExecutable: "Jones",
              CFBundleIdentifier: "com.jones.code",
              CFBundleShortVersionString: version,
            });
          }
          if (command === "/usr/bin/lipo") return "arm64";
          if (command === "/usr/bin/hdiutil" && args[0] === "detach") {
            if (fail.has("detach")) throw new Error("Injected detach failure");
            if (fail.has("mounted")) return "";
            await NodeFSP.rm(args[1]!, { recursive: true });
            return "";
          }
          throw new Error(`Unexpected native command ${command}`);
        })().then(
          (stdout) => {
            child.exitCode = 0;
            callback(null, stdout, "");
          },
          (cause: Error) => {
            if (!(command === "/usr/bin/ditto" && fail.has("writer"))) child.exitCode = 1;
            callback(cause, "", "");
          },
        );
        return child as ReturnType<typeof NodeChildProcess.execFile>;
      },
    );
    await run({
      root: stageRoot,
      artifact,
      fail: (...phases) => {
        fail = new Set(phases);
        for (const key of Object.keys(failures) as Array<keyof typeof failures>)
          failures[key] = fail.has(key);
      },
    });
  } finally {
    await NodeFSP.rm(allocated, { recursive: true, force: true });
    await expect(NodeFSP.lstat(allocated)).rejects.toMatchObject({ code: "ENOENT" });
  }
}

async function receiptOnlyRemnant(
  root: string,
  artifact: JonesStagedArtifact,
  ordinal = 1,
  handle = ordinal.toString(16).padStart(64, "0"),
) {
  const attempt = NodePath.join(root, "attempts", `stage-remnant${ordinal}`);
  const directory = NodePath.join(attempt, handle);
  const completionPath = NodePath.join(attempt, "completion.json");
  const receiptPath = NodePath.join(directory, "mac-app-receipt.json");
  const appPath = NodePath.join(directory, "Candidate.app");
  const completion = { schema: 1, handle, directory };
  const receipt = {
    app: {
      handle,
      receiptPath,
      appPath,
      executablePath: NodePath.join(appPath, "Contents", "MacOS", "Jones"),
      version: artifact.candidate.version,
      sourceSha: artifact.candidate.source,
      sourceTree: artifact.candidate.tree,
      appDigest: "a".repeat(64),
      asarDigest: "b".repeat(64),
      executableDigest: "c".repeat(64),
    },
    artifact,
    candidate: artifact.candidate,
  };
  await NodeFSP.mkdir(directory, { recursive: true });
  await NodeFSP.writeFile(completionPath, JSON.stringify(completion) + "\n");
  await NodeFSP.writeFile(receiptPath, JSON.stringify(receipt) + "\n");
  return { handle, attempt, directory, completionPath, receiptPath, appPath, completion, receipt };
}

async function snapshotTree(root: string): Promise<unknown> {
  const info = await NodeFSP.lstat(root);
  if (info.isSymbolicLink()) return { link: await NodeFSP.readlink(root) };
  if (info.isFile()) return { bytes: (await NodeFSP.readFile(root)).toString("base64") };
  return Object.fromEntries(
    await Promise.all(
      (await NodeFSP.readdir(root))
        .sort()
        .map(async (name) => [name, await snapshotTree(NodePath.join(root, name))]),
    ),
  );
}

it("admits a new candidate after four completed receipt-only remnants without changing their bytes", () =>
  fixture(async ({ root, artifact }) => {
    const remnants = [];
    for (let ordinal = 1; ordinal <= 4; ordinal++)
      remnants.push(await receiptOnlyRemnant(root, artifact, ordinal));
    const before = await Promise.all(remnants.map(({ attempt }) => snapshotTree(attempt)));
    const admit = vi.fn(async () => {
      expect(validateJonesStagedArtifact).toHaveBeenCalledTimes(1);
      expect(vi.mocked(NodeChildProcess.execFile)).not.toHaveBeenCalled();
      expect(await NodeFSP.readdir(NodePath.join(root, "attempts"))).toHaveLength(4);
    });
    const app = await stageJonesMacApp(artifact, root, "darwin", admit);
    expect(app.handle).toBe(artifact.stagedHandle);
    expect(admit).toHaveBeenCalledTimes(1);
    expect(vi.mocked(NodeChildProcess.execFile)).toHaveBeenCalled();
    expect(await Promise.all(remnants.map(({ attempt }) => snapshotTree(attempt)))).toEqual(
      before,
    );
    expect(await NodeFSP.readdir(NodePath.join(root, "completed"))).toEqual([
      `${artifact.stagedHandle}.json`,
    ]);
    const cachedAdmission = vi.fn();
    const nativeCalls = vi.mocked(NodeChildProcess.execFile).mock.calls.length;
    expect(await stageJonesMacApp(artifact, root, "darwin", cachedAdmission)).toEqual(app);
    expect(cachedAdmission).not.toHaveBeenCalled();
    expect(vi.mocked(NodeChildProcess.execFile).mock.calls).toHaveLength(nativeCalls);
  }));

it.each([
  "missing-completion",
  "malformed-completion",
  "completion-schema",
  "completion-handle",
  "completion-directory",
  "completion-size",
  "completion-link",
  "receipt-link",
  "payload-link",
  "attempt-link",
  "receipt-handle",
  "receipt-path",
  "receipt-app-parent",
  "malformed-receipt",
  "receipt-size",
  "residual-app",
  "residual-mount",
  "extra-payload",
  "extra-attempt",
  "conflicting-index",
  "conflicting-legacy",
  "completion-device",
  "completion-owner",
  "payload-device",
  "payload-owner",
  "receipt-device",
  "receipt-owner",
  "attempt-device",
  "attempt-owner",
] as const)("holds ambiguous receipt-only remnant %s before admission or native work", (shape) =>
  fixture(async ({ root, artifact }) => {
    const remnant = await receiptOnlyRemnant(root, artifact);
    const { attempt, directory, completionPath, receiptPath, receipt, completion } = remnant;
    if (shape === "missing-completion") await NodeFSP.unlink(completionPath);
    else if (shape === "malformed-completion") await NodeFSP.writeFile(completionPath, "{");
    else if (shape === "completion-schema")
      await NodeFSP.writeFile(completionPath, JSON.stringify({ ...completion, schema: 2 }));
    else if (shape === "completion-handle")
      await NodeFSP.writeFile(
        completionPath,
        JSON.stringify({ ...completion, handle: "f".repeat(64) }),
      );
    else if (shape === "completion-directory")
      await NodeFSP.writeFile(
        completionPath,
        JSON.stringify({ ...completion, directory: `${directory}/../${remnant.handle}` }),
      );
    else if (shape === "completion-size") await NodeFSP.writeFile(completionPath, " ".repeat(4097));
    else if (["completion-link", "receipt-link", "payload-link", "attempt-link"].includes(shape)) {
      const path =
        shape === "completion-link"
          ? completionPath
          : shape === "receipt-link"
            ? receiptPath
            : shape === "payload-link"
              ? directory
              : attempt;
      const moved = NodePath.join(NodePath.dirname(root), "retained-link-target");
      await NodeFSP.rename(path, moved);
      await NodeFSP.symlink(moved, path);
    } else if (shape === "receipt-handle") {
      receipt.app.handle = "f".repeat(64);
      await NodeFSP.writeFile(receiptPath, JSON.stringify(receipt));
    } else if (shape === "receipt-path") {
      receipt.app.receiptPath = NodePath.join(root, "other-receipt.json");
      await NodeFSP.writeFile(receiptPath, JSON.stringify(receipt));
    } else if (shape === "receipt-app-parent") {
      receipt.app.appPath = NodePath.join(root, "Other.app");
      await NodeFSP.writeFile(receiptPath, JSON.stringify(receipt));
    } else if (shape === "malformed-receipt") await NodeFSP.writeFile(receiptPath, "{}");
    else if (shape === "receipt-size") await NodeFSP.writeFile(receiptPath, " ".repeat(131073));
    else if (shape === "residual-app") await NodeFSP.mkdir(remnant.appPath);
    else if (shape === "residual-mount") await NodeFSP.mkdir(NodePath.join(directory, "mount"));
    else if (shape === "extra-payload")
      await NodeFSP.writeFile(NodePath.join(directory, "foreign"), "keep");
    else if (shape === "extra-attempt") await NodeFSP.mkdir(NodePath.join(attempt, "mount"));
    else if (shape === "conflicting-index") {
      const other = await receiptOnlyRemnant(root, artifact, 2, remnant.handle);
      await NodeFSP.mkdir(NodePath.join(root, "completed"));
      await NodeFSP.writeFile(
        NodePath.join(root, "completed", `${remnant.handle}.json`),
        JSON.stringify({ ...completion, directory: other.directory }),
      );
    } else if (shape === "conflicting-legacy")
      await NodeFSP.mkdir(NodePath.join(root, remnant.handle));
    else {
      remnantBoundary.path = shape.startsWith("completion-")
        ? completionPath
        : shape.startsWith("payload-")
          ? directory
          : shape.startsWith("receipt-")
            ? receiptPath
            : attempt;
      remnantBoundary.kind = shape.endsWith("device") ? "device" : "owner";
    }
    const before = await snapshotTree(root);
    const admit = vi.fn();
    await expect(stageJonesMacApp(artifact, root, "darwin", admit)).rejects.toThrow(
      "incomplete or unknown Mac stage",
    );
    expect(admit).not.toHaveBeenCalled();
    expect(vi.mocked(NodeChildProcess.execFile)).not.toHaveBeenCalled();
    expect(await snapshotTree(root)).toEqual(before);
  }),
);

it("does not allocate an attempt or start native work while admission is pending or denied", () =>
  fixture(async ({ root, artifact }) => {
    const admission = Promise.withResolvers<void>();
    const requested = Promise.withResolvers<void>();
    const pending = stageJonesMacApp(artifact, root, "darwin", () => {
      requested.resolve();
      return admission.promise;
    });
    const failure = expect(pending).rejects.toThrow("denied");
    await requested.promise;
    expect(await NodeFSP.readdir(root)).toEqual([]);
    expect(vi.mocked(NodeChildProcess.execFile)).not.toHaveBeenCalled();
    admission.reject(new Error("denied"));
    await failure;
    expect(await NodeFSP.readdir(root)).toEqual([]);
    expect(vi.mocked(NodeChildProcess.execFile)).not.toHaveBeenCalled();
  }));

it("fails artifact qualification before admission or attempt allocation", () =>
  fixture(async ({ root, artifact }) => {
    vi.mocked(validateJonesStagedArtifact).mockRejectedValueOnce(new Error("artifact changed"));
    const admit = vi.fn();
    await expect(stageJonesMacApp(artifact, root, "darwin", admit)).rejects.toThrow(
      "artifact changed",
    );
    expect(admit).not.toHaveBeenCalled();
    expect(vi.mocked(NodeChildProcess.execFile)).not.toHaveBeenCalled();
    expect(await NodeFSP.readdir(root)).toEqual([]);
  }));

it("fails completed-cache integrity before admission or new native work", () =>
  fixture(async ({ root, artifact }) => {
    const app = await stageJonesMacApp(artifact, root, "darwin");
    await NodeFSP.writeFile(app.executablePath, "changed");
    const admit = vi.fn();
    const before = await snapshotTree(root);
    const nativeCalls = vi.mocked(NodeChildProcess.execFile).mock.calls.length;
    await expect(stageJonesMacApp(artifact, root, "darwin", admit)).rejects.toThrow(
      "Staged app integrity changed",
    );
    expect(admit).not.toHaveBeenCalled();
    expect(vi.mocked(NodeChildProcess.execFile).mock.calls).toHaveLength(nativeCalls);
    expect(await snapshotTree(root)).toEqual(before);
  }));

it("validates mounted structure without reading source contents and preserves copied hashes", () =>
  fixture(async ({ root, artifact }) => {
    const app = await stageJonesMacApp(artifact, root, "darwin");
    expect(app.appDigest).toBe("ad6c5a6ce2026828e152bd1ffe3ffc59459e4eaaf06879707b725f9378f5af69");
    expect(mountedSource.streams).toBe(0);
    expect(mountedSource.copiedStreams).toBe(8);
    expect(app.startupGateProtocol).toBe(1);
  }));

it.each([
  ["escaping-link", "App symlink escapes the staged app."],
  ["dangling-link", "ENOENT"],
  ["type", "Unexpected app payload file type."],
  ["count", "The native app exceeds its file-count bound."],
  ["bytes", "The native app exceeds its payload-size bound."],
] as const)("refuses mounted-source %s before invoking the copier", (invalid, message) =>
  fixture(async ({ root, artifact }) => {
    mountedSource.invalid = invalid;
    await expect(stageJonesMacApp(artifact, root, "darwin")).rejects.toThrow(message);
    expect(
      vi
        .mocked(NodeChildProcess.execFile)
        .mock.calls.some(([command]) => command === "/usr/bin/ditto"),
    ).toBe(false);
    expect(mountedSource.streams).toBe(0);
    expect(mountedSource.copiedStreams).toBe(0);
    expect(await NodeFSP.readdir(NodePath.join(root, "attempts"))).toEqual([]);
  }),
);

it.each(["attach", "copy", "metadata", "sync", "receipt", "index"] as const)(
  "reclaims the exact failed app attempt after %s failure before retrying",
  (phase) =>
    fixture(async ({ root, artifact, fail }) => {
      fail(phase);
      for (let retry = 0; retry < 3; retry++) {
        await expect(stageJonesMacApp(artifact, root, "darwin")).rejects.toThrow();
        expect(await NodeFSP.readdir(NodePath.join(root, "attempts"))).toEqual([]);
        await expect(
          NodeFSP.lstat(NodePath.join(root, "completed", `${artifact.stagedHandle}.json`)),
        ).rejects.toMatchObject({ code: "ENOENT" });
      }
      fail();
      const result = await stageJonesMacApp(artifact, root, "darwin");
      expect(result.handle).toBe(artifact.stagedHandle);
      expect(result.startupGateProtocol).toBe(1);
      expect(await NodeFSP.readdir(NodePath.join(root, "attempts"))).toHaveLength(1);
      const commandCount = vi.mocked(NodeChildProcess.execFile).mock.calls.length;
      expect(await stageJonesMacApp(artifact, root, "darwin")).toEqual(result);
      expect(vi.mocked(NodeChildProcess.execFile).mock.calls).toHaveLength(commandCount);
    }),
);

it.each(["detach", "mounted", "cleanup", "foreignDevice", "writer"] as const)(
  "preserves an attempt with unproved %s cleanup and blocks further allocation",
  (phase) =>
    fixture(async ({ root, artifact, fail }) => {
      fail(phase, "copy");
      const failure = await stageJonesMacApp(artifact, root, "darwin").catch(
        (cause: unknown) => cause,
      );
      expect(failure).toBeInstanceOf(AggregateError);
      expect((failure as AggregateError).errors[0]).toMatchObject({
        message: expect.stringContaining("ditto"),
      });
      if (phase === "detach")
        expect((failure as AggregateError).errors).toEqual([
          expect.objectContaining({ message: expect.stringContaining("ditto") }),
          expect.objectContaining({ message: expect.stringContaining("hdiutil") }),
        ]);
      const retained = await NodeFSP.readdir(NodePath.join(root, "attempts"));
      expect(retained).toHaveLength(1);
      const commandCount = vi.mocked(NodeChildProcess.execFile).mock.calls.length;
      fail();
      for (let retry = 0; retry < 3; retry++)
        await expect(stageJonesMacApp(artifact, root, "darwin")).rejects.toThrow(
          "incomplete or unknown Mac stage",
        );
      expect(await NodeFSP.readdir(NodePath.join(root, "attempts"))).toEqual(retained);
      expect(vi.mocked(NodeChildProcess.execFile).mock.calls).toHaveLength(commandCount);
    }),
);

it("keeps a completion whose publication response was lost", () =>
  fixture(async ({ root, artifact, fail }) => {
    fail("publishedIndex");
    await expect(stageJonesMacApp(artifact, root, "darwin")).rejects.toThrow("index publication");
    const retained = await NodeFSP.readdir(NodePath.join(root, "attempts"));
    expect(retained).toHaveLength(1);
    fail();
    const result = await stageJonesMacApp(artifact, root, "darwin");
    expect(result.appPath).toContain(`${retained[0]}/`);
    expect(await NodeFSP.readdir(NodePath.join(root, "attempts"))).toEqual(retained);
  }));

it("preserves the completed app while a different candidate repeatedly fails", () =>
  fixture(async ({ root, artifact, fail }) => {
    const selected = await stageJonesMacApp(artifact, root, "darwin");
    const before = await NodeFSP.readdir(NodePath.join(root, "attempts"));
    fail("metadata");
    for (let retry = 0; retry < 3; retry++)
      await expect(
        stageJonesMacApp({ ...artifact, stagedHandle: "e".repeat(64) }, root, "darwin"),
      ).rejects.toThrow();
    expect(await NodeFSP.readdir(NodePath.join(root, "attempts"))).toEqual(before);
    fail();
    expect(await stageJonesMacApp(artifact, root, "darwin")).toEqual(selected);
  }));

it("holds foreign incomplete attempts without touching their payloads", () =>
  fixture(async ({ root, artifact }) => {
    const unknown = NodePath.join(root, "attempts", "stage-foreign", "Other.app");
    await NodeFSP.mkdir(unknown, { recursive: true });
    await NodeFSP.writeFile(NodePath.join(unknown, "keep"), "foreign");
    await expect(stageJonesMacApp(artifact, root, "darwin")).rejects.toThrow(
      "incomplete or unknown Mac stage",
    );
    expect(await NodeFSP.readFile(NodePath.join(unknown, "keep"), "utf8")).toBe("foreign");
    expect(vi.mocked(NodeChildProcess.execFile)).not.toHaveBeenCalled();
  }));
