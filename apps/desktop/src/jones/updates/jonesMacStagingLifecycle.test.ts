// @effect-diagnostics nodeBuiltinImport:off - Real isolated stage files with injected native mount/copy boundaries; never mounts a DMG or launches an app.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import type { JonesStagedArtifact } from "@t3tools/shared/jones/jonesActions";
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
