// @effect-diagnostics nodeBuiltinImport:off -- Synthetic native command fixture, with exact-root cleanup.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeChildProcess from "node:child_process";
import { describe, expect, it, vi } from "vite-plus/test";
import {
  initializeNativeStoreAuthority,
  nativeStoreAuthorityPaths,
  readNativeStoreAuthorityState,
} from "../../environment/nativeStoreAuthorityPersistence.ts";

vi.mock("node:fs", async (importOriginal) => {
  const fileSystem = await importOriginal<typeof NodeFS>();
  return { ...fileSystem, lstatSync: vi.fn(fileSystem.lstatSync) };
});
vi.mock("../cloud/qualifiedRuntime.ts", { spy: true });

vi.mock("../../../package.json", () => ({
  default: {
    version: "1.2.3",
    jonesSource: {
      repository: "Jones-Systems/Jones-Code",
      sha: "a".repeat(40),
      tree: "b".repeat(40),
    },
  },
}));
import * as QualifiedRuntime from "../cloud/qualifiedRuntime.ts";
import { executeNativeAuthorityTransition } from "./nativeAuthorityTransition.ts";

const rawFileSystem = await vi.importActual<typeof NodeFS>("node:fs");

const fixture = (run: (request: Record<string, unknown>, root: string) => void): void => {
  const root = NodeFS.realpathSync(
    NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-native-transition-")),
  );
  const argv = process.argv;
  const executable = Object.getOwnPropertyDescriptor(process, "execPath")!;
  const override = process.env.T3CODE_NATIVE_AUTHORITY_STATE_DIR;
  try {
    delete process.env.T3CODE_NATIVE_AUTHORITY_STATE_DIR;
    const baseDir = NodePath.join(root, "home");
    NodeFS.mkdirSync(NodePath.join(baseDir, "userdata"), { recursive: true, mode: 0o700 });
    NodeFS.writeFileSync(NodePath.join(baseDir, "userdata/environment-id"), "native-transition\n", {
      mode: 0o600,
    });
    const databasePath = NodePath.join(baseDir, "userdata/state.sqlite");
    NodeFS.writeFileSync(databasePath, "SQLite format 3\0", { mode: 0o600 });
    const contents = NodePath.join(root, "Jones.app/Contents");
    NodeFS.mkdirSync(NodePath.join(contents, "MacOS"), { recursive: true, mode: 0o700 });
    NodeFS.mkdirSync(NodePath.join(contents, "Resources"), { mode: 0o700 });
    const binary = NodePath.join(contents, "MacOS/Jones");
    NodeFS.writeFileSync(binary, "synthetic executable", { mode: 0o700 });
    NodeFS.writeFileSync(NodePath.join(contents, "Resources/app.asar"), "synthetic archive", {
      mode: 0o600,
    });
    Object.defineProperty(process, "execPath", { ...executable, value: binary });
    process.argv = [
      binary,
      NodePath.join(contents, "Resources/app.asar/apps/server/dist/bin.mjs"),
      "__jones-native-authority-transition",
    ];
    run(
      {
        protocol: 1,
        operation: "fence",
        baseDir,
        databasePath,
        environmentId: "native-transition",
        candidate: { version: "1.2.3", sourceSha: "a".repeat(40), sourceTree: "b".repeat(40) },
      },
      root,
    );
  } finally {
    process.argv = argv;
    Object.defineProperty(process, "execPath", executable);
    if (override === undefined) delete process.env.T3CODE_NATIVE_AUTHORITY_STATE_DIR;
    else process.env.T3CODE_NATIVE_AUTHORITY_STATE_DIR = override;
    NodeFS.rmSync(root, { recursive: true, force: true });
  }
};
const execute = (request: unknown) =>
  executeNativeAuthorityTransition(Buffer.from(JSON.stringify(request)));

describe("bounded native authority command", () => {
  it("uses the stamped backend and persisted identity to fence then advance", () =>
    fixture((request) => {
      const directory = NodePath.join(request.baseDir as string, "native-store-authority");
      const initial = initializeNativeStoreAuthority(directory, "native-transition");
      const result = execute(request) as { guard: unknown };
      expect(readNativeStoreAuthorityState(directory).state).toBe("fenced");
      expect(execute({ ...request, operation: "advance", guard: result.guard })).toEqual({
        protocol: 1,
        operation: "advance",
        state: "advanced",
        authorityNamespace: initial.authority_namespace,
        storeGeneration: 2,
      });
      expect(() => execute({ ...request, operation: "advance", guard: result.guard })).toThrow();
    }));

  it("checks the raw archive when Electron exposes its virtual directory", () =>
    fixture((request, root) => {
      const archive = NodePath.join(root, "Jones.app/Contents/Resources/app.asar");
      const virtualDirectory = NodeFS.lstatSync(NodePath.dirname(archive));
      const patchedStat = vi
        .spyOn(NodeFS, "lstatSync")
        .mockImplementation((...args) =>
          args[0] === archive ? virtualDirectory : rawFileSystem.lstatSync(...args),
        );
      const selectRaw = vi
        .spyOn(QualifiedRuntime, "bundleFileSystem")
        .mockReturnValue(rawFileSystem);
      try {
        expect(NodeFS.lstatSync(archive).isDirectory()).toBe(true);
        expect(rawFileSystem.lstatSync(archive).isFile()).toBe(true);
        const directory = NodePath.join(request.baseDir as string, "native-store-authority");
        initializeNativeStoreAuthority(directory, "native-transition");
        const result = execute(request) as { guard: unknown };
        expect(readNativeStoreAuthorityState(directory).state).toBe("fenced");
        expect(execute({ ...request, operation: "advance", guard: result.guard })).toMatchObject({
          state: "advanced",
          storeGeneration: 2,
        });
        expect(selectRaw).toHaveBeenCalledTimes(2);
      } finally {
        patchedStat.mockRestore();
        selectRaw.mockRestore();
      }
    }));

  it("preserves record absence across both operations", () =>
    fixture((request) => {
      expect(execute(request)).toEqual({
        protocol: 1,
        operation: "fence",
        guard: { kind: "absent" },
      });
      expect(execute({ ...request, operation: "advance", guard: { kind: "absent" } })).toEqual({
        protocol: 1,
        operation: "advance",
        state: "absent",
      });
      expect(
        NodeFS.existsSync(
          nativeStoreAuthorityPaths(
            NodePath.join(request.baseDir as string, "native-store-authority"),
          ).statePath,
        ),
      ).toBe(false);
    }));

  it.each([
    { protocol: 2 },
    { extra: true },
    { guard: { kind: "absent" } },
    { operation: "advance" },
    { environmentId: "wrong" },
    { databasePath: "/other/state.sqlite" },
    { baseDir: "relative" },
    { candidate: { version: "other", sourceSha: "a".repeat(40), sourceTree: "b".repeat(40) } },
    { candidate: { version: "1.2.3", sourceSha: "c".repeat(40), sourceTree: "b".repeat(40) } },
    { candidate: { version: "1.2.3", sourceSha: "a".repeat(40), sourceTree: "c".repeat(40) } },
  ])("refuses unbound requests before authority creation: %j", (change) =>
    fixture((request) => {
      expect(() => execute({ ...request, ...change })).toThrow();
      expect(
        NodeFS.existsSync(NodePath.join(request.baseDir as string, "native-store-authority")),
      ).toBe(false);
    }),
  );

  it.each(["{", "{} trailing", " ".repeat(16385)])("rejects malformed or oversized JSON", (raw) =>
    fixture(() => {
      expect(() => executeNativeAuthorityTransition(Buffer.from(raw))).toThrow();
      expect(() => executeNativeAuthorityTransition(Uint8Array.of(0xff))).toThrow();
    }),
  );

  it.each([
    "argument",
    "backend",
    "binary",
    "archive",
    "archive-directory",
    "archive-mode",
    "base",
    "override",
  ])("refuses unsafe %s", (fault) =>
    fixture((request, root) => {
      if (fault === "argument") process.argv.push("extra");
      if (fault === "backend") process.argv[1] = NodePath.join(root, "shadow/bin.mjs");
      if (fault === "binary") NodeFS.chmodSync(process.execPath, 0o777);
      if (fault === "archive") {
        const archive = NodePath.join(root, "Jones.app/Contents/Resources/app.asar");
        NodeFS.unlinkSync(archive);
        NodeFS.symlinkSync(request.databasePath as string, archive);
      }
      if (fault === "archive-directory" || fault === "archive-mode") {
        const archive = NodePath.join(root, "Jones.app/Contents/Resources/app.asar");
        if (fault === "archive-directory") {
          NodeFS.unlinkSync(archive);
          NodeFS.mkdirSync(archive, { mode: 0o700 });
        } else NodeFS.chmodSync(archive, 0o666);
      }
      if (fault === "base") NodeFS.chmodSync(request.baseDir as string, 0o777);
      if (fault === "override")
        process.env.T3CODE_NATIVE_AUTHORITY_STATE_DIR = NodePath.join(root, "other");
      expect(() => execute(request)).toThrow();
      expect(
        NodeFS.existsSync(NodePath.join(request.baseDir as string, "native-store-authority")),
      ).toBe(false);
    }),
  );

  it("dispatches malformed stdin without starting the ordinary CLI and emits only a bounded error", () => {
    const result = NodeChildProcess.spawnSync(
      process.execPath,
      [new URL("../../bin.ts", import.meta.url).pathname, "__jones-native-authority-transition"],
      {
        input: "{} trailing",
        encoding: "utf8",
        timeout: 10000,
        maxBuffer: 16384,
      },
    );
    expect(result.status).toBe(1);
    expect(result.stdout).toBe("");
    expect(result.stderr).toContain(
      "Native authority transition refused; reconciliation required.",
    );
    expect(result.stderr).not.toContain("SyntaxError");
  });
});
