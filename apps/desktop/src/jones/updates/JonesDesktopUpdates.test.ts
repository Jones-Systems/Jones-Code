// @effect-diagnostics nodeBuiltinImport:off - Source-qualified synthetic desktop fixture, never launches a native app.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import type {
  JonesActionsCandidate,
  JonesStagedArtifact,
} from "@t3tools/shared/jones/jonesActions";
import {
  JonesDesktopUpdateController,
  type JonesDesktopUpdateOptions,
} from "./JonesDesktopUpdates.ts";
import { createInitialDesktopUpdateState } from "../../updates/updateMachine.ts";
import { hashMacApp, hashMacFile } from "./jonesMacStaging.ts";

vi.mock("node:child_process", async (importOriginal) => {
  const original = await importOriginal<typeof import("node:child_process")>();
  return { ...original, execFile: vi.fn(original.execFile) };
});

const candidate: JonesActionsCandidate = {
  schema: 1,
  repository: "Jones-Systems/Jones-Code",
  source: "b".repeat(40),
  tree: "c".repeat(40),
  installedSource: "a".repeat(40),
  workflow: ".github/workflows/artifact-desktop-mac.yml",
  workflowId: 10,
  runId: 11,
  runAttempt: 1,
  ciRunId: 12,
  artifactId: 13,
  artifactName: "qualified-mac",
  artifactDigest: "d".repeat(64),
  artifactBytes: 100,
  expiresAt: "2030-01-01T00:00:00Z",
  version: "0.0.44-preview.20261002.11.1",
  platform: "darwin",
  architecture: "arm64",
};
const handle = "e".repeat(64);

const fixtures = new Set<string>();
async function cleanupFixture(root: string): Promise<void> {
  await NodeFSP.rm(root, { recursive: true, force: true });
  fixtures.delete(root);
}
afterEach(async () => {
  for (const root of fixtures) await cleanupFixture(root);
});

async function fixture(bootstrap = true, terminal?: "committed" | "rolled-back") {
  const parent = NodePath.join(NodeOS.homedir(), ".cache", "jones-updater-test-fixtures");
  await NodeFSP.mkdir(parent, { recursive: true, mode: 0o700 });
  const home = await NodeFSP.mkdtemp(NodePath.join(parent, "desktop-controller-"));
  fixtures.add(home);
  const app = NodePath.join(home, "Previous.app"),
    profile = NodePath.join(home, "profile");
  await NodeFSP.mkdir(app);
  await NodeFSP.mkdir(profile);
  await NodeFSP.mkdir(NodePath.join(home, "userdata"));
  await NodeFSP.mkdir(NodePath.join(home, "runtime"));
  await NodeFSP.writeFile(NodePath.join(profile, "opaque"), "same-host-profile");
  const databasePath = NodePath.join(home, "userdata", "state.sqlite");
  await NodeFSP.writeFile(databasePath, "live-state");
  const executable = NodePath.join(app, "Jones");
  await NodeFSP.writeFile(executable, "native-executable");
  const version = "0.0.44-preview.20261002.10";
  await NodeFSP.writeFile(
    NodePath.join(app, "package.json"),
    JSON.stringify({
      version,
      jonesSource: {
        repository: candidate.repository,
        sha: candidate.installedSource,
        tree: "f".repeat(40),
      },
    }),
  );
  const active = {
    protocol: 1,
    owner: "desktop",
    generation: terminal === "committed" ? handle : "previous-generation",
    transactionId: terminal === "committed" ? handle : "bootstrap",
    home,
    databasePath,
    profile,
    environmentId: "native-environment",
    appPath: app,
    executablePath: executable,
    version,
    sourceSha: candidate.installedSource,
    sourceTree: "f".repeat(40),
    appDigest: await hashMacApp(app),
  };
  await NodeFSP.writeFile(
    NodePath.join(home, "runtime", "jones-active-install.json"),
    JSON.stringify(active),
  );
  if (terminal !== undefined) {
    const transaction = NodePath.join(home, "runtime", "jones-updates", "transactions", handle);
    await NodeFSP.mkdir(transaction, { recursive: true });
    await NodeFSP.writeFile(
      NodePath.join(transaction, "journal.json"),
      JSON.stringify({
        phase: terminal === "committed" ? "resumed" : "rolled-back",
        intent: {
          protocol: 1,
          transactionId: handle,
          expected:
            terminal === "rolled-back"
              ? active
              : {
                  ...active,
                  generation: "prior",
                  transactionId: "prior",
                  sourceSha: "0".repeat(40),
                },
          continuationReceipt: NodePath.join(transaction, "continuation.json"),
          staged: {
            handle,
            receiptPath: NodePath.join(transaction, "app-receipt.json"),
            appPath: active.appPath,
            executablePath: active.executablePath,
            version: active.version,
            sourceSha: active.sourceSha,
            sourceTree: active.sourceTree,
            appDigest: active.appDigest,
            asarDigest: "1".repeat(64),
            executableDigest: "2".repeat(64),
          },
        },
      }),
    );
  }
  const selections = [
    candidate,
    {
      ...candidate,
      source: "f".repeat(40),
      artifactId: 99,
      version: "0.0.44-preview.20261002.99.1",
    },
  ];
  let checks = 0,
    stages = 0;
  let ownedArtifact: JonesStagedArtifact | undefined;
  const options: JonesDesktopUpdateOptions = {
    home,
    appRoot: app,
    appPath: app,
    executablePath: executable,
    profile,
    activeGeneration: bootstrap ? active.generation : undefined,
    architecture: "arm64",
    platform: "darwin",
    disabledByEnv: false,
    initialState: createInitialDesktopUpdateState(
      version,
      { hostArch: "arm64", appArch: "arm64", runningUnderArm64Translation: false },
      "latest",
    ),
    onState: async () => {},
    processProofs: async () => [],
    listener: async () => "http://127.0.0.1:3777",
    timestamp: async () => "2026-10-02T00:00:00Z",
    client: {
      check: async () => ({ state: "available", candidate: selections[Math.min(checks++, 1)]! }),
      stage: async (selection) => {
        stages++;
        const artifact: JonesStagedArtifact = {
          schema: 1,
          source: "jones-actions",
          channel: "jones-main",
          stagedHandle: handle,
          candidate: selection,
          payloadPath: NodePath.join(
            home,
            "runtime",
            "jones-updates",
            "artifacts",
            handle,
            "qualified.dmg",
          ),
          receipt: {
            schema: 1,
            repository: selection.repository,
            source: selection.source,
            tree: selection.tree,
            workflow: selection.workflow,
            event: "push",
            ref: "refs/heads/main",
            runId: String(selection.runId),
            runAttempt: String(selection.runAttempt),
            version: selection.version,
            platform: "darwin",
            architecture: "arm64",
            artifact: "qualified.dmg",
            sha256: "d".repeat(64),
          },
        };
        await NodeFSP.mkdir(NodePath.dirname(artifact.payloadPath), { recursive: true });
        ownedArtifact = artifact;
        return artifact;
      },
    },
    stageApp: async (artifact, stageRoot) => {
      const appPath = NodePath.join(stageRoot, handle, "Candidate.app");
      const executablePath = NodePath.join(appPath, "Contents", "MacOS", "Jones");
      const asarPath = NodePath.join(appPath, "Contents", "Resources", "app.asar");
      await NodeFSP.mkdir(NodePath.dirname(executablePath), { recursive: true });
      await NodeFSP.mkdir(NodePath.dirname(asarPath));
      await NodeFSP.writeFile(executablePath, "staged-native-executable");
      await NodeFSP.writeFile(asarPath, "staged-asar");
      const staged = {
        handle,
        receiptPath: NodePath.join(stageRoot, handle, "mac-app-receipt.json"),
        appPath,
        executablePath,
        version: artifact.candidate.version,
        sourceSha: artifact.candidate.source,
        sourceTree: artifact.candidate.tree,
        appDigest: await hashMacApp(appPath),
        asarDigest: await hashMacFile(asarPath),
        executableDigest: await hashMacFile(executablePath),
      };
      await NodeFSP.writeFile(
        staged.receiptPath,
        JSON.stringify({ app: staged, artifact, candidate: artifact.candidate }),
      );
      return staged;
    },
    validateArtifact: async (directory) => {
      if (ownedArtifact === undefined || NodePath.dirname(ownedArtifact.payloadPath) !== directory)
        throw new Error("Unknown artifact fixture.");
      return ownedArtifact;
    },
  };
  const controller = new JonesDesktopUpdateController(options);
  return {
    home,
    app,
    active,
    options,
    profile,
    databasePath,
    controller,
    restart: () => new JonesDesktopUpdateController(options),
    stages: () => stages,
    cleanup: () => cleanupFixture(home),
  };
}

describe("Jones desktop updates", () => {
  it("adopts a source-qualified stable bundle without a helper generation or full-app scan", async () => {
    const f = await fixture(false);
    const nativeCommand = vi.mocked(NodeChildProcess.execFile);
    try {
      const contents = NodePath.join(f.app, "Contents");
      const executable = NodePath.join(contents, "MacOS", "Jones");
      const infoPlist = NodePath.join(contents, "Info.plist");
      nativeCommand.mockImplementation((command, args, options, callback) => {
        expect(command).toBe("/usr/bin/plutil");
        expect(args).toEqual(["-convert", "json", "-o", "-", infoPlist]);
        expect(options).toEqual({ encoding: "utf8", maxBuffer: 1024 * 1024, timeout: 120000 });
        if (typeof callback !== "function")
          throw new Error("The plist command requires a callback.");
        void NodeFSP.readFile(infoPlist, "utf8").then(
          (xml) => {
            const info = Object.fromEntries(
              [...xml.matchAll(/<key>([^<]+)<\/key>\s*<string>([^<]+)<\/string>/g)].map((match) => [
                match[1],
                match[2],
              ]),
            );
            callback(null, JSON.stringify(info), "");
          },
          (cause: NodeChildProcess.ExecFileException) => callback(cause, "", ""),
        );
        return new NodeChildProcess.ChildProcess();
      });
      await NodeFSP.mkdir(NodePath.dirname(executable), { recursive: true });
      await NodeFSP.rename(f.active.executablePath, executable);
      await NodeFSP.writeFile(
        infoPlist,
        `<?xml version="1.0"?><plist version="1.0"><dict>
        <key>CFBundleExecutable</key><string>Jones</string>
        <key>CFBundleIdentifier</key><string>com.jones.code</string>
        <key>CFBundleShortVersionString</key><string>${f.active.version}</string>
      </dict></plist>`,
      );
      await NodeFSP.rename(f.databasePath, NodePath.join(f.home, "userdata", "statev2.sqlite"));
      await NodeFSP.writeFile(
        NodePath.join(f.home, "userdata", "environment-id"),
        "native-environment\n",
      );
      await NodeFSP.unlink(NodePath.join(f.home, "runtime", "jones-active-install.json"));
      // An escaping payload link would fail a complete app hash. Configure reads only bound metadata.
      await NodeFSP.symlink(f.home, NodePath.join(f.app, "opaque-link"));
      const controller = new JonesDesktopUpdateController({
        ...f.options,
        executablePath: executable,
      });
      await controller.configure();
      expect(controller.state.jones?.capability.install).toBe(true);
      const active = JSON.parse(
        await NodeFSP.readFile(
          NodePath.join(f.home, "runtime", "jones-active-install.json"),
          "utf8",
        ),
      );
      expect(active.bundleIdentifier).toBe("com.jones.code");
      expect(active.sourceSha).toBe(candidate.installedSource);
      expect(active.databasePath).toBe(NodePath.join(f.home, "userdata", "statev2.sqlite"));
      const restarted = new JonesDesktopUpdateController({
        ...f.options,
        executablePath: executable,
      });
      await restarted.configure();
      expect(restarted.state.jones?.capability.install).toBe(true);
      expect(nativeCommand).toHaveBeenCalledTimes(3);
      expect(await NodeFSP.readFile(active.databasePath, "utf8")).toBe("live-state");
      await NodeFSP.writeFile(
        infoPlist,
        (await NodeFSP.readFile(infoPlist, "utf8")).replace("com.jones.code", "com.other.code"),
      );
      await restarted.configure();
      expect(restarted.state.jones?.capability.install).toBe(false);
      expect(restarted.state.jones?.message).toBe(
        "The native manifest does not match the running bundle identity.",
      );
      expect(nativeCommand).toHaveBeenCalledTimes(4);
    } finally {
      nativeCommand.mockReset();
      await f.cleanup();
    }
  });

  it("preserves an occupied manifest that does not bind the current source", async () => {
    const f = await fixture(false);
    try {
      const file = NodePath.join(f.home, "runtime", "jones-active-install.json");
      const occupied = JSON.stringify({ ...f.active, sourceSha: "0".repeat(40) });
      await NodeFSP.writeFile(file, occupied);
      await f.controller.configure();
      expect(f.controller.state.jones?.capability.install).toBe(false);
      expect(f.controller.state.jones?.phase).toBe("blocked");
      expect(await NodeFSP.readFile(file, "utf8")).toBe(occupied);
    } finally {
      await f.cleanup();
    }
  });

  it("retains a markerless candidate without preparing or launching native activation", async () => {
    const f = await fixture();
    try {
      await f.controller.configure();
      await f.controller.check();
      await f.controller.download();
      expect(f.controller.state.jones?.capability.install).toBe(false);
      expect(await f.controller.install(handle)).toEqual({
        accepted: false,
        completed: false,
        failed: false,
        refusal: "startup-gate-unavailable",
      });
      expect(f.controller.state.jones?.stagedHandle).toBe(handle);
      expect(f.controller.state.jones?.provenance?.artifactId).toBe(candidate.artifactId);
      expect(f.controller.state.downloadedVersion).toBe(candidate.version);
      const transactions = NodePath.join(f.home, "runtime", "jones-updates", "transactions");
      await expect(NodeFSP.stat(transactions)).rejects.toMatchObject({ code: "ENOENT" });
      expect(await NodeFSP.readFile(f.databasePath, "utf8")).toBe("live-state");
      expect(await NodeFSP.readFile(NodePath.join(f.profile, "opaque"), "utf8")).toBe(
        "same-host-profile",
      );
      const restored = f.restart();
      await restored.configure();
      expect(restored.state.jones?.stagedHandle).toBe(handle);
      expect(restored.state.jones?.capability.install).toBe(false);
    } finally {
      await f.cleanup();
    }
  });
  it("refuses a snapshot selection changed by a host check before download reservation", async () => {
    const f = await fixture();
    try {
      await f.controller.configure();
      await f.controller.check();
      const selection = {
        artifactId: f.controller.state.jones!.provenance!.artifactId,
        sourceSha: f.controller.state.jones!.provenance!.sourceSha,
      };
      const concurrentCheck = f.controller.check();
      await concurrentCheck;
      expect(f.controller.state.jones?.provenance?.artifactId).toBe(99);
      expect(await f.controller.download(selection)).toEqual({
        accepted: false,
        completed: false,
        refusal: "selection-mismatch",
      });
      expect(f.stages()).toBe(0);
      expect(f.controller.state.jones?.stagedHandle).toBeUndefined();
      expect(await NodeFSP.readFile(f.databasePath, "utf8")).toBe("live-state");
      expect(
        await f.controller.download({
          artifactId: 99,
          sourceSha: "f".repeat(40),
        }),
      ).toEqual({ accepted: true, completed: true });
      expect(f.stages()).toBe(1);
    } finally {
      await f.cleanup();
    }
  });
  it.each(["intent.json", "prepare-intent.json"])(
    "holds a retained %s without a completed transaction receipt",
    async (marker) => {
      const f = await fixture();
      try {
        const transaction = NodePath.join(
          f.home,
          "runtime",
          "jones-updates",
          "transactions",
          handle,
        );
        await NodeFSP.mkdir(transaction, { recursive: true });
        const file = NodePath.join(transaction, marker);
        await NodeFSP.writeFile(file, "retained-unknown-intent");
        await f.controller.configure();
        expect(f.controller.state.jones?.phase).toBe("blocked");
        expect(f.controller.state.jones?.capability.install).toBe(false);
        expect(f.controller.state.jones?.capability.reason).toBe("blocked");
        expect(await NodeFSP.readFile(file, "utf8")).toBe("retained-unknown-intent");
        expect(await NodeFSP.readFile(f.databasePath, "utf8")).toBe("live-state");
      } finally {
        await f.cleanup();
      }
    },
  );
  it("restores the verified fixed staged app after a controller restart without downloading again", async () => {
    const f = await fixture();
    try {
      await f.controller.configure();
      await f.controller.check();
      await f.controller.download();
      const restored = f.restart();
      await restored.configure();
      expect(restored.state.jones?.phase).toBe("staged");
      expect(restored.state.jones?.stagedHandle).toBe(handle);
      expect(restored.state.jones?.provenance?.artifactId).toBe(candidate.artifactId);
      expect(await restored.download()).toEqual({ accepted: true, completed: true });
      expect(f.stages()).toBe(1);
      expect(await NodeFSP.readFile(f.databasePath, "utf8")).toBe("live-state");
    } finally {
      await f.cleanup();
    }
  });
  it.each(["committed", "rolled-back"] as const)(
    "reads %s after ordinary stable-path relaunch without a helper generation",
    async (terminal) => {
      const f = await fixture(true, terminal);
      try {
        await f.controller.configure();
        expect(f.controller.state.jones?.phase).toBe(terminal);
        expect(f.controller.state.jones?.capability.install).toBe(true);
      } finally {
        await f.cleanup();
      }
      const unqualified = await fixture(false, terminal);
      try {
        await unqualified.controller.configure();
        expect(unqualified.controller.state.jones?.phase).toBe(terminal);
        expect(unqualified.controller.state.jones?.capability.install).toBe(true);
      } finally {
        await unqualified.cleanup();
      }
    },
  );
  it("keeps Download as staging and fixes Install to the original artifact after a newer check", async () => {
    const f = await fixture();
    try {
      await f.controller.configure();
      expect(f.controller.state.jones?.capability.install).toBe(true);
      await f.controller.check();
      expect(await f.controller.download()).toEqual({ accepted: true, completed: true });
      await f.controller.check();
      expect(f.controller.state.jones?.provenance?.artifactId).toBe(candidate.artifactId);
      expect(f.controller.state.jones?.provenance?.sourceSha).toBe(candidate.source);
      expect(f.controller.state.jones?.stagedHandle).toBe(handle);
      expect(f.controller.state.downloadedVersion).toBe(candidate.version);
      expect(f.stages()).toBe(1);
      expect(await NodeFSP.readFile(f.databasePath, "utf8")).toBe("live-state");
      expect(await NodeFSP.readFile(NodePath.join(f.profile, "opaque"), "utf8")).toBe(
        "same-host-profile",
      );
      expect((await f.controller.install()).accepted).toBe(false);
      expect((await f.controller.install("different-handle")).accepted).toBe(false);
    } finally {
      await f.cleanup();
    }
  });

  it("allows stable-path checks and staging after a normal launch", async () => {
    const f = await fixture(false);
    try {
      await f.controller.configure();
      expect(f.controller.state.jones?.capability).toEqual({
        check: true,
        download: true,
        install: true,
      });
      await f.controller.check();
      await f.controller.download();
      expect((await f.controller.install(handle)).accepted).toBe(false);
      expect(await NodeFSP.readFile(f.databasePath, "utf8")).toBe("live-state");
    } finally {
      await f.cleanup();
    }
  });
});
