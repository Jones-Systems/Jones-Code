// @effect-diagnostics nodeBuiltinImport:off - Synthetic staging root; denied admission never invokes mount/copy.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import type {
  JonesActionsCandidate,
  JonesStagedArtifact,
} from "@t3tools/shared/jones/jonesActions";
import { expect, it, vi } from "vite-plus/test";
import type { JonesStagedMacApp } from "./jonesActivation.ts";
import type { stageJonesMacApp } from "./jonesMacStaging.ts";
import { prepareJonesStage } from "./stagingJobs.ts";
import { JonesStagingStopped } from "./stagingWorkerProtocol.ts";

it("waits for the parent's decision after artifact transfer and never mounts a denied download", async () => {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-admit-"));
  const admission = Promise.withResolvers<void>();
  const transferred = Promise.withResolvers<void>();
  const preflighted = Promise.withResolvers<void>();
  const stage = vi.fn(async () => {
    transferred.resolve();
    return {} as JonesStagedArtifact;
  });
  const nativeWork = vi.fn(async () => {
    throw new Error("Mount/copy must not run.");
  });
  const stageApp = vi.fn<typeof stageJonesMacApp>(async (_artifact, _root, _platform, admit) => {
    preflighted.resolve();
    await admit?.();
    return nativeWork();
  });
  const onVerifying = vi.fn();
  try {
    const pending = prepareJonesStage(
      {} as JonesActionsCandidate,
      { artifacts: NodePath.join(root, "artifacts"), apps: NodePath.join(root, "apps") },
      "darwin",
      {
        actions: { stage },
        stageApp,
        onVerifying,
        admit: () => admission.promise,
      },
    );
    const stopped = expect(pending).rejects.toBeInstanceOf(JonesStagingStopped);
    await transferred.promise;
    await preflighted.promise;
    expect(stageApp).toHaveBeenCalledTimes(1);
    expect(nativeWork).not.toHaveBeenCalled();
    expect(onVerifying).not.toHaveBeenCalled();
    admission.reject(new JonesStagingStopped());
    await stopped;
    expect(nativeWork).not.toHaveBeenCalled();
    expect(onVerifying).not.toHaveBeenCalled();
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
  }
});

it("finishes staging preflight before admission and verification, then permits native work", async () => {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-admit-"));
  const events: string[] = [];
  const artifact = {} as JonesStagedArtifact;
  const app = {} as JonesStagedMacApp;
  try {
    const result = await prepareJonesStage(
      {} as JonesActionsCandidate,
      { artifacts: NodePath.join(root, "artifacts"), apps: NodePath.join(root, "apps") },
      "darwin",
      {
        actions: {
          stage: vi.fn(async () => {
            events.push("transfer");
            return artifact;
          }),
        },
        stageApp: async (_artifact, _root, _platform, admit) => {
          events.push("preflight");
          await admit?.();
          events.push("native-work");
          return app;
        },
        admit: async () => {
          events.push("admission");
        },
        onVerifying: () => {
          events.push("verifying");
        },
      },
    );
    expect(events).toEqual(["transfer", "preflight", "admission", "verifying", "native-work"]);
    expect(result).toEqual({ artifact, app });
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
  }
});

it("returns an ordinary preflight failure without requesting parent admission", async () => {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-admit-"));
  const admit = vi.fn(async () => undefined);
  const onVerifying = vi.fn();
  const preflightFailure = new Error("incomplete or unknown Mac stage");
  try {
    await expect(
      prepareJonesStage(
        {} as JonesActionsCandidate,
        { artifacts: NodePath.join(root, "artifacts"), apps: NodePath.join(root, "apps") },
        "darwin",
        {
          actions: { stage: vi.fn(async () => ({}) as JonesStagedArtifact) },
          stageApp: async () => {
            throw preflightFailure;
          },
          admit,
          onVerifying,
        },
      ),
    ).rejects.toBe(preflightFailure);
    expect(admit).not.toHaveBeenCalled();
    expect(onVerifying).not.toHaveBeenCalled();
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
  }
});
