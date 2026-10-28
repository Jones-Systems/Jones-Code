// @effect-diagnostics nodeBuiltinImport:off - Synthetic staging root; denied admission never invokes mount/copy.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import type {
  JonesActionsCandidate,
  JonesStagedArtifact,
} from "@t3tools/shared/jones/jonesActions";
import { expect, it, vi } from "vite-plus/test";
import { prepareJonesStage } from "./stagingJobs.ts";
import { JonesStagingStopped } from "./stagingWorkerProtocol.ts";

it("waits for the parent's decision after artifact transfer and never mounts a denied download", async () => {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-admit-"));
  const admission = Promise.withResolvers<void>();
  const transferred = Promise.withResolvers<void>();
  const stage = vi.fn(async () => {
    transferred.resolve();
    return {} as JonesStagedArtifact;
  });
  const stageApp = vi.fn(async () => {
    throw new Error("Mount/copy must not run.");
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
    expect(stageApp).not.toHaveBeenCalled();
    expect(onVerifying).not.toHaveBeenCalled();
    admission.reject(new JonesStagingStopped());
    await stopped;
    expect(stageApp).not.toHaveBeenCalled();
    expect(onVerifying).not.toHaveBeenCalled();
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
  }
});
