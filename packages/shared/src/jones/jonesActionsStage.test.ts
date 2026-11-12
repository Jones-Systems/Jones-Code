// @effect-diagnostics nodeBuiltinImport:off - Isolated real-filesystem completion publication and recovery fixtures.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { expect, it } from "vite-plus/test";
import {
  createJonesStageAttempt,
  findJonesCompletedStage,
  publishJonesCompletedStage,
  requireJonesStageDirectory,
} from "./jonesActionsStage.ts";

const handle = "a".repeat(64);
const receipt = "receipt.json";

async function withRoot(run: (root: string) => Promise<void>): Promise<void> {
  const allocated = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-stage-test-"));
  try {
    await run(await NodeFSP.realpath(allocated));
  } finally {
    await NodeFSP.rm(allocated, { recursive: true, force: true });
    await expect(NodeFSP.lstat(allocated)).rejects.toMatchObject({ code: "ENOENT" });
  }
}

it("does not select incomplete attempts and retries without touching their data", () =>
  withRoot(async (root) => {
    const interrupted = await createJonesStageAttempt(root, handle);
    await NodeFSP.writeFile(NodePath.join(interrupted.directory, "partial"), "retain");
    expect(await findJonesCompletedStage(root, handle, receipt)).toBeUndefined();
    await expect(
      publishJonesCompletedStage(root, handle, interrupted.directory, receipt),
    ).rejects.toThrow();
    const retry = await createJonesStageAttempt(root, handle);
    await NodeFSP.writeFile(NodePath.join(retry.directory, receipt), "complete");
    expect(await publishJonesCompletedStage(root, handle, retry.directory, receipt)).toBe(
      retry.directory,
    );
    expect(await findJonesCompletedStage(root, handle, receipt)).toBe(retry.directory);
    expect(await NodeFSP.readFile(NodePath.join(interrupted.directory, "partial"), "utf8")).toBe(
      "retain",
    );
  }));

it("publishes one complete winner when two attempts race", () =>
  withRoot(async (root) => {
    const first = await createJonesStageAttempt(root, handle);
    const second = await createJonesStageAttempt(root, handle);
    for (const attempt of [first, second])
      await NodeFSP.writeFile(NodePath.join(attempt.directory, receipt), "complete");
    const published = await Promise.all([
      publishJonesCompletedStage(root, handle, first.directory, receipt),
      publishJonesCompletedStage(root, handle, second.directory, receipt),
    ]);
    expect(published[0]).toBe(published[1]);
    expect([first.directory, second.directory]).toContain(published[0]);
    expect(
      JSON.parse(
        await NodeFSP.readFile(NodePath.join(root, "completed", `${handle}.json`), "utf8"),
      ),
    ).toEqual({ schema: 1, handle, directory: published[0] });
  }));

it("preserves and rejects unknown occupied completion indexes", () =>
  withRoot(async (root) => {
    const attempt = await createJonesStageAttempt(root, handle);
    await NodeFSP.writeFile(NodePath.join(attempt.directory, receipt), "complete");
    await NodeFSP.mkdir(NodePath.join(root, "completed"));
    const index = NodePath.join(root, "completed", `${handle}.json`);
    await NodeFSP.writeFile(index, "unowned");
    await expect(
      publishJonesCompletedStage(root, handle, attempt.directory, receipt),
    ).rejects.toThrow();
    expect(await NodeFSP.readFile(index, "utf8")).toBe("unowned");
    expect(await NodeFSP.readFile(NodePath.join(attempt.directory, receipt), "utf8")).toBe(
      "complete",
    );
  }));

it("refuses traversal and symlinked attempt roots", () =>
  withRoot(async (root) => {
    const attempt = await createJonesStageAttempt(root, handle);
    await expect(
      requireJonesStageDirectory(root, handle, NodePath.dirname(root)),
    ).rejects.toThrow();
    const alias = NodePath.join(root, "attempts", "stage-alias");
    await NodeFSP.symlink(attempt.attemptRoot, alias);
    await expect(
      requireJonesStageDirectory(root, handle, NodePath.join(alias, handle)),
    ).rejects.toThrow();
  }));
