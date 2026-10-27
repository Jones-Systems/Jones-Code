// @effect-diagnostics nodeBuiltinImport:off
// Exercise the standalone native CLI in its actual Node process boundary.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { describe, expect, it } from "vite-plus/test";
import { parseRecoveryArguments } from "./recover-staged-selection.ts";

const args = [
  "--base-dir",
  "/synthetic",
  "--active-version",
  "old",
  "--expected-active-source",
  "a".repeat(40),
  "--environment-id",
  "synthetic",
  "--target-source",
  "b".repeat(40),
  "--target-version",
  "new",
  "--staged-handle",
  "handle",
];

describe("recovery CLI arguments", () => {
  it("defaults to dry-run and requires explicit apply", () => {
    expect(parseRecoveryArguments(args).apply).toBe(false);
    expect(parseRecoveryArguments([...args, "--apply"]).apply).toBe(true);
  });
  it.each(
    [
      [],
      [...args, "--unknown"],
      [...args, "--base-dir", "/other"],
      [...args, "positional"],
      [...args, "--apply", "--apply"],
    ].map((invalid) => ({ invalid })),
  )("refuses missing, unknown or repeated arguments", ({ invalid }) => {
    expect(() => parseRecoveryArguments(invalid)).toThrow();
  });
});

it("executes the CLI through a symlink and reports its refusal exit code", async () => {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-recovery-cli-"));
  try {
    const entry = NodePath.join(root, "recover.ts");
    await NodeFSP.symlink(
      NodeURL.fileURLToPath(new URL("./recover-staged-selection.ts", import.meta.url)),
      entry,
    );
    const result = NodeChildProcess.spawnSync(process.execPath, [entry], { encoding: "utf8" });
    expect(result.status).toBe(2);
    expect(JSON.parse(result.stdout)).toEqual({ status: "refused", reason: "invalid-arguments" });
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
    await expect(NodeFSP.lstat(root)).rejects.toMatchObject({ code: "ENOENT" });
  }
});
