// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { promisify } from "node:util";
import { afterEach, describe, expect, it } from "vite-plus/test";
import { bootstrapLauncherPointer, readLauncherPointer } from "./launcherPointer.ts";

const execFile = promisify(NodeChildProcess.execFile);
const current = "0.1.2-preview.20261026.100";
const pending = "0.1.2-preview.20261026.101";
const roots: string[] = [];
afterEach(async () => {
  for (const root of roots.splice(0)) await NodeFSP.rm(root, { recursive: true, force: true });
});

async function fixture() {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-launcher-pointer-"));
  roots.push(root);
  // realpath keeps the same fixture valid on macOS, where /var is a symlink.
  const base = NodePath.join(await NodeFSP.realpath(root), "service ' data");
  for (const [version, contents] of [
    [current, '#!/bin/sh\nprintf "current\\n"\nprintf "<%s>\\n" "$@"\n'],
    [pending, '#!/bin/sh\nprintf "pending\\n"\nexit 17\n'],
  ] as const) {
    const directory = NodePath.join(base, "runtime/versions", version);
    await NodeFSP.mkdir(directory, { recursive: true, mode: 0o700 });
    await NodeFSP.writeFile(NodePath.join(directory, "t3"), contents, { mode: 0o700 });
  }
  const previous = "[Service]\nExecStart=/retained/t3 serve\n";
  return { base, directory: NodePath.join(base, "runtime/launcher"), previous };
}

describe.skipIf(NodeOS.platform() === "win32")("stable launcher pointer", () => {
  it("executes the retained launcher with an empty PATH and preserves argument boundaries", async () => {
    const f = await fixture();
    const pointer = await bootstrapLauncherPointer(f.base, current, f.previous);
    const result = await execFile("/bin/sh", [pointer.program, "__service-launcher", "two words"], {
      env: { PATH: "" },
      timeout: 2000,
      maxBuffer: 4096,
    });
    expect(result.stdout).toBe("current\n<__service-launcher>\n<two words>\n");
    expect(
      await NodeFSP.readFile(NodePath.join(f.directory, "previous-service-definition"), "utf8"),
    ).toBe(f.previous);
  });

  it("consumes three broken pending starts then falls back without replacing the retained pointer", async () => {
    const f = await fixture();
    const pointer = await bootstrapLauncherPointer(f.base, current, f.previous);
    const original = await NodeFSP.lstat(NodePath.join(f.directory, "current"));
    await NodeFSP.symlink(`../versions/${pending}`, NodePath.join(f.directory, "pending"));
    for (let attempt = 1; attempt <= 3; attempt++) {
      await expect(
        execFile("/bin/sh", [pointer.program], {
          env: { PATH: "" },
          timeout: 2000,
          maxBuffer: 4096,
        }),
      ).rejects.toMatchObject({ code: 17, stdout: "pending\n" });
      expect((await readLauncherPointer(f.base))?.attempts).toBe(attempt);
    }
    const fallback = await execFile("/bin/sh", [pointer.program], {
      env: { PATH: "" },
      timeout: 2000,
      maxBuffer: 4096,
    });
    expect(fallback.stdout).toBe("current\n<>\n");
    expect((await readLauncherPointer(f.base))?.pendingVersion).toBeUndefined();
    expect(
      (await NodeFSP.readdir(f.directory)).filter((name) => name.startsWith("rejected-")),
    ).toHaveLength(1);
    const retained = await NodeFSP.lstat(NodePath.join(f.directory, "current"));
    expect([retained.dev, retained.ino]).toEqual([original.dev, original.ino]);
  });

  it("replays the same bootstrap without rewriting history and refuses a different preimage", async () => {
    const f = await fixture();
    const first = await bootstrapLauncherPointer(f.base, current, f.previous);
    const before = await NodeFSP.stat(first.program);
    expect(await bootstrapLauncherPointer(f.base, current, f.previous)).toEqual(first);
    expect((await NodeFSP.stat(first.program)).ino).toBe(before.ino);
    await expect(bootstrapLauncherPointer(f.base, current, "changed")).rejects.toThrow(
      "different or unsettled",
    );
    expect(
      await NodeFSP.readFile(NodePath.join(f.directory, "previous-service-definition"), "utf8"),
    ).toBe(f.previous);
  });

  it("refuses an escaped target, altered shim and malformed attempt budget", async () => {
    const f = await fixture();
    const pointer = await bootstrapLauncherPointer(f.base, current, f.previous);
    await NodeFSP.symlink("../../outside", NodePath.join(f.directory, "pending"));
    await expect(readLauncherPointer(f.base)).rejects.toThrow();
    await NodeFSP.unlink(NodePath.join(f.directory, "pending"));
    await NodeFSP.writeFile(NodePath.join(f.directory, "attempts"), "9\n");
    await expect(readLauncherPointer(f.base)).rejects.toThrow("attempt budget");
    await NodeFSP.writeFile(NodePath.join(f.directory, "attempts"), "0\n");
    await NodeFSP.appendFile(pointer.program, "# changed\n");
    await expect(readLauncherPointer(f.base)).rejects.toThrow("immutable format");
  });

  it("refuses a missing runtime before creating a bootstrap and preserves interrupted bootstrap evidence", async () => {
    const f = await fixture();
    await expect(
      bootstrapLauncherPointer(f.base, "0.1.2-preview.20261026.999", f.previous),
    ).rejects.toThrow();
    await expect(NodeFSP.lstat(f.directory)).rejects.toMatchObject({ code: "ENOENT" });
    await NodeFSP.mkdir(f.directory, { mode: 0o700 });
    await NodeFSP.writeFile(NodePath.join(f.directory, "interrupted"), "evidence", { mode: 0o600 });
    await expect(bootstrapLauncherPointer(f.base, current, f.previous)).rejects.toThrow();
    expect(await NodeFSP.readFile(NodePath.join(f.directory, "interrupted"), "utf8")).toBe(
      "evidence",
    );
  });
});
