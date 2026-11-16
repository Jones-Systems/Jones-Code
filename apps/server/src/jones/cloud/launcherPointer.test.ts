// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import { afterEach, describe, expect, it } from "vite-plus/test";
import {
  bootstrapLauncherPointer,
  readLauncherPointer,
  readLauncherHandoff,
  requestLauncherHandoff,
  settleLauncherHandoff,
} from "./launcherPointer.ts";

const execFile = NodeUtil.promisify(NodeChildProcess.execFile);
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

describe.skipIf(!NodeFS.existsSync("/bin/sh") || NodeOS.userInfo().uid < 0)(
  "stable launcher pointer",
  () => {
    it("executes the retained launcher with an empty PATH and preserves argument boundaries", async () => {
      const f = await fixture();
      const pointer = await bootstrapLauncherPointer(f.base, current, f.previous);
      const result = await execFile(
        "/bin/sh",
        [pointer.program, "__service-launcher", "two words"],
        {
          env: { PATH: "" },
          timeout: 2000,
          maxBuffer: 4096,
        },
      );
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
      await NodeFSP.writeFile(NodePath.join(f.directory, "interrupted"), "evidence", {
        mode: 0o600,
      });
      await expect(bootstrapLauncherPointer(f.base, current, f.previous)).rejects.toThrow();
      expect(await NodeFSP.readFile(NodePath.join(f.directory, "interrupted"), "utf8")).toBe(
        "evidence",
      );
    });

    it("retains intent before pending, promotes only a started target, and replays without losing history", async () => {
      const f = await fixture();
      const pointer = await bootstrapLauncherPointer(f.base, current, f.previous);
      const requestId = "11111111-1111-4111-8111-111111111111";
      const input = { requestId, fromVersion: current, targetVersion: pending };
      const requested = await requestLauncherHandoff(f.base, input);
      expect(requested?.status).toBe("requested");
      const requestedFile = NodePath.join(f.directory, "handoff", requestId + ".requested.json");
      const original = await NodeFSP.stat(requestedFile);
      expect(await requestLauncherHandoff(f.base, input)).toEqual(requested);
      expect((await NodeFSP.stat(requestedFile)).ino).toBe(original.ino);
      await expect(settleLauncherHandoff(f.base, pending, pending)).rejects.toThrow("not started");
      expect((await readLauncherPointer(f.base))?.currentVersion).toBe(current);
      await expect(
        execFile("/bin/sh", [pointer.program], {
          env: { PATH: "" },
          timeout: 2000,
          maxBuffer: 4096,
        }),
      ).rejects.toMatchObject({ code: 17 });
      await expect(settleLauncherHandoff(f.base, pending, current)).rejects.toThrow("health");
      const committed = await settleLauncherHandoff(f.base, pending, pending);
      expect(committed?.status).toBe("committed");
      expect(await readLauncherHandoff(f.base)).toEqual(committed);
      expect(await settleLauncherHandoff(f.base, pending, pending)).toEqual(committed);
      expect(await readLauncherPointer(f.base)).toMatchObject({
        currentVersion: pending,
        attempts: 0,
      });
      expect((await readLauncherPointer(f.base))?.pendingVersion).toBeUndefined();
      expect((await NodeFSP.stat(requestedFile)).ino).toBe(original.ino);
      expect(JSON.parse(await NodeFSP.readFile(requestedFile, "utf8"))).toEqual(requested);
    });

    it("records exhausted fallback without retrying the rejected target or changing retained current", async () => {
      const f = await fixture();
      const pointer = await bootstrapLauncherPointer(f.base, current, f.previous);
      const currentIdentity = await NodeFSP.lstat(NodePath.join(f.directory, "current"));
      const input = {
        requestId: "22222222-2222-4222-8222-222222222222",
        fromVersion: current,
        targetVersion: pending,
      };
      await requestLauncherHandoff(f.base, input);
      for (let attempt = 0; attempt < 3; attempt++)
        await expect(
          execFile("/bin/sh", [pointer.program], {
            env: { PATH: "" },
            timeout: 2000,
            maxBuffer: 4096,
          }),
        ).rejects.toMatchObject({ code: 17 });
      await execFile("/bin/sh", [pointer.program], {
        env: { PATH: "" },
        timeout: 2000,
        maxBuffer: 4096,
      });
      expect((await settleLauncherHandoff(f.base, current, pending))?.status).toBe("rejected");
      await expect(requestLauncherHandoff(f.base, input)).rejects.toThrow("already settled");
      await expect(
        requestLauncherHandoff(f.base, {
          ...input,
          requestId: "33333333-3333-4333-8333-333333333333",
        }),
      ).rejects.toThrow("already settled");
      const retained = await NodeFSP.lstat(NodePath.join(f.directory, "current"));
      expect([retained.dev, retained.ino]).toEqual([currentIdentity.dev, currentIdentity.ino]);
    });

    it("refuses competing requests and changed executable bytes while preserving pending evidence", async () => {
      const f = await fixture();
      await bootstrapLauncherPointer(f.base, current, f.previous);
      const input = {
        requestId: "44444444-4444-4444-8444-444444444444",
        fromVersion: current,
        targetVersion: pending,
      };
      const requested = await requestLauncherHandoff(f.base, input);
      await expect(
        requestLauncherHandoff(f.base, {
          ...input,
          requestId: "55555555-5555-4555-8555-555555555555",
        }),
      ).rejects.toThrow("unsettled");
      await NodeFSP.appendFile(
        NodePath.join(f.base, "runtime/versions", pending, "t3"),
        "# changed\n",
      );
      await expect(requestLauncherHandoff(f.base, input)).rejects.toThrow("unsettled");
      await expect(settleLauncherHandoff(f.base, pending, pending)).rejects.toThrow("immutable");
      expect(await readLauncherHandoff(f.base)).toEqual(requested);
      expect((await readLauncherPointer(f.base))?.pendingVersion).toBe(pending);
    });

    it("leaves unbootstrapped and byte-identical executable hosts untouched", async () => {
      const f = await fixture();
      const input = {
        requestId: "66666666-6666-4666-8666-666666666666",
        fromVersion: current,
        targetVersion: pending,
      };
      expect(await requestLauncherHandoff(f.base, input)).toBeUndefined();
      await bootstrapLauncherPointer(f.base, current, f.previous);
      await NodeFSP.copyFile(
        NodePath.join(f.base, "runtime/versions", current, "t3"),
        NodePath.join(f.base, "runtime/versions", pending, "t3"),
      );
      expect(await requestLauncherHandoff(f.base, input)).toBeUndefined();
      expect(await readLauncherHandoff(f.base)).toBeUndefined();
      expect((await readLauncherPointer(f.base))?.pendingVersion).toBeUndefined();
    });

    it("replays an intent published before pending without mistaking it for an exhausted handoff", async () => {
      const f = await fixture();
      await bootstrapLauncherPointer(f.base, current, f.previous);
      const input = {
        requestId: "77777777-7777-4777-8777-777777777777",
        fromVersion: current,
        targetVersion: pending,
      };
      const requested = await requestLauncherHandoff(f.base, input);
      const file = NodePath.join(f.directory, "handoff", input.requestId + ".requested.json");
      const history = await NodeFSP.stat(file);
      // Reproduce the durable prefix before publication; no manager attempt has been consumed.
      await NodeFSP.unlink(NodePath.join(f.directory, "pending"));
      expect(await settleLauncherHandoff(f.base, current, pending)).toEqual(requested);
      expect(await requestLauncherHandoff(f.base, input)).toEqual(requested);
      expect((await readLauncherPointer(f.base))?.pendingVersion).toBe(pending);
      expect((await readLauncherPointer(f.base))?.attempts).toBe(0);
      expect((await NodeFSP.stat(file)).ino).toBe(history.ino);
    });

    it("leaves no pending request when manager membership changes during executable hashing", async () => {
      const f = await fixture();
      await bootstrapLauncherPointer(f.base, current, f.previous);
      expect(
        await requestLauncherHandoff(
          f.base,
          {
            requestId: "88888888-8888-4888-8888-888888888888",
            fromVersion: current,
            targetVersion: pending,
          },
          async () => false,
        ),
      ).toEqual({ status: "deferred" });
      expect(await readLauncherHandoff(f.base)).toBeUndefined();
      expect(await readLauncherPointer(f.base)).toMatchObject({
        currentVersion: current,
        attempts: 0,
      });
      expect((await readLauncherPointer(f.base))?.pendingVersion).toBeUndefined();
    });
  },
);
