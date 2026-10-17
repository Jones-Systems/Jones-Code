// @effect-diagnostics nodeBuiltinImport:off -- Tests exercise real POSIX filesystem custody and exact scratch cleanup.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeChildProcess from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import { activationFromEnvironment, resolveWorkstreamActivation } from "./activation.ts";

const environment = {
  T3_WORKSTREAM_CONTROL_PLANE_URL: "http://127.0.0.1:8787/",
  T3_WORKSTREAM_OWNER_ID: "owner-synthetic",
  T3_WORKSTREAM_PRINCIPAL_ID: "11111111-1111-4111-8111-111111111111",
  T3_WORKSTREAM_KEY_ID: "key-synthetic",
  T3_WORKSTREAM_SIGNING_SECRET: Buffer.alloc(32, 42).toString("base64"),
  T3_WORKSTREAM_AUTHORIZATION_REVISION: "3",
};
// Exact stage-jones format: six unquoted assignments, first '=' separates the padded secret.
const staged = (values = environment) =>
  Object.entries(values)
    .map(([key, value]) => `${key}=${value}\n`)
    .join("");
const owned = new Set<string>();
function fixture() {
  // realpath only selects the test scratch root; production never resolves away symlinks.
  const base = NodeFS.mkdtempSync(
    NodePath.join(NodeFS.realpathSync(NodeOS.tmpdir()), "jones-activation-"),
  );
  owned.add(base);
  const directory = NodePath.join(base, "workstreams");
  NodeFS.mkdirSync(directory, { mode: 0o700 });
  const file = NodePath.join(directory, "activation.env");
  NodeFS.writeFileSync(file, staged(), { mode: 0o600 });
  return { base, directory, file };
}
afterEach(() => {
  vi.restoreAllMocks();
  for (const path of owned) {
    NodeFS.rmSync(path, { recursive: true, force: true });
    owned.delete(path);
  }
});

describe("Workstreams activation startup snapshot", () => {
  it("reads CP staging bytes without exporting secrets and reuses persistent files on restart", () => {
    const { base, file } = fixture();
    const before = { ...process.env };
    const first = resolveWorkstreamActivation(base, {});
    expect(first).toEqual(activationFromEnvironment(environment));
    expect(process.env).toEqual(before);
    NodeFS.writeFileSync(
      file,
      staged({ ...environment, T3_WORKSTREAM_AUTHORIZATION_REVISION: "4" }),
    );
    expect(first.state === "enabled" && first.value.authorizationRevision).toBe(3);
    expect(resolveWorkstreamActivation(base, {})).toEqual(
      activationFromEnvironment({ ...environment, T3_WORKSTREAM_AUTHORIZATION_REVISION: "4" }),
    );
    expect(NodeFS.readFileSync(file, "utf8")).toBe(
      staged({ ...environment, T3_WORKSTREAM_AUTHORIZATION_REVISION: "4" }),
    );
    expect(resolveWorkstreamActivation(fixture().base, {})).toEqual(first);
  });

  it("uses a complete environment group without touching an unsafe or missing file", () => {
    expect(resolveWorkstreamActivation("/deliberately/nonexistent", environment)).toEqual(
      activationFromEnvironment(environment),
    );
    const { base, directory } = fixture();
    NodeFS.chmodSync(directory, 0o777);
    expect(resolveWorkstreamActivation(base, environment)).toEqual(
      activationFromEnvironment(environment),
    );
  });

  it("every defined key including empty suppresses file fallback; partial groups never mix", () => {
    const { base } = fixture();
    for (const [key, value] of Object.entries(environment)) {
      expect(resolveWorkstreamActivation(base, { [key]: value })).toEqual({ state: "invalid" });
      expect(resolveWorkstreamActivation(base, { [key]: "" })).toEqual({ state: "invalid" });
      expect(resolveWorkstreamActivation(base, { ...environment, [key]: "" })).toEqual({
        state: "invalid",
      });
    }
  });

  it("distinguishes absent files and directories from invalid present data", () => {
    const { base, file, directory } = fixture();
    NodeFS.writeFileSync(file, "");
    expect(resolveWorkstreamActivation(base, {})).toEqual({ state: "invalid" });
    NodeFS.unlinkSync(file);
    expect(resolveWorkstreamActivation(base, {})).toEqual({ state: "disabled" });
    NodeFS.rmdirSync(directory);
    expect(resolveWorkstreamActivation(base, {})).toEqual({ state: "disabled" });
  });

  it("rejects incomplete, duplicate, unknown, quoted, shell-like and noncanonical file values", () => {
    const { base, file } = fixture();
    const text = staged();
    for (const value of [
      text.split("\n").slice(1).join("\n"),
      text.replace("T3_WORKSTREAM_OWNER_ID=owner-synthetic", "T3_WORKSTREAM_KEY_ID=duplicate"),
      text.replace("T3_WORKSTREAM_OWNER_ID", "UNRELATED_KEY"),
      text.replace("owner-synthetic", '"owner-synthetic"'),
      text.replace("owner-synthetic", "$(touch /never-execute)"),
      text.replace("owner-synthetic", "owner;command"),
      "export " + text,
      "\ufeff" + text,
      text + "\n",
      text.replaceAll("\n", "\r\n"),
      text.replace("=3\n", "=3e0\n"),
      text.replace("=3\n", "=9007199254740992\n"),
      text.replace("http://127.0.0.1:8787/", "http://localhost:8787/"),
      text.replace("http://127.0.0.1:8787/", "http://127.0.0.1:8787"),
      text.replace(environment.T3_WORKSTREAM_SIGNING_SECRET, "invalid-secret"),
    ]) {
      NodeFS.writeFileSync(file, value);
      expect(resolveWorkstreamActivation(base, {})).toEqual({ state: "invalid" });
    }
    NodeFS.writeFileSync(file, Buffer.from([0xc3, 0x28]));
    expect(resolveWorkstreamActivation(base, {})).toEqual({ state: "invalid" });
  });

  it("rejects file, private directory and base modes including a group-writable 0775 base", () => {
    const { base, directory, file } = fixture();
    for (const [target, modes, restore] of [
      [file, [0, 0o400, 0o644, 0o660, 0o4600], 0o600],
      [directory, [0o755, 0o770], 0o700],
      [base, [0o775, 0o777], 0o700],
    ] as const) {
      for (const mode of modes) {
        NodeFS.chmodSync(target, mode);
        expect(resolveWorkstreamActivation(base, {})).toEqual({ state: "invalid" });
      }
      NodeFS.chmodSync(target, restore);
    }
    NodeFS.chmodSync(base, 0o755);
    expect(resolveWorkstreamActivation(base, {}).state).toBe("enabled");
    expect(resolveWorkstreamActivation(`${base}/`, {})).toEqual({ state: "invalid" });
  });

  it("rejects final and intermediate symlinks, including dangling links", () => {
    const { base, directory, file } = fixture();
    const saved = NodePath.join(base, "saved.env");
    NodeFS.renameSync(file, saved);
    NodeFS.symlinkSync(saved, file);
    expect(resolveWorkstreamActivation(base, {})).toEqual({ state: "invalid" });
    NodeFS.unlinkSync(file);
    NodeFS.symlinkSync(NodePath.join(base, "absent"), file);
    expect(resolveWorkstreamActivation(base, {})).toEqual({ state: "invalid" });
    NodeFS.unlinkSync(file);
    NodeFS.renameSync(saved, file);
    const moved = NodePath.join(base, "moved");
    NodeFS.renameSync(directory, moved);
    NodeFS.symlinkSync(moved, directory);
    expect(resolveWorkstreamActivation(base, {})).toEqual({ state: "invalid" });
    const link = NodePath.join(base, "base-link");
    NodeFS.symlinkSync(base, link);
    expect(resolveWorkstreamActivation(link, {})).toEqual({ state: "invalid" });
  });

  it("rejects writable ancestors and foreign directory ownership", () => {
    const { base } = fixture();
    const nested = NodePath.join(base, "installation");
    NodeFS.mkdirSync(nested, { mode: 0o700 });
    NodeFS.mkdirSync(NodePath.join(nested, "workstreams"), { mode: 0o700 });
    NodeFS.writeFileSync(NodePath.join(nested, "workstreams", "activation.env"), staged(), {
      mode: 0o600,
    });
    NodeFS.chmodSync(base, 0o777);
    expect(resolveWorkstreamActivation(nested, {})).toEqual({ state: "invalid" });
    NodeFS.chmodSync(base, 0o700);
    const lstat = NodeFS.lstatSync;
    vi.spyOn(NodeFS, "lstatSync").mockImplementation((path) => {
      const stat = lstat(path);
      if (path === base) stat.uid += 1;
      return stat;
    });
    expect(resolveWorkstreamActivation(nested, {})).toEqual({ state: "invalid" });
  });

  it("detects directory replacement after opening and closes its exact descriptor", () => {
    const { base, directory } = fixture();
    const read = NodeFS.readSync;
    const close = vi.spyOn(NodeFS, "closeSync");
    let changed = false;
    vi.spyOn(NodeFS, "readSync").mockImplementation(
      (...args: Parameters<typeof NodeFS.readSync>) => {
        const result = read(...args);
        if (!changed) {
          changed = true;
          NodeFS.renameSync(directory, NodePath.join(base, "previous"));
          NodeFS.mkdirSync(directory, { mode: 0o700 });
          NodeFS.writeFileSync(NodePath.join(directory, "activation.env"), staged(), {
            mode: 0o600,
          });
        }
        return result;
      },
    );
    expect(resolveWorkstreamActivation(base, {})).toEqual({ state: "invalid" });
    expect(close).toHaveBeenCalled();
  });

  it("rejects nonregular files and oversize data without blocking on a FIFO", () => {
    const { base, file } = fixture();
    NodeFS.writeFileSync(file, "x".repeat(65_537));
    expect(resolveWorkstreamActivation(base, {})).toEqual({ state: "invalid" });
    NodeFS.unlinkSync(file);
    NodeFS.mkdirSync(file, { mode: 0o600 });
    expect(resolveWorkstreamActivation(base, {})).toEqual({ state: "invalid" });
    NodeFS.rmdirSync(file);
    NodeChildProcess.execFileSync("mkfifo", ["-m", "600", file]);
    expect(resolveWorkstreamActivation(base, {})).toEqual({ state: "invalid" });
  });

  it("redacts unreadable paths and validates descriptor owner and post-open identity", () => {
    const { base, file } = fixture();
    const original = NodeFS.fstatSync;
    const foreign = vi.spyOn(NodeFS, "fstatSync").mockImplementation((descriptor) => {
      const stat = original(descriptor);
      stat.uid += 1;
      return stat;
    });
    expect(resolveWorkstreamActivation(base, {})).toEqual({ state: "invalid" });
    foreign.mockRestore();
    const read = NodeFS.readSync;
    const drift = vi
      .spyOn(NodeFS, "readSync")
      .mockImplementation((...args: Parameters<typeof NodeFS.readSync>) => {
        const result = read(...args);
        NodeFS.chmodSync(file, 0o644);
        return result;
      });
    expect(resolveWorkstreamActivation(base, {})).toEqual({ state: "invalid" });
    drift.mockRestore();
    const unreadable = vi.spyOn(NodeFS, "openSync").mockImplementation(() => {
      throw Object.assign(new Error("PRIVATE-MARKER"), { code: "EACCES" });
    });
    expect(resolveWorkstreamActivation(base, {})).toEqual({ state: "invalid" });
    unreadable.mockRestore();
  });
});

vi.mock("node:fs", async (importOriginal) => ({
  ...(await importOriginal<typeof import("node:fs")>()),
}));
