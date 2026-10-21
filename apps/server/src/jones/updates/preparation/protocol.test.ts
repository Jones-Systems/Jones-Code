// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { JonesActionsError } from "@t3tools/shared/jones/jonesActions";
import { afterEach, describe, expect, it } from "vite-plus/test";
import { QualifiedRuntimeBlockedError } from "../../cloud/qualifiedRuntime.ts";
import {
  decodePreparationEvent,
  decodePreparationRequest,
  decodePreparationResult,
  describePreparationFailure,
  PREPARATION_LIMITS,
  preparationFailureError,
  preparationJobsDirectory,
  readBoundedJson,
  writeDurableJson,
} from "./protocol.ts";

const roots: string[] = [];
afterEach(async () => {
  for (const root of roots.splice(0)) await NodeFSP.rm(root, { recursive: true, force: true });
});
async function scratch(): Promise<string> {
  const root = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-preparation-protocol-")),
  );
  roots.push(root);
  return root;
}

const baseDir = "/srv/jones-home";
const jobId = NodeCrypto.randomUUID();
const jobDir = NodePath.join(preparationJobsDirectory(baseDir), jobId);
const host = { platform: "linux", architecture: "x64" } as const;
const request = {
  protocol: 1,
  jobId,
  baseDir,
  deadlineMs: 60_000,
  processGroup: true,
  job: { kind: "verify", stagedHandle: NodeCrypto.randomUUID(), activeVersion: "1.0.0", host },
};

describe("preparation requests", () => {
  it("binds a request to its own job directory and bounds its fields", () => {
    expect(decodePreparationRequest(request, jobDir)).toEqual(request);
    expect(
      decodePreparationRequest(
        { ...request, job: { kind: "sweep", cacheRoot: "/srv/jones-home/runtime/jones-actions" } },
        jobDir,
      )?.job,
    ).toEqual({ kind: "sweep", cacheRoot: "/srv/jones-home/runtime/jones-actions" });
    for (const value of [
      { ...request, protocol: 2 },
      { ...request, jobId: "../escape" },
      { ...request, baseDir: "relative/home" },
      { ...request, deadlineMs: 0 },
      { ...request, deadlineMs: 25 * 60 * 60 * 1000 },
      { ...request, processGroup: "yes" },
      { ...request, job: { ...request.job, kind: "install" } },
      { ...request, job: { ...request.job, stagedHandle: "f".repeat(64) } },
      { ...request, job: { ...request.job, host: { platform: "win32", architecture: "x64" } } },
      { ...request, job: { kind: "sweep", cacheRoot: "jones-actions" } },
      {
        ...request,
        job: {
          kind: "stage",
          candidate: {},
          cacheRoot: "/c",
          dbPath: "db",
          activeVersion: "1",
          host,
        },
      },
      [request],
      null,
    ])
      expect(decodePreparationRequest(value, jobDir)).toBeUndefined();
    expect(
      decodePreparationRequest(request, NodePath.join(preparationJobsDirectory("/other"), jobId)),
    ).toBeUndefined();
  });
});

describe("preparation events", () => {
  it("accepts only the ready handshake, known phases and heartbeats", () => {
    expect(decodePreparationEvent('{"type":"ready","protocol":1,"pid":42,"priority":19}')).toEqual({
      type: "ready",
      protocol: 1,
      pid: 42,
      priority: 19,
    });
    expect(
      decodePreparationEvent('{"type":"ready","protocol":1,"pid":42,"priority":null}'),
    ).toEqual({ type: "ready", protocol: 1, pid: 42, priority: null });
    expect(decodePreparationEvent('{"type":"phase","phase":"verifying"}')).toEqual({
      type: "phase",
      phase: "verifying",
    });
    expect(decodePreparationEvent('{"type":"heartbeat","phase":"waiting"}')).toEqual({
      type: "heartbeat",
      phase: "waiting",
    });
    for (const line of [
      "",
      "not json",
      '{"type":"ready","protocol":1,"pid":-1,"priority":0}',
      '{"type":"ready","protocol":1,"pid":42,"priority":0.5}',
      '{"type":"phase","phase":"installing"}',
      '{"type":"result","ok":true}',
      "[]",
    ])
      expect(decodePreparationEvent(line)).toBeUndefined();
  });
});

describe("preparation results", () => {
  it("accepts a result only for its own job and kind, with bounded failures", () => {
    const sweep = { kind: "sweep", removed: 1, retained: 2, failed: 0 };
    expect(
      decodePreparationResult({ protocol: 1, jobId, ok: true, outcome: sweep }, jobId, "sweep"),
    ).toEqual({
      protocol: 1,
      jobId,
      ok: true,
      outcome: sweep,
    });
    const failure = { name: "QualifiedRuntimeBlockedError", message: "Busy.", reason: "busy" };
    expect(
      decodePreparationResult({ protocol: 1, jobId, ok: false, error: failure }, jobId, "stage"),
    ).toEqual({ protocol: 1, jobId, ok: false, error: failure });
    for (const value of [
      { protocol: 1, jobId: NodeCrypto.randomUUID(), ok: true, outcome: sweep },
      { protocol: 1, jobId, ok: true, outcome: { ...sweep, removed: -1 } },
      { protocol: 1, jobId, ok: true, outcome: { kind: "verify", staged: {} } },
      { protocol: 1, jobId, ok: false, error: { ...failure, message: "x".repeat(513) } },
      { protocol: 1, jobId, ok: false, error: { ...failure, reason: "Not A Reason" } },
      { protocol: 1, jobId, ok: "maybe" },
    ])
      expect(decodePreparationResult(value, jobId, "sweep")).toBeUndefined();
    expect(
      decodePreparationResult({ protocol: 1, jobId, ok: true, outcome: sweep }, jobId, "verify"),
    ).toBeUndefined();
  });

  it("carries a worker failure as its name, bounded message and blocked reason only", () => {
    const blocked = new QualifiedRuntimeBlockedError("integrity-mismatch", "x".repeat(2_000));
    const described = describePreparationFailure(blocked);
    expect(described).toEqual({
      name: "QualifiedRuntimeBlockedError",
      message: "x".repeat(PREPARATION_LIMITS.messageChars),
      reason: "integrity-mismatch",
    });
    const rebuilt = preparationFailureError(described);
    expect(rebuilt).toBeInstanceOf(QualifiedRuntimeBlockedError);
    expect(rebuilt).toMatchObject({ reason: "integrity-mismatch" });
    const actions = preparationFailureError(
      describePreparationFailure(new JonesActionsError("integrity")),
    );
    expect(actions).toBeInstanceOf(JonesActionsError);
    expect(actions).toMatchObject({ reason: "integrity" });
    const unknown = preparationFailureError({
      name: "QualifiedRuntimeBlockedError",
      message: "Unknown reason.",
      reason: "made-up",
    });
    expect(unknown).not.toBeInstanceOf(QualifiedRuntimeBlockedError);
    expect(unknown).toMatchObject({
      name: "QualifiedRuntimeBlockedError",
      message: "Unknown reason.",
    });
    expect(describePreparationFailure("not an error")).toEqual({
      name: "Error",
      message: "Jones update preparation failed.",
    });
  });
});

describe("preparation files", () => {
  it("reads only bounded, owner-controlled regular files", async () => {
    const root = await scratch();
    const good = NodePath.join(root, "good.json");
    await NodeFSP.writeFile(good, '{"ok":true}', { mode: 0o600 });
    await expect(readBoundedJson(good, 64)).resolves.toEqual({ ok: true });
    await expect(readBoundedJson(good, 4)).rejects.toThrow(/bounded owner-controlled/);
    const shared = NodePath.join(root, "shared.json");
    await NodeFSP.writeFile(shared, "{}");
    await NodeFSP.chmod(shared, 0o622);
    await expect(readBoundedJson(shared, 64)).rejects.toThrow(/bounded owner-controlled/);
    const link = NodePath.join(root, "link.json");
    await NodeFSP.symlink(good, link);
    await expect(readBoundedJson(link, 64)).rejects.toMatchObject({ code: "ELOOP" });
  });

  it("publishes a complete file or nothing", async () => {
    const root = await scratch();
    const target = NodePath.join(root, "result.json");
    await expect(writeDurableJson(target, { padding: "x".repeat(64) }, 32)).rejects.toThrow(
      /exceeds its bound/,
    );
    expect(await NodeFSP.readdir(root)).toEqual([]);
    await writeDurableJson(target, { ok: true }, 64);
    expect(JSON.parse(await NodeFSP.readFile(target, "utf8"))).toEqual({ ok: true });
    expect((await NodeFSP.stat(target)).mode & 0o777).toBe(0o600);
    await NodeFSP.writeFile(`${target}.partial`, "stale");
    await expect(writeDurableJson(target, { ok: false }, 64)).rejects.toMatchObject({
      code: "EEXIST",
    });
    expect(JSON.parse(await NodeFSP.readFile(target, "utf8"))).toEqual({ ok: true });
  });
});
