// @effect-diagnostics nodeBuiltinImport:off globalDate:off - Exercises real Node archive/cache I/O with a fixed injected qualification clock.
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { afterEach, describe, expect, it } from "vite-plus/test";

import {
  JonesActionsClient,
  JonesActionsError,
  JONES_ACTIONS_RECEIPT_FILE,
  jonesCandidateHandle,
  validateJonesStagedArtifact,
  type JonesActionsCandidate,
  type JonesActionsTransport,
} from "./jonesActions.ts";

const REPO = "Jones-Systems/Jones-Code";
const ROOT = `repos/${REPO}`;
const OLD = "a".repeat(40),
  CURRENT = "b".repeat(40),
  TREE = "c".repeat(40);
const NOW = Date.parse("2026-10-02T12:00:00Z");
const sha256 = (value: Buffer | string) =>
  NodeCrypto.createHash("sha256").update(value).digest("hex");
const roots: string[] = [];
afterEach(async () => {
  for (const root of roots.splice(0)) {
    await NodeFSP.rm(root, { recursive: true, force: true });
    await expect(NodeFSP.lstat(root)).rejects.toMatchObject({ code: "ENOENT" });
  }
});
async function cache() {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-actions-test-"));
  roots.push(root);
  return await NodeFSP.realpath(root);
}

/** Flat stored ZIP fixture: the transport digest authenticates its complete envelope. */
function zip(entries: Array<{ name: string; content: string; mode?: number }>): Buffer {
  const locals: Buffer[] = [],
    centrals: Buffer[] = [];
  let offset = 0;
  for (const entry of entries) {
    const name = Buffer.from(entry.name),
      content = Buffer.from(entry.content);
    const local = Buffer.alloc(30);
    local.writeUInt32LE(0x04034b50);
    local.writeUInt16LE(20, 4);
    local.writeUInt32LE(content.length, 18);
    local.writeUInt32LE(content.length, 22);
    local.writeUInt16LE(name.length, 26);
    const central = Buffer.alloc(46);
    central.writeUInt32LE(0x02014b50);
    central.writeUInt16LE(0x0314, 4);
    central.writeUInt16LE(20, 6);
    central.writeUInt32LE(content.length, 20);
    central.writeUInt32LE(content.length, 24);
    central.writeUInt16LE(name.length, 28);
    central.writeUInt32LE(((entry.mode ?? 0o100600) << 16) >>> 0, 38);
    central.writeUInt32LE(offset, 42);
    locals.push(local, name, content);
    centrals.push(central, name);
    offset += local.length + name.length + content.length;
  }
  const central = Buffer.concat(centrals),
    end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50);
  end.writeUInt16LE(entries.length, 8);
  end.writeUInt16LE(entries.length, 10);
  end.writeUInt32LE(central.length, 12);
  end.writeUInt32LE(offset, 16);
  return Buffer.concat([...locals, central, end]);
}

function fixture(
  options: {
    run?: Record<string, unknown>;
    ci?: Record<string, unknown>;
    artifact?: Record<string, unknown>;
    receipt?: Record<string, unknown>;
    comparison?: Record<string, unknown>;
    commit?: Record<string, unknown>;
    extra?: { name: string; content: string; mode?: number };
    corrupt?: boolean;
    interrupted?: boolean;
  } = {},
) {
  const version = "0.0.0-preview.20261002.101.2";
  const workflow = ".github/workflows/artifact-cli-linux.yml";
  const payloadName = `t3-${version}-linux-x64.tar.gz`,
    payload = "verified archive fixture";
  const receipt = {
    schema: 1,
    repository: REPO,
    source: CURRENT,
    tree: TREE,
    workflow,
    event: "push",
    ref: "refs/heads/main",
    runId: "101",
    runAttempt: "2",
    version,
    platform: "linux",
    architecture: "x64",
    artifact: payloadName,
    sha256: sha256(payload),
    ...options.receipt,
  };
  const envelope = zip([
    { name: "ARTIFACT.json", content: JSON.stringify(receipt) },
    { name: "SOURCE_COMMIT", content: CURRENT },
    { name: "SHA256SUMS", content: `${sha256(payload)}  ${payloadName}\n` },
    { name: payloadName, content: payload },
    ...(options.extra ? [options.extra] : []),
  ]);
  const baseRun = {
    path: workflow,
    id: 101,
    workflow_id: 5,
    run_attempt: 2,
    head_sha: CURRENT,
    head_branch: "main",
    event: "push",
    status: "completed",
    conclusion: "success",
    repository: { id: 77, full_name: REPO },
    head_repository: { id: 77, full_name: REPO },
  };
  const run = { ...baseRun, ...options.run };
  const ci = { ...baseRun, id: 99, path: ".github/workflows/ci.yml", ...options.ci };
  const artifact = {
    id: 301,
    name: `jones-code-cli-linux-x64-101-2--${version}`,
    size_in_bytes: envelope.length,
    expired: false,
    expires_at: "2026-10-09T12:00:00Z",
    digest: `sha256:${sha256(envelope)}`,
    workflow_run: {
      id: 101,
      repository_id: 77,
      head_repository_id: 77,
      head_branch: "main",
      head_sha: CURRENT,
    },
    ...options.artifact,
  };
  const calls: string[] = [];
  const transport: JonesActionsTransport = {
    async api(endpoint) {
      calls.push(endpoint);
      if (endpoint === `${ROOT}/commits/main`) return { sha: CURRENT };
      if (endpoint === `${ROOT}/git/commits/${CURRENT}`)
        return { sha: CURRENT, tree: { sha: TREE }, ...options.commit };
      if (endpoint === `${ROOT}/compare/${OLD}...${CURRENT}`)
        return { status: "ahead", merge_base_commit: { sha: OLD }, ...options.comparison };
      if (endpoint.includes("/actions/workflows/artifact-cli-linux.yml/runs?"))
        return { workflow_runs: [run] };
      if (endpoint.includes("/actions/workflows/ci.yml/runs?")) return { workflow_runs: [ci] };
      if (endpoint === `${ROOT}/actions/runs/101`) return run;
      if (endpoint === `${ROOT}/actions/runs/99`) return ci;
      if (endpoint.includes("/actions/runs/101/artifacts?")) return { artifacts: [artifact] };
      if (endpoint === `${ROOT}/actions/artifacts/301`) return artifact;
      throw new Error(`Unexpected fixture endpoint: ${endpoint}`);
    },
    async download(id, destination) {
      expect(id).toBe(301);
      await NodeFSP.writeFile(destination, options.corrupt ? Buffer.from("corrupt") : envelope, {
        flag: "wx",
      });
      if (options.interrupted) throw new JonesActionsError("unavailable");
    },
  };
  return {
    client: new JonesActionsClient({ transport, now: () => NOW }),
    transport,
    calls,
    receipt,
    artifact,
    envelope,
  };
}
const check = (client: JonesActionsClient) =>
  client.check({ installedSource: OLD, platform: "linux", architecture: "x64" });
async function available(client: JonesActionsClient): Promise<JonesActionsCandidate> {
  const result = await check(client);
  expect(result.state).toBe("available");
  if (result.state !== "available") throw new Error("Fixture candidate unavailable");
  return result.candidate;
}

describe("Jones Actions qualification", () => {
  it("binds canonical main run/attempt, same-source CI and source ancestry", async () => {
    const { client, calls } = fixture();
    const candidate = await available(client);
    expect(candidate).toMatchObject({
      source: CURRENT,
      tree: TREE,
      installedSource: OLD,
      runAttempt: 2,
      ciRunId: 99,
      artifactId: 301,
    });
    expect(calls.some((call) => call.includes(`head_sha=${CURRENT}`))).toBe(true);
    expect(calls.every((call) => call.startsWith(`${ROOT}/`))).toBe(true);
  });
  it("pins a requested source even after canonical main advances", async () => {
    const f = fixture();
    const next = "d".repeat(40);
    const client = new JonesActionsClient({
      now: () => NOW,
      transport: {
        ...f.transport,
        async api(endpoint) {
          if (endpoint === `${ROOT}/commits/main`) return { sha: next };
          if (endpoint === `${ROOT}/compare/${OLD}...${next}`)
            return { status: "ahead", merge_base_commit: { sha: OLD } };
          if (endpoint === `${ROOT}/compare/${CURRENT}...${next}`)
            return { status: "ahead", merge_base_commit: { sha: CURRENT } };
          return f.transport.api(endpoint);
        },
      },
    });
    const result = await client.check({
      installedSource: OLD, targetSource: CURRENT, platform: "linux", architecture: "x64",
    });
    expect(result).toMatchObject({ state: "available", candidate: { source: CURRENT } });
    expect(f.calls.some((call) => call.includes(`head_sha=${CURRENT}&per_page=50`))).toBe(true);
  });
  it("never substitutes a different successful source for the requested source", async () => {
    const f = fixture({ run: { head_sha: OLD } });
    expect(await f.client.check({
      installedSource: OLD, targetSource: CURRENT, platform: "linux", architecture: "x64",
    })).toMatchObject({ state: "blocked", reason: "unavailable" });
  });
  it("reports an already installed requested source only after canonical qualification", async () => {
    const input = { installedSource: OLD, targetSource: OLD, platform: "linux", architecture: "x64" } as const;
    expect(await fixture().client.check(input)).toEqual({ state: "no-new" });
    expect(await fixture({ comparison: { status: "diverged" } }).client.check(input))
      .toMatchObject({ state: "blocked", reason: "unqualified" });
    expect(await fixture().client.check({ ...input, targetSource: "invalid" }))
      .toMatchObject({ state: "blocked", reason: "unqualified" });
  });
  it.each([
    { event: "pull_request" },
    { head_branch: "feature" },
    { path: ".github/workflows/other.yml" },
    { repository: { id: 77, full_name: "other/repo" } },
    { head_repository: { id: 88, full_name: "fork/Jones-Code" } },
    { conclusion: "failure" },
    { run_attempt: 3 },
    { head_sha: "invalid-source" },
  ])("rejects mismatched producer facts %j", async (run) => {
    expect((await check(fixture({ run }).client)).state).not.toBe("available");
  });
  it.each([
    { head_sha: OLD },
    { status: "in_progress" },
    { conclusion: "failure" },
    { event: "pull_request" },
  ])("requires exact successful main CI %j", async (ci) => {
    expect((await check(fixture({ ci }).client)).state).not.toBe("available");
  });
  it.each([
    { expired: true },
    { expires_at: "2026-10-01T12:00:00Z" },
    { digest: undefined },
    { name: "jones-code-cli-linux-arm64-101-2--0.0.0-preview.20261002.101.2" },
    {
      workflow_run: {
        id: 101,
        head_sha: OLD,
        head_branch: "main",
        repository_id: 77,
        head_repository_id: 77,
      },
    },
  ])("rejects expired or mismatched artifact %j", async (artifact) => {
    expect((await check(fixture({ artifact }).client)).state).not.toBe("available");
  });
  it("rejects diverged canonical main even when version/date would be newer", async () => {
    expect(await check(fixture({ comparison: { status: "diverged" } }).client)).toMatchObject({
      state: "blocked",
      reason: "unqualified",
    });
  });
  it.each([{ sha: OLD }, { tree: { sha: "bad-tree" } }, { tree: { sha: "d".repeat(64) } }])(
    "rejects invalid canonical commit/tree API binding %j",
    async (commit) => {
      expect(await check(fixture({ commit }).client)).toMatchObject({
        state: "blocked",
        reason: "unqualified",
      });
    },
  );
  it("reports building and authentication without exposing transport details", async () => {
    expect(
      await check(fixture({ run: { status: "in_progress", conclusion: null } }).client),
    ).toEqual({ state: "building", retryAfterMs: 240000 });
    const client = new JonesActionsClient({
      transport: {
        api: async () => {
          throw new JonesActionsError("authentication-required");
        },
        download: async () => {},
      },
    });
    expect(await check(client)).toMatchObject({
      state: "blocked",
      reason: "authentication-required",
    });
  });
  it("bounds main-run pagination and returns a retry interval when candidates are unavailable", async () => {
    const f = fixture();
    let pages = 0;
    const client = new JonesActionsClient({
      now: () => NOW,
      transport: {
        ...f.transport,
        async api(endpoint) {
          if (endpoint.includes("/actions/workflows/artifact-cli-linux.yml/runs?")) {
            pages++;
            return {
              workflow_runs: Array.from({ length: 50 }, () => ({
                path: ".github/workflows/unapproved.yml",
              })),
            };
          }
          return f.transport.api(endpoint);
        },
      },
    });
    expect(await check(client)).toEqual({
      state: "blocked",
      reason: "unavailable",
      retryAfterMs: 240000,
    });
    expect(pages).toBe(3);
  });
  it("sanitizes unexpected transport failures and returns no-new for the installed main source", async () => {
    const failing = new JonesActionsClient({
      transport: {
        api: async () => {
          throw new Error("HTTP header and signed download URL must remain private");
        },
        download: async () => {},
      },
    });
    expect(await check(failing)).toEqual({
      state: "blocked",
      reason: "unavailable",
      retryAfterMs: 240000,
    });
    expect(
      await fixture().client.check({
        installedSource: CURRENT,
        platform: "linux",
        architecture: "x64",
      }),
    ).toEqual({ state: "no-new" });
  });
});

describe("Jones Actions staging", () => {
  it("stages without extraction/activation, retains exact receipt and reuses verified cache", async () => {
    const { client } = fixture();
    const candidate = await available(client);
    const root = await cache();
    const staged = await client.stage(candidate, root);
    expect(await NodeFSP.readFile(staged.payloadPath, "utf8")).toBe("verified archive fixture");
    expect(await NodeFSP.readdir(NodePath.join(root, "completed"))).toEqual([`${staged.stagedHandle}.json`]);
    expect(await client.stage(candidate, root)).toEqual(staged);
    expect(
      await validateJonesStagedArtifact(NodePath.dirname(staged.payloadPath), candidate),
    ).toEqual(staged);
  });
  it.each([
    { name: "../escape", content: "bad" },
    { name: "/absolute", content: "bad" },
    { name: "link", content: "../../escape", mode: 0o120777 },
    { name: "unexpected", content: "bad" },
  ])("rejects malicious envelope %j before payload staging", async (extra) => {
    const { client } = fixture({ extra });
    const candidate = await available(client);
    const root = await cache();
    await expect(client.stage(candidate, root)).rejects.toMatchObject({ reason: "integrity" });
    expect(await NodeFSP.readdir(NodePath.join(root, "attempts"))).toEqual([]);
  });
  it.each([
    { platform: "darwin" },
    { architecture: "arm64" },
    { source: OLD },
    { tree: OLD },
    { runAttempt: "1" },
    { workflow: ".github/workflows/ci.yml" },
    { ref: "refs/pull/1/merge" },
    { sha256: "d".repeat(64) },
  ])("rejects wrong inner binding or digest %j", async (receipt) => {
    const { client } = fixture({ receipt });
    const candidate = await available(client);
    await expect(client.stage(candidate, await cache())).rejects.toBeInstanceOf(JonesActionsError);
  });
  it.each([{ corrupt: true }, { interrupted: true }])(
    "cleans its partial download on failed transport %j",
    async (options) => {
      const { client } = fixture(options);
      const candidate = await available(client);
      const root = await cache();
      await expect(client.stage(candidate, root)).rejects.toBeInstanceOf(JonesActionsError);
      expect(await NodeFSP.readdir(NodePath.join(root, "attempts"))).toEqual([]);
    },
  );
  it("does not overwrite occupied unknown cache paths", async () => {
    const { client } = fixture();
    const candidate = await available(client);
    const root = await cache();
    const occupied = NodePath.join(root, jonesCandidateHandle(candidate));
    await NodeFSP.mkdir(occupied);
    await NodeFSP.writeFile(NodePath.join(occupied, "owner-data"), "retain");
    const staged = await client.stage(candidate, root);
    expect(NodePath.dirname(staged.payloadPath)).not.toBe(occupied);
    expect(await NodeFSP.readdir(occupied)).toEqual(["owner-data"]);
    expect(await NodeFSP.readFile(NodePath.join(occupied, "owner-data"), "utf8")).toBe("retain");
  });
  it("rejects modified payload and forged cache receipt against original envelope", async () => {
    const { client } = fixture();
    const candidate = await available(client);
    const staged = await client.stage(candidate, await cache());
    const directory = NodePath.dirname(staged.payloadPath);
    await NodeFSP.writeFile(staged.payloadPath, "replacement");
    const modified = { ...staged, receipt: { ...staged.receipt, sha256: sha256("replacement") } };
    await NodeFSP.writeFile(
      NodePath.join(directory, JONES_ACTIONS_RECEIPT_FILE),
      JSON.stringify(modified),
    );
    await expect(validateJonesStagedArtifact(directory, candidate)).rejects.toMatchObject({
      reason: "integrity",
    });
  });
  it("binds source even for same-version cache and serializes concurrent reservations", async () => {
    const { client } = fixture();
    const candidate = await available(client);
    const root = await cache();
    expect(jonesCandidateHandle({ ...candidate, source: "d".repeat(40) })).not.toBe(
      jonesCandidateHandle(candidate),
    );
    const outcomes = await Promise.allSettled([
      client.stage(candidate, root),
      client.stage(candidate, root),
    ]);
    expect(outcomes.some((outcome) => outcome.status === "fulfilled")).toBe(true);
    const staged = outcomes.find((outcome) => outcome.status === "fulfilled");
    if (staged?.status !== "fulfilled") throw new Error("No completed reservation");
    await expect(
      validateJonesStagedArtifact(NodePath.dirname(staged.value.payloadPath), candidate),
    ).resolves.toEqual(staged.value);
    expect(outcomes.every((outcome) => outcome.status === "fulfilled")).toBe(true);
    expect(await NodeFSP.readdir(NodePath.join(root, "completed"))).toEqual([`${staged.value.stagedHandle}.json`]);
    expect(await NodeFSP.readdir(NodePath.join(root, "attempts"))).toHaveLength(1);
  });
  it("retries an interrupted download without consuming a retained partial attempt", async () => {
    const failed = fixture({ interrupted: true });
    const next = fixture();
    const candidate = await available(failed.client);
    const root = await cache();
    await expect(failed.client.stage(candidate, root)).rejects.toMatchObject({ reason: "unavailable" });
    const partial = NodePath.join(root, "attempts", "stage-interrupted", jonesCandidateHandle(candidate));
    await NodeFSP.mkdir(partial, { recursive: true });
    await NodeFSP.writeFile(NodePath.join(partial, "github-artifact.zip"), "partial");
    const staged = await next.client.stage(candidate, root);
    expect(await NodeFSP.readFile(staged.payloadPath, "utf8")).toBe("verified archive fixture");
    expect(await NodeFSP.readFile(NodePath.join(partial, "github-artifact.zip"), "utf8")).toBe("partial");
  });
  it("continues to reuse a verified legacy stage", async () => {
    const { client } = fixture();
    const candidate = await available(client);
    const root = await cache();
    const initial = await client.stage(candidate, root);
    const legacyRoot = await cache();
    const legacy = NodePath.join(legacyRoot, initial.stagedHandle);
    await NodeFSP.cp(NodePath.dirname(initial.payloadPath), legacy, { recursive: true });
    const receipt = { ...initial, payloadPath: NodePath.join(legacy, NodePath.basename(initial.payloadPath)) };
    await NodeFSP.writeFile(NodePath.join(legacy, JONES_ACTIONS_RECEIPT_FILE), JSON.stringify(receipt));
    expect(await client.stage(candidate, legacyRoot)).toEqual(receipt);
  });
  it("rechecks final run attempt rather than downloading a stale candidate", async () => {
    const f = fixture();
    const candidate = await available(f.client);
    const replacement = fixture({ run: { run_attempt: 3 } });
    await expect(replacement.client.stage(candidate, await cache())).rejects.toMatchObject({
      reason: "unqualified",
    });
  });
  it("rechecks exact canonical source tree before downloading a candidate", async () => {
    const f = fixture();
    const candidate = await available(f.client);
    await expect(f.client.stage({ ...candidate, tree: OLD }, await cache())).rejects.toMatchObject({
      reason: "unqualified",
    });
    const changedSource = fixture({ commit: { sha: OLD } });
    await expect(changedSource.client.stage(candidate, await cache())).rejects.toMatchObject({
      reason: "unqualified",
    });
  });
});
