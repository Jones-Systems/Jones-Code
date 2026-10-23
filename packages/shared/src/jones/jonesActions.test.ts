// @effect-diagnostics nodeBuiltinImport:off globalDate:off - Exercises real Node archive/cache I/O with a fixed injected qualification clock.
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeZlib from "node:zlib";
import { afterEach, describe, expect, it } from "vite-plus/test";

import {
  JonesActionsClient,
  JonesActionsError,
  JONES_ACTIONS_RECEIPT_FILE,
  jonesCandidateHandle,
  readJonesArtifactMetadata,
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

interface ZipFixtureEntry {
  readonly name: string;
  readonly content: string | Buffer;
  readonly mode?: number;
  /** Raw deflate (method 8) instead of stored. */
  readonly deflate?: boolean;
  /** Bit 3: zero local CRC/sizes followed by a data descriptor, as streaming writers emit. */
  readonly descriptor?: boolean;
  /** Recorded CRC-32 and expanded size; default to the content's real values. */
  readonly crc?: number;
  readonly bytes?: number;
  /** Recorded entry data; defaults to the stored or deflated content. */
  readonly data?: Buffer;
}

/** Flat ZIP fixture with real CRC-32: the transport digest authenticates its complete envelope. */
function zip(entries: ReadonlyArray<ZipFixtureEntry>): Buffer {
  const locals: Buffer[] = [],
    centrals: Buffer[] = [];
  let offset = 0;
  for (const entry of entries) {
    const name = Buffer.from(entry.name),
      content = Buffer.from(entry.content);
    const data = entry.data ?? (entry.deflate ? NodeZlib.deflateRawSync(content) : content);
    const crc = entry.crc ?? NodeZlib.crc32(content),
      bytes = entry.bytes ?? content.length;
    const flags = entry.descriptor ? 0x08 : 0,
      method = entry.deflate ? 8 : 0;
    const local = Buffer.alloc(30);
    local.writeUInt32LE(0x04034b50);
    local.writeUInt16LE(20, 4);
    local.writeUInt16LE(flags, 6);
    local.writeUInt16LE(method, 8);
    if (!entry.descriptor) {
      local.writeUInt32LE(crc, 14);
      local.writeUInt32LE(data.length, 18);
      local.writeUInt32LE(bytes, 22);
    }
    local.writeUInt16LE(name.length, 26);
    const descriptor = Buffer.alloc(entry.descriptor ? 16 : 0);
    if (entry.descriptor) {
      descriptor.writeUInt32LE(0x08074b50);
      descriptor.writeUInt32LE(crc, 4);
      descriptor.writeUInt32LE(data.length, 8);
      descriptor.writeUInt32LE(bytes, 12);
    }
    const central = Buffer.alloc(46);
    central.writeUInt32LE(0x02014b50);
    central.writeUInt16LE(0x0314, 4);
    central.writeUInt16LE(20, 6);
    central.writeUInt16LE(flags, 8);
    central.writeUInt16LE(method, 10);
    central.writeUInt32LE(crc, 16);
    central.writeUInt32LE(data.length, 20);
    central.writeUInt32LE(bytes, 24);
    central.writeUInt16LE(name.length, 28);
    central.writeUInt32LE(((entry.mode ?? 0o100600) << 16) >>> 0, 38);
    central.writeUInt32LE(offset, 42);
    locals.push(local, name, data, descriptor);
    centrals.push(central, name);
    offset += local.length + name.length + data.length + descriptor.length;
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
    desktop?: "legacy" | "production";
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
  const workflow = options.desktop === "production" ? ".github/workflows/jones-desktop-signing.yml" : options.desktop === "legacy" ? ".github/workflows/artifact-desktop-mac.yml" : ".github/workflows/artifact-cli-linux.yml";
  const artifactPrefix = options.desktop === "production" ? "desktop-mac-production-arm64" : options.desktop === "legacy" ? "desktop-mac-arm64" : "jones-code-cli-linux-x64";
  const payloadName = options.desktop ? `T3-Code-${version}-arm64.dmg` : `t3-${version}-linux-x64.tar.gz`,
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
    platform: options.desktop ? "darwin" : "linux",
    architecture: options.desktop ? "arm64" : "x64",
    ...(options.desktop === "production" ? { signed: true, notarized: true, bundleIdentifier: "com.jonessystems.jonescode", teamId: "2CUPSQS986" } : {}),
    artifact: payloadName,
    sha256: sha256(payload),
    ...options.receipt,
  };
  const envelope = zip([
    { name: "ARTIFACT.json", content: JSON.stringify(receipt) },
    { name: "SOURCE_COMMIT", content: CURRENT },
    { name: options.desktop ? `${payloadName}.sha256` : "SHA256SUMS", content: `${sha256(payload)}  ${payloadName}\n` },
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
    name: `${artifactPrefix}-101-2--${version}`,
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
      if (endpoint === `${ROOT}/compare/${OLD}...${CURRENT}?per_page=1`)
        return { status: "ahead", merge_base_commit: { sha: OLD }, ...options.comparison };
      if (endpoint.includes(`/actions/workflows/${NodePath.posix.basename(workflow)}/runs?`))
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
  it.each([undefined, "2026-10-02T10:00:00Z", "invalid", "2026-02-30T10:00:00Z"])(
    "keeps build time %s outside the candidate and its handle",
    async (run_started_at) => {
      const baseline = fixture();
      const stamped = fixture({ run: { run_started_at } });
      const original = await check(baseline.client);
      const result = await check(stamped.client);
      expect(original.state).toBe("available");
      expect(result.state).toBe("available");
      if (original.state !== "available" || result.state !== "available")
        throw new Error("Fixture candidate unavailable");
      expect(result.buildStartedAt).toBe(
        run_started_at === "2026-10-02T10:00:00Z" ? run_started_at : undefined,
      );
      expect(JSON.stringify(result.candidate)).toBe(JSON.stringify(original.candidate));
      expect(jonesCandidateHandle(result.candidate)).toBe(jonesCandidateHandle(original.candidate));
      expect(stamped.calls).toEqual(baseline.calls);
      if (result.buildStartedAt === undefined) expect(result).not.toHaveProperty("buildStartedAt");
    },
  );
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
          if (endpoint === `${ROOT}/compare/${OLD}...${next}?per_page=1`)
            return { status: "ahead", merge_base_commit: { sha: OLD } };
          if (endpoint === `${ROOT}/compare/${CURRENT}...${next}?per_page=1`)
            return { status: "ahead", merge_base_commit: { sha: CURRENT } };
          return f.transport.api(endpoint);
        },
      },
    });
    const result = await client.check({
      installedSource: OLD,
      targetSource: CURRENT,
      platform: "linux",
      architecture: "x64",
    });
    expect(result).toMatchObject({ state: "available", candidate: { source: CURRENT } });
    expect(f.calls.some((call) => call.includes(`head_sha=${CURRENT}&per_page=50`))).toBe(true);
  });
  it("never substitutes a different successful source for the requested source", async () => {
    const f = fixture({ run: { head_sha: OLD } });
    expect(
      await f.client.check({
        installedSource: OLD,
        targetSource: CURRENT,
        platform: "linux",
        architecture: "x64",
      }),
    ).toMatchObject({ state: "blocked", reason: "unavailable" });
  });
  it("reports an already installed requested source only after canonical qualification", async () => {
    const input = {
      installedSource: OLD,
      targetSource: OLD,
      platform: "linux",
      architecture: "x64",
    } as const;
    expect(await fixture().client.check(input)).toEqual({ state: "no-new" });
    expect(await fixture({ comparison: { status: "diverged" } }).client.check(input)).toMatchObject(
      { state: "blocked", reason: "unqualified" },
    );
    expect(await fixture().client.check({ ...input, targetSource: "invalid" })).toMatchObject({
      state: "blocked",
      reason: "unqualified",
    });
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
  it("reports pending main CI as building but a completed CI failure as blocked", async () => {
    expect(
      await check(fixture({ ci: { status: "in_progress", conclusion: null } }).client),
    ).toEqual({ state: "building", retryAfterMs: 240000 });
    expect(await check(fixture({ ci: { conclusion: "failure" } }).client)).toEqual({
      state: "blocked",
      reason: "unavailable",
      retryAfterMs: 240000,
    });
  });
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

describe("Jones Actions ancestry memo", () => {
  const compareOf = (calls: readonly string[]) =>
    calls.filter((call) => call.includes("/compare/"));

  it("reads each compare with a one-commit page", async () => {
    const { client, calls } = fixture();
    await check(client);
    expect(compareOf(calls)).toEqual([`${ROOT}/compare/${OLD}...${CURRENT}?per_page=1`]);
  });

  it("answers a repeated exact pair from memory but re-reads canonical main every check", async () => {
    const { client, calls } = fixture();
    expect((await check(client)).state).toBe("available");
    expect((await check(client)).state).toBe("available");
    expect(compareOf(calls)).toHaveLength(1);
    expect(calls.filter((call) => call === `${ROOT}/commits/main`)).toHaveLength(2);
  });

  it("re-proves main membership for the new main when main advances", async () => {
    const f = fixture();
    const next = "d".repeat(40);
    let main = CURRENT;
    const compares: string[] = [];
    const client = new JonesActionsClient({
      now: () => NOW,
      transport: {
        ...f.transport,
        async api(endpoint) {
          if (endpoint === `${ROOT}/commits/main`) return { sha: main };
          if (endpoint.includes("/compare/")) {
            compares.push(endpoint);
            return endpoint.includes(`...${next}`)
              ? { status: "diverged", merge_base_commit: { sha: "9".repeat(40) } }
              : f.transport.api(endpoint);
          }
          return f.transport.api(endpoint);
        },
      },
    });
    expect((await check(client)).state).toBe("available");
    // Main is rewritten to a commit that no longer contains the installed source.
    main = next;
    expect(await check(client)).toMatchObject({ state: "blocked", reason: "unqualified" });
    expect(compares).toEqual([
      `${ROOT}/compare/${OLD}...${CURRENT}?per_page=1`,
      `${ROOT}/compare/${OLD}...${next}?per_page=1`,
    ]);
  });

  it("memoizes a parsed negative relation", async () => {
    const { client, calls } = fixture({ comparison: { status: "diverged" } });
    for (let round = 0; round < 2; round++)
      expect(await check(client)).toMatchObject({ state: "blocked", reason: "unqualified" });
    expect(compareOf(calls)).toHaveLength(1);
  });

  it("never memoizes a transport failure or an unparsable comparison", async () => {
    const f = fixture();
    let failures = 1;
    const compares: string[] = [];
    const client = new JonesActionsClient({
      now: () => NOW,
      transport: {
        ...f.transport,
        async api(endpoint) {
          if (!endpoint.includes("/compare/")) return f.transport.api(endpoint);
          compares.push(endpoint);
          if (failures-- > 0) throw new Error("HTTP 502 private detail");
          return f.transport.api(endpoint);
        },
      },
    });
    expect(await check(client)).toMatchObject({ state: "blocked", reason: "unavailable" });
    expect((await check(client)).state).toBe("available");
    expect(compares).toHaveLength(2);

    const unparsable = fixture({ comparison: { status: "unrelated" } });
    for (let round = 0; round < 2; round++)
      expect((await check(unparsable.client)).state).toBe("blocked");
    expect(compareOf(unparsable.calls)).toHaveLength(2);
  });

  it("bounds the memo and evicts the oldest exact pair first", async () => {
    const sha = (index: number) => index.toString(16).padStart(40, "0");
    let main = sha(1);
    const compares: string[] = [];
    const client = new JonesActionsClient({
      now: () => NOW,
      transport: {
        async api(endpoint) {
          if (endpoint === `${ROOT}/commits/main`) return { sha: main };
          if (endpoint.includes("/compare/")) {
            compares.push(endpoint);
            return { status: "ahead", merge_base_commit: { sha: OLD } };
          }
          // No workflow data: the check ends after the ancestry proof.
          return { workflow_runs: [] };
        },
        download: async () => {},
      },
    });
    for (let index = 1; index <= 300; index++) {
      main = sha(index);
      await check(client);
    }
    expect(compares).toHaveLength(300);
    main = sha(300);
    await check(client);
    expect(compares).toHaveLength(300);
    main = sha(1);
    await check(client);
    expect(compares).toHaveLength(301);
  });
});

describe("Jones Actions staging", () => {
  it("stages without extraction/activation, retains exact receipt and reuses verified cache", async () => {
    const { client } = fixture();
    const candidate = await available(client);
    const root = await cache();
    const staged = await client.stage(candidate, root);
    expect(await NodeFSP.readFile(staged.payloadPath, "utf8")).toBe("verified archive fixture");
    expect(await NodeFSP.readdir(NodePath.join(root, "completed"))).toEqual([
      `${staged.stagedHandle}.json`,
    ]);
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
    expect(await NodeFSP.readdir(NodePath.join(root, "completed"))).toEqual([
      `${staged.value.stagedHandle}.json`,
    ]);
    expect(await NodeFSP.readdir(NodePath.join(root, "attempts"))).toHaveLength(1);
  });
  it("retries an interrupted download without consuming a retained partial attempt", async () => {
    const failed = fixture({ interrupted: true });
    const next = fixture();
    const candidate = await available(failed.client);
    const root = await cache();
    await expect(failed.client.stage(candidate, root)).rejects.toMatchObject({
      reason: "unavailable",
    });
    const partial = NodePath.join(
      root,
      "attempts",
      "stage-interrupted",
      jonesCandidateHandle(candidate),
    );
    await NodeFSP.mkdir(partial, { recursive: true });
    await NodeFSP.writeFile(NodePath.join(partial, "github-artifact.zip"), "partial");
    const staged = await next.client.stage(candidate, root);
    expect(await NodeFSP.readFile(staged.payloadPath, "utf8")).toBe("verified archive fixture");
    expect(await NodeFSP.readFile(NodePath.join(partial, "github-artifact.zip"), "utf8")).toBe(
      "partial",
    );
  });
  it("continues to reuse a verified legacy stage", async () => {
    const { client } = fixture();
    const candidate = await available(client);
    const root = await cache();
    const initial = await client.stage(candidate, root);
    const legacyRoot = await cache();
    const legacy = NodePath.join(legacyRoot, initial.stagedHandle);
    await NodeFSP.cp(NodePath.dirname(initial.payloadPath), legacy, { recursive: true });
    const receipt = {
      ...initial,
      payloadPath: NodePath.join(legacy, NodePath.basename(initial.payloadPath)),
    };
    await NodeFSP.writeFile(
      NodePath.join(legacy, JONES_ACTIONS_RECEIPT_FILE),
      JSON.stringify(receipt),
    );
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

describe("Jones artifact metadata envelope", () => {
  const metadata = JSON.stringify({ schema: 1, source: CURRENT, padding: "x".repeat(512) });
  // A case variant is a distinct entry; only the exact name is selected.
  const companions: ZipFixtureEntry[] = [
    { name: "SOURCE_COMMIT", content: CURRENT },
    { name: "artifact.json", content: "not the metadata entry" },
  ];
  async function archive(bytes: Buffer): Promise<string> {
    const file = NodePath.join(await cache(), "github-artifact.zip");
    await NodeFSP.writeFile(file, bytes, { flag: "wx" });
    return file;
  }
  async function expectIntegrity(bytes: Buffer) {
    await expect(readJonesArtifactMetadata(await archive(bytes))).rejects.toMatchObject({
      reason: "integrity",
    });
  }
  type Encoding = Pick<ZipFixtureEntry, "deflate" | "descriptor">;

  it.each<{ readonly label: string } & Encoding>([
    { label: "stored" },
    { label: "deflated", deflate: true },
    { label: "deflated data-descriptor", deflate: true, descriptor: true },
    { label: "stored data-descriptor", descriptor: true },
  ])("reads exact $label metadata natively", async ({ label: _label, ...encoding }) => {
    const bytes = zip([{ name: "ARTIFACT.json", content: metadata, ...encoding }, ...companions]);
    // Streaming writers leave local CRC/sizes zero; the central record governs.
    if (encoding.descriptor) expect(bytes.readUInt32LE(14)).toBe(0);
    expect((await readJonesArtifactMetadata(await archive(bytes))).toString("utf8")).toBe(metadata);
  });
  it.each<Encoding>([{}, { deflate: true }, { deflate: true, descriptor: true }])(
    "rejects decoded metadata that differs from its recorded CRC-32 %j",
    async (encoding) => {
      const crc = (NodeZlib.crc32(metadata) ^ 1) >>> 0;
      await expectIntegrity(
        zip([{ name: "ARTIFACT.json", content: metadata, crc, ...encoding }, ...companions]),
      );
    },
  );
  it.each<[string, ZipFixtureEntry[]]>([
    ["missing", companions],
    [
      "duplicate",
      [
        { name: "ARTIFACT.json", content: metadata },
        { name: "ARTIFACT.json", content: metadata },
      ],
    ],
    [
      "directory-mode",
      [{ name: "ARTIFACT.json", content: metadata, mode: 0o040755 }, ...companions],
    ],
  ])("rejects %s metadata entries", async (_label, entries) => {
    await expectIntegrity(zip(entries));
  });
  it.each<[string, ZipFixtureEntry]>([
    ["expanded stored", { name: "ARTIFACT.json", content: "x".repeat(65537) }],
    ["expanded deflated", { name: "ARTIFACT.json", content: "x".repeat(65537), deflate: true }],
    [
      "compressed",
      {
        name: "ARTIFACT.json",
        content: metadata,
        deflate: true,
        data: Buffer.concat([NodeZlib.deflateRawSync(metadata), Buffer.alloc(131072)]),
      },
    ],
    [
      "decompression",
      { name: "ARTIFACT.json", content: Buffer.alloc(1024 * 1024), deflate: true, bytes: 512 },
    ],
    [
      "actual expanded length",
      { name: "ARTIFACT.json", content: metadata, deflate: true, bytes: metadata.length + 1 },
    ],
  ])("rejects metadata beyond its %s bound", async (_label, entry) => {
    await expectIntegrity(zip([entry, ...companions]));
  });
  // Each mutation targets the first (metadata) local header, its central record, or both.
  it.each<[string, (bytes: Buffer) => Buffer | undefined]>([
    ["non-ZIP bytes", () => Buffer.from("corrupt")],
    ["truncated end record", (bytes) => bytes.subarray(0, bytes.length - 1)],
    ["local signature", (bytes) => void bytes.writeUInt32LE(0, 0)],
    ["local flags", (bytes) => void bytes.writeUInt16LE(0x08, 6)],
    ["local method", (bytes) => void bytes.writeUInt16LE(8, 8)],
    ["local CRC without data descriptor", (bytes) => void bytes.writeUInt32LE(0, 14)],
    ["local size without data descriptor", (bytes) => void bytes.writeUInt32LE(0, 22)],
    ["local name", (bytes) => void bytes.write("a", 30)],
    [
      // Bit 0 in both copies keeps local/central flags equal, isolating the encryption refusal.
      "encrypted entry",
      (bytes) => {
        bytes.writeUInt16LE(1, 6);
        bytes.writeUInt16LE(1, bytes.readUInt32LE(bytes.length - 6) + 8);
        return undefined;
      },
    ],
  ])("rejects a malformed %s header", async (_label, malform) => {
    const bytes = zip([{ name: "ARTIFACT.json", content: metadata }, ...companions]);
    await expectIntegrity(malform(bytes) ?? bytes);
  });
});

describe("JonesActionsClient.relation", () => {
  const BASE = "1".repeat(40),
    HEAD = "2".repeat(40),
    MERGE = "3".repeat(40);
  function compare(response: unknown) {
    const endpoints: string[] = [];
    const client = new JonesActionsClient({
      transport: {
        api: async (endpoint) => {
          endpoints.push(endpoint);
          if (response instanceof Error) throw response;
          return response;
        },
        download: async () => {
          throw new Error("relation must never download");
        },
      },
    });
    return { client, endpoints };
  }
  const github = (overrides: Record<string, unknown>) => ({
    status: "ahead",
    ahead_by: 3,
    behind_by: 0,
    base_commit: { sha: BASE },
    merge_base_commit: { sha: BASE },
    ...overrides,
  });

  it("reads one compare of the exact pair from the fixed repository", async () => {
    const { client, endpoints } = compare(github({}));
    expect(await client.relation(BASE, HEAD)).toBe("ahead");
    expect(endpoints).toEqual([`${ROOT}/compare/${BASE}...${HEAD}?per_page=1`]);
  });

  it.each([
    [
      "behind",
      github({ status: "behind", ahead_by: 0, behind_by: 2, merge_base_commit: { sha: HEAD } }),
    ],
    [
      "diverged",
      github({ status: "diverged", ahead_by: 1, behind_by: 2, merge_base_commit: { sha: MERGE } }),
    ],
  ])("accepts %s only with its merge-base binding", async (relation, response) => {
    expect(await compare(response).client.relation(BASE, HEAD)).toBe(relation);
  });

  it("answers identical for one commit without reading GitHub", async () => {
    const { client, endpoints } = compare(new Error("must not be called"));
    expect(await client.relation(BASE, BASE)).toBe("identical");
    expect(endpoints).toEqual([]);
  });

  it.each([
    ["ahead without base as merge base", github({ merge_base_commit: { sha: MERGE } })],
    ["behind without head as merge base", github({ status: "behind", ahead_by: 0, behind_by: 2 })],
    [
      "diverged sharing an endpoint",
      github({ status: "diverged", ahead_by: 1, behind_by: 2, merge_base_commit: { sha: HEAD } }),
    ],
    ["ahead with inconsistent counts", github({ behind_by: 1 })],
    ["identical for distinct commits", github({ status: "identical", ahead_by: 0 })],
    ["unknown status", github({ status: "unrelated" })],
    ["another base commit", github({ base_commit: { sha: MERGE } })],
    ["abbreviated merge base", github({ merge_base_commit: { sha: BASE.slice(0, 7) } })],
    ["negative count", github({ ahead_by: -1 })],
    ["missing merge base", github({ merge_base_commit: null })],
    ["non-object response", ["ahead"]],
  ])("refuses %s", async (_label, response) => {
    await expect(compare(response).client.relation(BASE, HEAD)).rejects.toMatchObject({
      reason: "unqualified",
    });
  });

  it("refuses abbreviated or non-hex input before any request", async () => {
    const { client, endpoints } = compare(github({}));
    await expect(client.relation(BASE.slice(0, 7), HEAD)).rejects.toBeInstanceOf(JonesActionsError);
    await expect(client.relation(BASE, `${HEAD.slice(1)}/`)).rejects.toBeInstanceOf(
      JonesActionsError,
    );
    expect(endpoints).toEqual([]);
  });

  it("sanitizes transport failures and keeps gh classifications", async () => {
    await expect(
      compare(new Error("HTTP 404 with private detail")).client.relation(BASE, HEAD),
    ).rejects.toEqual(new JonesActionsError("unavailable"));
    await expect(
      compare(new JonesActionsError("authentication-required")).client.relation(BASE, HEAD),
    ).rejects.toMatchObject({ reason: "authentication-required" });
  });
});


describe("production desktop workflow selection", () => {
  const input = { installedSource: OLD, platform: "darwin", architecture: "arm64", desktopBundleIdentifier: "com.jonessystems.jonescode" } as const;
  it("discovers and stages only the exact signed main artifact", async () => {
    const f = fixture({ desktop: "production" });
    const result = await f.client.check(input);
    expect(result.state).toBe("available");
    if (result.state !== "available") throw new Error("Production fixture unavailable");
    expect(result.candidate.workflow).toBe(".github/workflows/jones-desktop-signing.yml");
    const staged = await f.client.stage(result.candidate, await cache());
    expect(staged.receipt).toMatchObject({ signed: true, notarized: true, teamId: "2CUPSQS986" });
    await expect(validateJonesStagedArtifact(NodePath.dirname(staged.payloadPath), result.candidate)).resolves.toMatchObject({ candidate: result.candidate });
  });
  it("keeps omitted/legacy IDs on preview and rejects unknown or empty IDs", async () => {
    for (const desktopBundleIdentifier of [undefined, "com.t3tools.t3code"]) {
      const f = fixture({ desktop: "legacy" });
      const result = await f.client.check({ ...input, desktopBundleIdentifier });
      expect(result).toMatchObject({ state: "available", candidate: { workflow: ".github/workflows/artifact-desktop-mac.yml" } });
    }
    for (const desktopBundleIdentifier of ["", "com.other.app"]) {
      expect(await fixture({ desktop: "production" }).client.check({ ...input, desktopBundleIdentifier })).toMatchObject({ state: "blocked", reason: "unqualified" });
    }
  });
  it.each([{ event: "pull_request" }, { event: "workflow_dispatch" }, { head_branch: "topic" }, { path: ".github/workflows/artifact-desktop-mac.yml" }, { head_repository: { full_name: "other/fork" } }])("rejects wrong production run provenance %j", async (run) => {
    expect((await fixture({ desktop: "production", run }).client.check(input)).state).not.toBe("available");
  });
  it.each([{ signed: false }, { notarized: false }, { teamId: "WRONGTEAM1" }, { bundleIdentifier: "com.t3tools.t3code" }])("refuses a production receipt downgrade %j", async (receipt) => {
    const f = fixture({ desktop: "production", receipt });
    const result = await f.client.check(input);
    if (result.state !== "available") throw new Error("Production fixture unavailable");
    await expect(f.client.stage(result.candidate, await cache())).rejects.toMatchObject({ reason: "unqualified" });
  });
});
