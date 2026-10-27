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
  classifyJonesActionsCheck,
  type JonesActionsCandidate,
  type JonesActionsCheckResult,
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
    platform?: "linux" | "darwin";
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
  const desktop = options.desktop ?? (options.platform === "darwin" ? "legacy" : undefined);
  const workflow =
    desktop === "production"
      ? ".github/workflows/jones-desktop-signing.yml"
      : desktop === "legacy"
        ? ".github/workflows/artifact-desktop-mac.yml"
        : ".github/workflows/artifact-cli-linux.yml";
  const artifactPrefix =
    desktop === "production"
      ? "desktop-mac-production-arm64"
      : desktop === "legacy"
        ? "desktop-mac-arm64"
        : "jones-code-cli-linux-x64";
  const payloadName = desktop ? `T3-Code-${version}-arm64.dmg` : `t3-${version}-linux-x64.tar.gz`,
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
    platform: desktop ? "darwin" : "linux",
    architecture: desktop ? "arm64" : "x64",
    ...(desktop === "production"
      ? {
          signed: true,
          notarized: true,
          bundleIdentifier: "com.jonessystems.jonescode",
          teamId: "2CUPSQS986",
        }
      : {}),
    artifact: payloadName,
    sha256: sha256(payload),
    ...options.receipt,
  };
  const envelope = zip([
    { name: "ARTIFACT.json", content: JSON.stringify(receipt) },
    { name: "SOURCE_COMMIT", content: CURRENT },
    {
      name: desktop ? `${payloadName}.sha256` : "SHA256SUMS",
      content: `${sha256(payload)}  ${payloadName}\n`,
    },
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
    run,
    ci,
  };
}
const check = async (client: JonesActionsClient, platform: "linux" | "darwin" = "linux") => {
  // These existing cases qualify the unchanged legacy projection. Classification is tested below.
  const { classification: _classification, ...projection } = await client.check({
    installedSource: OLD,
    platform,
    architecture: platform === "darwin" ? "arm64" : "x64",
  });
  return projection;
};
async function available(
  client: JonesActionsClient,
  platform: "linux" | "darwin" = "linux",
): Promise<JonesActionsCandidate> {
  const result = await check(client, platform);
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
  it.each(["linux", "darwin"] as const)(
    "skips installed-source run details across the bounded %s discovery pages",
    async (platform) => {
      const f = fixture({ platform, run: { head_sha: OLD } });
      const run = (await f.transport.api(`${ROOT}/actions/runs/101`)) as Record<string, unknown>;
      const calls: string[] = [];
      const workflow =
        platform === "darwin" ? "artifact-desktop-mac.yml" : "artifact-cli-linux.yml";
      const client = new JonesActionsClient({
        now: () => NOW,
        transport: {
          ...f.transport,
          async api(endpoint) {
            calls.push(endpoint);
            if (endpoint.includes(`/actions/workflows/${workflow}/runs?`)) {
              const page = Number(new URLSearchParams(endpoint.split("?")[1]).get("page"));
              return {
                workflow_runs: Array.from({ length: 50 }, (_, index) => ({
                  ...run,
                  id: 1000 + (page - 1) * 50 + index,
                })),
              };
            }
            if (endpoint.startsWith(`${ROOT}/actions/runs/`))
              return { ...run, id: Number(endpoint.split("/").at(-1)) };
            return f.transport.api(endpoint);
          },
        },
      });
      expect(await check(client, platform)).toMatchObject({
        state: "blocked",
        reason: "unavailable",
      });
      expect(calls.filter((endpoint) => endpoint.startsWith(`${ROOT}/actions/runs/`))).toEqual([]);
      expect(
        calls.filter((endpoint) => endpoint.includes(`/actions/workflows/${workflow}/runs?`)),
      ).toHaveLength(platform === "darwin" ? 6 : 3);
      expect(calls).toHaveLength(platform === "darwin" ? 8 : 5);
    },
  );
  it("retains pending-build reporting for an installed-source rerun", async () => {
    const f = fixture({ run: { head_sha: OLD, status: "in_progress", conclusion: null } });
    expect(await check(f.client)).toMatchObject({ state: "building" });
    expect(f.calls.some((endpoint) => endpoint.startsWith(`${ROOT}/actions/runs/`))).toBe(false);
  });
  it("reports an already installed requested source only after canonical qualification", async () => {
    const input = {
      installedSource: OLD,
      targetSource: OLD,
      platform: "linux",
      architecture: "x64",
    } as const;
    expect(await fixture().client.check(input)).toEqual({
      state: "no-new",
      classification: expect.objectContaining({ kind: "target-already-running" }),
    });
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
    ).toEqual({
      state: "no-new",
      classification: expect.objectContaining({ kind: "target-already-running" }),
    });
  });
});

describe("Jones Actions bounded event discovery", () => {
  async function discoveryFixture() {
    const f = fixture({ platform: "darwin" });
    const baseRun = (await f.transport.api(`${ROOT}/actions/runs/101`)) as Record<string, unknown>;
    const ci = (await f.transport.api(`${ROOT}/actions/runs/99`)) as Record<string, unknown>;
    const calls: string[] = [];
    const run = (id: number, event: string, extra: Record<string, unknown> = {}) => ({
      ...baseRun,
      id,
      event,
      created_at: new Date(NOW + id * 1000).toISOString(),
      ...extra,
    });
    const artifact = (id: number) => ({
      ...f.artifact,
      name: `desktop-mac-arm64-${id}-2--0.0.0-preview.20261002.${id}.2`,
      workflow_run: { ...f.artifact.workflow_run, id },
    });
    const client = (api: JonesActionsTransport["api"]) =>
      new JonesActionsClient({
        now: () => NOW,
        transport: {
          ...f.transport,
          async api(endpoint) {
            calls.push(endpoint);
            return api(endpoint);
          },
        },
      });
    const listing = (endpoint: string) =>
      endpoint.includes("/actions/workflows/artifact-desktop-mac.yml/runs?");
    return { f, ci, calls, run, artifact, client, listing };
  }

  it.each(["push", "schedule"])(
    "finds a %s Mac build without spending its budget on 150 manual runs",
    async (event) => {
      const d = await discoveryFixture();
      const accepted = fixture({ platform: "darwin", run: { event }, receipt: { event } });
      expect((await check(accepted.client, "darwin")).state).toBe("available");
      const client = d.client(async (endpoint) => {
        if (d.listing(endpoint)) {
          const query = new URLSearchParams(endpoint.split("?")[1]);
          const page = Number(query.get("page"));
          if (query.has("event"))
            return { workflow_runs: query.get("event") === event ? [d.run(101, event)] : [] };
          return {
            workflow_runs:
              page <= 3
                ? Array.from({ length: 50 }, (_, index) =>
                    d.run(1000 + page * 50 + index, "workflow_dispatch"),
                  )
                : [d.run(101, event)],
          };
        }
        return accepted.transport.api(endpoint);
      });
      expect(await check(client, "darwin")).toMatchObject({
        state: "available",
        candidate: { runId: 101 },
      });
      expect(
        d.calls
          .filter(d.listing)
          .map((endpoint) => new URLSearchParams(endpoint.split("?")[1]).get("event")),
      ).toEqual(["push", "schedule"]);
      expect(
        d.calls.filter((endpoint) => /^repos\/.*\/actions\/runs\/\d+$/.test(endpoint)),
      ).toEqual([`${ROOT}/actions/runs/101`]);
    },
  );

  it.each(["push", "schedule"])(
    "refills the %s page frontier before choosing an older build from the other event",
    async (leadingEvent) => {
      const d = await discoveryFixture();
      const otherEvent = leadingEvent === "push" ? "schedule" : "push";
      const client = d.client(async (endpoint) => {
        if (d.listing(endpoint)) {
          const query = new URLSearchParams(endpoint.split("?")[1]);
          if (query.get("event") !== leadingEvent)
            return { workflow_runs: [d.run(101, otherEvent)] };
          return {
            workflow_runs:
              query.get("page") === "1"
                ? Array.from({ length: 50 }, (_, index) =>
                    d.run(300 - index, leadingEvent, { head_sha: OLD }),
                  )
                : [d.run(201, leadingEvent)],
          };
        }
        if (endpoint === `${ROOT}/actions/runs/201`) return d.run(201, leadingEvent);
        if (endpoint.includes("/actions/runs/201/artifacts?"))
          return { artifacts: [d.artifact(201)] };
        return d.f.transport.api(endpoint);
      });
      expect(await check(client, "darwin")).toMatchObject({
        state: "available",
        candidate: { runId: 201 },
      });
      expect(
        d.calls.filter(d.listing).map((endpoint) => {
          const query = new URLSearchParams(endpoint.split("?")[1]);
          return [query.get("event"), query.get("page")];
        }),
      ).toEqual([
        ["push", "1"],
        ["schedule", "1"],
        [leadingEvent, "2"],
      ]);
      expect(
        d.calls.filter((endpoint) => /^repos\/.*\/actions\/runs\/\d+$/.test(endpoint)),
      ).toEqual([`${ROOT}/actions/runs/201`]);
    },
  );

  it.each([
    ["2026-10-02T12:00:00Z", 102],
    ["2026-10-02T11:00:00Z", 101],
  ] as const)(
    "orders event streams by creation time %s with run-ID ties, selecting %s",
    async (scheduledAt, expectedRunId) => {
      const d = await discoveryFixture();
      const client = d.client(async (endpoint) => {
        if (d.listing(endpoint)) {
          const event = new URLSearchParams(endpoint.split("?")[1]).get("event")!;
          return {
            workflow_runs: [
              d.run(event === "push" ? 101 : 102, event, {
                created_at: event === "push" ? "2026-10-02T12:00:00Z" : scheduledAt,
              }),
            ],
          };
        }
        if (endpoint === `${ROOT}/actions/runs/102`) return d.run(102, "schedule");
        if (endpoint.includes("/actions/runs/102/artifacts?"))
          return { artifacts: [d.artifact(102)] };
        return d.f.transport.api(endpoint);
      });
      expect(await check(client, "darwin")).toMatchObject({
        state: "available",
        candidate: { runId: expectedRunId },
      });
    },
  );

  it("qualifies each run ID once across duplicate pages and event streams", async () => {
    const d = await discoveryFixture();
    const client = d.client(async (endpoint) => {
      if (d.listing(endpoint)) {
        const query = new URLSearchParams(endpoint.split("?")[1]);
        if (query.get("event") === "schedule") return { workflow_runs: [d.run(110, "schedule")] };
        return {
          workflow_runs:
            query.get("page") === "1"
              ? Array.from({ length: 50 }, () => d.run(110, "push"))
              : [d.run(110, "push"), d.run(101, "push")],
        };
      }
      if (endpoint === `${ROOT}/actions/runs/110`) return d.run(110, "push");
      if (endpoint.includes("/actions/runs/110/artifacts?")) return { artifacts: [] };
      return d.f.transport.api(endpoint);
    });
    expect(await check(client, "darwin")).toMatchObject({
      state: "available",
      candidate: { runId: 101 },
    });
    expect(d.calls.filter((endpoint) => endpoint === `${ROOT}/actions/runs/110`)).toHaveLength(1);
    expect(
      d.calls.filter((endpoint) => endpoint.includes("/actions/runs/110/artifacts?")),
    ).toHaveLength(1);
    expect(
      d.calls.filter((endpoint) => endpoint.includes("/actions/workflows/ci.yml/runs?")),
    ).toHaveLength(1);
  });

  it.each([false, true])(
    "keeps the exact target on all six bounded pages and preserves pending=%s at exhaustion",
    async (pending) => {
      const d = await discoveryFixture();
      const client = d.client(async (endpoint) => {
        if (d.listing(endpoint)) {
          const query = new URLSearchParams(endpoint.split("?")[1]);
          const page = Number(query.get("page"));
          const event = query.get("event")!;
          return {
            workflow_runs: Array.from({ length: 50 }, (_, index) =>
              d.run((event === "push" ? 2000 : 1000) - page * 50 - index, event, {
                head_sha: index === 0 ? OLD : CURRENT,
                status: pending && page === 3 && index === 49 ? "in_progress" : "completed",
                conclusion: index === 0 ? "success" : "failure",
              }),
            ),
          };
        }
        return d.f.transport.api(endpoint);
      });
      expect(
        await client.check({
          installedSource: OLD,
          targetSource: CURRENT,
          platform: "darwin",
          architecture: "arm64",
        }),
      ).toEqual(
        pending
          ? { state: "building", retryAfterMs: 240000, classification: expect.any(Object) }
          : {
              state: "blocked",
              reason: "unavailable",
              retryAfterMs: 240000,
              classification: expect.any(Object),
            },
      );
      const queries = d.calls
        .filter(d.listing)
        .map((endpoint) => new URLSearchParams(endpoint.split("?")[1]));
      expect(queries.map((query) => [query.get("event"), query.get("page")])).toEqual([
        ["push", "1"],
        ["schedule", "1"],
        ["push", "2"],
        ["push", "3"],
        ["schedule", "2"],
        ["schedule", "3"],
      ]);
      for (const query of queries) {
        expect(query.get("branch")).toBe("main");
        expect(query.get("head_sha")).toBe(CURRENT);
      }
      expect(d.calls.some((endpoint) => endpoint.startsWith(`${ROOT}/actions/runs/`))).toBe(false);
    },
  );

  it("refuses manual runs even if an event-filtered response includes them", async () => {
    const d = await discoveryFixture();
    const client = d.client(async (endpoint) =>
      d.listing(endpoint)
        ? {
            workflow_runs: [
              d.run(101, "workflow_dispatch"),
              d.run(102, "workflow_dispatch", { status: "in_progress" }),
            ],
          }
        : d.f.transport.api(endpoint),
    );
    expect(await check(client, "darwin")).toMatchObject({
      state: "blocked",
      reason: "unavailable",
    });
    expect(
      d.calls.some(
        (endpoint) =>
          endpoint.startsWith(`${ROOT}/actions/runs/`) || endpoint.includes("/ci.yml/runs?"),
      ),
    ).toBe(false);
  });

  it("shares source CI within one check and refreshes success, missing, pending and errors between checks", async () => {
    const d = await discoveryFixture();
    let observation = "success";
    let liveArtifact = false;
    const client = d.client(async (endpoint) => {
      if (d.listing(endpoint)) {
        const event = new URLSearchParams(endpoint.split("?")[1]).get("event")!;
        return { workflow_runs: [d.run(event === "push" ? 102 : 101, event)] };
      }
      if (endpoint === `${ROOT}/actions/runs/102`) return d.run(102, "push");
      if (endpoint.includes("/actions/workflows/ci.yml/runs?")) {
        if (observation === "error") throw new JonesActionsError("rate-limited");
        return {
          workflow_runs:
            observation === "missing"
              ? []
              : [
                  {
                    ...d.ci,
                    ...(observation === "pending"
                      ? { status: "in_progress", conclusion: null }
                      : {}),
                  },
                ],
        };
      }
      if (/\/actions\/runs\/10[12]\/artifacts\?/.test(endpoint))
        return { artifacts: liveArtifact ? [d.artifact(102)] : [] };
      return d.f.transport.api(endpoint);
    });
    for (const state of ["success", "pending", "missing", "error", "success"]) {
      observation = state;
      d.calls.splice(0);
      const result = await check(client, "darwin");
      expect(result).toMatchObject(
        state === "pending" || state === "missing"
          ? { state: "building" }
          : { state: "blocked", reason: state === "error" ? "rate-limited" : "unavailable" },
      );
      expect(
        d.calls.filter((endpoint) => endpoint.includes("/actions/workflows/ci.yml/runs?")),
      ).toEqual([
        `${ROOT}/actions/workflows/ci.yml/runs?branch=main&event=push&head_sha=${CURRENT}&per_page=100&page=1`,
      ]);
      expect(d.calls.filter((endpoint) => endpoint.includes("/artifacts?"))).toHaveLength(
        state === "success" ? 2 : 0,
      );
    }
    liveArtifact = true;
    expect(await check(client, "darwin")).toMatchObject({
      state: "available",
      candidate: { runId: 102 },
    });
  });

  it("keeps different sources' CI observations separate within the same check", async () => {
    const d = await discoveryFixture();
    const other = "d".repeat(40);
    const client = d.client(async (endpoint) => {
      if (d.listing(endpoint)) {
        const event = new URLSearchParams(endpoint.split("?")[1]).get("event")!;
        return {
          workflow_runs: [
            event === "push" ? d.run(102, event, { head_sha: other }) : d.run(101, event),
          ],
        };
      }
      if (endpoint === `${ROOT}/actions/runs/102`) return d.run(102, "push", { head_sha: other });
      if (endpoint === `${ROOT}/compare/${OLD}...${other}?per_page=1`)
        return { status: "ahead", merge_base_commit: { sha: OLD } };
      if (endpoint === `${ROOT}/compare/${other}...${CURRENT}?per_page=1`)
        return { status: "ahead", merge_base_commit: { sha: other } };
      if (
        endpoint.includes("/actions/workflows/ci.yml/runs?") &&
        endpoint.includes(`head_sha=${other}`)
      )
        return {
          workflow_runs: [
            { ...d.ci, id: 199, head_sha: other, status: "in_progress", conclusion: null },
          ],
        };
      return d.f.transport.api(endpoint);
    });
    expect(await check(client, "darwin")).toMatchObject({
      state: "available",
      candidate: { runId: 101, source: CURRENT, ciRunId: 99 },
    });
    expect(
      d.calls.filter((endpoint) => endpoint.includes("/actions/workflows/ci.yml/runs?")),
    ).toEqual(
      [other, CURRENT].map(
        (source) =>
          `${ROOT}/actions/workflows/ci.yml/runs?branch=main&event=push&head_sha=${source}&per_page=100&page=1`,
      ),
    );
    expect(d.calls.some((endpoint) => endpoint.includes("/actions/runs/102/artifacts?"))).toBe(
      false,
    );
  });

  it("refreshes the candidate's exact CI run at stage time after a successful check", async () => {
    const f = fixture();
    let pending = false;
    let downloads = 0;
    const client = new JonesActionsClient({
      now: () => NOW,
      transport: {
        ...f.transport,
        async api(endpoint) {
          const result = await f.transport.api(endpoint);
          return endpoint === `${ROOT}/actions/runs/99` && pending
            ? { ...(result as object), status: "in_progress", conclusion: null }
            : result;
        },
        async download(id, destination) {
          downloads++;
          await f.transport.download(id, destination);
        },
      },
    });
    const candidate = await available(client);
    pending = true;
    const root = await cache();
    await expect(client.stage(candidate, root)).rejects.toMatchObject({ reason: "unqualified" });
    expect(downloads).toBe(0);
    pending = false;
    expect((await client.stage(candidate, root)).candidate).toEqual(candidate);
    expect(downloads).toBe(1);
    expect(f.calls.filter((endpoint) => endpoint === `${ROOT}/actions/runs/99`)).toHaveLength(2);
    expect(
      f.calls.filter((endpoint) => endpoint.includes("/actions/workflows/ci.yml/runs?")),
    ).toHaveLength(1);
  });
});

describe("Jones Actions check observations", () => {
  const input = { installedSource: OLD, platform: "linux", architecture: "x64" } as const;
  const macInput = { ...input, platform: "darwin", architecture: "arm64" } as const;
  const unavailable = { state: "blocked", reason: "unavailable", retryAfterMs: 240000 } as const;
  const building = { state: "building", retryAfterMs: 240000 } as const;
  const producerList = (endpoint: string) =>
    /\/actions\/workflows\/artifact-.*\/runs\?/.test(endpoint);
  const ciList = (endpoint: string) => endpoint.includes("/actions/workflows/ci.yml/runs?");
  const artifactList = (endpoint: string) => endpoint.includes("/artifacts?");
  type Fixture = ReturnType<typeof fixture>;
  function scenario(
    options: Parameters<typeof fixture>[0] = {},
    respond?: (endpoint: string, f: Fixture) => unknown | Promise<unknown>,
    now: () => number = () => NOW,
  ) {
    const f = fixture(options);
    const calls: string[] = [];
    const client = new JonesActionsClient({
      now,
      transport: {
        ...f.transport,
        async api(endpoint) {
          calls.push(endpoint);
          return respond ? respond(endpoint, f) : f.transport.api(endpoint);
        },
      },
    });
    return { client, calls };
  }
  async function observeAndProject(
    make: () => ReturnType<typeof scenario>,
    expected: JonesActionsCheckResult,
    selection: Parameters<JonesActionsClient["check"]>[0] = input,
  ) {
    const observed = make();
    const observation = await observed.client.observeCheck(selection);
    const legacy = make();
    expect(await legacy.client.check(selection)).toEqual({
      ...expected,
      classification: classifyJonesActionsCheck(observation),
    });
    expect(legacy.calls).toEqual(observed.calls);
    return { observation, calls: observed.calls };
  }

  it("proves only qualified target equality without starting discovery", async () => {
    for (const selection of [
      { ...input, installedSource: CURRENT },
      { ...input, targetSource: OLD },
    ]) {
      const { observation, calls } = await observeAndProject(
        () => scenario(),
        { state: "no-new" },
        selection,
      );
      expect(observation).toMatchObject({
        kind: "target-already-running",
        checkedAt: new Date(NOW).toISOString(),
        mainObservedAt: new Date(NOW).toISOString(),
        scope: { installedSource: selection.installedSource, mainSource: CURRENT },
        discovery: { termination: "target-proved", streams: [] },
      });
      expect(calls).toEqual(
        selection.installedSource === CURRENT
          ? [`${ROOT}/commits/main`]
          : [`${ROOT}/commits/main`, `${ROOT}/compare/${OLD}...${CURRENT}?per_page=1`],
      );
    }
    const { observation } = await observeAndProject(
      () => scenario({ comparison: { status: "diverged" } }),
      { ...unavailable, reason: "unqualified" },
      { ...input, targetSource: OLD },
    );
    expect(observation).toMatchObject({
      kind: "failure",
      failure: { operation: "ancestry", origin: "validation" },
    });
  });

  it("omits unvalidated requested and installed identities on input failures", async () => {
    for (const selection of [
      { ...input, installedSource: "secret-looking-invalid" },
      { ...input, targetSource: "https://invalid.example/token" },
    ]) {
      const { observation, calls } = await observeAndProject(
        () => scenario(),
        { ...unavailable, reason: "unqualified" },
        selection,
      );
      expect(observation).toMatchObject({
        kind: "failure",
        failure: { operation: "input", origin: "validation" },
      });
      expect(observation.scope).not.toHaveProperty("requestedSource");
      if (selection.installedSource !== OLD)
        expect(observation.scope).not.toHaveProperty("installedSource");
      expect(observation).not.toHaveProperty("mainObservedAt");
      expect(JSON.stringify(observation)).not.toContain("invalid");
      expect(calls).toEqual([]);
    }
  });

  it.each(["linux", "darwin"] as const)(
    "records the observed empty %s window",
    async (platform) => {
      const { observation, calls } = await observeAndProject(
        () =>
          scenario({ platform }, (endpoint, f) =>
            producerList(endpoint) ? { workflow_runs: [] } : f.transport.api(endpoint),
          ),
        unavailable,
        platform === "darwin" ? macInput : input,
      );
      expect(observation.kind).toBe("no-eligible-package-in-window");
      expect(observation.discovery).toMatchObject({
        termination: "window-ended",
        unresolvedLimits: [],
        frontierWithinBudgetSatisfied: false,
      });
      expect(observation.discovery.streams).toEqual(
        (platform === "darwin" ? ["push", "schedule"] : ["push"]).map((event) => ({
          event,
          pagesRead: 1,
          lastRawRowCount: 0,
          end: "short-page",
          bufferedRunCount: 0,
        })),
      );
      expect(calls).toHaveLength(platform === "darwin" ? 4 : 3);
    },
  );

  it.each([
    { artifact: { expired: true }, counter: "expiredArtifact" },
    { artifact: { name: "wrong-platform" }, counter: "noMatchingArtifact" },
    { artifact: { workflow_run: { id: 999 } }, counter: "bindingMismatch" },
  ] as const)(
    "distinguishes healthy artifact rejection $counter",
    async ({ artifact, counter }) => {
      const { observation } = await observeAndProject(() => scenario({ artifact }), unavailable);
      expect(observation.kind).toBe("no-eligible-package-in-window");
      expect(observation.rejections[counter]).toBe(1);
      expect(observation.failedQualification.count).toBe(0);
    },
  );

  it.each([
    {
      label: "producer",
      options: { run: { status: "in_progress", conclusion: null } },
      basis: "producer-not-completed",
      runId: 101,
      assurance: "listed-main-only",
    },
    {
      label: "CI",
      options: { ci: { status: "queued", conclusion: null } },
      basis: "ci-not-completed",
      runId: 99,
      assurance: "ancestry-proved",
    },
    {
      label: "unknown producer status",
      options: { run: { status: "private-unknown-value", conclusion: null, head_sha: "invalid" } },
      basis: "producer-not-completed",
      runId: 101,
      assurance: "listed-main-only",
    },
  ] as const)(
    "keeps the compatibility pending basis for $label",
    async ({ label, options, basis, runId, assurance }) => {
      const { observation, calls } = await observeAndProject(() => scenario(options), building);
      expect(observation.kind).toBe("qualification-pending");
      expect(observation.pending[basis]).toMatchObject({
        count: 1,
        first: { runId, sourceAssurance: assurance },
      });
      if (basis === "producer-not-completed") expect(calls.some(ciList)).toBe(false);
      if (label === "unknown producer status") {
        expect(observation.pending[basis].first).toEqual({
          runId,
          sourceAssurance: assurance,
          reportedStatus: "unknown",
        });
        expect(JSON.stringify(observation)).not.toContain("private-unknown-value");
      }
    },
  );

  it("distinguishes unobserved CI from an observed unfinished job", async () => {
    const { observation } = await observeAndProject(
      () =>
        scenario({}, (endpoint, f) =>
          ciList(endpoint) ? { workflow_runs: [] } : f.transport.api(endpoint),
        ),
      building,
    );
    expect(observation.kind).toBe("qualification-pending");
    expect(observation.pending["ci-not-observed"]).toEqual({
      count: 1,
      first: { sourceSha: CURRENT, sourceAssurance: "ancestry-proved" },
    });
    expect(observation.pending["ci-not-completed"]).toEqual({ count: 0 });
  });

  it.each(["producer-list", "producer-detail", "ci-list"] as const)(
    "records explicit failed qualification at %s",
    async (stage) => {
      const { observation } = await observeAndProject(
        () =>
          scenario({}, (endpoint, f) => {
            if (stage === "producer-list" && producerList(endpoint))
              return { workflow_runs: [{ ...f.run, conclusion: "failure" }] };
            if (stage === "producer-detail" && endpoint === `${ROOT}/actions/runs/101`)
              return { ...f.run, conclusion: "cancelled" };
            if (stage === "ci-list" && ciList(endpoint))
              return { workflow_runs: [{ ...f.ci, conclusion: "timed_out" }] };
            return f.transport.api(endpoint);
          }),
        unavailable,
      );
      expect(observation.kind).toBe("qualification-failed");
      expect(observation.failedQualification).toEqual({
        count: 1,
        first: {
          stage,
          sourceSha: CURRENT,
          runId: stage === "ci-list" ? 99 : 101,
          conclusion:
            stage === "ci-list"
              ? "timed_out"
              : stage === "producer-detail"
                ? "cancelled"
                : "failure",
          sourceAssurance: stage === "ci-list" ? "ancestry-proved" : "listed-main-only",
        },
      });
    },
  );

  it.each([null, "unrecognized-secret-status"])(
    "keeps completed CI conclusion %s unknown",
    async (conclusion) => {
      const { observation } = await observeAndProject(
        () => scenario({ ci: { conclusion } }),
        unavailable,
      );
      expect(observation.kind).toBe("incomplete-discovery");
      expect(observation.discovery.unresolvedLimits).toEqual(["unknown-terminal-evidence"]);
      expect(observation.failedQualification.count).toBe(0);
      expect(JSON.stringify(observation)).not.toContain("unrecognized-secret-status");
    },
  );

  it.each(["producer-list", "producer-detail"] as const)(
    "keeps unknown terminal evidence at %s incomplete",
    async (stage) => {
      const { observation } = await observeAndProject(
        () =>
          scenario({}, (endpoint, f) => {
            if (stage === "producer-list" && producerList(endpoint))
              return { workflow_runs: [{ ...f.run, conclusion: null }] };
            if (stage === "producer-detail" && endpoint === `${ROOT}/actions/runs/101`)
              return { ...f.run, conclusion: null };
            return f.transport.api(endpoint);
          }),
        unavailable,
      );
      expect(observation.kind).toBe("incomplete-discovery");
      expect(observation.discovery.unresolvedLimits).toEqual(["unknown-terminal-evidence"]);
      expect(observation.failedQualification.count).toBe(0);
    },
  );

  it("collects optional failed-source evidence leniently without reading rejected run details", async () => {
    const { observation, calls } = await observeAndProject(
      () => scenario({ run: { head_sha: "not-a-source", conclusion: "failure" } }),
      unavailable,
    );
    expect(observation.kind).toBe("qualification-failed");
    expect(observation.failedQualification.first).toEqual({
      stage: "producer-list",
      sourceAssurance: "listed-main-only",
      runId: 101,
      conclusion: "failure",
    });
    expect(calls.some((endpoint) => endpoint.startsWith(`${ROOT}/actions/runs/`))).toBe(false);
    expect(JSON.stringify(observation)).not.toContain("not-a-source");
  });

  it("preserves unsupported-platform refusal before any request", async () => {
    const { observation, calls } = await observeAndProject(
      () => scenario(),
      { ...unavailable, reason: "unsupported-platform" },
      { ...input, platform: "darwin", architecture: "x64" },
    );
    expect(observation).toMatchObject({
      kind: "failure",
      failure: { operation: "input", origin: "validation" },
    });
    expect(calls).toEqual([]);
  });

  it.each(["gh-missing", "authentication-required", "rate-limited", "unavailable"] as const)(
    "retains sanitized request failure %s",
    async (reason) => {
      const { observation } = await observeAndProject(
        () =>
          scenario({}, () => {
            throw new JonesActionsError(reason);
          }),
        { ...unavailable, reason },
      );
      expect(observation).toMatchObject({
        kind: "failure",
        failure: { reason, operation: "main", origin: "request" },
        discovery: { termination: "interrupted", streams: [] },
      });
      expect(observation).not.toHaveProperty("mainObservedAt");
    },
  );

  it("never serializes unexpected exception details", async () => {
    const { observation } = await observeAndProject(
      () =>
        scenario({}, () => {
          throw new Error("token=SECRET https://private.invalid stderr");
        }),
      unavailable,
    );
    expect(observation).toMatchObject({
      kind: "failure",
      failure: { reason: "unavailable", operation: "main", origin: "request" },
    });
    expect(JSON.stringify(observation)).not.toMatch(/SECRET|https:|stderr|token=/);
  });

  it.each([
    { options: { ci: { repository: null } }, operation: "ci-list", reason: "unqualified" },
    {
      options: { commit: { tree: { sha: "invalid" } } },
      operation: "commit",
      reason: "unqualified",
    },
    { options: { commit: { sha: OLD } }, operation: "commit", reason: "unqualified" },
    {
      options: { artifact: { size_in_bytes: 2 * 1024 * 1024 * 1024 + 1 } },
      operation: "artifact-list",
      reason: "integrity",
    },
  ] as const)(
    "records validation failure $reason at $operation",
    async ({ options, operation, reason }) => {
      const { observation } = await observeAndProject(() => scenario(options), {
        ...unavailable,
        reason,
      });
      expect(observation).toMatchObject({
        kind: "failure",
        failure: { reason, operation, origin: "validation" },
      });
    },
  );

  it("keeps mixed pending and failed witnesses, with pending taking precedence", async () => {
    const { observation } = await observeAndProject(
      () =>
        scenario({}, (endpoint, f) =>
          producerList(endpoint)
            ? {
                workflow_runs: [
                  { ...f.run, id: 103, conclusion: "failure" },
                  { ...f.run, id: 102, status: "waiting", conclusion: null },
                ],
              }
            : f.transport.api(endpoint),
        ),
      building,
    );
    expect(observation.kind).toBe("qualification-pending");
    expect(observation.failedQualification).toMatchObject({
      count: 1,
      first: { runId: 103, conclusion: "failure" },
    });
    expect(observation.pending["producer-not-completed"]).toMatchObject({
      count: 1,
      first: { runId: 102, reportedStatus: "waiting" },
    });
  });

  it.each(["pending", "failed"])(
    "returns a candidate immediately after earlier %s evidence",
    async (earlier) => {
      const expected = await check(fixture().client);
      const { observation, calls } = await observeAndProject(
        () =>
          scenario({}, (endpoint, f) =>
            producerList(endpoint)
              ? {
                  workflow_runs: [
                    {
                      ...f.run,
                      id: 102,
                      status: earlier === "pending" ? "queued" : "completed",
                      conclusion: "failure",
                    },
                    f.run,
                  ],
                }
              : f.transport.api(endpoint),
          ),
        expected,
      );
      expect(observation.kind).toBe("available");
      expect(observation.discovery.termination).toBe("candidate-selected");
      expect(
        earlier === "pending"
          ? observation.pending["producer-not-completed"].count
          : observation.failedQualification.count,
      ).toBe(1);
      expect(calls.filter((endpoint) => /^repos\/.*\/actions\/runs\/\d+$/.test(endpoint))).toEqual([
        `${ROOT}/actions/runs/101`,
      ]);
    },
  );

  it("lets a later request failure override earlier pending evidence", async () => {
    const { observation } = await observeAndProject(
      () =>
        scenario({}, (endpoint, f) => {
          if (producerList(endpoint))
            return { workflow_runs: [{ ...f.run, id: 102, status: "queued" }, f.run] };
          if (ciList(endpoint)) throw new JonesActionsError("rate-limited");
          return f.transport.api(endpoint);
        }),
      { ...unavailable, reason: "rate-limited" },
    );
    expect(observation).toMatchObject({ kind: "failure", failure: { operation: "ci-list" } });
    expect(observation.pending["producer-not-completed"].count).toBe(1);
  });

  it("keeps successful CI sufficient on a full mixed page", async () => {
    const expected = await check(fixture().client);
    const { observation, calls } = await observeAndProject(
      () =>
        scenario({}, (endpoint, f) =>
          ciList(endpoint)
            ? {
                workflow_runs: [
                  { ...f.ci, id: 200, conclusion: "failure" },
                  { ...f.ci, id: 199, status: "queued", conclusion: null },
                  ...Array.from({ length: 97 }, (_, i) => ({
                    ...f.ci,
                    id: 300 + i,
                    conclusion: null,
                  })),
                  f.ci,
                ],
              }
            : f.transport.api(endpoint),
        ),
      expected,
    );
    expect(observation.kind).toBe("available");
    expect(observation.failedQualification.count).toBe(1);
    expect(observation.pending["ci-not-completed"].count).toBe(0);
    expect(observation.discovery.unresolvedLimits).toEqual([]);
    expect(calls.filter(ciList)).toHaveLength(1);
  });

  it.each(["ci", "artifact"] as const)(
    "records an unresolved full %s page without adding requests",
    async (page) => {
      const { observation, calls } = await observeAndProject(
        () =>
          scenario({}, (endpoint, f) => {
            if (page === "ci" && ciList(endpoint))
              return {
                workflow_runs: Array.from({ length: 100 }, (_, i) => ({
                  ...f.ci,
                  id: 1000 + i,
                  head_sha: OLD,
                })),
              };
            if (page === "artifact" && artifactList(endpoint))
              return {
                artifacts: Array.from({ length: 100 }, () => ({ ...f.artifact, expired: true })),
              };
            return f.transport.api(endpoint);
          }),
        page === "ci" ? building : unavailable,
      );
      expect(observation.kind).toBe("incomplete-discovery");
      expect(observation.discovery.unresolvedLimits).toEqual([
        page === "ci" ? "ci-page" : "artifact-page",
      ]);
      expect(calls.filter(ciList)).toHaveLength(1);
      expect(calls.filter(artifactList)).toHaveLength(page === "ci" ? 0 : 1);
    },
  );

  it("still blocks when a full CI page contains only explicit failures", async () => {
    const { observation } = await observeAndProject(
      () =>
        scenario({}, (endpoint, f) =>
          ciList(endpoint)
            ? {
                workflow_runs: Array.from({ length: 100 }, (_, i) => ({
                  ...f.ci,
                  id: 1000 + i,
                  conclusion: "failure",
                })),
              }
            : f.transport.api(endpoint),
        ),
      unavailable,
    );
    expect(observation.kind).toBe("incomplete-discovery");
    expect(observation.failedQualification.count).toBe(100);
    expect(observation.pending["ci-not-observed"].count).toBe(0);
  });

  it.each(["linux", "darwin"] as const)(
    "retains raw %s page limits with installed, filtered and duplicate rows",
    async (platform) => {
      for (const rowsKind of ["installed", "filtered", "duplicates"] as const) {
        const { observation, calls } = await observeAndProject(
          () =>
            scenario({ platform }, (endpoint, f) => {
              if (!producerList(endpoint)) return f.transport.api(endpoint);
              const q = new URLSearchParams(endpoint.split("?")[1]);
              return {
                workflow_runs: Array.from({ length: 50 }, (_, i) => ({
                  ...f.run,
                  event: rowsKind === "filtered" ? "workflow_dispatch" : q.get("event"),
                  id: rowsKind === "duplicates" ? 500 : 1000 + Number(q.get("page")) * 50 + i,
                  head_sha: OLD,
                })),
              };
            }),
          unavailable,
          platform === "darwin" ? macInput : input,
        );
        expect(observation.kind).toBe("incomplete-discovery");
        expect(observation.discovery.unresolvedLimits).toEqual(["producer-pages"]);
        for (const stream of observation.discovery.streams)
          expect(stream).toMatchObject({
            pagesRead: 3,
            lastRawRowCount: 50,
            end: "page-limit",
            bufferedRunCount: 0,
          });
        expect(calls.filter(producerList)).toHaveLength(platform === "darwin" ? 6 : 3);
        expect(calls.some((endpoint) => endpoint.startsWith(`${ROOT}/actions/runs/`))).toBe(false);
        if (rowsKind === "filtered")
          expect(observation.rejections.noncanonicalOrWrongTarget).toBe(
            platform === "darwin" ? 300 : 150,
          );
        if (rowsKind === "duplicates") expect(observation.rejections.installedSource).toBe(1);
      }
    },
  );

  it.each([false, true])(
    "retains exact target filters on all capped Mac pages, pending=%s",
    async (pending) => {
      const { observation, calls } = await observeAndProject(
        () =>
          scenario({ platform: "darwin" }, (endpoint, f) => {
            if (!producerList(endpoint)) return f.transport.api(endpoint);
            const q = new URLSearchParams(endpoint.split("?")[1]);
            return {
              workflow_runs: Array.from({ length: 50 }, (_, i) => ({
                ...f.run,
                id: (q.get("event") === "push" ? 2000 : 1000) - Number(q.get("page")) * 50 - i,
                event: q.get("event"),
                head_sha: i === 0 ? OLD : CURRENT,
                status: pending && i === 49 ? "pending" : "completed",
                conclusion: "failure",
              })),
            };
          }),
        pending ? building : unavailable,
        { ...macInput, targetSource: CURRENT },
      );
      expect(observation.kind).toBe("incomplete-discovery");
      expect(observation.scope).toEqual({
        selection: "exact",
        installedSource: OLD,
        requestedSource: CURRENT,
        mainSource: CURRENT,
        platform: "darwin",
        architecture: "arm64",
      });
      const queries = calls
        .filter(producerList)
        .map((endpoint) => new URLSearchParams(endpoint.split("?")[1]));
      expect(queries.map((q) => [q.get("event"), q.get("page")])).toEqual([
        ["push", "1"],
        ["schedule", "1"],
        ["push", "2"],
        ["push", "3"],
        ["schedule", "2"],
        ["schedule", "3"],
      ]);
      for (const q of queries) expect(q.get("head_sha")).toBe(CURRENT);
    },
  );

  it("records a short third page separately from a full third page", async () => {
    const { observation } = await observeAndProject(
      () =>
        scenario({}, (endpoint, f) => {
          if (!producerList(endpoint)) return f.transport.api(endpoint);
          const page = Number(new URLSearchParams(endpoint.split("?")[1]).get("page"));
          return {
            workflow_runs: Array.from({ length: page === 3 ? 49 : 50 }, (_, i) => ({
              ...f.run,
              id: 1000 + page * 50 + i,
              head_sha: OLD,
            })),
          };
        }),
      unavailable,
    );
    expect(observation.kind).toBe("no-eligible-package-in-window");
    expect(observation.discovery.streams[0]).toMatchObject({
      pagesRead: 3,
      lastRawRowCount: 49,
      end: "short-page",
    });
    expect(observation.rejections.installedSource).toBe(149);
  });

  it("preserves partial discovery when the next page fails", async () => {
    const { observation } = await observeAndProject(
      () =>
        scenario({}, (endpoint, f) => {
          if (!producerList(endpoint)) return f.transport.api(endpoint);
          const page = new URLSearchParams(endpoint.split("?")[1]).get("page");
          if (page === "2") throw new JonesActionsError("rate-limited");
          return {
            workflow_runs: Array.from({ length: 50 }, (_, i) => ({
              ...f.run,
              id: 1000 + i,
              status: "queued",
            })),
          };
        }),
      { ...unavailable, reason: "rate-limited" },
    );
    expect(observation).toMatchObject({
      kind: "failure",
      failure: { operation: "producer-list", origin: "request" },
      discovery: { termination: "interrupted" },
    });
    expect(observation.discovery.streams[0]).toMatchObject({
      pagesRead: 1,
      lastRawRowCount: 50,
      end: "error",
      bufferedRunCount: 0,
    });
    expect(observation.pending["producer-not-completed"].count).toBe(50);
  });

  function runFor(f: Fixture, id: number, event: string, extra: Record<string, unknown> = {}) {
    return { ...f.run, id, event, created_at: new Date(NOW + id * 1000).toISOString(), ...extra };
  }
  function artifactFor(f: Fixture, id: number) {
    return {
      ...f.artifact,
      name: `desktop-mac-arm64-${id}-2--0.0.0-preview.20261002.${id}.2`,
      workflow_run: { ...f.artifact.workflow_run, id },
    };
  }
  async function macAvailable(runId = 101) {
    const candidate = await available(fixture({ platform: "darwin" }).client, "darwin");
    return {
      state: "available" as const,
      candidate: {
        ...candidate,
        runId,
        version: `0.0.0-preview.20261002.${runId}.2`,
        artifactName: `desktop-mac-arm64-${runId}-2--0.0.0-preview.20261002.${runId}.2`,
      },
    };
  }

  it("distinguishes a successful empty artifact response from a failed request", async () => {
    const { observation } = await observeAndProject(
      () =>
        scenario({}, (endpoint, f) =>
          artifactList(endpoint) ? { artifacts: [] } : f.transport.api(endpoint),
        ),
      unavailable,
    );
    expect(observation.kind).toBe("no-eligible-package-in-window");
    expect(observation.rejections.noMatchingArtifact).toBe(1);
  });

  it("returns the accepted live artifact on a full page without claiming an unresolved artifact limit", async () => {
    const { observation, calls } = await observeAndProject(
      () =>
        scenario({}, (endpoint, f) =>
          artifactList(endpoint)
            ? {
                artifacts: [
                  ...Array.from({ length: 99 }, () => ({ ...f.artifact, expired: true })),
                  f.artifact,
                ],
              }
            : f.transport.api(endpoint),
        ),
      await check(fixture().client),
    );
    expect(observation.kind).toBe("available");
    expect(observation.rejections.expiredArtifact).toBe(99);
    expect(observation.discovery.unresolvedLimits).toEqual([]);
    expect(calls.filter(artifactList)).toHaveLength(1);
  });

  it.each(["push", "schedule"])(
    "refills %s before selection and records both event frontiers",
    async (leading) => {
      const other = leading === "push" ? "schedule" : "push";
      const { observation, calls } = await observeAndProject(
        () =>
          scenario({ platform: "darwin" }, (endpoint, f) => {
            if (producerList(endpoint)) {
              const q = new URLSearchParams(endpoint.split("?")[1]);
              if (q.get("event") === other) return { workflow_runs: [runFor(f, 101, other)] };
              return {
                workflow_runs:
                  q.get("page") === "1"
                    ? Array.from({ length: 50 }, (_, i) =>
                        runFor(f, 300 - i, leading, { head_sha: OLD }),
                      )
                    : [runFor(f, 201, leading)],
              };
            }
            if (endpoint === `${ROOT}/actions/runs/201`) return runFor(f, 201, leading);
            if (endpoint.includes("/actions/runs/201/artifacts?"))
              return { artifacts: [artifactFor(f, 201)] };
            return f.transport.api(endpoint);
          }),
        await macAvailable(201),
        macInput,
      );
      expect(observation.kind).toBe("available");
      expect(observation.discovery).toMatchObject({
        termination: "candidate-selected",
        frontierWithinBudgetSatisfied: true,
      });
      expect(
        observation.discovery.streams.find((stream) => stream.event === leading),
      ).toMatchObject({
        pagesRead: 2,
        end: "short-page",
        bufferedRunCount: 0,
        lastYielded: { runId: 201, createdAt: new Date(NOW + 201000).toISOString() },
      });
      expect(observation.discovery.streams.find((stream) => stream.event === other)).toMatchObject({
        pagesRead: 1,
        bufferedRunCount: 1,
        nextBuffered: { runId: 101 },
      });
      expect(
        calls.filter(producerList).map((endpoint) => {
          const q = new URLSearchParams(endpoint.split("?")[1]);
          return [q.get("event"), q.get("page")];
        }),
      ).toEqual([
        ["push", "1"],
        ["schedule", "1"],
        [leading, "2"],
      ]);
      expect(calls.slice(-4)).toEqual([
        `${ROOT}/actions/runs/201`,
        `${ROOT}/actions/workflows/ci.yml/runs?branch=main&event=push&head_sha=${CURRENT}&per_page=100&page=1`,
        `${ROOT}/actions/runs/201/artifacts?per_page=100&page=1`,
        `${ROOT}/git/commits/${CURRENT}`,
      ]);
    },
  );

  it.each([
    ["2026-10-02T12:00:00Z", 102],
    ["2026-10-02T11:00:00Z", 101],
  ] as const)(
    "records the creation-time/run-ID frontier tie at %s",
    async (scheduledAt, selected) => {
      const { observation } = await observeAndProject(
        () =>
          scenario({ platform: "darwin" }, (endpoint, f) => {
            if (producerList(endpoint)) {
              const event = new URLSearchParams(endpoint.split("?")[1]).get("event");
              return {
                workflow_runs: [
                  runFor(f, event === "push" ? 101 : 102, event ?? "push", {
                    created_at: event === "push" ? "2026-10-02T12:00:00Z" : scheduledAt,
                  }),
                ],
              };
            }
            if (endpoint === `${ROOT}/actions/runs/102`) return runFor(f, 102, "schedule");
            if (endpoint.includes("/actions/runs/102/artifacts?"))
              return { artifacts: [artifactFor(f, 102)] };
            return f.transport.api(endpoint);
          }),
        await macAvailable(selected),
        macInput,
      );
      expect(
        observation.discovery.streams.find((stream) => stream.lastYielded)?.lastYielded?.runId,
      ).toBe(selected);
      expect(
        observation.discovery.streams.find((stream) => stream.nextBuffered)?.nextBuffered?.runId,
      ).toBe(selected === 101 ? 102 : 101);
    },
  );

  it.each([false, true])(
    "retains a positive candidate with buffered or capped other history, capped=%s",
    async (capped) => {
      const { observation, calls } = await observeAndProject(
        () =>
          scenario({ platform: "darwin" }, (endpoint, f) => {
            if (!producerList(endpoint)) return f.transport.api(endpoint);
            const q = new URLSearchParams(endpoint.split("?")[1]);
            if (q.get("event") === "push")
              return {
                workflow_runs: [runFor(f, 101, "push"), runFor(f, 100, "push", { head_sha: OLD })],
              };
            return {
              workflow_runs: capped
                ? Array.from({ length: 50 }, () => runFor(f, 999, "workflow_dispatch"))
                : [runFor(f, 99, "schedule")],
            };
          }),
        await macAvailable(),
        macInput,
      );
      expect(observation.kind).toBe("available");
      expect(observation.discovery.termination).toBe("candidate-selected");
      expect(observation.discovery.streams[0]).toMatchObject({
        bufferedRunCount: 1,
        lastYielded: { runId: 101 },
        nextBuffered: { runId: 100 },
      });
      expect(observation.discovery.streams[1]).toMatchObject(
        capped
          ? { pagesRead: 3, lastRawRowCount: 50, end: "page-limit", bufferedRunCount: 0 }
          : { pagesRead: 1, end: "short-page", bufferedRunCount: 1, nextBuffered: { runId: 99 } },
      );
      expect(observation.discovery.unresolvedLimits).toEqual(capped ? ["producer-pages"] : []);
      expect(calls.filter(producerList)).toHaveLength(capped ? 4 : 2);
    },
  );

  it("retains duplicate elimination and per-source CI coalescing in the observed traversal", async () => {
    const { observation, calls } = await observeAndProject(
      () =>
        scenario({ platform: "darwin" }, (endpoint, f) => {
          if (producerList(endpoint)) {
            const q = new URLSearchParams(endpoint.split("?")[1]);
            if (q.get("event") === "schedule")
              return { workflow_runs: [runFor(f, 110, "schedule")] };
            return {
              workflow_runs:
                q.get("page") === "1"
                  ? Array.from({ length: 50 }, () => runFor(f, 110, "push"))
                  : [runFor(f, 110, "push"), runFor(f, 101, "push")],
            };
          }
          if (endpoint === `${ROOT}/actions/runs/110`) return runFor(f, 110, "push");
          if (endpoint.includes("/actions/runs/110/artifacts?")) return { artifacts: [] };
          return f.transport.api(endpoint);
        }),
      await macAvailable(),
      macInput,
    );
    expect(observation.kind).toBe("available");
    expect(observation.rejections.noMatchingArtifact).toBe(1);
    expect(calls.filter((endpoint) => endpoint === `${ROOT}/actions/runs/110`)).toHaveLength(1);
    expect(calls.filter(ciList)).toHaveLength(1);
  });

  it("keeps exact requested, advancing main and selected source separate", async () => {
    const newerMain = "d".repeat(40);
    const { observation, calls } = await observeAndProject(
      () =>
        scenario({}, (endpoint, f) => {
          if (endpoint === `${ROOT}/commits/main`) return { sha: newerMain };
          if (endpoint === `${ROOT}/compare/${OLD}...${newerMain}?per_page=1`)
            return { status: "ahead", merge_base_commit: { sha: OLD } };
          if (endpoint === `${ROOT}/compare/${CURRENT}...${newerMain}?per_page=1`)
            return { status: "ahead", merge_base_commit: { sha: CURRENT } };
          return f.transport.api(endpoint);
        }),
      await check(fixture().client),
      { ...input, targetSource: CURRENT },
    );
    expect(observation.scope).toMatchObject({
      installedSource: OLD,
      requestedSource: CURRENT,
      mainSource: newerMain,
      selection: "exact",
    });
    if (observation.kind !== "available") throw new Error("Expected observed candidate");
    expect(observation.candidate.source).toBe(CURRENT);
    expect(calls.filter(producerList)[0]).toContain(`head_sha=${CURRENT}`);
  });

  it("refreshes evidence and completion times while coalescing CI only within each attempt", async () => {
    const make = () => {
      let state = "success";
      let clock = NOW;
      const f = scenario(
        { platform: "darwin" },
        (endpoint, base) => {
          if (producerList(endpoint)) {
            const event = new URLSearchParams(endpoint.split("?")[1]).get("event") ?? "push";
            return { workflow_runs: [runFor(base, event === "push" ? 102 : 101, event)] };
          }
          if (endpoint === `${ROOT}/actions/runs/102`) return runFor(base, 102, "push");
          if (ciList(endpoint)) {
            if (state === "error") throw new JonesActionsError("rate-limited");
            return {
              workflow_runs:
                state === "missing"
                  ? []
                  : [
                      {
                        ...base.ci,
                        ...(state === "pending" ? { status: "queued", conclusion: null } : {}),
                      },
                    ],
            };
          }
          if (artifactList(endpoint)) return { artifacts: [] };
          return base.transport.api(endpoint);
        },
        () => clock,
      );
      return {
        ...f,
        advance(next: string) {
          state = next;
          clock += 1000;
        },
      };
    };
    const observed = make(),
      legacy = make();
    for (const [i, state] of ["success", "missing", "pending", "error", "success"].entries()) {
      observed.advance(state);
      legacy.advance(state);
      observed.calls.splice(0);
      legacy.calls.splice(0);
      const observation = await observed.client.observeCheck(macInput);
      const expected =
        state === "missing" || state === "pending"
          ? building
          : state === "error"
            ? { ...unavailable, reason: "rate-limited" as const }
            : unavailable;
      expect(await legacy.client.check(macInput)).toEqual({
        ...expected,
        classification: classifyJonesActionsCheck(observation),
      });
      expect(legacy.calls).toEqual(observed.calls);
      expect(observation.checkedAt).toBe(new Date(NOW + (i + 1) * 1000).toISOString());
      expect(observation.mainObservedAt).toBe(observation.checkedAt);
      expect(observation.kind).toBe(
        state === "missing" || state === "pending"
          ? "qualification-pending"
          : state === "error"
            ? "failure"
            : "no-eligible-package-in-window",
      );
      expect(observed.calls.filter(ciList)).toHaveLength(1);
      expect(observed.calls.filter((endpoint) => endpoint.includes("/compare/"))).toHaveLength(
        i === 0 ? 1 : 0,
      );
      expect(observation.pending["ci-not-observed"].count).toBe(state === "missing" ? 1 : 0);
      expect(observation.pending["ci-not-completed"].count).toBe(state === "pending" ? 1 : 0);
      expect(observation.rejections.noMatchingArtifact).toBe(state === "success" ? 2 : 0);
    }
  });

  it("keeps separate-source CI witnesses and lets a later qualified source win", async () => {
    const other = "d".repeat(40);
    const { observation, calls } = await observeAndProject(
      () =>
        scenario({ platform: "darwin" }, (endpoint, f) => {
          if (producerList(endpoint)) {
            const event = new URLSearchParams(endpoint.split("?")[1]).get("event") ?? "push";
            return {
              workflow_runs: [
                runFor(f, event === "push" ? 102 : 101, event, {
                  head_sha: event === "push" ? other : CURRENT,
                }),
              ],
            };
          }
          if (endpoint === `${ROOT}/actions/runs/102`)
            return runFor(f, 102, "push", { head_sha: other });
          if (endpoint === `${ROOT}/compare/${OLD}...${other}?per_page=1`)
            return { status: "ahead", merge_base_commit: { sha: OLD } };
          if (endpoint === `${ROOT}/compare/${other}...${CURRENT}?per_page=1`)
            return { status: "ahead", merge_base_commit: { sha: other } };
          if (ciList(endpoint) && endpoint.includes(`head_sha=${other}`))
            return {
              workflow_runs: [
                { ...f.ci, id: 199, head_sha: other, status: "in_progress", conclusion: null },
              ],
            };
          return f.transport.api(endpoint);
        }),
      await macAvailable(),
      macInput,
    );
    expect(observation.kind).toBe("available");
    expect(observation.pending["ci-not-completed"].first).toMatchObject({
      sourceSha: other,
      runId: 199,
      sourceAssurance: "ancestry-proved",
    });
    expect(calls.filter(ciList)).toEqual(
      [other, CURRENT].map(
        (sha) =>
          `${ROOT}/actions/workflows/ci.yml/runs?branch=main&event=push&head_sha=${sha}&per_page=100&page=1`,
      ),
    );
    expect(calls.some((endpoint) => endpoint.includes("/actions/runs/102/artifacts?"))).toBe(false);
  });

  it("timestamps completion after the last request without changing candidate identity or stage requalification", async () => {
    const f = fixture({ run: { run_started_at: "2026-10-02T10:00:00Z" } });
    let clock = NOW,
      changedCi = false,
      downloads = 0;
    const client = new JonesActionsClient({
      now: () => clock,
      transport: {
        ...f.transport,
        async api(endpoint) {
          const response = await f.transport.api(endpoint);
          clock += 1000;
          if (endpoint === `${ROOT}/actions/runs/99` && changedCi)
            return { ...f.ci, status: "in_progress", conclusion: null };
          return response;
        },
        async download(id, destination) {
          downloads++;
          await f.transport.download(id, destination);
        },
      },
    });
    const observation = await client.observeCheck(input);
    if (observation.kind !== "available") throw new Error("Expected observed candidate");
    expect(observation.mainObservedAt).toBe(new Date(NOW + 1000).toISOString());
    expect(observation.checkedAt).toBe(new Date(clock).toISOString());
    expect(Date.parse(observation.checkedAt)).toBeGreaterThan(
      Date.parse(observation.mainObservedAt!),
    );
    const baseline = await available(fixture().client);
    expect(observation.candidate).toEqual(baseline);
    expect(jonesCandidateHandle(observation.candidate)).toBe(jonesCandidateHandle(baseline));
    expect(observation.buildStartedAt).toBe("2026-10-02T10:00:00Z");
    const legacy = await check(fixture({ run: { run_started_at: "2026-10-02T10:00:00Z" } }).client);
    expect(legacy).toEqual({
      state: "available",
      candidate: baseline,
      buildStartedAt: "2026-10-02T10:00:00Z",
    });
    changedCi = true;
    await expect(client.stage(observation.candidate, await cache())).rejects.toMatchObject({
      reason: "unqualified",
    });
    expect(downloads).toBe(0);
    expect(f.calls.filter((endpoint) => endpoint === `${ROOT}/actions/runs/99`)).toHaveLength(1);
  });

  it("keeps an unfinished installed-source rerun scoped to installed rather than main", async () => {
    const { observation, calls } = await observeAndProject(
      () => scenario({ run: { head_sha: OLD, status: "requested", conclusion: null } }),
      building,
    );
    expect(observation.scope.mainSource).toBe(CURRENT);
    expect(observation.pending["producer-not-completed"].first).toEqual({
      sourceSha: OLD,
      runId: 101,
      sourceAssurance: "listed-main-only",
      reportedStatus: "requested",
    });
    expect(calls.some((endpoint) => endpoint.startsWith(`${ROOT}/actions/runs/`))).toBe(false);
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

describe("Jones desktop producer events", () => {
  it.each(["push", "schedule"])(
    "discovers, stages and reuses an authenticated %s desktop build",
    async (event) => {
      const f = fixture({ platform: "darwin", run: { event }, receipt: { event } });
      const candidate = await available(f.client, "darwin");
      const root = await cache();
      const staged = await f.client.stage(candidate, root);
      expect(staged.receipt.event).toBe(event);
      expect(await f.client.stage(candidate, root)).toEqual(staged);
      expect(
        await validateJonesStagedArtifact(NodePath.dirname(staged.payloadPath), candidate),
      ).toEqual(staged);
      expect(
        f.calls
          .filter((call) => call.includes("artifact-desktop-mac.yml/runs?"))
          .map((call) => new URLSearchParams(call.split("?")[1]).get("event")),
      ).toEqual(["push", "schedule"]);
      expect(f.calls.find((call) => call.includes("ci.yml/runs?"))).toContain(
        `event=push&head_sha=${CURRENT}`,
      );
    },
  );
  it.each([
    ["darwin", "workflow_dispatch"],
    ["darwin", "pull_request"],
    ["linux", "schedule"],
    ["linux", "workflow_dispatch"],
  ] as const)(
    "rejects %s producer event %s in listings and refetched runs",
    async (platform, event) => {
      const f = fixture({ platform, run: { event, status: "in_progress" } });
      expect(await check(f.client, platform)).toMatchObject({
        state: "blocked",
        reason: "unavailable",
      });
      const accepted = fixture({ platform });
      const client = new JonesActionsClient({
        now: () => NOW,
        transport: {
          ...accepted.transport,
          async api(endpoint) {
            const result = await accepted.transport.api(endpoint);
            return endpoint === `${ROOT}/actions/runs/101`
              ? { ...(result as object), event }
              : result;
          },
        },
      });
      expect(await check(client, platform)).toMatchObject({
        state: "blocked",
        reason: "unavailable",
      });
      await expect(
        client.stage(await available(accepted.client, platform), await cache()),
      ).rejects.toMatchObject({ reason: "unqualified" });
      if (platform === "linux")
        expect(f.calls.find((call) => call.includes("artifact-cli-linux.yml/runs?"))).toContain(
          "event=push",
        );
    },
  );
  it.each(["linux", "darwin"] as const)(
    "requires push CI for %s discovery and staging",
    async (platform) => {
      const accepted = fixture({ platform });
      const candidate = await available(accepted.client, platform);
      for (const event of ["schedule", "workflow_dispatch"]) {
        const f = fixture({ platform, ci: { event } });
        expect((await check(f.client, platform)).state).not.toBe("available");
        await expect(f.client.stage(candidate, await cache())).rejects.toMatchObject({
          reason: "unqualified",
        });
      }
    },
  );
  it.each(["push", "schedule"])(
    "rejects a live producer event differing from authenticated %s metadata, including reuse",
    async (event) => {
      const f = fixture({ platform: "darwin", run: { event }, receipt: { event } });
      const candidate = await available(f.client, "darwin");
      const root = await cache();
      await f.client.stage(candidate, root);
      const changed = fixture({
        platform: "darwin",
        run: { event: event === "push" ? "schedule" : "push" },
        receipt: { event },
      });
      await expect(changed.client.stage(candidate, await cache())).rejects.toMatchObject({
        reason: "unqualified",
      });
      await expect(changed.client.stage(candidate, root)).rejects.toMatchObject({
        reason: "unqualified",
      });
    },
  );
  it("rejects a stored event changed independently of the authenticated ZIP", async () => {
    const f = fixture({
      platform: "darwin",
      run: { event: "schedule" },
      receipt: { event: "schedule" },
    });
    const candidate = await available(f.client, "darwin");
    const root = await cache();
    const staged = await f.client.stage(candidate, root);
    const directory = NodePath.dirname(staged.payloadPath);
    await NodeFSP.writeFile(
      NodePath.join(directory, JONES_ACTIONS_RECEIPT_FILE),
      JSON.stringify({ ...staged, receipt: { ...staged.receipt, event: "push" } }),
    );
    await expect(validateJonesStagedArtifact(directory, candidate)).rejects.toMatchObject({
      reason: "integrity",
    });
    await expect(f.client.stage(candidate, root)).rejects.toMatchObject({ reason: "integrity" });
  });
  it("rejects a concurrent winner whose stored event differs from its authenticated ZIP", async () => {
    const f = fixture({
      platform: "darwin",
      run: { event: "schedule" },
      receipt: { event: "schedule" },
    });
    const candidate = await available(f.client, "darwin");
    const root = await cache();
    const client = new JonesActionsClient({
      now: () => NOW,
      transport: {
        ...f.transport,
        async download(id, destination) {
          await f.transport.download(id, destination);
          const winner = await f.client.stage(candidate, root);
          await NodeFSP.writeFile(
            NodePath.join(NodePath.dirname(winner.payloadPath), JONES_ACTIONS_RECEIPT_FILE),
            JSON.stringify({ ...winner, receipt: { ...winner.receipt, event: "push" } }),
          );
        },
      },
    });
    await expect(client.stage(candidate, root)).rejects.toMatchObject({ reason: "integrity" });
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
    { event: "schedule" },
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

describe("additive check classification", () => {
  it("publishes bounded discovery separately from unchanged legacy phases without private witnesses", async () => {
    const cases = [
      fixture(),
      fixture({ ci: { status: "in_progress", conclusion: null } }),
      fixture({ ci: { conclusion: "failure" } }),
    ];
    const expected = ["available", "qualification-pending", "qualification-failed"];
    for (const [index, f] of cases.entries()) {
      const result = await f.client.check({
        installedSource: OLD,
        platform: "linux",
        architecture: "x64",
      });
      expect(result.classification?.kind).toBe(expected[index]);
      expect(result.state).toBe(index === 0 ? "available" : index === 1 ? "building" : "blocked");
      expect(Object.keys(result.classification ?? {}).sort()).toEqual([
        "checkedAt",
        "discovery",
        "failedQualificationCount",
        "kind",
        "pendingCount",
      ]);
      expect(result.classification).not.toHaveProperty("pending");
      expect(result.classification).not.toHaveProperty("failure");
    }
    const f = fixture();
    const observation = await f.client.observeCheck({
      installedSource: CURRENT,
      platform: "linux",
      architecture: "x64",
    });
    const classification = classifyJonesActionsCheck(observation);
    expect(classification).toMatchObject({
      kind: "target-already-running",
      pendingCount: 0,
      failedQualificationCount: 0,
      discovery: { termination: "target-proved", bounded: false, unresolvedLimits: [] },
    });
    expect(classification.discovery.unresolvedLimits).not.toBe(
      observation.discovery.unresolvedLimits,
    );
  });
});

describe("production desktop workflow selection", () => {
  const input = {
    installedSource: OLD,
    platform: "darwin",
    architecture: "arm64",
    desktopBundleIdentifier: "com.jonessystems.jonescode",
  } as const;
  it("discovers and stages only the exact signed main artifact", async () => {
    const f = fixture({ desktop: "production" });
    const result = await f.client.check(input);
    expect(result.state).toBe("available");
    if (result.state !== "available") throw new Error("Production fixture unavailable");
    expect(result.candidate.workflow).toBe(".github/workflows/jones-desktop-signing.yml");
    const staged = await f.client.stage(result.candidate, await cache());
    expect(staged.receipt).toMatchObject({ signed: true, notarized: true, teamId: "2CUPSQS986" });
    await expect(
      validateJonesStagedArtifact(NodePath.dirname(staged.payloadPath), result.candidate),
    ).resolves.toMatchObject({ candidate: result.candidate });
  });
  it("keeps omitted/legacy IDs on preview and rejects unknown or empty IDs", async () => {
    for (const desktopBundleIdentifier of [undefined, "com.t3tools.t3code"]) {
      const f = fixture({ desktop: "legacy" });
      const { desktopBundleIdentifier: _, ...legacyInput } = input;
      const result = await f.client.check({
        ...legacyInput,
        ...(desktopBundleIdentifier === undefined ? {} : { desktopBundleIdentifier }),
      });
      expect(result).toMatchObject({
        state: "available",
        candidate: { workflow: ".github/workflows/artifact-desktop-mac.yml" },
      });
    }
    for (const desktopBundleIdentifier of ["", "com.other.app"]) {
      expect(
        await fixture({ desktop: "production" }).client.check({
          ...input,
          desktopBundleIdentifier,
        }),
      ).toMatchObject({ state: "blocked", reason: "unqualified" });
    }
  });
  it.each([
    { event: "pull_request" },
    { event: "workflow_dispatch" },
    { event: "schedule" },
    { head_branch: "topic" },
    { path: ".github/workflows/artifact-desktop-mac.yml" },
    { head_repository: { full_name: "other/fork" } },
  ])("rejects wrong production run provenance %j", async (run) => {
    expect((await fixture({ desktop: "production", run }).client.check(input)).state).not.toBe(
      "available",
    );
  });
  it.each([
    { signed: false },
    { notarized: false },
    { event: "schedule" },
    { teamId: "WRONGTEAM1" },
    { bundleIdentifier: "com.t3tools.t3code" },
  ])("refuses a production receipt downgrade %j", async (receipt) => {
    const f = fixture({ desktop: "production", receipt });
    const result = await f.client.check(input);
    if (result.state !== "available") throw new Error("Production fixture unavailable");
    await expect(f.client.stage(result.candidate, await cache())).rejects.toMatchObject({
      reason: "unqualified",
    });
  });
});
