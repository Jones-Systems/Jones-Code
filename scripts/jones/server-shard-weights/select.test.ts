// @effect-diagnostics nodeBuiltinImport:off - Fixtures build artifact zip archives in memory with Node's zlib.
import * as NodeZlib from "node:zlib";

import { describe, expect, it } from "vite-plus/test";

import {
  manifestDigest,
  serializeWeightsEnvelope,
  weightsDigest,
  type WeightsSource,
} from "./envelope.ts";
import {
  MAX_SOURCE_RUNS,
  MAX_TRANSPORT_BYTES,
  SelectionRefusal,
  WEIGHTS_FILE_NAME,
  formatOutputs,
  formatSummary,
  parseShardCount,
  readSingleZipEntry,
  selectWeights,
  type ActionsClient,
  type RunArtifact,
  type Selection,
  type WorkflowRun,
} from "./select.ts";

const REPO = "Jones-Systems/Jones-Code";
const CURRENT = "9000";

/** A one-file zip archive, as upload-artifact produces for a single file. */
function zip(entries: { name: string; content: string; method?: 0 | 8 }[]): Uint8Array {
  const locals: Buffer[] = [];
  const centrals: Buffer[] = [];
  let offset = 0;
  for (const { name, content, method = 8 } of entries) {
    const data = Buffer.from(content);
    const stored = method === 8 ? NodeZlib.deflateRawSync(data) : data;
    const nameBytes = Buffer.from(name);
    const local = Buffer.alloc(30);
    local.writeUInt32LE(0x04034b50, 0);
    local.writeUInt16LE(20, 4);
    local.writeUInt16LE(method, 8);
    local.writeUInt32LE(NodeZlib.crc32(data), 14);
    local.writeUInt32LE(stored.length, 18);
    local.writeUInt32LE(data.length, 22);
    local.writeUInt16LE(nameBytes.length, 26);
    const central = Buffer.alloc(46);
    central.writeUInt32LE(0x02014b50, 0);
    central.writeUInt16LE(20, 4);
    central.writeUInt16LE(20, 6);
    central.writeUInt16LE(method, 10);
    central.writeUInt32LE(NodeZlib.crc32(data), 16);
    central.writeUInt32LE(stored.length, 20);
    central.writeUInt32LE(data.length, 24);
    central.writeUInt16LE(nameBytes.length, 28);
    central.writeUInt32LE(offset, 42);
    locals.push(local, nameBytes, stored);
    centrals.push(central, nameBytes);
    offset += local.length + nameBytes.length + stored.length;
  }
  const directory = Buffer.concat(centrals);
  const end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0);
  end.writeUInt16LE(entries.length, 8);
  end.writeUInt16LE(entries.length, 10);
  end.writeUInt32LE(directory.length, 12);
  end.writeUInt32LE(offset, 16);
  return Buffer.concat([...locals, directory, end]);
}

function run(id: number, overrides: Partial<WorkflowRun> = {}): WorkflowRun {
  return {
    id,
    run_attempt: 1,
    event: "push",
    head_branch: "main",
    head_sha: String(id).padStart(40, "0"),
    path: ".github/workflows/ci.yml",
    status: "completed",
    conclusion: "success",
    repository: { full_name: REPO },
    head_repository: { full_name: REPO },
    ...overrides,
  };
}

interface WeightsOptions {
  attempt?: number;
  files?: string[];
  change?: Partial<WeightsSource>;
}

function weightsText(source: WorkflowRun, options: WeightsOptions = {}): string {
  const { attempt = 1, files = ["src/a.test.ts", "src/b.test.ts"], change = {} } = options;
  return serializeWeightsEnvelope({
    schema: "jones.server-shard-weights/v1",
    semantics: "wall-seconds",
    source: {
      repository: REPO,
      workflowRef: `${REPO}/.github/workflows/ci.yml@refs/heads/main`,
      runId: String(source.id),
      runAttempt: attempt,
      event: "push",
      ref: "refs/heads/main",
      headSha: source.head_sha,
      shardCount: 6,
      fileCount: files.length,
      manifestDigest: manifestDigest(files),
      weightsInput: { mode: "checked-in" },
      ...change,
    },
    weights: Object.fromEntries(files.map((file, i) => [file, (i % 500) + 1])),
  });
}

interface Published {
  run: WorkflowRun;
  artifacts?: (Partial<RunArtifact> & { archive?: Uint8Array })[];
}

/** An Actions API whose runs, artifacts and archives come from `published`. */
function fakeClient(published: Published[], failures: { list?: boolean; download?: boolean } = {}) {
  const calls: string[] = [];
  const archives = new Map<number, Uint8Array>();
  let nextId = 1;
  const artifactsByRun = new Map(
    published.map(({ run, artifacts = [] }): [number, RunArtifact[]] => [
      run.id,
      artifacts.map(({ archive, ...artifact }) => {
        const id = nextId++;
        if (archive) archives.set(id, archive);
        return {
          id,
          name: "server-shard-weights-a1",
          size_in_bytes: archive?.length ?? 0,
          expired: false,
          workflow_run: { id: run.id, head_sha: run.head_sha },
          ...artifact,
        } satisfies RunArtifact;
      }),
    ]),
  );
  const client: ActionsClient = {
    listSourceRuns: async (limit) => {
      calls.push(`list ${limit}`);
      if (failures.list) throw new Error("/actions/workflows/ci.yml/runs returned 502");
      return published.map(({ run }) => run);
    },
    getRun: async (runId) => {
      calls.push(`get ${runId}`);
      const found = published.find(({ run }) => String(run.id) === runId);
      if (!found) throw new Error(`/actions/runs/${runId} returned 404`);
      return found.run;
    },
    listArtifacts: async (runId) => {
      calls.push(`artifacts ${runId}`);
      return artifactsByRun.get(Number(runId)) ?? [];
    },
    downloadArtifact: async (artifactId) => {
      calls.push(`download ${artifactId}`);
      if (failures.download) throw new Error("artifact download returned 500");
      const archive = archives.get(artifactId);
      if (!archive) throw new Error("artifact download returned 404");
      return archive;
    },
  };
  return { client, calls };
}

const archiveOf = (text: string) => zip([{ name: WEIGHTS_FILE_NAME, content: text }]);
const valid = (source: WorkflowRun, attempt = 1): Published => ({
  run: source,
  artifacts: [
    {
      name: `server-shard-weights-a${attempt}`,
      archive: archiveOf(weightsText(source, { attempt })),
    },
  ],
});
const artifactOf = (source: WorkflowRun) => valid(source).artifacts![0]!;
const options = { repository: REPO, currentRunId: CURRENT };

describe("selectWeights", () => {
  it("uses the newest qualifying main push with valid weights", async () => {
    const newest = run(8002);
    const { client } = fakeClient([valid(newest), valid(run(8001))]);

    const selection = await selectWeights(client, options);

    expect(selection).toMatchObject({
      mode: "runtime",
      sourceRunId: "8002",
      artifactName: "server-shard-weights-a1",
      text: weightsText(newest),
      digest: weightsDigest(weightsText(newest)),
      skipped: [],
    });
  });

  it("rejects an earlier attempt when the current attempt has no usable weights", async () => {
    const source = run(8002, { run_attempt: 3 });
    const { client } = fakeClient([
      {
        run: source,
        artifacts: [
          { name: "server-shard-weights-a1", archive: archiveOf(weightsText(source)) },
          {
            name: "server-shard-weights-a2",
            archive: archiveOf(weightsText(source, { attempt: 2 })),
          },
          { name: "server-shard-weights-a3", expired: true },
          { name: "server-timings-a3-s1-of-6" },
        ],
      },
    ]);

    const selection = await selectWeights(client, options);
    expect(selection).toMatchObject({ mode: "checked-in" });
  });

  it.each<[string, Partial<WorkflowRun>]>([
    ["a pull request run", { event: "pull_request" }],
    ["a dispatch run", { event: "workflow_dispatch" }],
    ["another branch", { head_branch: "feature" }],
    ["another workflow", { path: ".github/workflows/jones-update-qualification.yml" }],
    ["another repository", { repository: { full_name: "someone/fork" } }],
    ["a fork head", { head_repository: { full_name: "someone/fork" } }],
    ["a failed run", { conclusion: "failure" }],
    ["an unfinished run", { status: "in_progress", conclusion: null }],
    ["the current run", { id: Number(CURRENT) }],
  ])("skips %s", async (_name, overrides) => {
    const skippedRun = run(8002, overrides);
    const { client } = fakeClient([valid(skippedRun), valid(run(8001))]);

    const selection = await selectWeights(client, options);

    expect(selection).toMatchObject({ mode: "runtime", sourceRunId: "8001" });
    expect(selection.skipped).toHaveLength(1);
  });

  it.each<[string, (source: WorkflowRun) => Published]>([
    ["no weights artifact", (source) => ({ run: source })],
    [
      "an expired artifact",
      (source) => ({ run: source, artifacts: [{ ...artifactOf(source), expired: true }] }),
    ],
    [
      "an oversized artifact",
      (source) => ({
        run: source,
        artifacts: [{ ...artifactOf(source), size_in_bytes: 2_000_000 }],
      }),
    ],
    [
      "an artifact from another run",
      (source) => ({
        run: source,
        artifacts: [{ ...artifactOf(source), workflow_run: { id: 1 } }],
      }),
    ],
    [
      "an attempt the run never had",
      (source) => ({
        run: source,
        artifacts: [
          {
            name: "server-shard-weights-a2",
            archive: archiveOf(weightsText(source, { attempt: 2 })),
          },
        ],
      }),
    ],
    [
      "an archive with the wrong file",
      (source) => ({
        run: source,
        artifacts: [{ archive: zip([{ name: "other.json", content: weightsText(source) }]) }],
      }),
    ],
    [
      "invalid weights",
      (source) => ({ run: source, artifacts: [{ archive: archiveOf('{"schema":"x"}\n') }] }),
    ],
    [
      "weights too large to hand to the shards",
      (source) => {
        const files = Array.from({ length: 2000 }, (_, i) => `src/${"x".repeat(60)}${i}.test.ts`);
        const text = weightsText(source, { files });
        expect(Buffer.byteLength(text)).toBeGreaterThan(MAX_TRANSPORT_BYTES);
        return { run: source, artifacts: [{ archive: archiveOf(text) }] };
      },
    ],
  ])("skips a run with %s", async (_name, publish) => {
    const { client } = fakeClient([publish(run(8002)), valid(run(8001))]);

    const selection = await selectWeights(client, options);

    expect(selection).toMatchObject({ mode: "runtime", sourceRunId: "8001" });
    expect(selection.skipped).toEqual([expect.stringMatching(/^run 8002: /)]);
  });

  it.each<[string, Partial<WeightsSource>]>([
    ["run ID", { runId: "1" }],
    ["head SHA", { headSha: "f".repeat(40) }],
    ["attempt", { runAttempt: 2 }],
    ["event", { event: "pull_request" }],
    ["ref", { ref: "refs/heads/feature" }],
    ["workflow", { workflowRef: `${REPO}/.github/workflows/x.yml@refs/heads/main` }],
    ["repository", { repository: "someone/fork" }],
  ])("skips weights that record another %s", async (_name, change) => {
    const source = run(8002);
    const archive = archiveOf(weightsText(source, { change }));
    const { client } = fakeClient([{ run: source, artifacts: [{ archive }] }, valid(run(8001))]);

    const selection = await selectWeights(client, options);

    expect(selection).toMatchObject({ mode: "runtime", sourceRunId: "8001" });
    expect(selection.skipped).toEqual([expect.stringMatching(/^run 8002: .* records a different/)]);
  });

  it(`looks at no more than the newest ${MAX_SOURCE_RUNS} runs`, async () => {
    const unusable = Array.from({ length: MAX_SOURCE_RUNS }, (_, i) => ({ run: run(8100 - i) }));
    const { client, calls } = fakeClient([...unusable, valid(run(8000))]);

    const selection = await selectWeights(client, options);

    expect(selection.mode).toBe("checked-in");
    expect(selection.skipped).toHaveLength(MAX_SOURCE_RUNS);
    expect(calls[0]).toBe(`list ${MAX_SOURCE_RUNS}`);
    expect(calls).not.toContain("artifacts 8000");
  });

  it("falls back to the checked-in map when nothing qualifies", async () => {
    const { client } = fakeClient([]);
    expect(await selectWeights(client, options)).toEqual({
      mode: "checked-in",
      reason: `no usable weights in the newest ${MAX_SOURCE_RUNS} successful main pushes`,
      skipped: [],
    });
  });

  it.each([
    ["listing runs", { list: true }],
    ["downloading an artifact", { download: true }],
  ] as const)("falls back to the checked-in map when %s fails", async (_name, failures) => {
    const { client } = fakeClient([valid(run(8002))], failures);

    const selection = await selectWeights(client, options);

    expect(selection.mode).toBe("checked-in");
    expect(selection.reason).toMatch(/^Actions API: /);
  });

  it("uses exactly a pinned source", async () => {
    const pinned = run(7000);
    const { client, calls } = fakeClient([valid(run(8002)), valid(pinned)]);

    const selection = await selectWeights(client, { ...options, pinnedRunId: "7000" });

    expect(selection).toMatchObject({ mode: "runtime", sourceRunId: "7000" });
    expect(calls).not.toContain(`list ${MAX_SOURCE_RUNS}`);
  });

  it.each<[string, string, Published[]]>([
    ["a value that is not a run ID", "70x", []],
    ["a run the API cannot find", "7000", []],
    ["a run that does not qualify", "7000", [valid(run(7000, { head_branch: "feature" }))]],
    ["a run without weights", "7000", [{ run: run(7000) }]],
    ["the current run", CURRENT, [valid(run(Number(CURRENT)))]],
  ])("refuses a pinned source that is %s", async (_name, pinnedRunId, published) => {
    const { client } = fakeClient(published);
    const selection = selectWeights(client, { ...options, pinnedRunId });
    await expect(selection).rejects.toThrow(SelectionRefusal);
  });

  it("refuses a pinned source when the download fails", async () => {
    const { client } = fakeClient([valid(run(7000))], { download: true });
    await expect(selectWeights(client, { ...options, pinnedRunId: "7000" })).rejects.toThrow(
      SelectionRefusal,
    );
  });
});

/** Reads step outputs the way the runner does: `name=value` or `name<<delimiter` blocks. */
function readOutputs(text: string): Record<string, string> {
  const outputs: Record<string, string> = {};
  const lines = text.split("\n");
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i]!;
    if (line === "") continue;
    const block = /^([a-z_]+)<<(.+)$/.exec(line);
    if (!block) {
      const [name, ...value] = line.split("=");
      outputs[name!] = value.join("=");
      continue;
    }
    const end = lines.indexOf(block[2]!, i + 1);
    outputs[block[1]!] = lines.slice(i + 1, end).join("\n");
    i = end;
  }
  return outputs;
}

describe("formatOutputs", () => {
  const text = weightsText(run(8002));
  const runtime: Extract<Selection, { mode: "runtime" }> = {
    mode: "runtime",
    reason: "using server-shard-weights-a1 from run 8002",
    skipped: [],
    sourceRunId: "8002",
    artifactName: "server-shard-weights-a1",
    text,
    digest: weightsDigest(text),
  };

  it("hands the shards one digest-checked map and the shard list", () => {
    const outputs = readOutputs(formatOutputs(runtime, 6));

    expect(outputs).toMatchObject({
      mode: "runtime",
      source_run_id: "8002",
      digest: weightsDigest(text),
      shard_count: "6",
      shards: "[1,2,3,4,5,6]",
    });
    // The shard step restores the final newline before checking the digest.
    expect(weightsDigest(`${outputs.weights}\n`)).toBe(outputs.digest);
  });

  it("chooses another delimiter when one appears in the value", () => {
    const hostile = { ...runtime, text: `${text}JONES_WEIGHTS_clash\n` };
    const values = ["clash", "safe"];

    const output = formatOutputs(hostile, 6, () => values.shift()!);

    expect(output).toContain("weights<<JONES_WEIGHTS_safe\n");
    expect(readOutputs(output).weights).toBe(hostile.text.slice(0, -1));
  });

  it("keeps the checked-in map and lists eight shards when asked", () => {
    const outputs = readOutputs(
      formatOutputs({ mode: "checked-in", reason: "a\nb", skipped: [] }, 8),
    );

    expect(outputs).toEqual({
      mode: "checked-in",
      reason: "a b",
      source_run_id: "",
      digest: "",
      shard_count: "8",
      shards: "[1,2,3,4,5,6,7,8]",
      weights: "",
    });
  });

  it("summarizes the choice and every skipped source", () => {
    const skipped = ["run 8003: it was a pull_request run"];
    const summary = formatSummary({ ...runtime, skipped }, 6);

    expect(summary).toContain("Mode: `runtime`");
    expect(summary).toContain(weightsDigest(text));
    expect(summary).toContain("- run 8003: it was a pull_request run");
  });
});

describe("parseShardCount", () => {
  it.each([
    [undefined, 6],
    ["", 6],
    ["6", 6],
    ["8", 8],
  ])("reads %s as %i shards", (input, count) => {
    expect(parseShardCount(input)).toBe(count);
  });

  it.each(["7", "10", "six"])("rejects %s", (input) => {
    expect(() => parseShardCount(input)).toThrow();
  });
});

describe("readSingleZipEntry", () => {
  const read = (archive: Uint8Array, maxBytes = 1000) =>
    readSingleZipEntry(archive, { name: WEIGHTS_FILE_NAME, maxBytes });

  it.each([0, 8] as const)("reads a single entry stored with method %i", (method) => {
    expect(read(zip([{ name: WEIGHTS_FILE_NAME, content: "{}\n", method }]))).toBe("{}\n");
  });

  it("rejects an archive with more than one file", () => {
    const archive = zip([
      { name: WEIGHTS_FILE_NAME, content: "{}\n" },
      { name: "extra.json", content: "{}\n" },
    ]);
    expect(() => read(archive)).toThrow(/exactly one file/);
  });

  it("rejects an entry with another name", () => {
    expect(() => read(zip([{ name: "other.json", content: "{}\n" }]))).toThrow(/expected/);
  });

  it("rejects an entry over the size limit", () => {
    expect(() => read(zip([{ name: WEIGHTS_FILE_NAME, content: "x".repeat(1001) }]))).toThrow(
      /size limit/,
    );
  });

  it("rejects an entry whose CRC does not match", () => {
    const archive = Buffer.from(zip([{ name: WEIGHTS_FILE_NAME, content: "{}\n", method: 0 }]));
    archive[30 + WEIGHTS_FILE_NAME.length] = "[".charCodeAt(0);
    expect(() => read(archive)).toThrow(/CRC/);
  });

  it("rejects bytes that are not a zip archive", () => {
    expect(() => read(Buffer.from("not a zip archive at all"))).toThrow(/end of central directory/);
  });
});
