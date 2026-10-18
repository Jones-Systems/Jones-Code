// @effect-diagnostics nodeBuiltinImport:off - Fixtures build exact release closures with modes, links and hashes.
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { describe, expect, it } from "vite-plus/test";

import { type CollectorBinding, HOST_BINDING_PATH, verifyRelease } from "./release.ts";
import { monotonicSeconds, runBoundCollector } from "./collector.ts";

const COMMIT = "a".repeat(40);
const GENERATOR = {
  repository: "Jones-Systems/Codex-V3",
  commit: COMMIT,
  path: "tools/install-decision-snapshot-release.py",
};
const ENTRYPOINT = "tools/decision-snapshot.py";
const SOURCES = JSON.stringify({ schema: "synthetic-host-binding", accounts: {} });

type Provenance = Record<string, unknown>;
interface Entry {
  readonly content: string;
  readonly provenance: Provenance;
  readonly mode?: number;
}

const source = (path: string): Provenance => ({
  kind: "source",
  repository: "Jones-Systems/Codex-V3",
  commit: COMMIT,
  path,
});
const hostBinding: Provenance = { kind: "host_binding", generator: GENERATOR };

const productionEntries = (): Record<string, Entry> => ({
  [ENTRYPOINT]: { content: "print('unused')\n", provenance: source(ENTRYPOINT) },
  [HOST_BINDING_PATH]: { content: SOURCES, provenance: hostBinding, mode: 0o444 },
});

const sha = (value: string | Buffer) => NodeCrypto.createHash("sha256").update(value).digest("hex");

async function withRelease(
  entries: Record<string, Entry>,
  check: (binding: CollectorBinding, directory: string) => Promise<void>,
  options: { python?: string; runtimeScript?: string } = {},
) {
  const owner = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "decision-snapshot-release-"));
  const releaseId = "2".repeat(64);
  const directory = NodePath.join(owner, releaseId);
  try {
    await NodeFSP.mkdir(directory);
    const files = [];
    for (const [path, entry] of Object.entries(entries)) {
      const target = NodePath.join(directory, path);
      await NodeFSP.mkdir(NodePath.dirname(target), { recursive: true });
      await NodeFSP.writeFile(target, entry.content);
      if (entry.mode !== undefined) await NodeFSP.chmod(target, entry.mode);
      files.push({
        path,
        size_bytes: Buffer.byteLength(entry.content),
        sha256: sha(entry.content),
        provenance: entry.provenance,
      });
    }
    let python = options.python ?? "/usr/bin/python3";
    if (options.runtimeScript !== undefined) {
      python = NodePath.join(await NodeFSP.realpath(owner), "python-binding");
      await NodeFSP.writeFile(python, options.runtimeScript, { mode: 0o755 });
    }
    const descriptor = JSON.stringify({
      schema: "codex.decision-snapshot-release/v1",
      release_id: releaseId,
      custody: "immutable-by-api",
      entrypoint: ENTRYPOINT,
      external_runtime: {
        python,
        stdlib: true,
        account_native_bindings: HOST_BINDING_PATH,
      },
      files,
    });
    await NodeFSP.writeFile(NodePath.join(directory, "decision-snapshot-release.json"), descriptor);
    const releaseDirectory = await NodeFSP.realpath(directory);
    await check(
      {
        releaseDirectory,
        releaseId,
        custody: "immutable-by-api",
        manifestSha256: sha(descriptor),
      },
      releaseDirectory,
    );
  } finally {
    await NodeFSP.rm(owner, { recursive: true, force: true });
  }
}

describe("decision snapshot release host binding provenance", () => {
  it("accepts a generated account binding on a production release", async () => {
    await withRelease(productionEntries(), async (binding, directory) => {
      await expect(verifyRelease(binding)).resolves.toMatchObject({
        entrypoint: NodePath.join(directory, ENTRYPOINT),
      });
    });
  });

  it("rejects host binding provenance on any file other than the account binding", async () => {
    for (const path of [ENTRYPOINT, "src/codex_v3/decision_snapshot.py", "tools/other.json"]) {
      const entries = productionEntries();
      entries[path] = { content: "pass\n", provenance: source(path) };
      await withRelease(entries, async (binding) => {
        await expect(verifyRelease(binding)).resolves.toBeDefined();
      });
      entries[path] = { content: "pass\n", provenance: hostBinding };
      await withRelease(entries, async (binding) => {
        await expect(verifyRelease(binding)).rejects.toThrow();
      });
    }
  });

  it("rejects an executable account binding", async () => {
    const entries = productionEntries();
    entries[HOST_BINDING_PATH] = { content: SOURCES, provenance: hostBinding, mode: 0o755 };
    await withRelease(entries, async (binding) => {
      await expect(verifyRelease(binding)).rejects.toThrow();
    });
  });

  it("keeps the provenance closed and requires the exact generator source", async () => {
    const invalid: ReadonlyArray<Provenance> = [
      { kind: "host_binding" },
      { kind: "host_binding", generator: { ...GENERATOR, commit: "HEAD" } },
      { kind: "host_binding", generator: { ...GENERATOR, repository: "someone/else" } },
      { kind: "host_binding", generator: { ...GENERATOR, path: "../escape.py" } },
      { kind: "host_binding", generator: { ...GENERATOR, extra: true } },
      { kind: "host_binding", generator: GENERATOR, generated_from: "/Users/owner/binding.json" },
    ];
    for (const provenance of invalid) {
      const entries = productionEntries();
      entries[HOST_BINDING_PATH] = { content: SOURCES, provenance, mode: 0o444 };
      await withRelease(entries, async (binding) => {
        await expect(verifyRelease(binding)).rejects.toThrow();
      });
    }
  });

  it("still binds generated bytes by hash, single link and no symlink", async () => {
    await withRelease(productionEntries(), async (binding, directory) => {
      await expect(verifyRelease(binding)).resolves.toBeDefined();
      const target = NodePath.join(directory, HOST_BINDING_PATH);
      await NodeFSP.chmod(target, 0o644);
      await NodeFSP.writeFile(target, SOURCES.replace("synthetic", "Synthetic"));
      await expect(verifyRelease(binding)).rejects.toThrow();
    });
    await withRelease(productionEntries(), async (binding, directory) => {
      await expect(verifyRelease(binding)).resolves.toBeDefined();
      await NodeFSP.link(
        NodePath.join(directory, HOST_BINDING_PATH),
        NodePath.join(NodePath.dirname(directory), "outside-link.json"),
      );
      await expect(verifyRelease(binding)).rejects.toThrow();
    });
    await withRelease(productionEntries(), async (binding, directory) => {
      await expect(verifyRelease(binding)).resolves.toBeDefined();
      const target = NodePath.join(directory, HOST_BINDING_PATH);
      const moved = NodePath.join(NodePath.dirname(directory), "moved.json");
      await NodeFSP.rename(target, moved);
      await NodeFSP.symlink(moved, target);
      await expect(verifyRelease(binding)).rejects.toThrow();
    });
  });

  it("does not relax fixture rejection when a host binding is present", async () => {
    const entries = productionEntries();
    entries["src/helper.py"] = {
      content: "pass\n",
      provenance: { kind: "fixture", fixture_id: "synthetic", path: "src/helper.py" },
    };
    await withRelease(entries, async (binding) => {
      await expect(verifyRelease(binding)).rejects.toThrow();
      await expect(
        verifyRelease({ ...binding, allowFixtureProvenance: true }),
      ).resolves.toBeDefined();
    });
  });
});

describe("decision snapshot bound Python runtime", () => {
  it("executes the absolute runtime bound by the verified manifest", async () => {
    await withRelease(
      productionEntries(),
      async (binding) => {
        const result = await runBoundCollector(
          binding,
          "display",
          "{}",
          monotonicSeconds() + 10,
          new AbortController().signal,
        );
        expect(JSON.parse(result)).toEqual({ runtime: "manifest-bound", isolated: true });
      },
      {
        runtimeScript:
          "#!/usr/bin/python3\nimport json,sys\nsys.stdin.read()\nprint(json.dumps({'runtime':'manifest-bound','isolated':sys.argv[1:4] == ['-I','-S','-B']}))\n",
      },
    );
  });

  it("rejects relative, non-normalized and NUL-bearing runtime bindings", async () => {
    for (const python of [
      "python3",
      "relative/python3",
      "/usr/bin/../bin/python3",
      "/usr//bin/python3",
      "/usr/bin/python3\0extra",
    ]) {
      await withRelease(
        productionEntries(),
        async (binding) => {
          await expect(verifyRelease(binding)).rejects.toThrow();
        },
        { python },
      );
    }
  });
});
