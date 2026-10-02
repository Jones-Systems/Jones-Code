import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { describe, expect, it } from "vite-plus/test";
import { bindArtifactInputs, qualifyPackageSourceDiff, withRunScratch } from "./support.mjs";

const source = "f4053108054c708dda0bfb5cb03ef0a2e6908459";
const observed = { repository: "Jones-Systems/Jones-Code", commit: source, clean: true };

async function descriptor(root) {
  const bytes = Buffer.from("synthetic qualification archive; never an installed package");
  const path = NodePath.join(root, "candidate.tar.gz");
  await NodeFSP.writeFile(path, bytes);
  return {
    schema: "jones-runtime-artifact-inputs/v1",
    repository: observed.repository,
    packagingBase: source,
    acceptedCumulativeSource: source,
    candidate: {
      version: "0.0.44-preview.20261001.4",
      channel: "preview",
      platform: "linux",
      architecture: "x64",
      sourceCommit: source,
      path,
      sha256: NodeCrypto.createHash("sha256").update(bytes).digest("hex"),
      runnerPath: null,
      runnerSha256: null,
    },
  };
}

describe("artifact binding qualification", () => {
  it("keeps early package inputs unbound instead of accepting the packaging base", async () => {
    const result = await bindArtifactInputs(
      {
        schema: "jones-runtime-artifact-inputs/v1",
        repository: observed.repository,
        packagingBase: source,
        acceptedCumulativeSource: null,
        candidate: { version: null, path: null, sha256: null },
      },
      observed,
    );
    expect(result.status).toBe("unbound");
    expect(result.reasons.length).toBeGreaterThan(0);
  });

  it("binds exact source and archive bytes while leaving operational claims unproved", async () => {
    await withRunScratch({ label: "artifact-binding" }, async ({ root, record }) => {
      const inputs = await descriptor(root);
      const result = await bindArtifactInputs(inputs, observed);
      expect(result.status).toBe("bound");
      await record({
        checkId: "synthetic-artifact-binding",
        proofKind: "synthetic-descriptor",
        result: "passed",
        nativeResume: "unproved",
        installedAdoption: "unproved",
      });
    });
  });

  it("rejects altered archive bytes", async () => {
    await withRunScratch({ label: "artifact-corruption" }, async ({ root }) => {
      const inputs = await descriptor(root);
      await NodeFSP.writeFile(inputs.candidate.path, "altered");
      await expect(bindArtifactInputs(inputs, observed)).rejects.toThrow(/hash|checksum|sha256/i);
    });
  });

  it("rejects a candidate attributed to another source commit", async () => {
    await withRunScratch({ label: "candidate-source" }, async ({ root }) => {
      const inputs = await descriptor(root);
      inputs.candidate.sourceCommit = "0".repeat(40);
      await expect(bindArtifactInputs(inputs, observed)).rejects.toThrow(/source commit/i);
    });
  });

  it("rejects a foreign repository, mismatched source and dirty source", async () => {
    await withRunScratch({ label: "source-binding" }, async ({ root }) => {
      const inputs = await descriptor(root);
      for (const invalid of [
        { ...observed, repository: "pingdotgg/t3code" },
        { ...observed, commit: "0".repeat(40) },
        { ...observed, clean: false },
      ]) {
        await expect(bindArtifactInputs(inputs, invalid)).rejects.toThrow(
          /source|repository|dirty|clean/i,
        );
      }
    });
  });

  it("rejects a mismatched extracted runner hash", async () => {
    await withRunScratch({ label: "runner-binding" }, async ({ root }) => {
      const inputs = await descriptor(root);
      inputs.candidate.runnerPath = NodePath.join(root, "t3");
      inputs.candidate.runnerSha256 = "0".repeat(64);
      await NodeFSP.writeFile(inputs.candidate.runnerPath, "synthetic runner bytes");
      await expect(bindArtifactInputs(inputs, observed)).rejects.toThrow(/hash|checksum|sha256/i);
    });
  });
});

describe("package-source metadata boundary", () => {
  const original = Buffer.from(
    '{\n  "workspaces": {\n    "scripts": {\n      "entry": [\n        "smoke-cli-archive.ts",\n      ],\n    },\n  },\n}\n',
  );
  const entry = '        "runtime-adoption-qualification/run.mjs",\n';
  const inserted = Buffer.from(
    original
      .toString()
      .replace('        "smoke-cli-archive.ts",\n', '        "smoke-cli-archive.ts",\n' + entry),
  );

  it("accepts only the exact runner insertion and retains both byte hashes", () => {
    const result = qualifyPackageSourceDiff(
      ["knip.jsonc", "scripts/runtime-adoption-qualification/run.mjs"],
      original,
      inserted,
    );
    expect(result.productionDiffPaths).toEqual([]);
    expect(result.nonBuildMetadataDiff).toEqual([
      {
        path: "knip.jsonc",
        entry: "runtime-adoption-qualification/run.mjs",
        packageSha256: NodeCrypto.createHash("sha256").update(original).digest("hex"),
        qualificationSha256: NodeCrypto.createHash("sha256").update(inserted).digest("hex"),
        proofKind: "exact-one-entry-byte-insertion",
      },
    ]);
    expect(qualifyPackageSourceDiff(["scripts/runtime-adoption-qualification/run.mjs"])).toEqual({
      productionDiffPaths: [],
      nonBuildMetadataDiff: [],
    });
  });

  it("rejects extra entries, ignores, formatting changes and unchanged Knip bytes", () => {
    for (const bytes of [
      original,
      Buffer.from(inserted.toString().replace(entry, entry + '        "other.mjs",\n')),
      Buffer.from(
        inserted.toString().replace('"workspaces": {', '"ignore": ["**"], "workspaces": {'),
      ),
      Buffer.from(inserted.toString() + "\n"),
      Buffer.from(inserted.toString().replaceAll("\n", "\r\n")),
    ]) {
      expect(() => qualifyPackageSourceDiff(["knip.jsonc"], original, bytes)).toThrow(
        /exact runner entry/,
      );
    }
  });

  it("rejects runtime changes even alongside the valid metadata insertion", () => {
    for (const changed of [["apps/server/src/bin.ts"], ["knip.jsonc", "apps/server/src/bin.ts"]]) {
      expect(() => qualifyPackageSourceDiff(changed, original, inserted)).toThrow(
        /production source/,
      );
    }
  });

  it("rejects missing, duplicate or misplaced script anchors and missing bytes", () => {
    for (const bytes of [
      Buffer.from(original.toString().replace("smoke-cli-archive.ts", "other.ts")),
      Buffer.concat([original, original]),
      Buffer.from(original.toString().replace('"scripts": {', '"other": {')),
    ]) {
      expect(() => qualifyPackageSourceDiff(["knip.jsonc"], bytes, inserted)).toThrow(/anchor/);
    }
    expect(() => qualifyPackageSourceDiff(["knip.jsonc"])).toThrow(/bytes/);
  });
});
