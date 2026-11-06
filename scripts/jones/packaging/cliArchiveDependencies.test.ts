import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { fromYaml } from "@t3tools/shared/schemaYaml";
import { describe, expect, it } from "vite-plus/test";

import { selectCliRuntimeExternalDependencies } from "../../lib/cli-external-packages.ts";
import { resolveCatalogDependencies } from "../../lib/resolve-catalog.ts";
import { planCliArchiveDependencies } from "./cliArchiveDependencies.ts";

const ptyHash = "f1e680afec85f650699ea756f91f469844ec4c976f4f168492a45c6b3f67d651";
const fffHash = "cc71fcec015f4d39bc96ae2e2bf6b0ca678124c722c66a7841efce9d34b5983c";
const effectHash = "c45feb52a7b4010859c019b830ced3b3bcde8fffdd13f49ada808a256d72a724";

function fixture() {
  const dependencies: Record<string, string> = {
    "node-pty": "^1.2.0-beta.15",
    "@ff-labs/fff-node": "0.11.0",
    "@ff-labs/fff-node-linux-x64-gnu": "0.11.0",
    "@napi-rs/keyring": "^1.3.0",
  };
  const patches: Record<string, string> = {
    "node-pty@1.2.0-beta.15": "patches/node-pty@1.2.0-beta.15.patch",
    "@ff-labs/fff-node@0.11.0": "patches/@ff-labs__fff-node@0.11.0.patch",
    "effect@4.0.5": "patches/effect@4.0.5.patch",
  };
  const importer: Record<string, { specifier: string; version: string }> = {
    "node-pty": {
      specifier: "^1.2.0-beta.15",
      version: `1.2.0-beta.15(patch_hash=${ptyHash})`,
    },
    "@ff-labs/fff-node": { specifier: "0.11.0", version: `0.11.0(patch_hash=${fffHash})` },
    effect: { specifier: "4.0.5", version: `4.0.5(patch_hash=${effectHash})` },
  };
  const lock = {
    lockfileVersion: "9.0",
    patchedDependencies: {
      "node-pty@1.2.0-beta.15": ptyHash,
      "@ff-labs/fff-node@0.11.0": fffHash,
      "effect@4.0.5": effectHash,
    } as Record<string, string>,
    importers: { "apps/server": { dependencies: importer } },
    snapshots: Object.fromEntries(
      Object.entries(importer).map(([name, entry]) => [`${name}@${entry.version}`, {}]),
    ),
  };
  return { dependencies, patches, lock };
}

function plan(input: ReturnType<typeof fixture>) {
  return planCliArchiveDependencies({
    dependencies: input.dependencies,
    patchedDependencies: input.patches,
    lockfile: JSON.stringify(input.lock),
  });
}

describe("CLI archive patched dependencies", () => {
  it("binds the real committed server externals to their exact mandatory patches", async () => {
    const inputs = await Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* path.fromFileUrl(new URL("../../../", import.meta.url));
      return {
        manifest: yield* fs.readFileString(path.join(root, "apps/server/package.json")),
        workspace: yield* fs.readFileString(path.join(root, "pnpm-workspace.yaml")),
        lockfile: yield* fs.readFileString(path.join(root, "pnpm-lock.yaml")),
      };
    }).pipe(Effect.provide(NodeServices.layer), Effect.runPromise);
    const manifest = Schema.decodeSync(
      Schema.fromJsonString(
        Schema.Struct({ dependencies: Schema.Record(Schema.String, Schema.String) }),
      ),
    )(inputs.manifest);
    const workspace = Schema.decodeSync(
      fromYaml(
        Schema.Struct({
          catalog: Schema.Record(Schema.String, Schema.String),
          patchedDependencies: Schema.Record(Schema.String, Schema.String),
        }),
      ),
    )(inputs.workspace);
    const dependencies = selectCliRuntimeExternalDependencies(
      resolveCatalogDependencies(manifest.dependencies, workspace.catalog, "apps/server"),
    );
    expect(dependencies["node-pty"]).toBe("^1.2.0-beta.15");
    const result = planCliArchiveDependencies({
      dependencies,
      patchedDependencies: workspace.patchedDependencies,
      lockfile: inputs.lockfile,
    });
    expect(result.dependencies).toEqual({ ...dependencies, "node-pty": "1.2.0-beta.15" });
    expect(result.patchedDependencies).toEqual({
      "node-pty@1.2.0-beta.15": workspace.patchedDependencies["node-pty@1.2.0-beta.15"],
      "@ff-labs/fff-node@0.11.0": workspace.patchedDependencies["@ff-labs/fff-node@0.11.0"],
    });
  });

  it("pins the ranged root while preserving scoped patches and unpatched platform roots", () => {
    const input = fixture();
    const before = structuredClone(input);
    expect(plan(input)).toEqual({
      dependencies: { ...input.dependencies, "node-pty": "1.2.0-beta.15" },
      patchedDependencies: {
        "node-pty@1.2.0-beta.15": input.patches["node-pty@1.2.0-beta.15"],
        "@ff-labs/fff-node@0.11.0": input.patches["@ff-labs/fff-node@0.11.0"],
      },
    });
    expect(input).toEqual(before);
  });

  it("preserves the Effect patch if Effect is explicitly staged", () => {
    const input = fixture();
    input.dependencies.effect = "4.0.5";
    expect(plan(input).patchedDependencies).toEqual(input.patches);
  });

  it.each([
    [
      "missing root",
      (f: ReturnType<typeof fixture>) => {
        delete f.lock.importers["apps/server"].dependencies["node-pty"];
      },
    ],
    [
      "stale specifier",
      (f: ReturnType<typeof fixture>) => {
        f.dependencies["node-pty"] = "^1.2.0-beta.16";
      },
    ],
    [
      "different patch version",
      (f: ReturnType<typeof fixture>) => {
        f.lock.importers["apps/server"].dependencies["node-pty"] = {
          specifier: "^1.2.0-beta.15",
          version: `1.2.0-beta.16(patch_hash=${ptyHash})`,
        };
      },
    ],
    [
      "missing patch hash",
      (f: ReturnType<typeof fixture>) => {
        delete f.lock.patchedDependencies["node-pty@1.2.0-beta.15"];
      },
    ],
    [
      "mismatched patch hash",
      (f: ReturnType<typeof fixture>) => {
        f.lock.patchedDependencies["node-pty@1.2.0-beta.15"] = fffHash;
      },
    ],
    [
      "missing snapshot",
      (f: ReturnType<typeof fixture>) => {
        delete f.lock.snapshots[`node-pty@1.2.0-beta.15(patch_hash=${ptyHash})`];
      },
    ],
    [
      "ambiguous patches",
      (f: ReturnType<typeof fixture>) => {
        f.patches["node-pty@1.2.0-beta.16"] = "patches/other.patch";
      },
    ],
    [
      "non-exact patch",
      (f: ReturnType<typeof fixture>) => {
        delete f.patches["node-pty@1.2.0-beta.15"];
        f.patches["node-pty@^1.2.0-beta.15"] = "patches/other.patch";
      },
    ],
    [
      "unpatched resolution",
      (f: ReturnType<typeof fixture>) => {
        f.lock.importers["apps/server"].dependencies["node-pty"] = {
          specifier: "^1.2.0-beta.15",
          version: "1.2.0-beta.15",
        };
      },
    ],
    [
      "peer-qualified resolution",
      (f: ReturnType<typeof fixture>) => {
        f.lock.importers["apps/server"].dependencies["node-pty"] = {
          specifier: "^1.2.0-beta.15",
          version: `1.2.0-beta.15(patch_hash=${ptyHash})(peer@1.0.0)`,
        };
      },
    ],
    [
      "unsupported lock version",
      (f: ReturnType<typeof fixture>) => {
        f.lock.lockfileVersion = "10.0";
      },
    ],
  ] as const)("rejects %s before installation", (_, mutate) => {
    const input = fixture();
    mutate(input);
    expect(() => plan(input)).toThrow();
  });

  it("rejects a missing server importer", () => {
    const input = fixture();
    expect(() =>
      planCliArchiveDependencies({
        dependencies: input.dependencies,
        patchedDependencies: input.patches,
        lockfile: JSON.stringify({ ...input.lock, importers: {} }),
      }),
    ).toThrow("apps/server lock importer");
  });

  it.each(["", "importers: {}", "lockfileVersion: '9.0'\nlockfileVersion: '9.0'"])(
    "rejects a missing, incomplete or ambiguous lockfile: %s",
    (lockfile) => {
      const input = fixture();
      expect(() =>
        planCliArchiveDependencies({
          dependencies: input.dependencies,
          patchedDependencies: input.patches,
          lockfile,
        }),
      ).toThrow();
    },
  );
});
