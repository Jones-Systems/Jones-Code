import { describe, expect, it } from "vite-plus/test";
import { planCliArchiveDependencies } from "./cliArchiveDependencies.ts";

const hash = "f1e680afec85f650699ea756f91f469844ec4c976f4f168492a45c6b3f67d651";
const version = `1.2.0-beta.15(patch_hash=${hash})`;
const patchKey = "node-pty@1.2.0-beta.15";
const patchPath = "patches/node-pty@1.2.0-beta.15.patch";

function fixture() {
  return {
    environment: {
      lockfileVersion: "9.0",
      importers: {
        ".": {
          configDependencies: {},
          packageManagerDependencies: { pnpm: { specifier: "12.14.0", version: "12.14.0" } },
        },
      },
      packages: {},
      snapshots: {},
    },
    workspace: {
      lockfileVersion: "9.0",
      importers: {
        "apps/server": {
          dependencies: { "node-pty": { specifier: "^1.2.0-beta.15", version } },
        },
      },
      patchedDependencies: { [patchKey]: hash },
      snapshots: { [`node-pty@${version}`]: {} },
    },
  };
}

function plan(documents: ReadonlyArray<unknown>, specifier = "^1.2.0-beta.15") {
  return planCliArchiveDependencies({
    dependencies: { "node-pty": specifier, unpatched: "^2.0.0" },
    patchedDependencies: { [patchKey]: patchPath },
    lockfile: documents.map((document) => `---\n${JSON.stringify(document)}\n`).join(""),
  });
}

describe("CLI archive lockfile documents", () => {
  it("uses the workspace bindings after the pnpm environment document", () => {
    const { environment, workspace } = fixture();
    expect(plan([environment, workspace])).toEqual({
      dependencies: { "node-pty": "1.2.0-beta.15", unpatched: "^2.0.0" },
      patchedDependencies: { [patchKey]: patchPath },
    });
  });

  it("binds a direct lockfile override to the exact patched importer version", () => {
    const { environment, workspace } = fixture();
    workspace.importers["apps/server"].dependencies["node-pty"].specifier = "1.2.0-beta.15";
    expect(
      plan([environment, { ...workspace, overrides: { "node-pty": "1.2.0-beta.15" } }]),
    ).toEqual({
      dependencies: { "node-pty": "1.2.0-beta.15", unpatched: "^2.0.0" },
      patchedDependencies: { [patchKey]: patchPath },
    });
  });

  it("rejects a stale manifest even when the lockfile has a matching exact override", () => {
    const { environment, workspace } = fixture();
    workspace.importers["apps/server"].dependencies["node-pty"].specifier = "1.2.0-beta.15";
    expect(() =>
      plan(
        [environment, { ...workspace, overrides: { "node-pty": "1.2.0-beta.15" } }],
        "^1.2.0-beta.16",
      ),
    ).toThrow();
  });

  it.each(["1.2.0-beta.16", "^1.2.0-beta.15", "npm:node-pty@1.2.0-beta.15"])(
    "rejects an override that does not bind the exact patch version: %s",
    (override) => {
      const { environment, workspace } = fixture();
      workspace.importers["apps/server"].dependencies["node-pty"].specifier = override;
      expect(() =>
        plan([environment, { ...workspace, overrides: { "node-pty": override } }]),
      ).toThrow();
    },
  );

  it.each(["specifier", "patch hash", "snapshot"] as const)(
    "rejects a stale workspace %s even after a valid environment document",
    (kind) => {
      const { environment, workspace } = fixture();
      if (kind === "specifier") {
        workspace.importers["apps/server"].dependencies["node-pty"].specifier = "^1.2.0-beta.16";
      } else if (kind === "patch hash") {
        workspace.patchedDependencies[patchKey] = "0".repeat(64);
      }
      expect(() =>
        plan([environment, kind === "snapshot" ? { ...workspace, snapshots: {} } : workspace]),
      ).toThrow();
    },
  );

  it.each(["reversed", "two workspaces", "extra", "empty environment", "unsupported"] as const)(
    "rejects %s documents rather than falling back to another binding",
    (kind) => {
      const { environment, workspace } = fixture();
      const documents =
        kind === "reversed"
          ? [workspace, environment]
          : kind === "two workspaces"
            ? [workspace, workspace]
            : kind === "extra"
              ? [environment, workspace, workspace]
              : kind === "empty environment"
                ? [{ ...environment, importers: { ".": {} } }, workspace]
                : [{ ...environment, lockfileVersion: "10.0" }, workspace];
      expect(() => plan(documents)).toThrow();
    },
  );

  it.each(["environment", "workspace"] as const)("rejects malformed %s YAML", (kind) => {
    const { environment, workspace } = fixture();
    const document = kind === "environment" ? environment : workspace;
    const malformed = `${JSON.stringify(document)}\nlockfileVersion: '9.0'\n`;
    expect(() =>
      planCliArchiveDependencies({
        dependencies: { "node-pty": "^1.2.0-beta.15" },
        patchedDependencies: { [patchKey]: patchPath },
        lockfile:
          kind === "environment"
            ? `---\n${malformed}---\n${JSON.stringify(workspace)}`
            : `---\n${JSON.stringify(environment)}\n---\n${malformed}`,
      }),
    ).toThrow();
  });
});
