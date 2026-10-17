// @effect-diagnostics nodeBuiltinImport:off - entrypoint detection is a Node filesystem boundary.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import { describe, expect, it, vi } from "vite-plus/test";

import { isEntrypoint } from "./entrypoint.ts";
import { symlinksSupported } from "@t3tools/shared/testing/symlinks";

const makeTempDir = () => NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-entrypoint-test-"));

describe("isEntrypoint", () => {
  it("uses the runtime answer when Node provides one", () => {
    expect(
      isEntrypoint({
        moduleUrl: "file:///somewhere/bin.mjs",
        entryPath: "/elsewhere/other.mjs",
        runtimeMain: true,
      }),
    ).toBe(true);
    expect(
      isEntrypoint({
        moduleUrl: "file:///somewhere/bin.mjs",
        entryPath: "/somewhere/bin.mjs",
        runtimeMain: false,
      }),
    ).toBe(false);
  });

  it("matches the entrypoint path when the runtime has no import.meta.main", () => {
    // Node 22.16, 22.17 and 23.11 are inside `engines.node` but leave it undefined.
    const dir = makeTempDir();
    const entry = NodePath.join(dir, "bin.mjs");
    NodeFS.writeFileSync(entry, "");

    expect(
      isEntrypoint({
        moduleUrl: NodeURL.pathToFileURL(entry).href,
        entryPath: entry,
        runtimeMain: undefined,
      }),
    ).toBe(true);
  });

  it.skipIf(!symlinksSupported)(
    "matches through a symlinked entrypoint, as npm and npx install it",
    () => {
      const dir = makeTempDir();
      const real = NodePath.join(dir, "bin.mjs");
      const link = NodePath.join(dir, "t3");
      NodeFS.writeFileSync(real, "");
      NodeFS.symlinkSync(real, link);

      // Node resolves the module URL to the real path, and on macOS the temp
      // directory itself sits behind a symlink (`/var` -> `/private/var`).
      expect(
        isEntrypoint({
          moduleUrl: NodeURL.pathToFileURL(NodeFS.realpathSync(real)).href,
          entryPath: link,
          runtimeMain: undefined,
        }),
      ).toBe(true);
    },
  );

  it("stays false for an imported module that is not the entrypoint", () => {
    const dir = makeTempDir();
    const entry = NodePath.join(dir, "bin.mjs");
    const imported = NodePath.join(dir, "cli.mjs");
    NodeFS.writeFileSync(entry, "");
    NodeFS.writeFileSync(imported, "");

    expect(
      isEntrypoint({
        moduleUrl: NodeURL.pathToFileURL(imported).href,
        entryPath: entry,
        runtimeMain: undefined,
      }),
    ).toBe(false);
  });

  it("stays false when there is no entrypoint argument", () => {
    expect(
      isEntrypoint({
        moduleUrl: "file:///somewhere/bin.mjs",
        entryPath: undefined,
        runtimeMain: undefined,
      }),
    ).toBe(false);
  });
});

describe("bin dispatch", () => {
  it.for(["acp-mcp-bridge", "acp-mcp-call", undefined] as const)(
    "preserves lazy routing for %s",
    async (command) => {
      const previousArgv = process.argv;
      const events: unknown[] = [];
      vi.resetModules();
      vi.doMock("./entrypoint.ts", () => ({ isEntrypoint: () => true }));
      vi.doMock("./mcp/AcpMcpStdioBridge.ts", () => {
        events.push("load-acp");
        return {
          runAcpMcpCliFastPath: async (actual: string, args: string[]) => {
            events.push(["run-acp", actual, args]);
          },
        };
      });
      vi.doMock("./cli/consoleStdin.ts", () => {
        events.push("load-console");
        return {
          attachConsoleStdin: () => {
            events.push("attach-console");
          },
        };
      });
      vi.doMock("./binCli.ts", () => {
        events.push("load-cli");
        return {
          runCli: () => {
            events.push("run-cli");
          },
        };
      });
      vi.doMock("./jones/workstreams/nativeAuthorityTransition.ts", () => {
        throw new Error("Non-native command loaded the native transition module");
      });
      try {
        process.argv =
          command === undefined
            ? [process.execPath, "fixture-bin"]
            : [process.execPath, "fixture-bin", command, "--fixture", "value"];
        await import("./bin.ts");
        expect(events).toEqual(
          command === undefined
            ? ["load-console", "attach-console", "load-cli", "run-cli"]
            : ["load-acp", ["run-acp", command, ["--fixture", "value"]]],
        );
      } finally {
        process.argv = previousArgv;
        for (const module of [
          "./entrypoint.ts",
          "./mcp/AcpMcpStdioBridge.ts",
          "./cli/consoleStdin.ts",
          "./binCli.ts",
          "./jones/workstreams/nativeAuthorityTransition.ts",
        ])
          vi.doUnmock(module);
        vi.resetModules();
      }
    },
  );
});
