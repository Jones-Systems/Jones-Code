// @effect-diagnostics nodeBuiltinImport:off
import { assert, it } from "@effect/vitest";
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import type { JonesStagedArtifact } from "@t3tools/shared/jonesActions";
import {
  extractQualifiedDarwinRuntime,
  readQualifiedAsarMetadata,
} from "./qualifiedDarwinRuntime.ts";

const version = "0.0.44-preview.20261002.100.1";
const source = "a".repeat(40);
const tree = "b".repeat(40);
const expected = { version, source, tree };
function asar(
  metadata = {
    version,
    jonesSource: { repository: "Jones-Systems/Jones-Code", sha: source, tree },
  },
) {
  const packageJson = Buffer.from(JSON.stringify(metadata));
  const server = Buffer.from("server entry");
  const json = Buffer.from(
    JSON.stringify({
      files: {
        "package.json": { offset: "0", size: packageJson.length },
        apps: {
          files: {
            server: {
              files: {
                dist: {
                  files: { "bin.mjs": { offset: String(packageJson.length), size: server.length } },
                },
              },
            },
          },
        },
      },
    }),
  );
  const padded = Math.ceil(json.length / 4) * 4;
  const prefix = Buffer.alloc(16);
  prefix.writeUInt32LE(4, 0);
  prefix.writeUInt32LE(8 + padded, 4);
  prefix.writeUInt32LE(4 + padded, 8);
  prefix.writeUInt32LE(json.length, 12);
  return Buffer.concat([prefix, json, Buffer.alloc(padded - json.length), packageJson, server]);
}
async function fixture<A>(body: (root: string) => Promise<A>) {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-darwin-fixture-"));
  try {
    return await body(root);
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
  }
}

it("checks packed ASAR version/source and its headless server entry without launching Electron", async () => {
  await fixture(async (root) => {
    const file = NodePath.join(root, "app.asar");
    await NodeFSP.writeFile(file, asar());
    await readQualifiedAsarMetadata(file, expected);
    await NodeAssert.rejects(
      readQualifiedAsarMetadata(file, { ...expected, source: "c".repeat(40) }),
      /differs from its qualified/,
    );
    const bytes = asar();
    bytes.writeUInt32LE(32 * 1024 * 1024, 4);
    await NodeFSP.writeFile(file, bytes);
    await NodeAssert.rejects(readQualifiedAsarMetadata(file, expected), /header exceeds/);
  });
});

it("stages a readonly DMG app and exact Electron Node wrapper while preserving native home state", async () => {
  await fixture(async (root) => {
    const home = NodePath.join(root, "home");
    const destination = NodePath.join(root, "payload");
    await NodeFSP.mkdir(NodePath.join(home, "runtime"), { recursive: true });
    await NodeFSP.mkdir(destination);
    await NodeFSP.writeFile(NodePath.join(home, "native-state"), "unchanged");
    const calls: string[] = [];
    const artifact: JonesStagedArtifact = {
      schema: 1,
      source: "jones-actions",
      channel: "jones-main",
      stagedHandle: "qualified-artifact",
      payloadPath: NodePath.join(root, "candidate.dmg"),
      candidate: {
        schema: 1,
        repository: "Jones-Systems/Jones-Code",
        source,
        tree,
        installedSource: "c".repeat(40),
        workflow: ".github/workflows/artifact-desktop-mac.yml",
        workflowId: 1,
        runId: 100,
        runAttempt: 1,
        ciRunId: 101,
        artifactId: 102,
        artifactName: "desktop-mac-arm64-100-1",
        artifactDigest: `sha256:${"d".repeat(64)}`,
        artifactBytes: 1,
        expiresAt: "2026-10-10T00:00:00Z",
        version,
        platform: "darwin",
        architecture: "arm64",
      },
      receipt: {
        schema: 1,
        repository: "Jones-Systems/Jones-Code",
        source,
        tree,
        workflow: ".github/workflows/artifact-desktop-mac.yml",
        event: "push",
        ref: "refs/heads/main",
        runId: "100",
        runAttempt: "1",
        version,
        platform: "darwin",
        architecture: "arm64",
        artifact: "candidate.dmg",
        sha256: "e".repeat(64),
      },
    };
    await extractQualifiedDarwinRuntime({
      artifact,
      destination,
      baseDir: home,
      run: async ({ command, args }) => {
        calls.push(command);
        if (command === "/usr/bin/hdiutil" && args[0] === "attach") {
          assert.include(args, "-readonly");
          assert.include(args, "-nobrowse");
          const mount = args[args.indexOf("-mountpoint") + 1]!;
          const app = NodePath.join(mount, "T3 Code (Alpha).app");
          await NodeFSP.mkdir(NodePath.join(app, "Contents", "MacOS"), { recursive: true });
          await NodeFSP.mkdir(NodePath.join(app, "Contents", "Resources"));
          await NodeFSP.writeFile(
            NodePath.join(app, "Contents", "MacOS", "T3 Code (Alpha)"),
            "native executable fixture",
            { mode: 0o700 },
          );
          await NodeFSP.writeFile(NodePath.join(app, "Contents", "Resources", "app.asar"), asar());
          await NodeFSP.writeFile(NodePath.join(app, "Contents", "Info.plist"), "plist fixture");
        } else if (command === "/usr/bin/ditto")
          await NodeFSP.cp(args[2]!, args[3]!, { recursive: true });
        else if (command === "/usr/bin/plutil")
          return {
            code: 0,
            stdout: args[1] === "CFBundleExecutable" ? "T3 Code (Alpha)" : version,
          };
        else if (command === "/usr/bin/lipo") return { code: 0, stdout: "arm64" };
        return { code: 0, stdout: "" };
      },
    });
    assert.equal(await NodeFSP.readFile(NodePath.join(home, "native-state"), "utf8"), "unchanged");
    const wrapper = await NodeFSP.readFile(NodePath.join(destination, "t3"), "utf8");
    assert.include(wrapper, "ELECTRON_RUN_AS_NODE=1");
    assert.include(wrapper, "app.asar/apps/server/dist/bin.mjs");
    assert.isTrue(calls.every((command) => command.startsWith("/usr/bin/")));
    assert.isFalse(calls.some((command) => command.includes("Contents/MacOS")));
    assert.deepEqual(
      (await NodeFSP.readdir(NodePath.join(home, "runtime"))).filter((name) =>
        name.startsWith(".jones-dmg-mount-"),
      ),
      [],
    );
  });
});
