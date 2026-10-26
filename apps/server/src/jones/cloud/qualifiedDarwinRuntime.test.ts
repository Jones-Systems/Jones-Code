// @effect-diagnostics nodeBuiltinImport:off
import { assert, it } from "@effect/vitest";
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import type { JonesStagedArtifact } from "@t3tools/shared/jones/jonesActions";
import { vi } from "vite-plus/test";
import {
  extractQualifiedDarwinRuntime,
  readQualifiedAsarMetadata,
  type DarwinStageCommand,
  type DarwinStageRunner,
} from "./qualifiedDarwinRuntime.ts";
import { QualifiedRuntimeBlockedError, recentQualifiedDigestTimings } from "./qualifiedRuntime.ts";

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
    await NodeFSP.writeFile(NodePath.join(root, "candidate.dmg"), "synthetic DMG");
    return await body(root);
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
    await NodeAssert.rejects(NodeFSP.lstat(root), { code: "ENOENT" });
  }
}

const stagedArtifact = (root: string): JonesStagedArtifact => ({
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
});

function withInventory(root: string, run: DarwinStageRunner): DarwinStageRunner {
  let mount: string | undefined;
  return async (command) => {
    command.signal?.throwIfAborted();
    if (command.command === "/usr/bin/hdiutil" && command.args[0] === "info")
      return {
        code: 0,
        stdout: JSON.stringify({
          images:
            mount === undefined
              ? []
              : [
                  {
                    "image-path": NodePath.join(root, "candidate.dmg"),
                    "system-entities": [
                      { "dev-entry": "/dev/disk123" },
                      { "dev-entry": "/dev/disk123s1", "mount-point": mount },
                    ],
                  },
                ],
        }),
      };
    if (command.command === "/usr/bin/plutil" && command.args[0] === "-convert")
      return { code: 0, stdout: command.stdin ?? "" };
    const result = await run(command);
    if (command.args[0] === "attach" && result.code === 0)
      mount = command.args[command.args.indexOf("-mountpoint") + 1]!;
    if (command.args[0] === "detach" && result.code === 0) mount = undefined;
    return result;
  };
}

async function cancellationFixture(
  root: string,
  detach: DarwinStageRunner,
  omitSignal = false,
): Promise<{
  readonly failure: unknown;
  readonly primary: Error;
  readonly mount: string;
  readonly calls: ReadonlyArray<DarwinStageCommand>;
}> {
  const baseDir = NodePath.join(root, "home");
  const destination = NodePath.join(root, "payload");
  await NodeFSP.mkdir(NodePath.join(baseDir, "runtime"), { recursive: true });
  await NodeFSP.mkdir(destination);
  const controller = new AbortController();
  const primary = new Error("Synthetic cancellation during app copy.");
  const calls: DarwinStageCommand[] = [];
  let mount = "";
  const failure = await extractQualifiedDarwinRuntime({
    artifact: stagedArtifact(root),
    baseDir,
    destination,
    ...(omitSignal ? {} : { signal: controller.signal }),
    run: withInventory(root, async (command) => {
      calls.push(command);
      // Model the preparation runner's pre-spawn abort gate without native tools.
      (command.signal ?? controller.signal).throwIfAborted();
      if (command.command === "/usr/bin/hdiutil" && command.args[0] === "attach") {
        mount = command.args[command.args.indexOf("-mountpoint") + 1]!;
        await NodeFSP.mkdir(NodePath.join(mount, "Fixture.app"));
      } else if (command.command === "/usr/bin/ditto") {
        controller.abort(primary);
        throw primary;
      } else {
        assert.equal(command.command, "/usr/bin/hdiutil");
        assert.equal(command.args[0], "detach");
        assert.equal(command.args[1], mount);
        assert.notEqual(command.signal, controller.signal);
        return detach(command);
      }
      return { code: 0, stdout: "" };
    }),
  }).then(
    () => undefined,
    (cause: unknown) => cause,
  );
  return { failure, primary, mount, calls };
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

it.each(["extraction", "staging"] as const)(
  "stages a readonly DMG with %s verification and preserves native home state",
  async (payloadVerification) => {
    await fixture(async (root) => {
      const home = NodePath.join(root, "home");
      const destination = NodePath.join(root, "payload");
      await NodeFSP.mkdir(NodePath.join(home, "runtime"), { recursive: true });
      await NodeFSP.mkdir(destination);
      await NodeFSP.writeFile(NodePath.join(home, "native-state"), "unchanged");
      const calls: string[] = [];
      const artifact = stagedArtifact(root);
      const before = recentQualifiedDigestTimings();
      await extractQualifiedDarwinRuntime({
        artifact,
        payloadVerification,
        destination,
        baseDir: home,
        run: withInventory(root, async ({ command, args }) => {
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
            await NodeFSP.writeFile(
              NodePath.join(app, "Contents", "Resources", "app.asar"),
              asar(),
            );
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
        }),
      });
      const measured = recentQualifiedDigestTimings().filter((timing) => !before.includes(timing));
      assert.equal(measured.length, payloadVerification === "extraction" ? 1 : 0);
      assert.equal(
        await NodeFSP.readFile(NodePath.join(home, "native-state"), "utf8"),
        "unchanged",
      );
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
  },
);

it("detaches the owned mount after the job is cancelled and preserves the cancellation", async () => {
  await fixture(async (root) => {
    const detached: DarwinStageCommand[] = [];
    const result = await cancellationFixture(root, async (command) => {
      detached.push(command);
      return { code: 0, stdout: "" };
    });
    assert.equal(detached.length, 1);
    assert.deepEqual(detached[0]!.args, ["detach", result.mount]);
    assert.equal(result.failure, result.primary);
    await NodeAssert.rejects(NodeFSP.lstat(NodePath.dirname(result.mount)), { code: "ENOENT" });
  });
});

it("retries a failed detach with force on only the owned mount", async () => {
  await fixture(async (root) => {
    const detached: DarwinStageCommand[] = [];
    const result = await cancellationFixture(root, async (command) => {
      detached.push(command);
      return { code: command.args.includes("-force") ? 0 : 1, stdout: "" };
    });
    assert.deepEqual(
      detached.map(({ args }) => args),
      [
        ["detach", result.mount],
        ["detach", result.mount, "-force"],
      ],
    );
    assert.notEqual(detached[0]!.signal, detached[1]!.signal);
    assert.equal(result.failure, result.primary);
    await NodeAssert.rejects(NodeFSP.lstat(NodePath.dirname(result.mount)), { code: "ENOENT" });
  });
});

it("retains the exact mount and both staging and cleanup failures when forced detach fails", async () => {
  await fixture(async (root) => {
    const ordinary = new Error("Synthetic ordinary detach failure.");
    const forced = new Error("Synthetic forced detach failure.");
    const result = await cancellationFixture(root, async ({ args }) => {
      throw args.includes("-force") ? forced : ordinary;
    });
    assert.instanceOf(result.failure, QualifiedRuntimeBlockedError);
    const blocked = result.failure as QualifiedRuntimeBlockedError;
    assert.equal(blocked.reason, "busy");
    assert.include(blocked.message, result.mount);
    assert.instanceOf(blocked.cause, AggregateError);
    const [primary, cleanup] = (blocked.cause as AggregateError).errors;
    assert.equal(primary, result.primary);
    assert.instanceOf(cleanup, AggregateError);
    assert.deepEqual((cleanup as AggregateError).errors, [ordinary, forced]);
    assert.isTrue((await NodeFSP.lstat(result.mount)).isDirectory());
    assert.deepEqual(await NodeFSP.readdir(result.mount), ["Fixture.app"]);
  });
});

it.each([
  [false, false],
  [true, false],
  [false, true],
  [true, true],
])(
  "bounds each detach attempt after a forced deadline (timeout=%s, no job signal=%s)",
  async (forceTimesOut, omitSignal) => {
    await fixture(async (root) => {
      vi.useFakeTimers();
      try {
        const ordinaryStarted = Promise.withResolvers<void>();
        const forcedStarted = Promise.withResolvers<void>();
        const detached: DarwinStageCommand[] = [];
        const pending = cancellationFixture(
          root,
          async (command) => {
            detached.push(command);
            const forced = command.args.includes("-force");
            (forced ? forcedStarted : ordinaryStarted).resolve();
            if (forced && !forceTimesOut) return { code: 0, stdout: "" };
            assert.isDefined(command.signal);
            return new Promise((_, reject) => {
              command.signal!.addEventListener("abort", () => reject(command.signal!.reason), {
                once: true,
              });
            });
          },
          omitSignal,
        );
        await ordinaryStarted.promise;
        await vi.advanceTimersByTimeAsync(5_000);
        await forcedStarted.promise;
        assert.isTrue(detached[0]!.signal!.aborted);
        assert.isFalse(detached[1]!.signal!.aborted);
        await vi.advanceTimersByTimeAsync(5_000);
        const result = await pending;
        if (forceTimesOut) {
          assert.instanceOf(result.failure, QualifiedRuntimeBlockedError);
          assert.isTrue((await NodeFSP.lstat(result.mount)).isDirectory());
        } else {
          assert.equal(result.failure, result.primary);
          await NodeAssert.rejects(NodeFSP.lstat(NodePath.dirname(result.mount)), {
            code: "ENOENT",
          });
        }
        assert.equal(vi.getTimerCount(), 0);
      } finally {
        vi.useRealTimers();
      }
    });
  },
);

it("reconciles a failed attach that created a unique device without a mount point", async () => {
  await fixture(async (root) => {
    const baseDir = NodePath.join(root, "home");
    const destination = NodePath.join(root, "payload");
    await NodeFSP.mkdir(NodePath.join(baseDir, "runtime"), { recursive: true });
    await NodeFSP.mkdir(destination);
    await NodeFSP.writeFile(NodePath.join(root, "candidate.dmg"), "synthetic DMG");
    const artifact = stagedArtifact(root);
    const detaches: ReadonlyArray<string>[] = [];
    let attached = false;
    let mount = "";
    const inventory = () =>
      JSON.stringify({
        images: attached
          ? [
              {
                "image-path": artifact.payloadPath,
                "system-entities": [
                  { "dev-entry": "/dev/disk123" },
                  { "dev-entry": "/dev/disk123s1" },
                ],
              },
            ]
          : [],
      });
    const failure = await extractQualifiedDarwinRuntime({
      artifact,
      baseDir,
      destination,
      run: async (command) => {
        if (command.command === "/usr/bin/hdiutil" && command.args[0] === "info")
          return { code: 0, stdout: inventory() };
        if (command.command === "/usr/bin/plutil" && command.args[0] === "-convert")
          return { code: 0, stdout: command.stdin ?? "" };
        if (command.args[0] === "attach") {
          mount = command.args[command.args.indexOf("-mountpoint") + 1]!;
          attached = true;
          return { code: 1, stdout: "", stderr: "hdiutil: synthetic partial attach diagnostic" };
        }
        assert.equal(command.command, "/usr/bin/hdiutil");
        assert.equal(command.args[0], "detach");
        detaches.push(command.args);
        if (command.args[1] !== "/dev/disk123") return { code: 1, stdout: "" };
        attached = false;
        return { code: 0, stdout: "" };
      },
    }).then(
      () => undefined,
      (cause: unknown) => cause,
    );
    assert.deepEqual(detaches, [["detach", "/dev/disk123"]]);
    assert.instanceOf(failure, QualifiedRuntimeBlockedError);
    assert.include((failure as Error).message, "synthetic partial attach diagnostic");
    await NodeAssert.rejects(NodeFSP.lstat(NodePath.dirname(mount)), { code: "ENOENT" });
  });
});

it.each([
  ["no attachment", 0, false],
  ["ambiguous images", 0, true],
  ["preexisting device", 0, true],
  ["foreign mount", 0, true],
  ["missing device", 0, true],
  ["unattributed new image", 0, true],
  ["malformed inventory", 0, true],
  ["inventory lost after detach", 1, true],
  ["attachment survives detach", 2, true],
  ["device reused after detach", 1, true],
  ["force recovery", 2, false],
  ["both detach attempts fail", 2, true],
  ["incomplete attach response", 1, false],
] as const)(
  "preserves ownership and diagnostics with %s",
  async (scenario, expectedDetaches, retained) => {
    await fixture(async (root) => {
      const baseDir = NodePath.join(root, "home");
      const destination = NodePath.join(root, "payload");
      await NodeFSP.mkdir(NodePath.join(baseDir, "runtime"), { recursive: true });
      await NodeFSP.mkdir(destination);
      const artifact = stagedArtifact(root);
      let attempted = false;
      let attached = false;
      let mount = "";
      const detaches: ReadonlyArray<string>[] = [];
      const entities = () => [
        { "dev-entry": "/dev/disk123" },
        {
          "dev-entry": "/dev/disk123s1",
          ...(scenario === "foreign mount" ? { "mount-point": "/unrelated" } : {}),
        },
      ];
      const failure = await extractQualifiedDarwinRuntime({
        artifact,
        baseDir,
        destination,
        run: async (command) => {
          if (command.command === "/usr/bin/hdiutil" && command.args[0] === "info") {
            if (scenario === "inventory lost after detach" && detaches.length > 0)
              return { code: 1, stdout: "", stderr: "inventory unavailable" };
            if (scenario === "malformed inventory" && attempted)
              return { code: 0, stdout: JSON.stringify({ images: "unknown" }) };
            const images = [];
            if (
              (!attempted && scenario === "preexisting device") ||
              (detaches.length > 0 && scenario === "device reused after detach")
            )
              images.push({ "image-path": "/unrelated.dmg", "system-entities": entities() });
            else if (attached) {
              images.push({
                "image-path":
                  scenario === "unattributed new image"
                    ? "/unattributed.dmg"
                    : artifact.payloadPath,
                "system-entities": scenario === "missing device" ? [] : entities(),
              });
              if (scenario === "ambiguous images")
                images.push({
                  "image-path": artifact.payloadPath,
                  "system-entities": [{ "dev-entry": "/dev/disk124" }],
                });
            }
            return { code: 0, stdout: JSON.stringify({ images }) };
          }
          if (command.command === "/usr/bin/plutil" && command.args[0] === "-convert")
            return { code: 0, stdout: command.stdin ?? "" };
          if (command.args[0] === "attach") {
            attempted = true;
            attached = scenario !== "no attachment";
            mount = command.args[command.args.indexOf("-mountpoint") + 1]!;
            return {
              code: scenario === "incomplete attach response" ? 0 : 1,
              stdout: "",
              stderr: "primary attach diagnostic",
              timedOut: scenario === "incomplete attach response",
            };
          }
          assert.equal(command.command, "/usr/bin/hdiutil");
          assert.equal(command.args[0], "detach");
          assert.equal(command.args[1], "/dev/disk123");
          assert.isDefined(command.signal);
          detaches.push(command.args);
          if (
            scenario === "both detach attempts fail" ||
            (scenario === "force recovery" && !command.args.includes("-force"))
          )
            return { code: 1, stdout: "", stderr: "cleanup diagnostic" };
          if (scenario !== "attachment survives detach") attached = false;
          return { code: 0, stdout: "" };
        },
      }).then(
        () => undefined,
        (cause: unknown) => cause,
      );
      assert.equal(detaches.length, expectedDetaches);
      if (expectedDetaches === 2) assert.include(detaches[1]!, "-force");
      assert.instanceOf(failure, QualifiedRuntimeBlockedError);
      const primary = retained ? ((failure as Error).cause as AggregateError).errors[0] : failure;
      assert.include((primary as Error).message, "primary attach diagnostic");
      if (retained) {
        assert.equal((failure as QualifiedRuntimeBlockedError).reason, "busy");
        assert.isTrue((await NodeFSP.lstat(mount)).isDirectory());
      } else await NodeAssert.rejects(NodeFSP.lstat(NodePath.dirname(mount)), { code: "ENOENT" });
    });
  },
);

it.each(["preexisting image", "invalid baseline"] as const)(
  "refuses %s before attempting an attachment",
  async (scenario) => {
    await fixture(async (root) => {
      const baseDir = NodePath.join(root, "home");
      const destination = NodePath.join(root, "payload");
      await NodeFSP.mkdir(NodePath.join(baseDir, "runtime"), { recursive: true });
      await NodeFSP.mkdir(destination);
      const artifact = stagedArtifact(root);
      const calls: DarwinStageCommand[] = [];
      await NodeAssert.rejects(
        extractQualifiedDarwinRuntime({
          artifact,
          baseDir,
          destination,
          run: async (command) => {
            calls.push(command);
            if (command.command === "/usr/bin/hdiutil")
              return {
                code: 0,
                stdout: JSON.stringify({
                  images:
                    scenario === "invalid baseline"
                      ? null
                      : [
                          {
                            "image-path": artifact.payloadPath,
                            "system-entities": [{ "dev-entry": "/dev/disk123" }],
                          },
                        ],
                }),
              };
            assert.equal(command.command, "/usr/bin/plutil");
            return { code: 0, stdout: command.stdin ?? "" };
          },
        }),
        scenario === "preexisting image" ? /already has an attachment/ : /inventory is incomplete/,
      );
      assert.isFalse(calls.some((command) => ["attach", "detach"].includes(command.args[0]!)));
      assert.deepEqual(await NodeFSP.readdir(NodePath.join(baseDir, "runtime")), []);
    });
  },
);
