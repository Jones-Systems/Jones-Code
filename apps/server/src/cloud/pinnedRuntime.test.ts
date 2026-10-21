import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import { HttpClient, HttpClientResponse } from "effect/http";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as HostProcess from "@t3tools/shared/HostProcess";

import * as ProcessRunner from "../processRunner.ts";
import {
  ensurePinnedRuntimeInstalled,
  installPinnedRuntimeFromLocalArchive,
  verifyPinnedRuntimeProvenance,
  pinnedRuntimeCommand,
  pinnedRuntimePaths,
  PinnedRuntimeInstallError,
  type PinnedRuntimeProgress,
  prunePinnedRuntimes,
} from "./pinnedRuntime.ts";
import { SERVICE_LAUNCHER_PROTOCOL } from "./serviceProtocol.ts";

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const sourceCommit = "a".repeat(40);
const version = "1.2.3";
const archiveName = `t3-${version}-linux-x64.tar.gz`;
const archiveBytes = new TextEncoder().encode("not really a tarball");
const archiveHex = (bytes: Uint8Array) =>
  Effect.promise(() => crypto.subtle.digest("SHA-256", bytes)).pipe(
    Effect.map((digest) =>
      Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join(""),
    ),
  );
const validChecksums = archiveHex(archiveBytes).pipe(
  Effect.map((hex) => `${hex}  ${archiveName}\n`),
);
const releaseHttpClient = (checksums: string, requests: string[] = []) =>
  HttpClient.make((request) => {
    requests.push(request.url);
    const body = request.url.endsWith("/SHA256SUMS") ? checksums : archiveBytes;
    return Effect.succeed(HttpClientResponse.fromWeb(request, new Response(body)));
  });
const extractingRunner = (fs: FileSystem.FileSystem, path: Path.Path, commands: string[] = []) =>
  ProcessRunner.ProcessRunner.of({
    run: (input) =>
      Effect.gen(function* () {
        commands.push(input.command);
        const targetIndex = input.args.indexOf("-C");
        const stagingDir = input.args[targetIndex + 1];
        if (input.command !== "tar" || stagingDir === undefined) {
          return yield* Effect.die(`unexpected command ${input.command}`);
        }
        yield* fs.writeFileString(path.join(stagingDir, "t3"), "#!/bin/sh\n").pipe(Effect.orDie);
        return {
          stdout: "",
          stderr: "",
          code: ChildProcessSpawner.ExitCode(0),
          timedOut: false,
          stdoutTruncated: false,
          stderrTruncated: false,
          stdoutInvalidUtf8: false,
          stderrInvalidUtf8: false,
        };
      }),
  });

const writeProvenance = (
  fs: FileSystem.FileSystem,
  path: Path.Path,
  paths: ReturnType<typeof pinnedRuntimePaths>,
  entry: string,
  source = sourceCommit,
) =>
  archiveHex(new TextEncoder().encode(entry)).pipe(
    Effect.flatMap((entrySha256) =>
      fs.writeFileString(
        path.join(paths.versionDir, ".jones-provenance.json"),
        encodeJson({
          schema: 1,
          repository: "Jones-Systems/Jones-Code",
          source,
          version,
          platform: "linux",
          architecture: "x64",
          artifact: archiveName,
          sha256: "b".repeat(64),
          entrySha256,
        }),
      ),
    ),
  );

const localArtifact = Effect.fn(function* (fs: FileSystem.FileSystem, path: Path.Path) {
  const artifactDir = yield* fs.makeTempDirectoryScoped({ prefix: "jones-local-artifact-" });
  const sha256 = yield* archiveHex(archiveBytes);
  const metadata = {
    schema: 1,
    repository: "Jones-Systems/Jones-Code",
    source: sourceCommit,
    version,
    platform: "linux",
    architecture: "x64",
    artifact: archiveName,
    sha256,
  };
  yield* fs.writeFileString(path.join(artifactDir, "ARTIFACT.json"), encodeJson(metadata));
  yield* fs.writeFileString(path.join(artifactDir, "SOURCE_COMMIT"), `${sourceCommit}\n`);
  yield* fs.writeFileString(path.join(artifactDir, "SHA256SUMS"), `${sha256}  ${archiveName}\n`);
  yield* fs.writeFile(path.join(artifactDir, archiveName), archiveBytes);
  return { artifactDir, metadata };
});

const localRunner = (fs: FileSystem.FileSystem, path: Path.Path, reportedVersion = version) => {
  const extractor = extractingRunner(fs, path);
  return ProcessRunner.ProcessRunner.of({
    run: (input) =>
      input.command === "tar"
        ? extractor.run(input)
        : Effect.succeed({
            stdout: `t3 v${reportedVersion}\n`,
            stderr: "",
            code: ChildProcessSpawner.ExitCode(0),
            timedOut: false,
            stdoutTruncated: false,
            stderrTruncated: false,
            stdoutInvalidUtf8: false,
            stderrInvalidUtf8: false,
          }),
  });
};

it.layer(NodeServices.layer)("ensurePinnedRuntimeInstalled", (it) => {
  it.effect.each([true, false])(
    "refuses an unverified existing runtime and preserves its bytes (complete: %s)",
    (complete) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "jones-runtime-preserve-" });
        const paths = pinnedRuntimePaths(path, baseDir, version, "linux");
        yield* fs.makeDirectory(paths.versionDir, { recursive: true });
        yield* fs.writeFileString(paths.entryPath, "unknown runtime bytes");
        if (complete) yield* fs.writeFileString(paths.sentinelPath, `${version}\n`);
        const requests: string[] = [];
        const result = yield* ensurePinnedRuntimeInstalled({
          baseDir,
          version,
          fs,
          path,
          platform: "linux",
          arch: "x64",
          httpClient: releaseHttpClient(yield* validChecksums, requests),
          runner: extractingRunner(fs, path),
          validate: () => Effect.void,
        }).pipe(Effect.result);
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure")
          assert.include(result.failure.message, "t3 jones host stage-runtime");
        assert.deepEqual(requests, []);
        assert.equal(yield* fs.readFileString(paths.entryPath), "unknown runtime bytes");
        assert.equal(yield* fs.exists(paths.sentinelPath), complete);
      }),
  );

  it.effect("refuses the upstream default before creating a runtime tree", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "jones-runtime-default-" });
      const requests: string[] = [];
      const result = yield* ensurePinnedRuntimeInstalled({
        baseDir,
        version,
        fs,
        path,
        platform: "linux",
        arch: "x64",
        httpClient: releaseHttpClient(yield* validChecksums, requests),
        runner: extractingRunner(fs, path),
        validate: () => Effect.void,
      }).pipe(Effect.result);
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure")
        assert.include(result.failure.message, "t3 jones host stage-runtime");
      assert.deepEqual(requests, []);
      assert.isFalse(yield* fs.exists(path.join(baseDir, "runtime")));
    }),
  );

  it.effect("uses a verified cache without a release origin and refuses a swapped executable", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "jones-cache-" });
      const paths = pinnedRuntimePaths(path, baseDir, version, "linux");
      yield* fs.makeDirectory(paths.versionDir, { recursive: true });
      yield* fs.writeFileString(paths.entryPath, "verified bytes");
      yield* fs.writeFileString(paths.sentinelPath, `${version}\n`);
      yield* writeProvenance(fs, path, paths, "verified bytes");
      const requests: string[] = [];
      let validations = 0;
      const input = {
        baseDir,
        version,
        fs,
        path,
        platform: "linux" as const,
        arch: "x64",
        httpClient: releaseHttpClient(yield* validChecksums, requests),
        runner: extractingRunner(fs, path),
        validate: () =>
          Effect.sync(() => {
            validations += 1;
          }),
      };
      assert.deepEqual(yield* ensurePinnedRuntimeInstalled(input), paths);
      assert.equal(validations, 1);
      yield* fs.writeFileString(paths.entryPath, "swapped bytes");
      const error = yield* ensurePinnedRuntimeInstalled(input).pipe(Effect.flip);
      assert.include(error.message, "t3 jones host stage-runtime");
      assert.equal(validations, 1);
      assert.deepEqual(requests, []);
      assert.equal(yield* fs.readFileString(paths.entryPath), "swapped bytes");
    }),
  );

  it.effect("publishes local provenance before the sentinel and reuses the verified cache", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "jones-local-runtime-" });
      const { artifactDir, metadata } = yield* localArtifact(fs, path);
      const paths = pinnedRuntimePaths(path, baseDir, version, "linux");
      let provenanceRecordedBeforeSentinel = false;
      const observedFs: FileSystem.FileSystem = {
        ...fs,
        writeFileString: (target, contents, options) =>
          Effect.gen(function* () {
            if (path.basename(target) === ".install-complete") {
              provenanceRecordedBeforeSentinel = yield* fs.exists(
                path.join(path.dirname(target), ".jones-provenance.json"),
              );
            }
            yield* fs.writeFileString(target, contents, options);
          }),
      };
      const input = {
        baseDir,
        artifactDir,
        expectSourceCommit: sourceCommit,
        fs: observedFs,
        path,
        platform: "linux" as const,
        arch: "x64",
        runner: localRunner(fs, path),
        validate: (staging: typeof paths) =>
          Effect.gen(function* () {
            if (staging.versionDir !== paths.versionDir) {
              assert.isFalse(yield* fs.exists(staging.sentinelPath));
              assert.isFalse(yield* fs.exists(paths.versionDir));
            }
          }).pipe(Effect.orDie),
      };
      assert.deepEqual(yield* installPinnedRuntimeFromLocalArchive(input), paths);
      assert.isTrue(provenanceRecordedBeforeSentinel);
      const provenance = yield* verifyPinnedRuntimeProvenance({
        fs,
        path,
        paths,
        version,
        platform: "linux",
        arch: "x64",
      });
      assert.deepEqual(provenance, {
        ...metadata,
        entrySha256: yield* archiveHex(new TextEncoder().encode("#!/bin/sh\n")),
      });
      assert.equal(yield* fs.readFileString(paths.sentinelPath), `${version}\n`);
      assert.deepEqual(yield* installPinnedRuntimeFromLocalArchive(input), paths);
      assert.deepEqual(yield* fs.readDirectory(path.dirname(paths.versionDir)), [version]);
    }),
  );

  it.effect.each(["tampered", "wrong-version", "different-provenance", "unverified-incomplete"])(
    "refuses local staging: %s",
    (scenario) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "jones-local-refusal-" });
        const { artifactDir, metadata } = yield* localArtifact(fs, path);
        const paths = pinnedRuntimePaths(path, baseDir, version, "linux");
        if (scenario === "tampered")
          yield* fs.writeFile(
            path.join(artifactDir, archiveName),
            new TextEncoder().encode("tampered"),
          );
        if (scenario === "different-provenance" || scenario === "unverified-incomplete") {
          yield* fs.makeDirectory(paths.versionDir, { recursive: true });
          yield* fs.writeFileString(paths.entryPath, "preserve existing bytes");
          if (scenario === "different-provenance") {
            yield* fs.writeFileString(paths.sentinelPath, `${version}\n`);
            yield* fs.writeFileString(
              path.join(paths.versionDir, ".jones-provenance.json"),
              encodeJson({
                ...metadata,
                source: "d".repeat(40),
                entrySha256: yield* archiveHex(new TextEncoder().encode("preserve existing bytes")),
              }),
            );
          }
        }
        const result = yield* installPinnedRuntimeFromLocalArchive({
          baseDir,
          artifactDir,
          expectSourceCommit: sourceCommit,
          fs,
          path,
          platform: "linux",
          arch: "x64",
          runner: localRunner(fs, path, scenario === "wrong-version" ? "9.9.9" : version),
          validate: () => Effect.void,
        }).pipe(Effect.result);
        assert.equal(result._tag, "Failure");
        if (scenario === "different-provenance" || scenario === "unverified-incomplete") {
          assert.equal(yield* fs.readFileString(paths.entryPath), "preserve existing bytes");
          assert.deepEqual(yield* fs.readDirectory(path.dirname(paths.versionDir)), [version]);
        } else {
          assert.isFalse(yield* fs.exists(paths.versionDir));
          if (scenario === "wrong-version")
            assert.deepEqual(yield* fs.readDirectory(path.dirname(paths.versionDir)), []);
        }
      }),
  );

  it.effect.each([true, false])(
    "reports staging cleanup failure without hiding an install failure (%s)",
    (validationFails) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "jones-runtime-cleanup-" });
        const cleanupTargets: string[] = [];
        const failingFs: FileSystem.FileSystem = {
          ...fs,
          remove: (target, options) => {
            if (path.basename(target).startsWith(".staging-")) {
              cleanupTargets.push(target);
              return Effect.fail(
                PlatformError.systemError({
                  _tag: "PermissionDenied",
                  module: "FileSystem",
                  method: "remove",
                  pathOrDescriptor: target,
                }),
              );
            }
            return fs.remove(target, options);
          },
        };
        const exit = yield* ensurePinnedRuntimeInstalled({
          baseDir,
          version,
          fs: failingFs,
          path,
          platform: "linux",
          arch: "x64",
          releaseBaseUrl: "https://releases.example/download",
          httpClient: releaseHttpClient(yield* validChecksums),
          runner: extractingRunner(fs, path),
          validate: () =>
            validationFails
              ? Effect.fail(
                  new PinnedRuntimeInstallError({ step: "deliberate validation failure" }),
                )
              : Effect.void,
        }).pipe(Effect.exit);
        assert.equal(exit._tag, "Failure");
        if (exit._tag === "Failure") {
          const message = Cause.pretty(exit.cause);
          assert.include(message, "cleaning the pinned runtime staging directory");
          if (validationFails) assert.include(message, "deliberate validation failure");
        }
        assert.lengthOf(cleanupTargets, 1);
        assert.equal(path.dirname(cleanupTargets[0]!), path.join(baseDir, "runtime", "versions"));
        yield* fs.remove(cleanupTargets[0]!, { recursive: true, force: true });
      }),
  );

  it.effect("installs the verified release archive as the runtime executable", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pinned-archive-" });
      const requests: string[] = [];
      const commands: string[] = [];
      const paths = yield* ensurePinnedRuntimeInstalled({
        baseDir,
        version,
        fs,
        path,
        platform: "linux",
        arch: "x64",
        releaseBaseUrl: "https://releases.example/download",
        httpClient: releaseHttpClient(yield* validChecksums, requests),
        runner: extractingRunner(fs, path, commands),
        validate: (staging) =>
          fs.exists(staging.entryPath).pipe(
            Effect.flatMap((exists) => (exists ? Effect.void : Effect.die("missing runtime"))),
            Effect.orDie,
          ),
      });
      assert.equal(paths.entryPath, path.join(paths.versionDir, "t3"));
      assert.deepEqual(pinnedRuntimeCommand(paths), { command: paths.entryPath, args: [] });
      assert.deepEqual(requests, [
        `https://releases.example/download/v${version}/SHA256SUMS`,
        `https://releases.example/download/v${version}/${archiveName}`,
      ]);
      assert.deepEqual(commands, ["tar"]);
      assert.equal(yield* fs.readFileString(paths.sentinelPath), `${version}\n`);
      assert.isFalse(yield* fs.exists(path.join(paths.versionDir, "t3-runtime-archive")));
    }),
  );

  it.effect("cleans up an interrupted download without reporting verification or extraction", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pinned-progress-failed-" });
      const checksums = yield* validChecksums;
      const progress: PinnedRuntimeProgress[] = [];
      let cancelled = false;
      const client = HttpClient.make((request) =>
        Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            request.url.endsWith("/SHA256SUMS")
              ? new Response(checksums)
              : new Response(
                  new ReadableStream({
                    start(controller) {
                      controller.enqueue(archiveBytes.slice(0, 4));
                    },
                    cancel() {
                      cancelled = true;
                    },
                  }),
                ),
          ),
        ),
      );
      const firstChunk = yield* Deferred.make<void>();
      const install = yield* ensurePinnedRuntimeInstalled({
        baseDir,
        version,
        fs,
        path,
        platform: "linux",
        arch: "x64",
        releaseBaseUrl: "https://releases.example/download",
        httpClient: client,
        runner: extractingRunner(fs, path),
        validate: () => Effect.die("must not validate an interrupted archive"),
        onProgress: (event) => {
          progress.push(event);
          if (event.stage === "download" && event.received === 4)
            Deferred.doneUnsafe(firstChunk, Effect.void);
        },
      }).pipe(Effect.forkScoped);
      yield* Deferred.await(firstChunk);
      yield* Fiber.interrupt(install);
      assert.deepEqual(progress.at(-1), { stage: "download", received: 4, total: undefined });
      assert.isTrue(progress.every((event) => event.stage === "download"));
      assert.isTrue(cancelled);
      assert.deepEqual(yield* fs.readDirectory(path.join(baseDir, "runtime", "versions")), []);
    }),
  );

  it.effect("refuses an archive whose checksum does not match the release", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pinned-archive-bad-" });
      const commands: string[] = [];
      const error = yield* ensurePinnedRuntimeInstalled({
        baseDir,
        version,
        fs,
        path,
        platform: "linux",
        arch: "x64",
        releaseBaseUrl: "https://releases.example/download",
        httpClient: releaseHttpClient(`${"0".repeat(64)}  ${archiveName}\n`),
        runner: extractingRunner(fs, path, commands),
        validate: () => Effect.die("must not validate an unverified archive"),
      }).pipe(Effect.flip);
      assert.instanceOf(error, PinnedRuntimeInstallError);
      assert.equal(error.step, "verifying the t3 release archive checksum");
      assert.deepEqual(commands, []);
      assert.deepEqual(yield* fs.readDirectory(path.join(baseDir, "runtime", "versions")), []);
    }),
  );

  it.effect("validates a staging tree before atomically publishing it", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pinned-runtime-test-" });
      const finalPaths = pinnedRuntimePaths(path, baseDir, version, "linux");
      let validatedDirectory = "";

      const installed = yield* ensurePinnedRuntimeInstalled({
        baseDir,
        version,
        fs,
        path,
        platform: "linux",
        arch: "x64",
        releaseBaseUrl: "https://releases.example/download",
        httpClient: releaseHttpClient(yield* validChecksums),
        runner: extractingRunner(fs, path),
        validate: (staging) =>
          Effect.gen(function* () {
            validatedDirectory = staging.versionDir;
            assert.isFalse(yield* fs.exists(finalPaths.versionDir));
            assert.isTrue(yield* fs.exists(staging.entryPath));
          }).pipe(Effect.orDie),
      });

      assert.notEqual(validatedDirectory, finalPaths.versionDir);
      assert.deepEqual(installed, finalPaths);
      assert.isTrue(yield* fs.exists(finalPaths.entryPath));
      assert.equal(yield* fs.readFileString(finalPaths.sentinelPath), `${version}\n`);
    }),
  );

  it.effect("removes staging and leaves no final runtime when validation fails", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pinned-runtime-test-" });
      const finalPaths = pinnedRuntimePaths(path, baseDir, version, "linux");

      yield* ensurePinnedRuntimeInstalled({
        baseDir,
        version,
        fs,
        path,
        platform: "linux",
        arch: "x64",
        releaseBaseUrl: "https://releases.example/download",
        httpClient: releaseHttpClient(yield* validChecksums),
        runner: extractingRunner(fs, path),
        validate: () =>
          Effect.fail(new PinnedRuntimeInstallError({ step: "validating the staged runtime" })),
      }).pipe(Effect.flip);

      assert.isFalse(yield* fs.exists(finalPaths.versionDir));
      assert.deepEqual(
        (yield* fs.readDirectory(path.dirname(finalPaths.versionDir))).filter((entry) =>
          entry.startsWith(".staging-"),
        ),
        [],
      );
    }),
  );

  it.effect("replaces an incomplete runtime with verified Jones provenance", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pinned-runtime-repair-" });
      const finalPaths = pinnedRuntimePaths(path, baseDir, version, "linux");
      yield* fs.makeDirectory(finalPaths.versionDir, { recursive: true });
      yield* fs.writeFileString(path.join(finalPaths.versionDir, "partial"), "incomplete\n");
      yield* fs.writeFileString(finalPaths.entryPath, "old executable");
      yield* writeProvenance(fs, path, finalPaths, "old executable");

      yield* ensurePinnedRuntimeInstalled({
        baseDir,
        version,
        fs,
        path,
        platform: "linux",
        arch: "x64",
        releaseBaseUrl: "https://releases.example/download",
        httpClient: releaseHttpClient(yield* validChecksums),
        runner: extractingRunner(fs, path),
        validate: () => Effect.void,
      });

      assert.isFalse(yield* fs.exists(path.join(finalPaths.versionDir, "partial")));
      assert.isTrue(yield* fs.exists(finalPaths.entryPath));
    }),
  );

  it.effect("preserves a completed runtime when validation fails", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pinned-runtime-repair-" });
      const finalPaths = pinnedRuntimePaths(path, baseDir, version, "linux");
      yield* fs.makeDirectory(path.dirname(finalPaths.entryPath), { recursive: true });
      yield* fs.writeFileString(finalPaths.entryPath, "broken\n");
      yield* fs.writeFileString(finalPaths.sentinelPath, `${version}\n`);
      yield* writeProvenance(fs, path, finalPaths, "broken\n");

      let validations = 0;
      const requests: string[] = [];
      yield* ensurePinnedRuntimeInstalled({
        baseDir,
        version,
        fs,
        path,
        platform: "linux",
        arch: "x64",
        releaseBaseUrl: "https://releases.example/download",
        httpClient: releaseHttpClient(yield* validChecksums, requests),
        runner: extractingRunner(fs, path),
        validate: (paths) =>
          Effect.gen(function* () {
            validations += 1;
            const source = yield* fs.readFileString(paths.entryPath).pipe(Effect.orDie);
            if (source === "broken\n") {
              return yield* new PinnedRuntimeInstallError({ step: "validating the runtime" });
            }
          }),
      }).pipe(Effect.flip);

      assert.equal(validations, 1);
      assert.deepEqual(requests, []);
      assert.equal(yield* fs.readFileString(finalPaths.entryPath), "broken\n");
    }),
  );
});

it.layer(NodeServices.layer)("prunePinnedRuntimes", (it) => {
  const lsofRunner = (stdout: string) =>
    ProcessRunner.ProcessRunner.of({
      run: () =>
        Effect.succeed({
          stdout,
          stderr: "",
          code: ChildProcessSpawner.ExitCode(0),
          timedOut: false,
          stdoutTruncated: false,
          stderrTruncated: false,
          stdoutInvalidUtf8: false,
          stderrInvalidUtf8: false,
        }),
    });

  const setup = Effect.fn(function* (versions: ReadonlyArray<string>) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pinned-prune-" });
    const versionsDir = path.join(baseDir, "runtime", "versions");
    const now = Duration.toMillis(Duration.days(1));
    yield* TestClock.setTime(now);
    const install = Effect.fn(function* (
      version: string,
      completed: "long ago" | "just now" | "never" = "long ago",
    ) {
      const directory = path.join(versionsDir, version);
      yield* fs.makeDirectory(directory, { recursive: true });
      yield* fs.writeFileString(path.join(directory, "t3"), "");
      if (completed === "never") return;
      const sentinel = path.join(directory, ".install-complete");
      yield* fs.writeFileString(sentinel, `${version}\n`);
      const at = completed === "just now" ? now : 0;
      yield* fs.utimes(sentinel, at, at);
    });
    for (const version of versions) yield* install(version);
    return { baseDir, versionsDir, install };
  });

  it.effect.each([
    { marker: "jones-active-install.json", kind: "file" },
    { marker: "versions/1.1.0/.jones-runtime-receipt.json", kind: "file" },
    { marker: "versions/1.1.0/.jones-provenance.json", kind: "file" },
    { marker: "staged-updates", kind: "directory" },
    { marker: "jones-updates", kind: "directory" },
    { marker: "unavailable runtime observation", kind: "error" },
  ])("preserves Jones runtimes without discovery or launcher writes: $marker", ({ marker, kind }) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const { baseDir, versionsDir } = yield* setup(["1.0.0", "1.1.0", ".pruned-0.8.0"]);
      const runtimeDir = path.join(baseDir, "runtime");
      const markerPath = path.join(runtimeDir, marker);
      if (kind === "directory") yield* fs.makeDirectory(markerPath);
      else if (kind === "file") yield* fs.writeFileString(markerPath, "{}");
      const unreadableDirectory = path.join(baseDir, "not-a-directory");
      yield* fs.writeFileString(unreadableDirectory, "file");
      const binDir = path.join(baseDir, "bin");
      yield* fs.makeDirectory(binDir);
      yield* fs.symlink(path.join(versionsDir, "1.1.0", "t3"), path.join(binDir, "t3"));
      let processScans = 0;
      const observedFs = FileSystem.FileSystem.of({
        ...fs,
        readDirectory: (directory, options) => {
          if (directory === "/proc") {
            processScans += 1;
            return Effect.succeed(["123"]);
          }
          if (kind === "error" && directory === runtimeDir) {
            return fs.readDirectory(unreadableDirectory);
          }
          return fs.readDirectory(directory, options);
        },
        readLink: (link) =>
          link === "/proc/123/exe" ? Effect.succeed("/usr/bin/node") : fs.readLink(link),
      });

      const removed = yield* prunePinnedRuntimes({ baseDir, keep: [] }).pipe(
        Effect.provideService(FileSystem.FileSystem, observedFs),
        Effect.provideService(ProcessRunner.ProcessRunner, lsofRunner("")),
        Effect.provideService(HostProcess.Platform, "linux"),
        Effect.provideService(HostProcess.Environment, { T3CODE_INSTALL_BIN_DIR: binDir }),
      );

      assert.deepEqual(removed, []);
      assert.equal(processScans, 0);
      assert.isFalse(yield* fs.exists(path.join(runtimeDir, "launchers")));
      assert.deepEqual((yield* fs.readDirectory(versionsDir)).toSorted(), [
        ".pruned-0.8.0",
        "1.0.0",
        "1.1.0",
      ]);
    }),
  );

  it.effect("removes only completed runtimes nothing references", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const { baseDir, versionsDir, install } = yield* setup([
        "1.0.0",
        "1.1.0",
        "1.2.0",
        "1.2.5",
        "1.3.0",
        "1.3.5",
        "1.4.0",
        "1.5.0",
        "2.0.0",
        "2.1.0",
      ]);
      // A downgrade another process installed but has not pointed anything at yet.
      yield* install("0.5.0", "just now");
      yield* install("1.0.1", "never");
      yield* install(".staging-abc", "never");
      yield* install(".pruned-0.8.0");
      yield* install("other", "never");
      yield* fs.symlink(path.join(versionsDir, "other"), path.join(versionsDir, "0.9.0"));
      const binDir = path.join(baseDir, "bin");
      const customBinDir = path.join(baseDir, "custom-bin");
      yield* fs.makeDirectory(binDir);
      yield* fs.makeDirectory(customBinDir);
      // Chained: the launcher reaches its runtime through a second link.
      yield* fs.symlink(path.join(versionsDir, "1.2.0", "t3"), path.join(baseDir, "alias-t3"));
      yield* fs.symlink(path.join(baseDir, "alias-t3"), path.join(binDir, "t3"));
      yield* fs.symlink(path.join(versionsDir, "1.2.5", "t3"), path.join(customBinDir, "t3"));
      yield* fs.writeFileString(
        path.join(baseDir, "runtime", "launchers"),
        `${path.join(customBinDir, "t3")}\n`,
      );
      yield* fs.writeFileString(
        path.join(baseDir, "runtime", "service-state.json"),
        JSON.stringify({
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          activeVersion: "1.4.0",
          update: { id: "u", fromVersion: "1.4.0", targetVersion: "1.5.0", status: "rolled-back" },
        }),
      );

      const removed = yield* prunePinnedRuntimes({
        baseDir,
        keep: ["2.0.0"],
        references: [`ExecStart=${path.join(versionsDir, "1.3.5", "t3")} __service-launcher`],
      }).pipe(
        Effect.provideService(
          ProcessRunner.ProcessRunner,
          lsofRunner(`p42\nftxt\nn${path.join(versionsDir, "1.3.0", "t3")}\n`),
        ),
        Effect.provideService(HostProcess.Platform, "darwin"),
        Effect.provideService(HostProcess.Environment, { T3CODE_INSTALL_BIN_DIR: binDir }),
      );

      // 1.2.0 is the default launcher, 1.2.5 a recorded one, 1.3.0 running, 1.3.5 in the unit,
      // 1.4.0 active, 1.5.0 the rolled-back target, 2.0.0 the caller's, 0.5.0 just installed.
      // Being newer than the caller's version does not keep 2.1.0.
      assert.deepEqual(removed.toSorted(), ["0.8.0", "1.0.0", "1.1.0", "2.1.0"]);
      // The launcher found on PATH is remembered for a service that cannot see it.
      assert.equal(
        yield* fs.readFileString(path.join(baseDir, "runtime", "launchers")),
        `${path.join(customBinDir, "t3")}\n${path.join(binDir, "t3")}\n`,
      );
      assert.deepEqual((yield* fs.readDirectory(versionsDir)).toSorted(), [
        ".staging-abc",
        "0.5.0",
        "0.9.0",
        "1.0.1",
        "1.2.0",
        "1.2.5",
        "1.3.0",
        "1.3.5",
        "1.4.0",
        "1.5.0",
        "2.0.0",
        "other",
      ]);
    }),
  );

  it.effect("matches a Windows shim whatever its casing", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const { baseDir, versionsDir } = yield* setup(["1.0.0", "1.1.0"]);
      const shim = path.join(baseDir, "t3.cmd");
      yield* fs.writeFileString(
        shim,
        `@echo off\r\n"${path.join(versionsDir, "1.0.0", "t3.exe").toUpperCase()}" %*`,
      );
      yield* fs.writeFileString(path.join(baseDir, "runtime", "launchers"), `${shim}\n`);

      const removed = yield* prunePinnedRuntimes({ baseDir, keep: ["2.0.0"] }).pipe(
        Effect.provideService(
          ProcessRunner.ProcessRunner,
          lsofRunner("C:\\Windows\\explorer.exe\n"),
        ),
        Effect.provideService(HostProcess.Platform, "win32"),
        Effect.provideService(HostProcess.Environment, {}),
      );
      assert.deepEqual(removed, ["1.1.0"]);
    }),
  );

  it.effect.each([
    { name: "the process list is unavailable", state: undefined, lsof: "" },
    { name: "the service state is unparseable", state: "{", lsof: "p1\nftxt\nn/sbin/launchd\n" },
    {
      name: "the service state cannot be read",
      state: null,
      lsof: "p1\nftxt\nn/sbin/launchd\n",
    },
  ])("keeps everything when $name", ({ state, lsof }) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const { baseDir, versionsDir } = yield* setup(["1.0.0"]);
      const statePath = path.join(baseDir, "runtime", "service-state.json");
      if (state === null) yield* fs.makeDirectory(statePath);
      else if (state !== undefined) yield* fs.writeFileString(statePath, state);

      const removed = yield* prunePinnedRuntimes({ baseDir, keep: ["2.0.0"] }).pipe(
        Effect.provideService(ProcessRunner.ProcessRunner, lsofRunner(lsof)),
        Effect.provideService(HostProcess.Platform, "darwin"),
        Effect.provideService(HostProcess.Environment, { T3CODE_INSTALL_BIN_DIR: baseDir }),
      );
      assert.deepEqual(removed, []);
      assert.deepEqual(yield* fs.readDirectory(versionsDir), ["1.0.0"]);
    }),
  );

  it.effect.skipIf(HostProcess.Platform.defaultValue() !== "linux")(
    "keeps a runtime a process runs through a symlink",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const { baseDir, versionsDir } = yield* setup(["1.0.0", "1.1.0"]);
        const entry = path.join(versionsDir, "1.0.0", "t3");
        yield* fs.copyFile("/bin/sleep", entry);
        yield* fs.chmod(entry, 0o755);
        const link = path.join(baseDir, "t3");
        yield* fs.symlink(entry, link);
        // Its argv names only the link, as with `t3 serve` run from PATH. Removing the link after
        // spawning leaves the process's executable as the only reference.
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        yield* spawner.spawn(ChildProcess.make(link, ["30"]));
        yield* fs.remove(link);

        const removed = yield* prunePinnedRuntimes({ baseDir, keep: ["2.0.0"] }).pipe(
          Effect.provideService(HostProcess.Platform, "linux"),
          Effect.provideService(HostProcess.Environment, { T3CODE_INSTALL_BIN_DIR: baseDir }),
          Effect.provide(ProcessRunner.layer),
        );
        assert.deepEqual(removed, ["1.1.0"]);
      }),
  );
});
