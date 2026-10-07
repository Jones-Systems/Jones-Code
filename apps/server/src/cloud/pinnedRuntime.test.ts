import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

import * as ProcessRunner from "../processRunner.ts";
import {
  ensurePinnedRuntimeInstalled,
  installPinnedRuntimeFromLocalArchive,
  verifyPinnedRuntimeProvenance,
  pinnedRuntimeCommand,
  pinnedRuntimePaths,
  PinnedRuntimeInstallError,
  type PinnedRuntimeProgress,
} from "./pinnedRuntime.ts";

// Explicit network installs fetch the release archive and check SHA256SUMS,
// and unpacks it with tar. The fake client serves both files; the fake runner
// stands in for tar and drops the executable where extraction would.
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

  it.effect.each([true, false])(
    "reports bytes before completion, then verifies and extracts (known size: %s)",
    (knownSize) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pinned-progress-" });
        const firstChunk = yield* Deferred.make<void>();
        let archiveController: ReadableStreamDefaultController<Uint8Array> | undefined;
        const checksums = yield* validChecksums;
        const progress: PinnedRuntimeProgress[] = [];
        const client = HttpClient.make((request) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              request.url.endsWith("/SHA256SUMS")
                ? new Response(checksums)
                : new Response(
                    new ReadableStream({
                      start(controller) {
                        archiveController = controller;
                        controller.enqueue(archiveBytes.slice(0, 4));
                      },
                    }),
                    { headers: knownSize ? { "content-length": String(archiveBytes.length) } : {} },
                  ),
            ),
          ),
        );
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
          validate: () => Effect.void,
          onProgress: (event) => {
            progress.push(event);
            if (event.stage === "download" && event.received === 4) {
              Deferred.doneUnsafe(firstChunk, Effect.void);
            }
          },
        }).pipe(Effect.forkScoped);
        yield* Deferred.await(firstChunk);
        assert.deepEqual(progress.at(-1), {
          stage: "download",
          received: 4,
          total: knownSize ? archiveBytes.length : undefined,
        });
        assert.isFalse(progress.some((event) => event.stage === "extract"));
        assert.isDefined(archiveController);
        archiveController!.enqueue(archiveBytes.slice(4));
        archiveController!.close();
        const installed = yield* Fiber.join(install);
        assert.deepEqual(progress.slice(-4), [
          { stage: "download", received: archiveBytes.length, total: archiveBytes.length },
          { stage: "verify" },
          { stage: "extract" },
          { stage: "validate" },
        ]);
        assert.equal(yield* fs.readFileString(installed.sentinelPath), `${version}\n`);
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

  it.effect("removes staging when installation is interrupted", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pinned-runtime-interrupt-" });
      const started = yield* Deferred.make<void>();
      const runner = ProcessRunner.ProcessRunner.of({
        run: () => Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
      });
      const install = yield* ensurePinnedRuntimeInstalled({
        baseDir,
        version,
        fs,
        path,
        platform: "linux",
        arch: "x64",
        releaseBaseUrl: "https://releases.example/download",
        httpClient: releaseHttpClient(yield* validChecksums),
        runner,
        validate: () => Effect.void,
      }).pipe(Effect.forkScoped);

      yield* Deferred.await(started);
      yield* Fiber.interrupt(install);
      const versionsDir = path.join(baseDir, "runtime", "versions");
      assert.deepEqual(yield* fs.readDirectory(versionsDir), []);
    }),
  );
});
