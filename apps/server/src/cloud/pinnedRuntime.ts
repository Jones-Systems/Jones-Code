import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Encoding from "effect/Encoding";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as Semaphore from "effect/Semaphore";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";

import {
  CLI_RELEASE_CHECKSUMS_FILE,
  cliArchiveFileName,
  cliArchivePlatformKey,
  cliArchiveTarCommand,
  cliReleaseDownloadBaseUrl,
  parseChecksums,
} from "@t3tools/shared/cliRelease";

import * as ProcessRunner from "../processRunner.ts";
import * as JonesArtifact from "../jones/hostService/artifactVerification.ts";
import {
  JonesRuntimePolicyError,
  requireExplicitJonesReleaseBaseUrl,
} from "../jones/hostService/releasePolicy.ts";

/**
 * A pinned runtime is an exact t3 release archive unpacked into
 * <baseDir>/runtime/versions/<version>: the self-contained executable, the
 * web client, and the native packages beside it. The boot service points its
 * unit or launch agent at the executable, and server self-update installs the
 * target version here before switching over. The runtime never depends on a
 * Node or npm on the machine; the only npm involvement in T3 Code is the `t3`
 * package for people who prefer `npx t3` or `npm install -g t3`, and even a
 * CLI installed that way uses a staged runtime or an explicit release origin
 * when it sets up the service.
 */
const PINNED_RUNTIME_DIR = "runtime";
const PINNED_RUNTIME_INSTALL_TIMEOUT = Duration.minutes(10);
const PINNED_RUNTIME_ARCHIVE_FILE = "t3-runtime-archive";
const encodeJonesRuntimeProvenanceJson = Schema.encodeEffect(
  Schema.fromJsonString(JonesArtifact.JonesRuntimeProvenance),
);
// Boot-service setup and remote update can construct separate layers. Serialize
// the complete install transaction across every caller in this process.
const pinnedRuntimeInstallLock = Semaphore.makeUnsafe(1);

export interface PinnedRuntimePaths {
  readonly versionDir: string;
  /** The executable; provenance and the sentinel establish a completed install. */
  readonly entryPath: string;
  readonly sentinelPath: string;
}

/** The exact command that runs a pinned runtime. */
export function pinnedRuntimeCommand(paths: PinnedRuntimePaths): {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
} {
  return { command: paths.entryPath, args: [] };
}

export function pinnedRuntimeVersionsDir(path: Path.Path, baseDir: string): string {
  return path.join(baseDir, PINNED_RUNTIME_DIR, "versions");
}

export function pinnedRuntimePaths(
  path: Path.Path,
  baseDir: string,
  version: string,
  platform: NodeJS.Platform,
): PinnedRuntimePaths {
  const versionDir = path.join(pinnedRuntimeVersionsDir(path, baseDir), version);
  return {
    versionDir,
    entryPath: path.join(versionDir, platform === "win32" ? "t3.exe" : "t3"),
    sentinelPath: path.join(versionDir, ".install-complete"),
  };
}

export class PinnedRuntimeInstallError extends Schema.TaggedError<PinnedRuntimeInstallError>()(
  "PinnedRuntimeInstallError",
  {
    step: Schema.String,
    exitCode: Schema.optional(Schema.Number),
    stdoutLength: Schema.optional(Schema.Number),
    stderrLength: Schema.optional(Schema.Number),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.exitCode === undefined
      ? `Pinned runtime install failed while ${this.step}.`
      : `Pinned runtime install failed while ${this.step} (exit code ${this.exitCode}).`;
  }
}

export class PinnedRuntimePreflightBlockedError extends Schema.TaggedError<PinnedRuntimePreflightBlockedError>()(
  "PinnedRuntimePreflightBlockedError",
  {
    version: Schema.String,
    reason: Schema.String,
  },
) {
  override get message(): string {
    return this.reason;
  }
}

export type PinnedRuntimeProgress =
  | { readonly stage: "download"; readonly received: number; readonly total: number | undefined }
  | { readonly stage: "verify" | "extract" | "validate" | "cached" };

/**
 * Installs the t3 release archive for `version` into the pinned runtime
 * directory unless a complete verified Jones install is already there, and returns its
 * paths. The sentinel is written only after extraction and validation
 * succeed; checking the entry file alone is not enough, since tar writes the
 * executable before the last native package and a killed install leaves a
 * plausible-looking but broken tree behind.
 */

interface PinnedRuntimeInstallInput {
  readonly baseDir: string;
  readonly version: string;
  readonly fs: FileSystem.FileSystem;
  readonly path: Path.Path;
  readonly runner: ProcessRunner.ProcessRunner["Service"];
  readonly validate: (
    paths: PinnedRuntimePaths,
  ) => Effect.Effect<void, PinnedRuntimeInstallError | PinnedRuntimePreflightBlockedError>;
  readonly platform: NodeJS.Platform;
  readonly arch: string;
  readonly httpClient: HttpClient.HttpClient;
  readonly releaseBaseUrl?: string | undefined;
  readonly onProgress?: ((progress: PinnedRuntimeProgress) => void) | undefined;
}

const fetchReleaseAsset = Effect.fn("cloud.pinned_runtime.fetch_release_asset")(function* (
  httpClient: HttpClient.HttpClient,
  url: string,
  step: string,
  onProgress?: (progress: PinnedRuntimeProgress) => void,
) {
  // The install lock is held for the whole transaction, so a stalled download
  // must fail rather than block every other caller.
  return yield* httpClient.execute(HttpClientRequest.get(url)).pipe(
    Effect.flatMap(HttpClientResponse.filterStatusOk),
    Effect.flatMap(
      Effect.fn(function* (response) {
        if (onProgress === undefined) return new Uint8Array(yield* response.arrayBuffer);
        const length = Number(response.headers["content-length"]);
        const total = Number.isFinite(length) && length > 0 ? length : undefined;
        let received = 0;
        onProgress({ stage: "download", received, total });
        const chunks = yield* response.stream.pipe(
          Stream.tap((chunk) =>
            Effect.sync(() => {
              received += chunk.byteLength;
              onProgress({ stage: "download", received, total });
            }),
          ),
          Stream.runCollect,
        );
        // A completed chunked response finally gives us its total size.
        if (total === undefined && received > 0) {
          onProgress({ stage: "download", received, total: received });
        }
        const bytes = new Uint8Array(received);
        let offset = 0;
        for (const chunk of chunks) {
          bytes.set(chunk, offset);
          offset += chunk.byteLength;
        }
        return bytes;
      }),
    ),
    Effect.mapError((cause) => new PinnedRuntimeInstallError({ step, cause })),
    Effect.timeoutOrElse({
      duration: PINNED_RUNTIME_INSTALL_TIMEOUT,
      orElse: () => Effect.fail(new PinnedRuntimeInstallError({ step: `${step} (timed out)` })),
    }),
  );
});

/**
 * Downloads the release archive for this platform, verifies it against the
 * release's checksum file, and unpacks it so the executable sits directly in
 * the staging directory. Only `tar` is required on the host; every supported
 * OS ships one that reads gzip and zip.
 */
const installFromArchive = Effect.fn("cloud.pinned_runtime.install_archive")(function* (
  input: PinnedRuntimeInstallInput,
  stagingDir: string,
) {
  const platformKey = cliArchivePlatformKey(input.platform, input.arch);
  if (platformKey === undefined) {
    return yield* new PinnedRuntimeInstallError({
      step: `selecting a t3 release archive for ${input.platform}-${input.arch}`,
    });
  }
  const httpClient = input.httpClient;
  const explicitBaseUrl = yield* requireExplicitJonesReleaseBaseUrl(input.releaseBaseUrl);
  const baseUrl = cliReleaseDownloadBaseUrl(input.version, explicitBaseUrl);
  const fileName = cliArchiveFileName(input.version, platformKey);

  input.onProgress?.({ stage: "download", received: 0, total: undefined });
  const checksums = parseChecksums(
    new TextDecoder().decode(
      yield* fetchReleaseAsset(
        httpClient,
        `${baseUrl}/${CLI_RELEASE_CHECKSUMS_FILE}`,
        "downloading the t3 release checksums",
      ),
    ),
  );
  const expected = checksums.get(fileName);
  if (expected === undefined) {
    return yield* new PinnedRuntimeInstallError({
      step: `finding ${fileName} in the t3 release checksums`,
    });
  }
  const archive = yield* fetchReleaseAsset(
    httpClient,
    `${baseUrl}/${fileName}`,
    "downloading the t3 release archive",
    input.onProgress,
  );
  input.onProgress?.({ stage: "verify" });
  const digest = yield* Effect.tryPromise({
    try: () => crypto.subtle.digest("SHA-256", archive),
    catch: (cause) =>
      new PinnedRuntimeInstallError({ step: "verifying the t3 release archive", cause }),
  });
  if (Encoding.encodeHex(new Uint8Array(digest)) !== expected) {
    return yield* new PinnedRuntimeInstallError({
      step: "verifying the t3 release archive checksum",
    });
  }

  yield* extractArchive(input, stagingDir, archive);
});

interface PinnedRuntimeTransactionInput extends Omit<
  PinnedRuntimeInstallInput,
  "httpClient" | "releaseBaseUrl"
> {
  readonly installArchive: (
    stagingDir: string,
  ) => Effect.Effect<void, PinnedRuntimeInstallError | JonesRuntimePolicyError>;
  readonly requireInstall: Effect.Effect<void, JonesRuntimePolicyError>;
  readonly expectedProvenance?: JonesArtifact.JonesArtifactMetadata | undefined;
}

const extractArchive = Effect.fn("cloud.pinned_runtime.extract_archive")(function* (
  input: Pick<PinnedRuntimeInstallInput, "fs" | "path" | "runner" | "platform" | "onProgress">,
  stagingDir: string,
  archive: Uint8Array,
) {
  const { fs, path } = input;
  const archivePath = path.join(stagingDir, PINNED_RUNTIME_ARCHIVE_FILE);
  yield* fs
    .writeFile(archivePath, archive)
    .pipe(
      Effect.mapError(
        (cause) => new PinnedRuntimeInstallError({ step: "writing the t3 release archive", cause }),
      ),
    );
  input.onProgress?.({ stage: "extract" });
  const extractStep = "extracting the t3 release archive";
  // The archive wraps everything in one directory named after its stem;
  // strip it so the executable lands at <versionDir>/t3.
  yield* input.runner
    .run({
      command: cliArchiveTarCommand(input.platform, process.env),
      args: ["-xf", archivePath, "-C", stagingDir, "--strip-components=1"],
      timeout: PINNED_RUNTIME_INSTALL_TIMEOUT,
    })
    .pipe(
      Effect.mapError((cause) => new PinnedRuntimeInstallError({ step: extractStep, cause })),
      Effect.filterOrFail(
        (result) => result.code === 0,
        (result) =>
          new PinnedRuntimeInstallError({
            step: extractStep,
            exitCode: Number(result.code),
            stdoutLength: result.stdout.length,
            stderrLength: result.stderr.length,
          }),
      ),
    );
  yield* fs
    .remove(archivePath, { force: true })
    .pipe(
      Effect.mapError(
        (cause) =>
          new PinnedRuntimeInstallError({ step: "removing the staged runtime archive", cause }),
      ),
    );
});

const installPinnedRuntime = Effect.fn("cloud.pinned_runtime.ensure_installed")(function* (
  input: PinnedRuntimeTransactionInput,
) {
  const { fs } = input;
  const paths = pinnedRuntimePaths(input.path, input.baseDir, input.version, input.platform);
  const [versionDirExists, entryExists, sentinel] = yield* Effect.all([
    fs.exists(paths.versionDir),
    fs.exists(paths.entryPath),
    fs.readFileString(paths.sentinelPath).pipe(Effect.option),
  ]).pipe(
    Effect.mapError(
      (cause) => new PinnedRuntimeInstallError({ step: "checking the pinned runtime", cause }),
    ),
  );
  const alreadyPinned =
    entryExists && Option.isSome(sentinel) && sentinel.value.trim() === input.version;
  if (versionDirExists) {
    yield* verifyPinnedRuntimeProvenance({
      ...input,
      paths,
      expectedProvenance: input.expectedProvenance,
    });
  }
  if (alreadyPinned) {
    input.onProgress?.({ stage: "cached" });
    yield* input.validate(paths);
    return paths;
  }
  yield* input.requireInstall;
  if (versionDirExists) {
    yield* fs.remove(paths.versionDir, { recursive: true, force: true }).pipe(
      Effect.mapError(
        (cause) =>
          new PinnedRuntimeInstallError({
            step: "removing an incomplete pinned runtime",
            cause,
          }),
      ),
    );
  }

  const versionsDir = input.path.dirname(paths.versionDir);
  yield* fs.makeDirectory(versionsDir, { recursive: true }).pipe(
    Effect.mapError(
      (cause) =>
        new PinnedRuntimeInstallError({
          step: "preparing the pinned runtime directory",
          cause,
        }),
    ),
  );
  const stagingDir = yield* fs
    .makeTempDirectory({
      directory: versionsDir,
      prefix: ".staging-",
    })
    .pipe(
      Effect.mapError(
        (cause) =>
          new PinnedRuntimeInstallError({
            step: "preparing the pinned runtime directory",
            cause,
          }),
      ),
    );
  const stagingPaths: PinnedRuntimePaths = {
    versionDir: stagingDir,
    entryPath: input.path.join(stagingDir, input.path.relative(paths.versionDir, paths.entryPath)),
    sentinelPath: input.path.join(stagingDir, ".install-complete"),
  };

  return yield* Effect.gen(function* () {
    yield* input.installArchive(stagingDir);

    input.onProgress?.({ stage: "validate" });
    yield* input.validate(stagingPaths);
    if (input.expectedProvenance !== undefined) {
      const entrySha256 = yield* fs.readFile(stagingPaths.entryPath).pipe(
        Effect.mapError(
          (cause) =>
            new PinnedRuntimeInstallError({ step: "reading the staged Jones executable", cause }),
        ),
        Effect.flatMap((bytes) => runtimeSha256(bytes, "hashing the staged Jones executable")),
      );
      const provenance = {
        ...input.expectedProvenance,
        repository: "Jones-Systems/Jones-Code" as const,
        entrySha256,
      };
      const provenanceJson = yield* encodeJonesRuntimeProvenanceJson(provenance).pipe(
        Effect.mapError(
          (cause) =>
            new PinnedRuntimeInstallError({ step: "encoding Jones runtime provenance", cause }),
        ),
      );
      yield* fs
        .writeFileString(
          input.path.join(stagingDir, JonesArtifact.JONES_RUNTIME_PROVENANCE_FILE),
          provenanceJson,
        )
        .pipe(
          Effect.mapError(
            (cause) =>
              new PinnedRuntimeInstallError({ step: "recording Jones runtime provenance", cause }),
          ),
        );
    }
    yield* fs
      .writeFileString(stagingPaths.sentinelPath, `${input.version}\n`)
      .pipe(
        Effect.mapError(
          (cause) =>
            new PinnedRuntimeInstallError({ step: "recording the completed install", cause }),
        ),
      );
    if (
      yield* fs.exists(paths.versionDir).pipe(
        Effect.mapError(
          (cause) =>
            new PinnedRuntimeInstallError({
              step: "checking the runtime publication target",
              cause,
            }),
        ),
      )
    ) {
      yield* verifyPinnedRuntimeProvenance({ ...input, paths });
      const publishedSentinel = yield* fs.readFileString(paths.sentinelPath).pipe(
        Effect.mapError(
          (cause) =>
            new PinnedRuntimeInstallError({
              step: "checking a concurrently published pinned runtime",
              cause,
            }),
        ),
      );
      if (publishedSentinel.trim() !== input.version) {
        return yield* new PinnedRuntimeInstallError({
          step: "checking a concurrently published pinned runtime",
        });
      }
      yield* input.validate(paths);
      return paths;
    }
    const published = yield* fs.rename(stagingDir, paths.versionDir).pipe(
      Effect.as(true),
      Effect.catch((cause) =>
        Effect.all([
          fs.exists(paths.entryPath),
          fs.readFileString(paths.sentinelPath).pipe(Effect.option),
        ]).pipe(
          Effect.mapError(
            (checkCause) =>
              new PinnedRuntimeInstallError({
                step: "checking a concurrently published pinned runtime",
                cause: checkCause,
              }),
          ),
          Effect.flatMap(([publishedEntryExists, publishedSentinel]) =>
            publishedEntryExists &&
            Option.isSome(publishedSentinel) &&
            publishedSentinel.value.trim() === input.version
              ? Effect.succeed(false)
              : Effect.fail(
                  new PinnedRuntimeInstallError({
                    step: "publishing the pinned runtime",
                    cause,
                  }),
                ),
          ),
        ),
      ),
    );
    if (!published) {
      yield* verifyPinnedRuntimeProvenance({ ...input, paths });
      yield* input.validate(paths);
    }
    return paths;
  }).pipe(
    Effect.onExit(() =>
      fs.remove(stagingDir, { recursive: true, force: true }).pipe(
        Effect.mapError(
          (cause) =>
            new PinnedRuntimeInstallError({
              step: "cleaning the pinned runtime staging directory",
              cause,
            }),
        ),
      ),
    ),
  );
});

const runtimeSha256 = (bytes: Uint8Array, step: string) =>
  Effect.tryPromise({
    try: () => crypto.subtle.digest("SHA-256", bytes),
    catch: (cause) => new PinnedRuntimeInstallError({ step, cause }),
  }).pipe(Effect.map((digest) => Encoding.encodeHex(new Uint8Array(digest))));

export const verifyPinnedRuntimeProvenance = Effect.fn(
  "cloud.pinned_runtime.verify_jones_provenance",
)(function* (
  input: Pick<PinnedRuntimeInstallInput, "fs" | "path" | "version" | "platform" | "arch"> & {
    readonly paths: PinnedRuntimePaths;
    readonly expectedProvenance?: JonesArtifact.JonesArtifactMetadata | undefined;
  },
) {
  const [provenanceJson, entry] = yield* Effect.all([
    input.fs.readFileString(
      input.path.join(input.paths.versionDir, JonesArtifact.JONES_RUNTIME_PROVENANCE_FILE),
    ),
    input.fs.readFile(input.paths.entryPath),
  ]).pipe(
    Effect.mapError(
      (cause) =>
        new JonesRuntimePolicyError({
          reason: "The existing runtime has missing or unreadable Jones provenance or executable.",
          cause,
        }),
    ),
  );
  const entrySha256 = yield* runtimeSha256(entry, "hashing the existing Jones executable");
  return yield* JonesArtifact.verifyJonesRuntimeProvenance({
    ...input,
    provenanceJson,
    entrySha256,
  });
});

export const ensurePinnedRuntimeInstalled = (input: PinnedRuntimeInstallInput) =>
  input.version.includes("-preview.")
    ? Effect.fail(
        new PinnedRuntimeInstallError({
          step: "requiring a qualified Jones Actions artifact for preview runtime staging",
        }),
      )
    : pinnedRuntimeInstallLock.withPermit(
        installPinnedRuntime({
          ...input,
          requireInstall: requireExplicitJonesReleaseBaseUrl(input.releaseBaseUrl).pipe(
            Effect.asVoid,
          ),
          installArchive: (stagingDir) => installFromArchive(input, stagingDir),
        }),
      );

interface LocalJonesArtifactInput {
  readonly artifactDir: string;
  readonly expectSourceCommit: string;
  readonly fs: FileSystem.FileSystem;
  readonly path: Path.Path;
  readonly platform: NodeJS.Platform;
  readonly arch: string;
}

export const readLocalJonesArtifact = Effect.fn("cloud.pinned_runtime.read_local_jones_artifact")(
  function* (input: LocalJonesArtifactInput) {
    const fail = (reason: string, cause?: unknown) =>
      new JonesArtifact.JonesArtifactVerificationError({ reason, cause });
    const [metadataJson, sourceCommit, checksums] = yield* Effect.all([
      input.fs.readFileString(input.path.join(input.artifactDir, "ARTIFACT.json")),
      input.fs.readFileString(input.path.join(input.artifactDir, "SOURCE_COMMIT")),
      input.fs.readFileString(input.path.join(input.artifactDir, CLI_RELEASE_CHECKSUMS_FILE)),
    ]).pipe(Effect.mapError((cause) => fail("reading local artifact metadata", cause)));
    const metadata = yield* JonesArtifact.decodeJonesArtifactMetadata(metadataJson);
    const platformKey = cliArchivePlatformKey(input.platform, input.arch);
    if (
      platformKey === undefined ||
      metadata.artifact !== cliArchiveFileName(metadata.version, platformKey)
    ) {
      return yield* fail("artifact filename does not match its version and host platform");
    }
    const archive = yield* input.fs
      .readFile(input.path.join(input.artifactDir, metadata.artifact))
      .pipe(Effect.mapError((cause) => fail("reading the local Jones archive", cause)));
    const archiveSha256 = yield* runtimeSha256(archive, "hashing the local Jones archive").pipe(
      Effect.mapError((cause) => fail("hashing the local Jones archive", cause)),
    );
    const verified = yield* JonesArtifact.verifyJonesArtifact({
      ...input,
      metadataJson,
      sourceCommit,
      checksums,
      archiveSha256,
    });
    return { metadata: verified, archive };
  },
);

export const installPinnedRuntimeFromLocalArchive = Effect.fn(
  "cloud.pinned_runtime.install_local_jones_artifact",
)(function* (
  input: LocalJonesArtifactInput & {
    readonly baseDir: string;
    readonly runner: ProcessRunner.ProcessRunner["Service"];
    readonly validate: PinnedRuntimeInstallInput["validate"];
    readonly onProgress?: PinnedRuntimeInstallInput["onProgress"];
  },
) {
  const { metadata, archive } = yield* readLocalJonesArtifact(input);
  return yield* pinnedRuntimeInstallLock.withPermit(
    installPinnedRuntime({
      ...input,
      version: metadata.version,
      expectedProvenance: metadata,
      requireInstall: Effect.void,
      installArchive: (stagingDir) => extractArchive(input, stagingDir, archive),
      validate: (paths) =>
        Effect.gen(function* () {
          const result = yield* input.runner
            .run({
              ...pinnedRuntimeCommand(paths),
              args: ["--version"],
              timeout: PINNED_RUNTIME_INSTALL_TIMEOUT,
            })
            .pipe(
              Effect.mapError(
                (cause) =>
                  new PinnedRuntimeInstallError({
                    step: "checking the Jones artifact version",
                    cause,
                  }),
              ),
            );
          if (
            result.code !== 0 ||
            result.timedOut ||
            result.stdoutTruncated ||
            result.stdoutInvalidUtf8 ||
            /\bv(\S+)\s*$/.exec(result.stdout)?.[1] !== metadata.version
          ) {
            return yield* new PinnedRuntimeInstallError({
              step: "checking the Jones artifact version",
              exitCode: Number(result.code),
              stdoutLength: result.stdout.length,
              stderrLength: result.stderr.length,
            });
          }
          yield* input.validate(paths);
        }),
    }),
  );
});
