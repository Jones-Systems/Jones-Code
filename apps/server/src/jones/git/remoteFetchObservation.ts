import { GitCommandError, type GitCommandFailureReason } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Digest from "effect/crypto/Digest";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Semaphore from "effect/Semaphore";
import type { GitVcsDriver } from "../../vcs/GitVcsDriver.ts";
import { gitConfigEnv } from "../../vcs/VcsProcess.ts";

export interface RemoteFetchTarget {
  readonly remoteName: string;
  readonly coverage:
    | { readonly kind: "configured-refspecs" }
    | { readonly kind: "branch"; readonly branchName: string };
}

type FetchResult =
  | { readonly status: "succeeded" }
  | { readonly status: "failed"; readonly reason?: GitCommandFailureReason };

export interface RemoteFetchObservation {
  readonly latestVerifiedAtMs: number | null;
  readonly latestOutcome: (FetchResult & { readonly completedAtMs: number }) | null;
}

const unknown: RemoteFetchObservation = {
  latestVerifiedAtMs: null,
  latestOutcome: null,
};

const observationKey = (identity: string, { remoteName, coverage }: RemoteFetchTarget) =>
  JSON.stringify([
    identity,
    remoteName,
    coverage.kind,
    "branchName" in coverage ? coverage.branchName : null,
  ]);

/** Driver-lifetime evidence. A fresh server starts unknown, even if tracking refs already exist. */
export function makeRemoteFetchObservation(capacity = 2048) {
  let sequence = 0;
  const entries = new Map<
    string,
    {
      latest: RemoteFetchObservation;
      outcomeSequence: number;
      successSequence: number;
    }
  >();

  // Called within the existing fetch spans. The sequence orders overlapping attempts independently
  // of wall-clock time, including an older success that finishes after a newer failure.
  const observe = Effect.fnUntraced(function* <A, R>(
    identity: string | null,
    target: RemoteFetchTarget,
    run: Effect.Effect<A, GitCommandError, R>,
    classify: (value: A) => FetchResult,
  ) {
    if (identity === null || target.remoteName === "." || target.remoteName === "") {
      return yield* run;
    }
    const key = observationKey(identity, target);
    const entry = entries.get(key) ?? { latest: unknown, outcomeSequence: 0, successSequence: 0 };
    entries.delete(key);
    entries.set(key, entry);
    const oldest = entries.keys().next().value;
    if (entries.size > capacity && oldest !== undefined) entries.delete(oldest);
    const attempt = ++sequence;
    const record = (result: FetchResult) =>
      Clock.currentTimeMillis.pipe(
        Effect.map((completedAtMs) => {
          // An evicted in-flight attempt cannot recreate evidence or replace its newer entry.
          if (entries.get(key) !== entry) return;
          const { outcomeSequence, successSequence, latest } = entry;
          const isLatestOutcome = attempt > outcomeSequence;
          const isLatestSuccess = result.status === "succeeded" && attempt > successSequence;
          if (!isLatestOutcome && !isLatestSuccess) return;
          entry.latest = {
            latestVerifiedAtMs: isLatestSuccess ? completedAtMs : latest.latestVerifiedAtMs,
            latestOutcome: isLatestOutcome ? { ...result, completedAtMs } : latest.latestOutcome,
          };
          if (isLatestOutcome) entry.outcomeSequence = attempt;
          if (isLatestSuccess) entry.successSequence = attempt;
        }),
      );

    return yield* run.pipe(
      Effect.tap((value) => record(classify(value))),
      Effect.tapError((error) =>
        record(error.reason ? { status: "failed", reason: error.reason } : { status: "failed" }),
      ),
    );
  });

  return {
    observe,
    read: (gitCommonDir: string | null, target: RemoteFetchTarget): RemoteFetchObservation =>
      (gitCommonDir === null
        ? undefined
        : entries.get(observationKey(gitCommonDir, target))?.latest) ?? unknown,
  };
}

/** The only commands that see the URL overlay run through this closure. */
export function makeRemoteFetchBinding(input: {
  readonly execute: GitVcsDriver["Service"]["execute"];
  readonly fs: FileSystem.FileSystem;
  readonly hostEnv: Effect.Effect<NodeJS.ProcessEnv>;
  readonly platform: NodeJS.Platform;
  readonly remoteEnv: NodeJS.ProcessEnv;
  readonly digest: Digest.Digest["Service"];
  readonly random: SecureRandom.SecureRandom["Service"];
}) {
  const { execute, fs, hostEnv, platform, remoteEnv, digest, random } = input;
  const bind = Effect.fn("GitVcsDriver.bindRemoteFetch")(
    function* (cwd: string, remoteName: string, commonDir?: string) {
      const physicalCwd = yield* fs.realPath(cwd);
      const read = (args: readonly string[], env?: NodeJS.ProcessEnv) =>
        execute({
          operation: "GitVcsDriver.bindRemoteFetch",
          cwd: physicalCwd,
          args,
          ...(env !== undefined ? { env } : {}),
          timeoutMs: 5_000,
          allowNonZeroExit: true,
        });
      const paths = yield* read([
        ...(commonDir ? ["--git-dir", commonDir] : []),
        "rev-parse",
        "--path-format=absolute",
        "--git-dir",
        "--git-common-dir",
      ]);
      const lines = paths.stdout.slice(0, -1).split("\n");
      if (paths.exitCode !== 0 || paths.stdoutTruncated || lines.length !== 2) return null;
      const [gitDir, gitCommonDir] = yield* Effect.forEach(lines, (line) => fs.realPath(line));
      if (gitDir === undefined || gitCommonDir === undefined) return null;
      const directoryId = (location: string) =>
        fs.stat(location).pipe(
          Effect.map((info) =>
            Option.map(info.ino, (ino) => `${info.dev}/${ino}`).pipe(Option.getOrNull),
          ),
          Effect.orElseSucceed(() => null),
        );
      const commonId = yield* directoryId(gitCommonDir);
      const adminId = gitDir === gitCommonDir ? commonId : yield* directoryId(gitDir);
      if (commonId === null || adminId === null) return null;
      const selectors = { GIT_DIR: gitDir, GIT_COMMON_DIR: gitCommonDir };
      const urlsResult = yield* read(["remote", "get-url", "--all", "--", remoteName], selectors);
      if (urlsResult.exitCode !== 0 || urlsResult.stdoutTruncated) return null;
      const urls = urlsResult.stdout.slice(0, -1).split("\n");
      if (urls.some((url) => !url || /[\r\0]/.test(url))) return null;
      const refspecResult = yield* read(
        ["config", "--null", "--get-all", `remote.${remoteName}.fetch`],
        selectors,
      );
      if (
        (refspecResult.exitCode !== 0 && refspecResult.exitCode !== 1) ||
        refspecResult.stdoutTruncated
      )
        return null;
      if (refspecResult.exitCode === 0 && !refspecResult.stdout.endsWith("\0")) return null;
      const refspecs =
        refspecResult.exitCode === 0 ? refspecResult.stdout.slice(0, -1).split("\0") : [];
      const hash = (value: unknown) =>
        digest
          .digest(Digest.sha256, new TextEncoder().encode(JSON.stringify(value)))
          .pipe(Effect.map(Hex.encode));
      const relativeTransport = urls.some(
        (url) => !url.startsWith("/") && !url.startsWith("\\") && !url.includes(":"),
      );
      const identity = yield* hash([
        gitCommonDir,
        commonId,
        remoteName,
        urls,
        relativeTransport ? physicalCwd : null,
      ]);
      const context = yield* hostEnv;
      let overlay: NodeJS.ProcessEnv = {};
      const set = (key: string, value: string) => {
        overlay = { ...overlay, ...gitConfigEnv({ ...context, ...overlay }, key, value, platform) };
      };
      set(`remote.${remoteName}.url`, "");
      for (const url of urls) {
        // A unique source forces exactly one rewrite to the captured effective URL.
        const transport = `jones-fetch-${yield* random.uuidV4()}`;
        set(`url.${url}.insteadOf`, transport);
        set(`remote.${remoteName}.url`, transport);
      }
      const pinned = yield* read(["remote", "get-url", "--all", "--", remoteName], {
        ...selectors,
        ...overlay,
      });
      if (pinned.exitCode !== 0 || pinned.stdout !== urlsResult.stdout) return null;
      const configuredIdentity = yield* hash([identity, refspecs]);
      const assertDirectories = Effect.gen(function* () {
        const common = yield* directoryId(gitCommonDir);
        const admin = gitDir === gitCommonDir ? common : yield* directoryId(gitDir);
        if (common !== commonId || admin !== adminId) return yield* invalidRemoteFetch(physicalCwd);
      });
      return {
        cwd: physicalCwd,
        gitDir,
        gitCommonDir,
        remoteName,
        identity,
        configuredIdentity,
        refspecs,
        assertDirectories,
        run: (
          command: Omit<Parameters<typeof execute>[0], "cwd" | "env">,
          objects?: {
            readonly GIT_OBJECT_DIRECTORY: string;
            readonly GIT_ALTERNATE_OBJECT_DIRECTORIES: string;
          },
        ) =>
          assertDirectories.pipe(
            Effect.andThen(
              execute({
                ...command,
                cwd: physicalCwd,
                env: {
                  ...remoteEnv,
                  LC_ALL: "C",
                  ...selectors,
                  ...overlay,
                  ...(objects === undefined
                    ? {}
                    : {
                        GIT_OBJECT_DIRECTORY: objects.GIT_OBJECT_DIRECTORY,
                        GIT_ALTERNATE_OBJECT_DIRECTORIES: objects.GIT_ALTERNATE_OBJECT_DIRECTORIES,
                      }),
                },
              }),
            ),
            Effect.tap(() => assertDirectories),
          ),
        read: (command: Omit<Parameters<typeof execute>[0], "cwd" | "env">) =>
          assertDirectories.pipe(
            Effect.andThen(
              execute({
                ...command,
                cwd: physicalCwd,
                env: { ...selectors, GIT_NO_LAZY_FETCH: "1", GIT_ALLOW_PROTOCOL: "" },
              }),
            ),
          ),
      };
    },
    Effect.orElseSucceed(() => null),
  );
  return bind;
}

/** Share one permit per active Git write target and release idle entries. */
export function makeKeyedGitWriters() {
  const writers = new Map<string, { readonly lock: Semaphore.Semaphore; users: number }>();
  return <A, E, R>(key: string, effect: Effect.Effect<A, E, R>) =>
    Effect.acquireUseRelease(
      Effect.sync(() => {
        const entry = writers.get(key) ?? { lock: Semaphore.makeUnsafe(1), users: 0 };
        entry.users++;
        writers.set(key, entry);
        return entry;
      }),
      (entry) => entry.lock.withPermit(effect),
      (entry) =>
        Effect.sync(() => {
          if (--entry.users === 0) writers.delete(key);
        }),
    );
}

export const remoteFetchWriterKey = (gitCommonDir: string, remoteName: string) =>
  `${gitCommonDir}\0${remoteName}`;

export const invalidRemoteFetch = (cwd: string) =>
  new GitCommandError({
    operation: "GitVcsDriver.boundRemoteFetch",
    command: "git",
    cwd,
    detail: "The Git remote or checkout changed during the check.",
  });
