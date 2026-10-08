import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import type * as FileSystem from "effect/FileSystem";
import type * as Path from "effect/Path";

import { pinnedRuntimePaths, verifyPinnedRuntimeProvenance } from "../../cloud/pinnedRuntime.ts";
import { parseServiceState, SERVICE_STATE_FILE } from "../../cloud/serviceProtocol.ts";
import type * as ProcessRunner from "../../processRunner.ts";
import { QUALIFIED_RUNTIME_RECEIPT } from "../cloud/qualifiedRuntime.ts";
import { JonesRuntimePolicyError } from "./releasePolicy.ts";

interface PrivateRuntimeInput {
  readonly baseDir: string;
  readonly version: string;
  readonly activeVersion?: string | undefined;
  readonly fs: FileSystem.FileSystem;
  readonly path: Path.Path;
  readonly platform: NodeJS.Platform;
  readonly arch: string;
}

export const assertPrivateServiceRuntimeOwnership = Effect.fn(
  "jones.host_service.assert_private_runtime_ownership",
)(function* (input: PrivateRuntimeInput) {
  const fail = (reason: string, cause?: unknown) => new JonesRuntimePolicyError({ reason, cause });
  const statePath = input.path.join(input.baseDir, "runtime", SERVICE_STATE_FILE);
  let activeVersion: string | undefined;
  if (
    yield* input.fs
      .exists(statePath)
      .pipe(
        Effect.mapError((cause) => fail("Could not establish private runtime ownership.", cause)),
      )
  ) {
    const state = parseServiceState(
      yield* input.fs
        .readFileString(statePath)
        .pipe(
          Effect.mapError((cause) =>
            fail("Could not read the existing private service state.", cause),
          ),
        ),
    );
    if (state === undefined)
      return yield* fail(
        "The existing private service state requires reconciliation before setup.",
      );
    if (state.update?.status === "pending")
      return yield* fail(
        "An existing service update is pending; reconcile it before private artifact setup or staging.",
      );
    activeVersion = state.activeVersion;
  }
  const protectedPaths = [
    input.path.join(input.baseDir, "runtime", "jones-active-install.json"),
    ...Array.from(
      new Set(
        [input.version, input.activeVersion, activeVersion].filter(
          (version): version is string => version !== undefined,
        ),
      ),
    ).map((version) =>
      input.path.join(
        pinnedRuntimePaths(input.path, input.baseDir, version, input.platform).versionDir,
        QUALIFIED_RUNTIME_RECEIPT,
      ),
    ),
  ];
  for (const protectedPath of protectedPaths) {
    if (
      yield* input.fs
        .exists(protectedPath)
        .pipe(
          Effect.mapError(
            (cause) =>
              new JonesRuntimePolicyError({
                reason: "Could not establish private runtime ownership.",
                cause,
              }),
          ),
        )
    )
      return yield* new JonesRuntimePolicyError({
        reason:
          "This home requires its qualified Actions or desktop activation flow; private artifact setup is unavailable.",
      });
  }
  return activeVersion;
});

export const verifyPrivateServiceRuntimeCache = Effect.fn(
  "jones.host_service.verify_private_runtime_cache",
)(function* (
  input: PrivateRuntimeInput & {
    readonly runner: ProcessRunner.ProcessRunner["Service"];
  },
) {
  const activeVersion = yield* assertPrivateServiceRuntimeOwnership(input);
  const versions = Array.from(
    new Set(
      [input.activeVersion, activeVersion, input.version].filter(
        (version): version is string => version !== undefined,
      ),
    ),
  );
  for (const version of versions) {
    const paths = pinnedRuntimePaths(input.path, input.baseDir, version, input.platform);
    yield* verifyPinnedRuntimeProvenance({ ...input, version, paths });
    const sentinel = yield* input.fs
      .readFileString(paths.sentinelPath)
      .pipe(
        Effect.mapError(
          (cause) =>
            new JonesRuntimePolicyError({
              reason: "The verified private runtime cache is incomplete.",
              cause,
            }),
        ),
      );
    if (sentinel.trim() !== version)
      return yield* new JonesRuntimePolicyError({
        reason: "The private runtime cache completion marker does not match its version.",
      });
    const result = yield* input.runner
      .run({
        command: paths.entryPath,
        args: ["--version"],
        timeout: Duration.seconds(30),
      })
      .pipe(
        Effect.mapError(
          (cause) =>
            new JonesRuntimePolicyError({
              reason: "Could not execute the verified private runtime cache.",
              cause,
            }),
        ),
      );
    if (
      result.code !== 0 ||
      result.timedOut ||
      result.stdoutTruncated ||
      result.stdoutInvalidUtf8 ||
      /\bv(\S+)\s*$/.exec(result.stdout)?.[1] !== version
    )
      return yield* new JonesRuntimePolicyError({
        reason: "The verified private runtime cache did not report its exact version.",
      });
  }
  return pinnedRuntimePaths(input.path, input.baseDir, input.version, input.platform);
});
