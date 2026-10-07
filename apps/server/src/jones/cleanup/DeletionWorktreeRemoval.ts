// @effect-diagnostics nodeBuiltinImport:off - Direct lstat distinguishes ENOENT from unreadable targets.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import type { GitCommandError } from "@t3tools/contracts";

import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";

import {
  DeletionWorktreeRemovalStartV1,
  type DeletionWorktreeRemovalTargetV1,
} from "./DeletionWorktreeRemovalTypes.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import type { ExecuteGitInput, ExecuteGitResult } from "../../vcs/GitVcsDriver.ts";

export interface DeletionWorktreeRegistrationEntryV1 {
  readonly path: string;
  readonly head: string | null;
  readonly branch: string | null;
  readonly bare: boolean;
}

export type DeletionWorktreeRegistrationV1 =
  | {
      readonly status: "complete";
      readonly projectRoot: string;
      readonly gitCommonDirectory: string;
      readonly entries: ReadonlyArray<DeletionWorktreeRegistrationEntryV1>;
    }
  | { readonly status: "unavailable"; readonly reason: string };

export type DeletionWorktreeFilesystemV1 =
  | { readonly status: "present" | "absent"; readonly path: string }
  | { readonly status: "unavailable"; readonly path: string; readonly reason: string };

export interface DeletionWorktreeReadbackV1 {
  readonly registration: DeletionWorktreeRegistrationV1;
  readonly filesystem: DeletionWorktreeFilesystemV1;
}

export interface DeletionWorktreeRemovalObservationV1 {
  readonly version: 1;
  readonly start: DeletionWorktreeRemovalStartV1;
  readonly startOrdinal: number;
  readonly operation:
    | {
        readonly kind: "executed";
        readonly exitCode: number | null;
        readonly completion: "exited" | "unknown";
      }
    | { readonly kind: "reconciled"; readonly completion: "unknown" }
    | { readonly kind: "already_absent"; readonly completion: "not_invoked" };
  readonly before: DeletionWorktreeReadbackV1;
  readonly after: DeletionWorktreeReadbackV1;
  readonly observedAt: string;
}

export class DeletionWorktreeRemovalPreconditionError extends Schema.TaggedError<DeletionWorktreeRemovalPreconditionError>()(
  "DeletionWorktreeRemovalPreconditionError",
  { reason: Schema.String, cause: Schema.optional(Schema.Defect()) },
) {}

export type DeletionWorktreeStartRevalidation = (
  start: DeletionWorktreeRemovalStartV1,
  startOrdinal: number,
) => Effect.Effect<void, EventSink.EventSinkV2Error | DeletionWorktreeRemovalPreconditionError>;

export interface DeletionWorktreeRemovalProducer {
  readonly inspectTarget: (
    target: DeletionWorktreeRemovalTargetV1,
  ) => Effect.Effect<DeletionWorktreeReadbackV1>;
  readonly executeStarted: (
    start: DeletionWorktreeRemovalStartV1,
    startOrdinal: number,
    revalidateStart: DeletionWorktreeStartRevalidation,
  ) => Effect.Effect<
    DeletionWorktreeRemovalObservationV1,
    DeletionWorktreeRemovalPreconditionError
  >;
  readonly observeStarted: (
    start: DeletionWorktreeRemovalStartV1,
    startOrdinal: number,
  ) => Effect.Effect<
    DeletionWorktreeRemovalObservationV1,
    DeletionWorktreeRemovalPreconditionError
  >;
}

const completeOutput = (result: ExecuteGitResult): boolean =>
  result.exitCode === 0 &&
  !result.stdoutTruncated &&
  !result.stderrTruncated &&
  result.stderr.length === 0;

function parseRegistration(
  stdout: string,
): ReadonlyArray<DeletionWorktreeRegistrationEntryV1> | null {
  if (!stdout.endsWith("\0\0")) return null;
  const entries: DeletionWorktreeRegistrationEntryV1[] = [];
  for (const record of stdout.slice(0, -2).split("\0\0")) {
    const fields = record.split("\0");
    const first = fields.shift();
    if (!first?.startsWith("worktree ")) return null;
    const path = first.slice("worktree ".length);
    if (!NodePath.isAbsolute(path) || entries.some((entry) => entry.path === path)) return null;
    let head: string | null = null;
    let branch: string | null = null;
    let bare = false;
    let detached = false;
    const seen = new Set<string>();
    for (const field of fields) {
      const key = field.split(" ", 1)[0]!;
      if (seen.has(key)) return null;
      seen.add(key);
      if (field.startsWith("HEAD ")) {
        head = field.slice(5);
        if (!/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/.test(head)) return null;
      } else if (field.startsWith("branch refs/heads/")) {
        branch = field.slice(7);
      } else if (field === "bare") bare = true;
      else if (field === "detached") detached = true;
      else if (
        field === "locked" ||
        field.startsWith("locked ") ||
        field === "prunable" ||
        field.startsWith("prunable ")
      ) {
        // These registration attributes do not change the target's identity.
      } else return null;
    }
    if (
      bare
        ? head !== null || branch !== null || detached
        : head === null || (detached ? branch !== null : branch === null)
    )
      return null;
    entries.push(Object.freeze({ path, head, branch, bare }));
  }
  return entries.length === 0 ? null : Object.freeze(entries);
}

const frozenStart = (start: DeletionWorktreeRemovalStartV1): DeletionWorktreeRemovalStartV1 =>
  Object.freeze({ ...start, target: Object.freeze({ ...start.target }) });

const assertOrdinal = (startOrdinal: number) =>
  Number.isSafeInteger(startOrdinal) && startOrdinal >= 0
    ? Effect.void
    : Effect.fail(
        new DeletionWorktreeRemovalPreconditionError({ reason: "invalid_start_ordinal" }),
      );

/** Reports observations only. Durable start, consent, reservation and outcome qualification belong to EventSink. */
const makeDeletionWorktreeRemoval = Effect.gen(function* () {
  const filesystem = yield* DeletionWorktreeFilesystem;
  const attemptedStarts = new Set<string>();
  const git = yield* DeletionWorktreeGit;
  const executeRead = (cwd: string, args: ReadonlyArray<string>, maxOutputBytes: number) =>
    git.execute({
      operation: "DeletionWorktreeRemoval.inspectTarget",
      cwd,
      args,
      timeoutMs: 5_000,
      maxOutputBytes,
      appendTruncationMarker: true,
      allowNonZeroExit: true,
    });

  const inspectRegistration = (
    target: DeletionWorktreeRemovalTargetV1,
  ): Effect.Effect<DeletionWorktreeRegistrationV1> =>
    Effect.gen(function* () {
      if (
        !NodePath.isAbsolute(target.projectRoot) ||
        !NodePath.isAbsolute(target.path) ||
        NodePath.resolve(target.projectRoot) !== target.projectRoot ||
        NodePath.resolve(target.path) !== target.path
      ) {
        return { status: "unavailable", reason: "target_paths_not_absolute" } as const;
      }
      const common = yield* executeRead(
        target.projectRoot,
        ["rev-parse", "--path-format=absolute", "--git-common-dir"],
        4_096,
      );
      if (!completeOutput(common))
        return { status: "unavailable", reason: "common_directory_read_incomplete" } as const;
      const gitCommonDirectory = common.stdout.endsWith("\n")
        ? common.stdout.slice(0, -1)
        : common.stdout;
      if (!NodePath.isAbsolute(gitCommonDirectory) || /[\0\r\n]/.test(gitCommonDirectory)) {
        return { status: "unavailable", reason: "common_directory_malformed" } as const;
      }
      const listed = yield* executeRead(
        target.projectRoot,
        ["worktree", "list", "--porcelain", "-z"],
        524_288,
      );
      if (!completeOutput(listed))
        return { status: "unavailable", reason: "registration_read_incomplete" } as const;
      const entries = parseRegistration(listed.stdout);
      return entries === null
        ? ({ status: "unavailable", reason: "registration_malformed" } as const)
        : ({
            status: "complete",
            projectRoot: target.projectRoot,
            gitCommonDirectory,
            entries,
          } as const);
    }).pipe(
      Effect.catch(() =>
        Effect.succeed({ status: "unavailable", reason: "registration_read_failed" } as const),
      ),
      Effect.map((registration) => Object.freeze(registration)),
    );

  const inspectFilesystem = (path: string): Effect.Effect<DeletionWorktreeFilesystemV1> =>
    filesystem.inspect(path).pipe(Effect.map((readback) => Object.freeze(readback)));

  const inspectTarget: DeletionWorktreeRemovalProducer["inspectTarget"] = (target) =>
    Effect.gen(function* () {
      const registration = yield* inspectRegistration(target);
      const filesystem = yield* inspectFilesystem(target.path);
      return Object.freeze({ registration, filesystem });
    });
  const observation = (
    start: DeletionWorktreeRemovalStartV1,
    startOrdinal: number,
    operation: DeletionWorktreeRemovalObservationV1["operation"],
    before: DeletionWorktreeReadbackV1,
    after: DeletionWorktreeReadbackV1,
  ) =>
    DateTime.now.pipe(
      Effect.map((now): DeletionWorktreeRemovalObservationV1 =>
        Object.freeze({
          version: 1,
          start,
          startOrdinal,
          operation: Object.freeze(operation),
          before,
          after,
          observedAt: DateTime.formatIso(now),
        }),
      ),
    );

  const executeStarted: DeletionWorktreeRemovalProducer["executeStarted"] = (
    original,
    startOrdinal,
    revalidateStart,
  ) =>
    Effect.gen(function* () {
      yield* assertOrdinal(startOrdinal);
      const start = frozenStart(
        yield* Schema.decodeUnknownEffect(DeletionWorktreeRemovalStartV1)(original, {
          onExcessProperty: "error",
        }).pipe(
          Effect.mapError(
            (cause) =>
              new DeletionWorktreeRemovalPreconditionError({
                reason: "invalid_original_start",
                cause,
              }),
          ),
        ),
      );
      const startedAt = DateTime.make(start.startedAt);
      if (Option.isNone(startedAt) || DateTime.formatIso(startedAt.value) !== start.startedAt) {
        return yield* new DeletionWorktreeRemovalPreconditionError({
          reason: "invalid_original_start_time",
        });
      }
      const target = start.target;
      if (attemptedStarts.has(start.effectId)) {
        return yield* new DeletionWorktreeRemovalPreconditionError({
          reason: "original_start_already_attempted",
        });
      }
      const before = yield* inspectTarget(target);
      if (before.registration.status !== "complete" || before.filesystem.status === "unavailable") {
        return yield* new DeletionWorktreeRemovalPreconditionError({
          reason: "target_readback_unavailable",
        });
      }
      const entry = before.registration.entries.find((entry) => entry.path === target.path);
      const alreadyAbsent = entry === undefined && before.filesystem.status === "absent";
      if (
        !alreadyAbsent &&
        (entry === undefined ||
          entry.bare ||
          target.path === target.projectRoot ||
          (entry.branch !== target.branch &&
            entry.branch !== (target.branch === null ? null : `refs/heads/${target.branch}`)))
      ) {
        return yield* new DeletionWorktreeRemovalPreconditionError({
          reason: "registered_target_mismatch",
        });
      }
      // There is exactly one removal invocation after the recorded start is revalidated.
      // Timeout, cancellation and recovery never replay an uncertain invocation.
      yield* revalidateStart(start, startOrdinal).pipe(
        Effect.mapError(
          (cause) =>
            new DeletionWorktreeRemovalPreconditionError({
              reason: "recorded_start_revalidation_failed",
              cause,
            }),
        ),
      );
      // Reserve before any removal effect; uncertainty leaves this invocation consumed.
      if (attemptedStarts.has(start.effectId)) {
        return yield* new DeletionWorktreeRemovalPreconditionError({
          reason: "original_start_already_attempted",
        });
      }
      attemptedStarts.add(start.effectId);
      if (alreadyAbsent)
        return yield* observation(
          start,
          startOrdinal,
          { kind: "already_absent", completion: "not_invoked" },
          before,
          yield* inspectTarget(target),
        );
      const operation = yield* git
        .execute({
          operation: "DeletionWorktreeRemoval.executeStarted",
          cwd: target.projectRoot,
          args: ["worktree", "remove", ...(target.force ? ["--force"] : []), "--", target.path],
          timeoutMs: 30_000,
          maxOutputBytes: 65_536,
          appendTruncationMarker: true,
          allowNonZeroExit: true,
        })
        .pipe(
          Effect.map((result): DeletionWorktreeRemovalObservationV1["operation"] => ({
            kind: "executed",
            exitCode: result.exitCode,
            completion: result.stdoutTruncated || result.stderrTruncated ? "unknown" : "exited",
          })),
          Effect.catch(() =>
            Effect.succeed({ kind: "executed", exitCode: null, completion: "unknown" } as const),
          ),
        );
      return yield* observation(
        start,
        startOrdinal,
        operation,
        before,
        yield* inspectTarget(target),
      );
    });
  const observeStarted: DeletionWorktreeRemovalProducer["observeStarted"] = (
    original,
    startOrdinal,
  ) =>
    Effect.gen(function* () {
      yield* assertOrdinal(startOrdinal);
      const start = frozenStart(
        yield* Schema.decodeUnknownEffect(DeletionWorktreeRemovalStartV1)(original, {
          onExcessProperty: "error",
        }).pipe(
          Effect.mapError(
            (cause) =>
              new DeletionWorktreeRemovalPreconditionError({
                reason: "invalid_original_start",
                cause,
              }),
          ),
        ),
      );
      const startedAt = DateTime.make(start.startedAt);
      if (Option.isNone(startedAt) || DateTime.formatIso(startedAt.value) !== start.startedAt) {
        return yield* new DeletionWorktreeRemovalPreconditionError({
          reason: "invalid_original_start_time",
        });
      }
      const before = yield* inspectTarget(start.target);
      return yield* observation(
        start,
        startOrdinal,
        { kind: "reconciled", completion: "unknown" },
        before,
        yield* inspectTarget(start.target),
      );
    });
  return {
    inspectTarget,
    executeStarted,
    observeStarted,
  } satisfies DeletionWorktreeRemovalProducer;
});

class DeletionWorktreeLstatError extends Schema.TaggedError<DeletionWorktreeLstatError>()(
  "DeletionWorktreeLstatError",
  {
    code: Schema.NullOr(Schema.String),
    cause: Schema.Defect(),
  },
) {}

export class DeletionWorktreeFilesystem extends Context.Reference<{
  readonly inspect: (path: string) => Effect.Effect<DeletionWorktreeFilesystemV1>;
}>("jones/cleanup/DeletionWorktreeFilesystem", {
  defaultValue: () => ({
    inspect: (path) =>
      Effect.tryPromise({
        try: () => NodeFSP.lstat(path),
        catch: (cause) =>
          new DeletionWorktreeLstatError({
            code:
              typeof cause === "object" &&
              cause !== null &&
              "code" in cause &&
              typeof cause.code === "string"
                ? cause.code
                : null,
            cause,
          }),
      }).pipe(
        Effect.match({
          onSuccess: () => ({ status: "present", path }) as const,
          onFailure: (cause) =>
            cause.code === "ENOENT"
              ? ({ status: "absent", path } as const)
              : ({ status: "unavailable", path, reason: "lstat_failed" } as const),
        }),
      ),
  }),
}) {}

export class DeletionWorktreeRemoval extends Context.Service<
  DeletionWorktreeRemoval,
  DeletionWorktreeRemovalProducer
>()("t3/jones/cleanup/DeletionWorktreeRemoval") {}
export class DeletionWorktreeGit extends Context.Service<
  DeletionWorktreeGit,
  {
    readonly execute: (input: ExecuteGitInput) => Effect.Effect<ExecuteGitResult, GitCommandError>;
  }
>()("t3/jones/cleanup/DeletionWorktreeRemoval/DeletionWorktreeGit") {}
export const producerLayer = Layer.effect(DeletionWorktreeRemoval, makeDeletionWorktreeRemoval);

export class QualifiedDeletionWorktreeRemoval extends Context.Service<
  QualifiedDeletionWorktreeRemoval,
  {
    readonly execute: (
      effectId: string,
    ) => Effect.Effect<
      DeletionWorktreeRemovalObservationV1,
      DeletionWorktreeRemovalPreconditionError
    >;
    readonly observe: (
      effectId: string,
    ) => Effect.Effect<
      DeletionWorktreeRemovalObservationV1,
      DeletionWorktreeRemovalPreconditionError
    >;
  }
>()("t3/jones/cleanup/DeletionWorktreeRemoval/QualifiedDeletionWorktreeRemoval") {}

// Only the durable EventSink owner can admit a start or qualify an observation.
// The receiving owner has no native deletion ledger yet, so absence denies before Git.
export const layer = Layer.effect(
  QualifiedDeletionWorktreeRemoval,
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const producer = yield* DeletionWorktreeRemoval;
    const run = (effectId: string, execute: boolean) =>
      Effect.gen(function* () {
        const read = sink.readDeletionWorktreeRemovalStart;
        const revalidate = sink.revalidateDeletionWorktreeRemovalStart;
        const qualify = sink.qualifyDeletionWorktreeRemovalObservation;
        if (read === undefined || revalidate === undefined || qualify === undefined) {
          return yield* new DeletionWorktreeRemovalPreconditionError({
            reason: "native_deletion_owner_unavailable",
          });
        }
        const admitted = yield* read(effectId);
        if (admitted === null || admitted.start.effectId !== effectId) {
          return yield* new DeletionWorktreeRemovalPreconditionError({
            reason: "original_removal_start_unavailable",
          });
        }
        const observed = execute
          ? yield* producer.executeStarted(admitted.start, admitted.ordinal, revalidate)
          : yield* producer.observeStarted(admitted.start, admitted.ordinal);
        yield* qualify(observed);
        return observed;
      }).pipe(
        Effect.mapError((cause) =>
          Schema.is(DeletionWorktreeRemovalPreconditionError)(cause)
            ? cause
            : new DeletionWorktreeRemovalPreconditionError({
                reason: "native_deletion_owner_refused",
                cause,
              }),
        ),
      );
    return QualifiedDeletionWorktreeRemoval.of({
      execute: (id) => run(id, true),
      observe: (id) => run(id, false),
    });
  }),
);
