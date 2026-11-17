// @effect-diagnostics nodeBuiltinImport:off -- C4 readback observes both Git registration and the filesystem after unforced removal.
import { RESERVATION_DDL } from "./ParticipantGuard.ts";
import * as NodeSqlite from "node:sqlite";
import * as NodeFSP from "node:fs/promises";
import * as NodeCrypto from "node:crypto";
import * as Effect from "effect/Effect";
import * as NodePath from "node:path";
import { canonical, exists, openAbsolute, git } from "./Filesystem.ts";
import { binding, incarnation } from "./OutputAttestation.ts";
import { finalCheck, qualify, type QualificationInput } from "./Qualification.ts";
import {
  Receipts,
  RetirementError,
  RetirementReservationOwner,
  type InterruptedRetirement,
  type RetirementReceipt,
} from "./Receipts.ts";
import {
  acquireReservation,
  holderAbsent,
  holderIdentity,
  readReservation,
  releaseReservation,
  type ReservationRecord,
} from "./Reservation.ts";
import { enumerateDisposal, disposePath, verifyClean } from "./Disposal.ts";
import { CONSUMER_LIMITS } from "./ConsumerProbe.ts";
import { withWorkspaceLease } from "../../workspace/workspaceLease.ts";

export interface RetirementRequest {
  cause: RetirementReceipt["cause"];
  projectRoot: string;
  worktree: string;
  maxLevel: "contained" | "branch-retained";
  // Existing native admission and automatic assessment run BEFORE acquiring either reservation.
  admit: Effect.Effect<void, RetirementError>;
  observe: Effect.Effect<QualificationInput, RetirementError>;
  // Read-only revalidation, including durable settlement completion and checkpoint state.
  revalidate: Effect.Effect<void, RetirementError>;
  finalizeOutputs: (receipt: RetirementReceipt) => Effect.Effect<void, RetirementError>;
  // The owning server's native Git adapter may supply the exact unforced command.
  remove?: Effect.Effect<void, RetirementError>;
}
// A worktree with dependencies installed can take far longer than the Git driver's 30 s default to remove.
export const REMOVAL_TIMEOUT_MS = 30 * 60 * 1000;
// A removal that did not return a clean exit may have deleted part of the checkout.
const REMOVAL_REFUSED = "git_removal_refused";
const REMOVAL_INTERRUPTED = "removal_interrupted";
export interface NativeRemovalDriver {
  readonly execute: (input: {
    readonly operation: string;
    readonly cwd: string;
    readonly args: readonly string[];
    readonly allowNonZeroExit: boolean;
    readonly timeoutMs: number;
  }) => Effect.Effect<
    { readonly exitCode: number; readonly stderr: string },
    { readonly message: string }
  >;
}
// Only a completed, non-zero git exit is a refusal. A timeout, kill, signal or spawn error is not.
export const nativeWorktreeRemoval = (
  driver: NativeRemovalDriver,
  input: { readonly projectRoot: string; readonly worktree: string },
): Effect.Effect<void, RetirementError> =>
  driver
    .execute({
      operation: "WorkspaceRetirement.remove",
      cwd: input.projectRoot,
      args: ["worktree", "remove", "--", input.worktree],
      allowNonZeroExit: true,
      timeoutMs: REMOVAL_TIMEOUT_MS,
    })
    .pipe(
      Effect.mapError(
        (cause) =>
          new RetirementError({
            target: input.worktree,
            reason: REMOVAL_INTERRUPTED,
            cause: cause.message,
          }),
      ),
      Effect.flatMap((result) =>
        result.exitCode === 0
          ? Effect.void
          : Effect.fail(
              new RetirementError({
                target: input.worktree,
                reason: REMOVAL_REFUSED,
                cause: `git worktree remove: ${result.stderr.trim().split(/\r?\n/)[0] || "native removal refused"}`,
              }),
            ),
      ),
    );
const defaultWorktreeRemoval = (projectRoot: string, worktree: string) =>
  Effect.tryPromise({
    try: () => git(projectRoot, ["worktree", "remove", "--", worktree], REMOVAL_TIMEOUT_MS),
    catch: (cause) => {
      const failure = cause as { code?: unknown; killed?: unknown; signal?: unknown } | null;
      const refused =
        typeof failure?.code === "number" && failure.killed !== true && !failure.signal;
      return new RetirementError({
        target: worktree,
        reason: refused ? REMOVAL_REFUSED : REMOVAL_INTERRUPTED,
        cause,
      });
    },
  });
const io = <A>(target: string, action: () => Promise<A>) =>
  Effect.tryPromise({
    try: action,
    catch: (cause) => new RetirementError({ target, reason: "observation_failed", cause }),
  });
export const retire = Effect.fn("WorkspaceRetirement.retire")(function* (
  request: RetirementRequest,
) {
  const store = yield* Receipts;
  yield* request.admit;
  // A restart after begin leaves an intent receipt and both exclusions; settle it before observing.
  const reconciled = yield* withWorkspaceLease(
    request.worktree,
    Effect.uninterruptibleMask(() => reconcileInterrupted(request)),
  );
  if (reconciled) return reconciled;
  const input = yield* request.observe;
  const previous = yield* io(request.worktree, () => qualify(input));
  if (
    previous.outcome !== "retirable" ||
    (previous.level === "branch-retained" && request.maxLevel === "contained")
  )
    return {
      state: "not_retired" as const,
      reasons: previous.reasons.length ? previous.reasons : ["retention_scope"],
      qualification: previous,
    };
  return yield* withWorkspaceLease(
    request.worktree,
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        yield* request.revalidate;
        const bound = input.expected,
          id = NodeCrypto.randomUUID();
        const record: ReservationRecord = {
          schema: "workspace-reservation/v1",
          admin_id: bound.admin_id,
          incarnation: incarnation(bound),
          executor: "jones",
          receipt: id,
          ...(yield* io(request.worktree, holderIdentity)),
        };
        yield* io(request.worktree, () => acquireReservation(bound, record));
        let began = false,
          effected = false,
          terminal = false;
        const receipt: RetirementReceipt = {
          schema: "worktree-retirement/v1",
          id,
          cause: request.cause,
          state: "intent",
          binding: bound,
          incarnation: record.incarnation,
          qualification: previous,
          coverage: input.coverage ?? {},
          preservationRefs: [],
          journal: id,
          disposed: [],
          reasons: [],
          limits: CONSUMER_LIMITS,
          bytes: { logical: null, allocated: null, statvfs_delta: null },
        };
        const finish = Effect.gen(function* () {
          yield* store.finish(receipt);
          terminal = true;
        });
        const verifyReservation = Effect.tryPromise({
          try: async () => {
            const path = NodePath.join(
              bound.git_common_dir,
              "workspace-lifecycle",
              "reservations.sqlite3",
            );
            const file = await openAbsolute(path);
            try {
              const before = await file.stat();
              const db = new NodeSqlite.DatabaseSync(path, { readOnly: true });
              try {
                const row = db
                  .prepare("SELECT incarnation,record FROM reservations WHERE admin_id=?")
                  .get(bound.admin_id);
                const after = await NodeFSP.lstat(path);
                if (
                  db.prepare("PRAGMA user_version").get()?.user_version !== 1 ||
                  db.prepare("SELECT sql FROM sqlite_master WHERE name='reservations'").get()
                    ?.sql !== RESERVATION_DDL ||
                  !row ||
                  before.dev !== after.dev ||
                  before.ino !== after.ino ||
                  after.isSymbolicLink() ||
                  row.incarnation !== record.incarnation ||
                  row.record !== canonical(record)
                )
                  throw new Error("reservation_ownership_lost");
              } finally {
                db.close();
              }
            } finally {
              await file.close();
            }
          },
          catch: (cause) =>
            new RetirementError({
              target: request.worktree,
              reason: "reservation_ownership_lost",
              cause,
            }),
        });
        const revalidate = verifyReservation.pipe(Effect.andThen(request.revalidate));
        const execution = Effect.gen(function* () {
          yield* store.begin(receipt);
          began = true;
          yield* revalidate;
          const freshInput = yield* request.observe;
          const final = yield* io(request.worktree, () => finalCheck(freshInput, previous));
          if (final.conflict || canonical(freshInput.expected) !== canonical(bound))
            return yield* new RetirementError({
              reason: "final_check_conflict",
              target: request.worktree,
            });
          receipt.qualification = final.result;
          const journal = yield* io(request.worktree, () =>
            enumerateDisposal(bound, input.coverage?.keepMode ?? "uncommitted-changes"),
          );
          const selected = (paths: readonly string[]) => canonical([...paths].sort());
          if (
            selected(journal.filter((e) => e.kind === "untracked").map((e) => e.path)) !==
              selected(
                (final.result.untrackedPaths ?? []).map((p) => NodePath.resolve(bound.path, p)),
              ) ||
            selected(journal.filter((e) => e.kind === "ignored").map((e) => e.path)) !==
              selected((final.result.ignored ?? []).map((r) => r.path))
          )
            return yield* new RetirementError({
              reason: "disposal_selection_changed",
              target: request.worktree,
            });
          // The journal is durable before preservation/disposal starts; its entries are the only deletion selections.
          yield* store.journal(id, journal);
          for (const [n, head] of (previous.preservationTips ?? []).entries()) {
            const ref = `refs/retired-worktrees/${record.incarnation}/${n}`;
            receipt.preservationRefs.push({ ref, head });
          }
          // Persist the complete restore plan before any ref or filesystem mutation.
          yield* store.finish({ ...receipt, state: "intent" });
          for (const ref of receipt.preservationRefs) {
            effected = true;
            yield* io(request.worktree, () =>
              git(request.projectRoot, [
                "update-ref",
                ref.ref,
                ref.head,
                "0".repeat(ref.head.length),
              ]),
            );
          }
          for (const [n, entry] of journal.entries())
            if (entry.kind === "untracked") {
              yield* store.completePath(id, n, "invoking");
              effected = true;
              const result = yield* io(entry.path, () => disposePath(bound, entry)).pipe(
                Effect.result,
              );
              if (result._tag === "Failure") {
                // A lost unlink reply is reconciled by exact path readback, never another unlink.
                if (!(yield* io(request.worktree, () => exists(entry.path)))) {
                  receipt.disposed.push(entry.path);
                  yield* store.completePath(id, n, "completed");
                }
                return yield* result.failure;
              }
              receipt.disposed.push(entry.path);
              yield* store.completePath(id, n, "completed");
            }
          yield* io(request.worktree, () => verifyClean(bound, journal));
          yield* revalidate;
          const remainingInput = yield* request.observe;
          const remaining = yield* io(request.worktree, () => qualify(remainingInput));
          if (
            remaining.outcome !== "retirable" ||
            canonical(remaining.binding) !== canonical(bound) ||
            remaining.head !== final.result.head ||
            remaining.branch !== final.result.branch ||
            canonical(remaining.containingRefs) !== canonical(final.result.containingRefs) ||
            canonical(remaining.ignored) !== canonical(final.result.ignored) ||
            canonical(remaining.capturedConfig) !== canonical(final.result.capturedConfig) ||
            remaining.sparsePatterns !== final.result.sparsePatterns ||
            remaining.consumers?.coverage.process?.processPolicy?.basisDigest !==
              final.result.consumers?.coverage.process?.processPolicy?.basisDigest
          )
            return yield* new RetirementError({
              reason: "remaining_fact_changed",
              target: request.worktree,
            });
          // The common-store row and Jones retiring state still exclude every participant here.
          effected = true;
          const removed = yield* restore(
            request.remove ?? defaultWorktreeRemoval(request.projectRoot, bound.path),
          ).pipe(Effect.result);
          if (removed._tag === "Failure") {
            const native = removed.failure.cause;
            receipt.removalDiagnostic = (
              typeof native === "string"
                ? native
                : native instanceof Error
                  ? native.message
                  : removed.failure.message
            )
              .split(/\r?\n/)[0]!
              .slice(0, 2048);
          }
          const readback = yield* io(request.worktree, () =>
            removalReadback(request.projectRoot, receipt),
          );
          if (readback !== "retired") {
            // Only a clean git refusal proves the checkout was left alone; anything else may be half removed.
            const interrupted =
              removed._tag === "Failure" && removed.failure.reason !== REMOVAL_REFUSED;
            receipt.state =
              readback === "present" &&
              removed._tag === "Failure" &&
              !interrupted &&
              !receipt.disposed.length &&
              !receipt.preservationRefs.length
                ? "not_retired"
                : "partial";
            receipt.reasons = [
              ...(interrupted ? [REMOVAL_INTERRUPTED] : []),
              ...(readback === "present"
                ? interrupted
                  ? []
                  : [REMOVAL_REFUSED]
                : ["removal_readback_unavailable"]),
            ];
          } else {
            receipt.state = "retired";
            receipt.reasons = [];
          }
          yield* finish;
          if (receipt.state === "retired")
            yield* request.finalizeOutputs(receipt).pipe(
              Effect.catch(() => {
                receipt.reasons.push("output_finalization_pending");
                return store.finish(receipt);
              }),
            );
          return receipt;
        });
        return yield* execution.pipe(
          Effect.provideService(RetirementReservationOwner, { worktree: bound.path, record }),
          Effect.tapError((error) =>
            Effect.sync(() => {
              receipt.reasons = [error.reason];
              receipt.qualification = {
                ...receipt.qualification,
                details: [
                  ...(receipt.qualification.details ?? []),
                  {
                    reason: error.reason,
                    target: request.worktree,
                    root: error.target,
                    category: "unavailable_observation",
                    fact: error.reason,
                    missingFact: error.reason,
                    lineage: [],
                    nextAction: effected
                      ? "Reconcile this exact receipt and journal by readback before any further removal."
                      : "Repeat the named target observation before requesting a fresh proposal.",
                  },
                ],
              };
            }),
          ),
          Effect.catchCause((cause) =>
            Effect.gen(function* () {
              if (!began) return yield* Effect.failCause(cause);
              if (terminal) return receipt;
              receipt.state = effected ? "partial" : "not_retired";
              if (!receipt.reasons.length)
                receipt.reasons = [effected ? "partial_effect_reconcile_required" : "conflict"];
              yield* finish;
              return receipt;
            }),
          ),
          Effect.ensuring(
            Effect.suspend(() =>
              !began || terminal
                ? io(request.worktree, () => releaseReservation(bound, record)).pipe(Effect.orDie)
                : Effect.void,
            ),
          ),
        );
      }),
    ),
  );
});
const INTERRUPTED_NEXT_ACTION: Record<string, string> = {
  interrupted_removal_unregistered_checkout_present:
    "Git no longer registers this checkout but its directory remains. Inspect it by hand; do not force or prune.",
  interrupted_removal_registered_checkout_absent:
    "Git still registers this checkout but its directory is gone. Inspect the registration by hand; do not prune.",
  interrupted_removal_refs_unverified:
    "The checkout is gone but its branch, containing or preservation refs did not read back. Restore them from this receipt before anything else.",
  interrupted_checkout_identity_unverified:
    "The checkout's Git identity could not be read. Inspect its .git link and admin directory by hand.",
  interrupted_checkout_replaced:
    "A different checkout now occupies this path. Inspect it by hand before releasing this receipt.",
  interrupted_disposal_journal_incomplete:
    "Disposal had started. Reconcile each journal entry by exact path readback; never retry an unlink.",
  interrupted_journal_path_missing:
    "A journaled path is missing although disposal never started. Inspect the checkout by hand.",
  interrupted_preservation_refs_present:
    "Preservation refs were already written. Verify them against this receipt by hand; do not delete them.",
  interrupted_tracked_deletions:
    "Tracked files are missing, so removal may have started. Restore or inspect the checkout by hand; do not force.",
};
// Proves the interrupted executor is gone, then settles its receipt from what is actually on disk.
const reconcileInterrupted = Effect.fn("WorkspaceRetirement.reconcileInterrupted")(function* (
  request: RetirementRequest,
) {
  const store = yield* Receipts;
  const stale = yield* store.interrupted(request.worktree);
  if (!stale) return null;
  // Only an executor that never wrote a terminal state is reconciled; partial receipts keep holding.
  if (stale.state !== "intent")
    return yield* new RetirementError({ target: request.worktree, reason: "retiring" });
  const receipt = stale.receipt,
    bound = receipt.binding;
  if (receipt.incarnation !== incarnation(bound))
    return yield* new RetirementError({ target: request.worktree, reason: "retiring" });
  const prior = yield* io(request.worktree, () => readReservation(bound));
  if (!prior || prior.receipt !== receipt.id || prior.incarnation !== receipt.incarnation)
    return yield* new RetirementError({
      target: request.worktree,
      reason: "reservation_recovery_required",
    });
  // The reconciler takes the same receipt id, so a crash here leaves a recoverable record again.
  const record: ReservationRecord = {
    schema: "workspace-reservation/v1",
    admin_id: bound.admin_id,
    incarnation: receipt.incarnation,
    executor: "jones",
    receipt: receipt.id,
    ...(yield* io(request.worktree, holderIdentity)),
  };
  yield* Effect.tryPromise({
    try: () =>
      acquireReservation(bound, record, {
        // This fiber holds the in-process lease for the target, so a same-process holder has ended.
        holderAbsent: (held) => holderAbsent(held, bound.path === request.worktree),
        receiptReadback: async (held) =>
          held.receipt === receipt.id && held.incarnation === receipt.incarnation,
      }),
    catch: (cause) =>
      new RetirementError({
        target: request.worktree,
        reason:
          cause instanceof Error &&
          ["retiring", "reservation_recovery_required"].includes(cause.message)
            ? cause.message
            : "observation_failed",
        cause,
      }),
  });
  const outcome = yield* io(request.worktree, () =>
    interruptedReadback(request.projectRoot, stale),
  );
  receipt.state = outcome.state;
  receipt.reasons = [outcome.reason];
  if (outcome.state === "partial")
    receipt.qualification = {
      ...receipt.qualification,
      details: [
        ...(receipt.qualification.details ?? []),
        {
          reason: outcome.reason,
          target: request.worktree,
          root: bound.path,
          category: "unavailable_observation",
          fact: outcome.reason,
          missingFact: outcome.reason,
          lineage: [],
          nextAction: INTERRUPTED_NEXT_ACTION[outcome.reason]!,
        },
      ],
    };
  // Until this terminal write lands, the reservation keeps this receipt id and stays recoverable.
  yield* store.finish(receipt);
  yield* io(request.worktree, () => releaseReservation(bound, record));
  if (receipt.state === "retired")
    yield* request.finalizeOutputs(receipt).pipe(
      Effect.catch(() => {
        receipt.reasons.push("output_finalization_pending");
        return store.finish(receipt);
      }),
    );
  // A checkout left untouched is released for a fresh attempt in this same call.
  return receipt.state === "not_retired" ? null : receipt;
});
async function interruptedReadback(
  projectRoot: string,
  stale: InterruptedRetirement,
): Promise<{ state: "retired" | "not_retired" | "partial"; reason: string }> {
  const receipt = stale.receipt,
    path = receipt.binding.path;
  const partial = (reason: string) => ({ state: "partial" as const, reason });
  if ((await removalReadback(projectRoot, receipt)) === "retired")
    return { state: "retired", reason: "interrupted_after_removal" };
  const listed = (await git(projectRoot, ["worktree", "list", "--porcelain", "-z"]))
    .toString()
    .split("\0");
  const present = await exists(path),
    registered = listed.includes("worktree " + path);
  if (!present || !registered)
    return partial(
      present
        ? "interrupted_removal_unregistered_checkout_present"
        : registered
          ? "interrupted_removal_registered_checkout_absent"
          : "interrupted_removal_refs_unverified",
    );
  const current = await binding(path).catch(() => null);
  if (!current) return partial("interrupted_checkout_identity_unverified");
  if (incarnation(current) !== receipt.incarnation) return partial("interrupted_checkout_replaced");
  if (stale.journal.some((entry) => entry.state !== "planned"))
    return partial("interrupted_disposal_journal_incomplete");
  for (const entry of stale.journal)
    if (!(await exists(entry.path))) return partial("interrupted_journal_path_missing");
  const refs = (
    await git(projectRoot, [
      "for-each-ref",
      "--format=%(refname)",
      `refs/retired-worktrees/${receipt.incarnation}/`,
    ])
  )
    .toString()
    .trim();
  if (refs) return partial("interrupted_preservation_refs_present");
  const status = (
    await git(path, ["status", "--porcelain=v1", "-z", "--untracked-files=no", "--no-renames"])
  )
    .toString()
    .split("\0")
    .filter(Boolean);
  if (status.some((entry) => entry[0] === "D" || entry[1] === "D"))
    return partial("interrupted_tracked_deletions");
  return { state: "not_retired", reason: "interrupted_before_effect" };
}
async function removalReadback(
  projectRoot: string,
  receipt: RetirementReceipt,
): Promise<"retired" | "present" | "unknown"> {
  try {
    const listed = (await git(projectRoot, ["worktree", "list", "--porcelain", "-z"]))
      .toString()
      .split("\0");
    const present = await exists(receipt.binding.path),
      registered = listed.includes("worktree " + receipt.binding.path);
    if (present || registered) return present && registered ? "present" : "unknown";
    for (const ref of receipt.preservationRefs)
      if ((await git(projectRoot, ["rev-parse", ref.ref])).toString().trim() !== ref.head)
        return "unknown";
    if (
      receipt.qualification.branch &&
      (await git(projectRoot, ["rev-parse", receipt.qualification.branch])).toString().trim() !==
        receipt.qualification.head
    )
      return "unknown";
    for (const ref of receipt.qualification.containingRefs ?? []) {
      await git(projectRoot, ["merge-base", "--is-ancestor", receipt.qualification.head!, ref]);
    }
    // No restore input is read from the removed admin directory.
    await NodeFSP.stat(receipt.binding.git_common_dir);
    return "retired";
  } catch {
    return "unknown";
  }
}
