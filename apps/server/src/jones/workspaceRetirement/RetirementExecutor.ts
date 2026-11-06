// @effect-diagnostics nodeBuiltinImport:off -- C4 readback observes both Git registration and the filesystem after unforced removal.
import { RESERVATION_DDL } from "./ParticipantGuard.ts";
import * as NodeSqlite from "node:sqlite";
import * as NodeFSP from "node:fs/promises";
import * as NodeCrypto from "node:crypto";
import * as Effect from "effect/Effect";
import * as NodePath from "node:path";
import { canonical, exists, openAbsolute, git } from "./Filesystem.ts";
import { incarnation } from "./OutputAttestation.ts";
import { finalCheck, qualify, type QualificationInput } from "./Qualification.ts";
import {
  Receipts,
  RetirementError,
  RetirementReservationOwner,
  type RetirementReceipt,
} from "./Receipts.ts";
import {
  acquireReservation,
  holderIdentity,
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
                  db.prepare("SELECT sql FROM sqlite_master WHERE name='reservations'").get()?.sql !== RESERVATION_DDL ||
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
            remaining.sparsePatterns !== final.result.sparsePatterns
          )
            return yield* new RetirementError({
              reason: "remaining_fact_changed",
              target: request.worktree,
            });
          // The common-store row and Jones retiring state still exclude every participant here.
          effected = true;
          const removed = yield* restore(
            request.remove ??
              io(request.worktree, () =>
                git(request.projectRoot, ["worktree", "remove", "--", bound.path]),
              ),
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
            receipt.state =
              readback === "present" &&
              removed._tag === "Failure" &&
              !receipt.disposed.length &&
              !receipt.preservationRefs.length
                ? "not_retired"
                : "partial";
            receipt.reasons = [
              readback === "present" ? "git_removal_refused" : "removal_readback_unavailable",
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
