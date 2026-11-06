// @effect-diagnostics nodeBuiltinImport:off -- Reads bounded native recreation breadcrumbs and chooses a fresh sibling managed path.
import * as Path from "node:path";
import * as Crypto from "node:crypto";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Git from "../../vcs/GitVcsDriver.ts";
import * as Duty from "../workspaceRetirement/Duty.ts";
import { RetirementReceiptJson } from "../workspaceRetirement/DurableCodecs.ts";
import { RetirementError } from "../workspaceRetirement/Receipts.ts";
import { exists, git } from "../workspaceRetirement/Filesystem.ts";
import * as Transitions from "../workspaceLifecycle/TransitionStore.ts";
import {
  acknowledgeInvalidation,
  generationInvalidated,
  plannedPath,
  missingSource,
  readBreadcrumb,
  knownRecreation,
  RecreationHold,
  type RecreationSource,
  type RecreationResult,
} from "./Native.ts";

class TurnClaims extends Context.Service<
  TurnClaims,
  { readonly hold: (path: string) => Effect.Effect<void, RetirementError> }
>()("jones/workspaceRecreation/TurnClaims") {}

const observe = <A>(target: string, owner: string, action: () => Promise<A>) =>
  Effect.tryPromise({
    try: action,
    catch: (cause) =>
      cause instanceof RecreationHold
        ? cause
        : new RecreationHold(
            "unknown",
            target,
            owner,
            "checkout recreation observation unavailable",
            "Reconcile this exact receipt, registration and journal before retrying.",
            { cause },
          ),
  });

// The outer start owner awaits startup and the returned provider turn before releasing these claims.
export const withTurnClaims = Effect.fn("CheckoutRecreation.withTurnClaims")(function* <A, E, R>(
  owner: string,
  effect: Effect.Effect<A, E, R>,
) {
  const duty = yield* Duty.hooks;
  const releases: Effect.Effect<void, RetirementError>[] = [];
  return yield* effect.pipe(
    Effect.provideService(TurnClaims, {
      hold: (path) =>
        duty.hold(path, "turn", owner).pipe(
          Effect.tap((release) => Effect.sync(() => releases.push(release))),
          Effect.asVoid,
          Effect.uninterruptible,
        ),
    }),
    Effect.ensuring(
      Effect.suspend(() =>
        Effect.forEach(releases, (release) => release, { discard: true }).pipe(Effect.orDie),
      ),
    ),
  );
});

const invalidateSource = Effect.fn("CheckoutRecreation.invalidateSource")(function* (
  worktree: string,
  result: RecreationResult,
) {
  if (yield* observe(worktree, result.breadcrumb, () => generationInvalidated(result))) return;
  const sql = yield* Effect.serviceOption(SqlClient.SqlClient);
  if (Option.isSome(sql)) {
    const store = yield* Transitions.TransitionStore.pipe(
      Effect.provide(Transitions.layer),
      Effect.provideService(SqlClient.SqlClient, sql.value),
    );
    yield* store.invalidateRecreated(worktree);
    yield* observe(worktree, result.breadcrumb, () => acknowledgeInvalidation(result));
  }
});

const retiredSource = Effect.fn("CheckoutRecreation.retiredSource")(function* (worktree: string) {
  const sql = yield* Effect.serviceOption(SqlClient.SqlClient);
  if (Option.isNone(sql)) return null;
  const rows = yield* sql.value<{
    state: string;
    receipt_json: string | null;
  }>`SELECT s.state,r.receipt_json
    FROM jones_workspace_retirement_state s
    LEFT JOIN jones_workspace_retirement_receipts r ON r.receipt_id=s.receipt_id
    WHERE s.worktree=${worktree}`;
  if (!rows[0]) return null;
  if (rows[0].state !== "retired")
    return yield* new RetirementError({ reason: "retiring", target: worktree });
  const receipt =
    rows[0].receipt_json === null
      ? null
      : yield* Schema.decodeEffect(RetirementReceiptJson)(rows[0].receipt_json);
  if (
    !receipt ||
    receipt.state !== "retired" ||
    receipt.binding.path !== worktree ||
    !receipt.qualification.head
  )
    return yield* new RetirementError({ reason: "terminal_receipt_unavailable", target: worktree });
  return {
    id: receipt.id,
    incarnation: receipt.incarnation,
    binding: receipt.binding,
    head: receipt.qualification.head,
    branch: receipt.qualification.branch ?? null,
    ...(receipt.qualification.capturedConfig === undefined
      ? {}
      : { capturedConfig: receipt.qualification.capturedConfig }),
    ...(receipt.qualification.sparsePatterns === undefined
      ? {}
      : { sparsePatterns: receipt.qualification.sparsePatterns }),
  } satisfies RecreationSource;
});

export const prepareTurn = Effect.fn("CheckoutRecreation.prepareTurn")(function* (input: {
  readonly threadId: string;
  readonly runId: string;
  readonly projectRoot: string;
  readonly worktree: string;
  readonly managed: boolean;
  readonly nativeGuarded: boolean;
  readonly detached?: { readonly receiptId: string; readonly mode: "detached" };
}) {
  const present = yield* observe(input.worktree, input.threadId, () => exists(input.worktree));
  let source = yield* retiredSource(input.worktree);
  if (present && source === null) {
    const known = yield* observe(input.worktree, input.threadId, () =>
      knownRecreation(input.projectRoot, input.worktree),
    );
    if (known !== null) yield* invalidateSource(input.worktree, known);
    return null;
  }
  if (input.nativeGuarded)
    return yield* Effect.fail(
      new RecreationHold(
        "native_recreation_unsupported",
        input.worktree,
        input.threadId,
        "native verified workspace disappeared or was retired",
        "Recover through the native workspace owner.",
      ),
    );
  if (!input.managed) return null;
  if (present && source !== null) {
    const marker = yield* observe(input.worktree, input.threadId, async () => {
      try {
        const admin = (await git(input.worktree, ["rev-parse", "--absolute-git-dir"]))
          .toString()
          .trim();
        return await readBreadcrumb(admin);
      } catch {
        return null;
      }
    });
    if (marker?.predecessor === source.incarnation) {
      yield* invalidateSource(input.worktree, marker);
      return null;
    }
  }
  if (source === null)
    source = yield* observe(input.worktree, input.threadId, () =>
      missingSource(input.projectRoot, input.worktree),
    );
  if (input.detached !== undefined && input.detached.receiptId !== source.id)
    return yield* Effect.fail(
      new RecreationHold(
        "unknown",
        input.worktree,
        input.threadId,
        "detached request does not name the current terminal receipt",
        "Inspect the current receipt and submit a new explicit detached request.",
      ),
    );
  const claims = yield* TurnClaims;
  const alternate = Path.join(
    Path.dirname(input.worktree),
    `${Path.basename(input.worktree)}-recreated-${Crypto.randomUUID()}`,
  );
  const selectedSource = source;
  const sql = yield* Effect.serviceOption(SqlClient.SqlClient);
  if (Option.isSome(sql)) {
    const pending = yield* sql.value`SELECT 1 FROM jones_workspace_lifecycle_transitions
      WHERE worktree=${input.worktree} AND state!='confirmed' LIMIT 1`;
    if (pending.length)
      return yield* Effect.fail(
        new RecreationHold(
          "unknown",
          input.worktree,
          input.threadId,
          "an earlier lifecycle transition still requires reconciliation",
          "Reconcile its original transition before recreating this checkout.",
        ),
      );
  }
  const target = yield* observe(input.worktree, input.threadId, () =>
    plannedPath(selectedSource, alternate),
  );
  yield* claims.hold(target);
  const driver = yield* Effect.serviceOption(Git.GitVcsDriver);
  if (Option.isNone(driver) || driver.value.recreateCheckout === undefined)
    return yield* Effect.fail(
      new RecreationHold(
        "unknown",
        input.worktree,
        input.threadId,
        "native recreation adapter unavailable",
        "Use a server with the checkout recreation adapter.",
      ),
    );
  const result = yield* driver.value.recreateCheckout({
    cwd: input.projectRoot,
    source,
    owner: input.runId,
    alternatePath: alternate,
    detached: input.detached !== undefined,
  });
  yield* invalidateSource(input.worktree, result);
  // Rebind the claim to the observed new incarnation before provider/setup consumers can start.
  yield* claims.hold(result.path);
  return result;
});

// Restore changes thread placement only. Its next turn owns recreation after a terminal receipt.
export const deferRestore = Effect.fn("CheckoutRecreation.deferRestore")(function* (
  threadId: string,
) {
  const sql = yield* Effect.serviceOption(SqlClient.SqlClient);
  if (Option.isNone(sql)) return false;
  const rows = yield* sql.value<{
    worktree: string | null;
  }>`SELECT json_extract(payload_json,'$.worktreePath') AS worktree
    FROM orchestration_v2_projection_threads WHERE thread_id=${threadId}`;
  const worktree = rows[0]?.worktree;
  return worktree == null ? false : (yield* retiredSource(worktree)) !== null;
});

export const recreatedDetached = Effect.fn("CheckoutRecreation.recreatedDetached")(function* (
  threadId: string,
) {
  const sql = yield* Effect.serviceOption(SqlClient.SqlClient);
  if (Option.isNone(sql)) return false;
  const rows = yield* sql.value<{
    worktree: string | null;
    branch: string | null;
  }>`SELECT json_extract(payload_json,'$.worktreePath') AS worktree,
      json_extract(payload_json,'$.branch') AS branch
      FROM orchestration_v2_projection_threads WHERE thread_id=${threadId}`;
  if (!rows[0]?.worktree || rows[0].branch !== null) return false;
  const worktree = rows[0].worktree;
  return yield* observe(worktree, threadId, async () => {
    const admin = (await git(worktree, ["rev-parse", "--absolute-git-dir"])).toString().trim();
    const marker = await readBreadcrumb(admin);
    return (
      marker !== null &&
      marker.branch === null &&
      marker.path === worktree &&
      (await exists(worktree))
    );
  });
});
