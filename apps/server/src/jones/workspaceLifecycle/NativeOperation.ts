import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import { canonical, digest } from "../ordinaryCheckout/CheckoutProtocol.ts";
import { NativeCheckoutOwner } from "./NativeSource.ts";
import { LifecycleError } from "./Protocol.ts";
import type { NativeTransition } from "./WorkspaceLifecycle.ts";

interface Operation {
  readonly id: string;
  readonly kind:
    | "effect"
    | "provider-start"
    | "provider-session"
    | "native-start"
    | "command"
    | "preservation";
  readonly threadId?: string;
  readonly worktree?: string;
  readonly boundaries: readonly NativeTransition["boundary"][];
  readonly check: Effect.Effect<void, LifecycleError>;
  readonly sqlOnly?: boolean;
  readonly inherit?: boolean;
}
class CurrentOperation extends Context.Service<CurrentOperation, object>()(
  "t3/jones/workspaceLifecycle/NativeOperation/CurrentOperation",
) {}
const issued = new WeakMap<
  object,
  { readonly operation: Operation; readonly parent: object | undefined; active: boolean }
>();

export const sameOperationRequest = (left: unknown, right: unknown) =>
  canonical(left) === canonical(right);

// Only native effect owners call this wrapper. Neither persisted provenance nor configuration can recreate its live handle.
export const withOperation = Effect.fn("NativeOperation.withOperation")(function* <A, E, R, C>(
  operation: Omit<Operation, "check"> & { readonly check: Effect.Effect<void, C> },
  effect: Effect.Effect<A, E, R>,
) {
  const parent = yield* Effect.serviceOption(CurrentOperation);
  const handle = {};
  const ancestor = Option.getOrUndefined(parent);
  const state = {
    operation: {
      ...operation,
      check: operation.check.pipe(
        Effect.mapError((cause) => new LifecycleError({ reason: "stale", cause })),
      ),
    },
    parent:
      operation.inherit === false || ancestor === undefined || !issued.get(ancestor)?.active
        ? undefined
        : ancestor,
    active: true,
  };
  issued.set(handle, state);
  return yield* effect.pipe(
    Effect.provideService(CurrentOperation, handle),
    Effect.ensuring(
      Effect.sync(() => {
        state.active = false;
      }),
    ),
  );
});

const assert = Effect.fn("NativeOperation.assert")(function* (
  input: NativeTransition,
  worktree?: string,
) {
  const current = yield* Effect.serviceOption(CurrentOperation);
  if (Option.isNone(current)) return yield* new LifecycleError({ reason: "identity" });
  const sql = yield* Effect.serviceOption(SqlClient.SqlClient);
  if (Option.isNone(sql)) return yield* new LifecycleError({ reason: "unavailable" });
  const transactional = Option.isSome(yield* Effect.serviceOption(sql.value.transactionService));
  let handle: object | undefined = current.value;
  const provenance: { id: string; kind: string }[] = [];
  while (handle !== undefined) {
    const state = issued.get(handle);
    if (
      !state?.active ||
      (state.operation.threadId !== undefined && state.operation.threadId !== input.threadId) ||
      (state.operation.worktree !== undefined && state.operation.worktree !== worktree) ||
      !state.operation.boundaries.includes(input.boundary) ||
      (transactional && !state.operation.sqlOnly)
    )
      return yield* new LifecycleError({ reason: "identity" });
    yield* state.operation.check;
    provenance.push({ id: state.operation.id, kind: state.operation.kind });
    handle = state.parent;
  }
  return provenance;
});

export const layer = Layer.succeed(NativeCheckoutOwner, {
  verify: Effect.fn("NativeOperation.verify")(function* (input, target, birth) {
    const provenance = yield* assert(input, target.checkoutKey);
    const lineage = {
      threadId: input.threadId,
      birth,
      checkout: target.checkoutKey,
      directory: target.directoryIdentity,
      common: target.gitCommonIdentity,
      git: target.gitDirectoryIdentity,
    };
    return { id: `jones-native:${input.threadId}`, generation: digest(lineage), provenance };
  }),
  validate: Effect.fn("NativeOperation.validate")(function* (input, _evidence) {
    yield* assert(input);
  }),
});
