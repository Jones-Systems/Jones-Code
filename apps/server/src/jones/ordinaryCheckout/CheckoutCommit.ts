import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import type {
  CommandReceiptStoreV2,
  CommandReceiptV2,
} from "../../orchestration-v2/CommandReceiptStore.ts";
import type { PreparedIntent, CommittedEffects, IntentPlan } from "./CheckoutIntent.ts";
import type { LocalAcceptance, PreparationHandle } from "./CheckoutPreparation.ts";
import { CheckoutCommitAuthority, type CommittedCheckoutOperation } from "./OrdinaryCheckout.ts";
import { canonical, CheckoutError, digest } from "./CheckoutProtocol.ts";
import * as LegacyStorage from "../legacyBootstrap/Storage.ts";
import type { CommandBinding } from "../legacyBootstrap/StorageSchema.ts";
import { decodedCommand, planDigests } from "./CheckoutPreparationSchema.ts";

export interface PreparationContext {
  readonly preparation?: PreparationHandle;
  readonly localAcceptance?: LocalAcceptance;
}

// The real runtime executor must bind its retained current use/claim to these
// effects. Receiving guards alone cannot choose an admission or issue custody.
export class DerivedExecution extends Context.Service<
  DerivedExecution,
  {
    readonly assertCurrent: (operation: CommittedEffects) => Effect.Effect<void, CheckoutError>;
  }
>()("t3/jones/ordinaryCheckout/CheckoutCommit/DerivedExecution") {}

const refused = () => Effect.fail(new CheckoutError({ reason: "identity" }));
const receiptDigest = (receipt: CommandReceiptV2) =>
  digest({ ...receipt, acceptedAt: DateTime.formatIso(receipt.acceptedAt) });
const storedDigest = (operation: CommittedEffects) =>
  digest({
    plan: planDigests({
      events: operation.storedEvents.map(({ event }) => event),
      effects: operation.effects,
    }).actual,
    positions: operation.storedEvents.map(({ sequence, commandId, event }) => [
      sequence,
      commandId,
      event.id,
    ]),
  });

export const validatePlan = Effect.fn("CheckoutCommit.validatePlan")(function* (
  prepared: PreparedIntent,
  plan: IntentPlan,
) {
  if (
    prepared.plan.command !== plan.command ||
    prepared.plan.threadId !== plan.threadId ||
    prepared.plan.preparation !== plan.preparation ||
    prepared.plan.localAcceptance !== plan.localAcceptance ||
    planDigests(prepared.plan).authority !== planDigests(plan).authority
  )
    return yield* refused();
});

// This helper owns the real reservation. There is no boolean/token constructor
// that can turn a receipt read, caller claim or historical acceptance into a witness.
const reserve = Effect.fn("CheckoutCommit.reserve")(function* <E>(
  sql: SqlClient.SqlClient,
  receipt: CommandReceiptV2,
  prepared: PreparedIntent | undefined,
  insert: Effect.Effect<boolean, E>,
) {
  const transaction = Option.getOrUndefined(yield* Effect.serviceOption(sql.transactionService));
  const fiber = yield* Effect.fiberId;
  if (
    transaction === undefined ||
    Option.getOrUndefined(yield* Effect.serviceOption(SqlClient.SqlClient)) !== sql
  )
    return yield* refused();
  const command = prepared === undefined ? undefined : yield* decodedCommand(prepared.plan.command);
  if (
    prepared !== undefined &&
    (receipt.commandId !== prepared.plan.command.commandId ||
      receipt.commandType !== prepared.plan.command.type ||
      receipt.threadId !== prepared.plan.threadId)
  )
    return yield* refused();
  if (!(yield* insert)) return undefined;
  let spent = false;
  return {
    withAuthority: Effect.fn("CheckoutCommit.withAuthority")(function* <A, E, R>(
      committed: CommittedEffects & { readonly receipt: CommandReceiptV2 },
      write: Effect.Effect<A, E, R>,
    ) {
      if (spent || prepared === undefined || command === undefined) return yield* refused();
      spent = true;
      const actual = storedDigest(committed);
      const finalReceipt = receiptDigest(committed.receipt);
      let open = true;
      const check = Effect.fn("CheckoutCommit.assertCommand")(
        function* (operation: CommittedCheckoutOperation) {
          const currentCommand = yield* decodedCommand(operation.command);
          if (
            !open ||
            fiber !== (yield* Effect.fiberId) ||
            Option.getOrUndefined(yield* Effect.serviceOption(SqlClient.SqlClient)) !== sql ||
            Option.getOrUndefined(yield* Effect.serviceOption(sql.transactionService)) !==
              transaction ||
            operation.prepared !== prepared ||
            operation.command !== prepared.plan.command ||
            currentCommand.digest !== command.digest ||
            operation.threadId !== receipt.threadId ||
            operation.receipt !== committed.receipt ||
            operation.storedEvents !== committed.storedEvents ||
            operation.effects !== committed.effects ||
            operation.receipt.status !== "accepted" ||
            operation.receipt.error !== null ||
            DateTime.formatIso(operation.receipt.acceptedAt) !==
              DateTime.formatIso(receipt.acceptedAt) ||
            operation.receipt.commandId !== receipt.commandId ||
            operation.receipt.commandType !== receipt.commandType ||
            operation.receipt.threadId !== receipt.threadId ||
            operation.storedEvents.some((stored) => stored.commandId !== receipt.commandId) ||
            receiptDigest(operation.receipt) !== finalReceipt ||
            storedDigest(operation) !== actual ||
            planDigests({
              events: operation.storedEvents.map(({ event }) => event),
              effects: operation.effects,
            }).authority !== planDigests(prepared.plan).authority
          )
            return yield* refused();
        },
        Effect.tapError(() =>
          Effect.sync(() => {
            open = false;
          }),
        ),
      );
      const authority = CheckoutCommitAuthority.of({
        assertOrigin: Effect.fn("CheckoutCommitAuthority.assertOrigin")((operation) =>
          prepared.preparation === null ? check(operation) : refused(),
        ),
        assertSwitch: Effect.fn("CheckoutCommitAuthority.assertSwitch")((operation) =>
          prepared.preparation?.kind === "switch" &&
          operation.origin === prepared.preparation.origin
            ? check(operation)
            : refused(),
        ),
        assertRelease: Effect.fn("CheckoutCommitAuthority.assertRelease")((operation) =>
          prepared.preparation?.kind === "release" &&
          operation.origin === prepared.preparation.origin
            ? check(operation)
            : refused(),
        ),
        assertDerived: Effect.fn("CheckoutCommitAuthority.assertDerived")(() => refused()),
      });
      return yield* check({
        prepared,
        command: prepared.plan.command,
        threadId: receipt.threadId,
        ...committed,
      }).pipe(
        Effect.andThen(write.pipe(Effect.provideService(CheckoutCommitAuthority, authority))),
        Effect.ensuring(
          Effect.sync(() => {
            open = false;
          }),
        ),
      );
    }),
  };
});

export const reserveCommand = Effect.fn("CheckoutCommit.reserveCommand")(
  (
    sql: SqlClient.SqlClient,
    receipts: CommandReceiptStoreV2["Service"],
    receipt: CommandReceiptV2,
    prepared: PreparedIntent | undefined,
  ) => reserve(sql, receipt, prepared, receipts.insertIfAbsent(receipt)),
);

// Private deletion owns its receipt in Jones storage. Only this exact new
// insertion can issue the ordinary transaction witness; replay issues none.
export const reserveLegacyDeleteCommand = Effect.fn("CheckoutCommit.reserveLegacyDeleteCommand")(
  function* (
    sql: SqlClient.SqlClient,
    binding: CommandBinding,
    receipt: CommandReceiptV2,
    prepared: PreparedIntent,
    committed: CommittedEffects,
  ) {
    const decoded = yield* decodedCommand(prepared.plan.command);
    if (
      binding.kind !== "delete" ||
      binding.commandId !== receipt.commandId ||
      binding.threadId !== receipt.threadId ||
      binding.bindingJson !== canonical(decoded.command) ||
      (decoded.command.type !== "legacy-bootstrap.guard-rejection-delete" &&
        decoded.command.type !== "legacy-bootstrap.failure-delete") ||
      prepared.preparation !== null ||
      prepared.origins.length !== 0 ||
      prepared.candidates.length !== 0 ||
      prepared.preflight.length !== 0 ||
      committed.effects.some(
        (effect) =>
          effect.nativeCreationExecutionReference !== undefined ||
          [
            "provider-turn.start",
            "provider-turn.interrupt",
            "provider-turn.steer",
            "provider-turn.steer-batch",
            "provider-turn.restart",
            "runtime-request.respond",
            "provider-thread.rollback",
            "provider-runtime.continue",
            "checkpoint.capture",
            "terminal.cleanup",
          ].includes(effect.request.type),
      )
    )
      return yield* refused();
    yield* validatePlan(prepared, prepared.plan);
    const writer = yield* LegacyStorage.make(sql).forTransaction;
    return yield* reserve(
      sql,
      receipt,
      prepared,
      writer
        .recordDelete(binding, receipt, committed.storedEvents)
        .pipe(Effect.mapError((cause) => new CheckoutError({ reason: "storage", cause }))),
    );
  },
);

// Called only after the sink's actual receiving guards and enqueue succeeded.
// The witness is bounded to this one SQL operation, including batch savepoints.
export const withDerived = Effect.fn("CheckoutCommit.withDerived")(function* <A, E, R>(
  sql: SqlClient.SqlClient,
  committed: CommittedEffects,
  write: Effect.Effect<A, E, R>,
) {
  const transaction = Option.getOrUndefined(yield* Effect.serviceOption(sql.transactionService));
  const fiber = yield* Effect.fiberId;
  const actual = storedDigest(committed);
  const execution = yield* Effect.serviceOption(DerivedExecution);
  const needsExecution = committed.effects.some(
    (effect) =>
      effect.nativeCreationExecutionReference === undefined &&
      [
        "provider-turn.start",
        "provider-turn.interrupt",
        "provider-turn.steer",
        "provider-turn.steer-batch",
        "provider-turn.restart",
        "runtime-request.respond",
        "provider-thread.rollback",
        "provider-runtime.continue",
        "checkpoint.capture",
      ].includes(effect.request.type),
  );
  let open = true;
  const check = Effect.fn("CheckoutCommit.assertDerived")(function* (operation: CommittedEffects) {
    if (
      !open ||
      transaction === undefined ||
      fiber !== (yield* Effect.fiberId) ||
      Option.getOrUndefined(yield* Effect.serviceOption(SqlClient.SqlClient)) !== sql ||
      Option.getOrUndefined(yield* Effect.serviceOption(sql.transactionService)) !== transaction ||
      operation.storedEvents !== committed.storedEvents ||
      operation.effects !== committed.effects ||
      storedDigest(operation) !== actual
    )
      return yield* refused();
    if (needsExecution) {
      if (Option.isNone(execution)) return yield* new CheckoutError({ reason: "unavailable" });
      yield* execution.value.assertCurrent(operation);
    }
  });
  return yield* write.pipe(
    Effect.provideService(CheckoutCommitAuthority, {
      assertOrigin: () => refused(),
      assertSwitch: () => refused(),
      assertRelease: () => refused(),
      assertDerived: check,
    }),
    Effect.ensuring(
      Effect.sync(() => {
        open = false;
      }),
    ),
  );
});
