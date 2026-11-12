import type { ProviderRuntimeBinding } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as SqlClient from "effect/sql/SqlClient";
import {
  awaitCapturedCodexProducerLifetime,
  type CodexCapturedProducerIdentity,
} from "../provider/codex/CodexCapturedLifetime.ts";
import {
  canonical,
  digest,
  RuntimeUseError,
  runtimeUseError,
  type RuntimeUseBinding,
  type RuntimeUseProducer,
  type RuntimeUseNativeBinding,
} from "./RuntimeUseProtocol.ts";
import { makeRuntimeUseStore } from "./RuntimeUseStore.ts";

const fail = (reason: string) => Effect.fail(new RuntimeUseError({ reason }));
export interface RuntimeUseProducerValidation {
  readonly validate: Effect.Effect<void, RuntimeUseError>;
  readonly revalidate: Effect.Effect<void, RuntimeUseError>;
}
class ActiveRuntimeUseOperation extends Context.Service<
  ActiveRuntimeUseOperation,
  {
    readonly owner: object;
    readonly active: () => boolean;
  }
>()("t3/jones/cleanup/RuntimeUseLifetime/ActiveRuntimeUseOperation") {}

/** A live opening owns this observer. Persisted journal rows cannot reconstruct
 * it. Sealing fences further producers; it does not itself prove retirement. */
export const makeRuntimeUseLifetime = Effect.fn("RuntimeUseLifetime.make")(function* (input: {
  readonly binding: RuntimeUseBinding;
  readonly observationScope: Scope.Scope;
  readonly prepareProducer: (
    identity: CodexCapturedProducerIdentity,
  ) => Effect.Effect<RuntimeUseProducerValidation, RuntimeUseError>;
}) {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* makeRuntimeUseStore;
  const mutex = yield* Semaphore.make(1);
  const operationOwner = Object.freeze({});
  type Proof = Effect.Success<ReturnType<typeof awaitCapturedCodexProducerLifetime>>;
  const tickets = new Map<
    string,
    {
      readonly identity: CodexCapturedProducerIdentity;
      observation: RuntimeUseProducer | undefined;
      receipt: unknown;
      attached: boolean;
      proof: Proof | undefined;
    }
  >();
  const uses = new Map([[input.binding.useId, input.binding]]);
  const nativeWriters: ProviderRuntimeBinding[] = [];
  const producerForUse = (
    producer: RuntimeUseProducer,
    binding: RuntimeUseBinding,
  ): RuntimeUseProducer => ({
    ...producer,
    useId: binding.useId,
    bindingSha256: digest(binding),
  });
  const nativeForUse = (
    writer: ProviderRuntimeBinding,
    binding: RuntimeUseBinding,
  ): RuntimeUseNativeBinding => ({
    schema: "jones.workspace-runtime-native-binding/v1",
    useId: binding.useId,
    bindingSha256: digest(binding),
    producerId: writer.runtimeGeneration,
    generation: writer.runtimeGeneration,
    binding: writer,
  });
  let sealed = false;
  let uncertain = false;
  let operations = 0;
  const assertActive = Effect.suspend(() =>
    sealed ? fail("runtime_use_producer_admission_sealed") : Effect.void,
  );
  const retireIfComplete = Effect.fn("RuntimeUseLifetime.retireIfComplete")(function* () {
    if (!sealed || uncertain || operations !== 0 || tickets.size === 0) return;
    if (
      [...tickets.values()].some(
        (ticket) => ticket.proof === undefined || ticket.observation === undefined,
      )
    )
      return;
    yield* sql
      .withTransaction(
        Effect.gen(function* () {
          for (const binding of uses.values()) {
            const record = yield* store.read(binding.useId);
            if (record === null || canonical(record.binding) !== canonical(binding))
              return yield* fail("runtime_use_original_open_changed");
            if (
              record.producers.length !== tickets.size ||
              canonical(record.nativeBindings) !==
                canonical(nativeWriters.map((writer) => nativeForUse(writer, binding)))
            )
              return yield* fail("runtime_use_live_inventory_changed");
            const proofs = [];
            for (const original of record.producers) {
              const ticket = tickets.get(original.producerId);
              if (
                ticket?.proof === undefined ||
                ticket.observation === undefined ||
                canonical(original) !== canonical(producerForUse(ticket.observation, binding))
              )
                return yield* fail("runtime_use_live_producer_changed");
              proofs.push({
                ...ticket.proof,
                producerId: original.producerId,
                observationSha256: digest(original),
                writerBindingsSha256: digest(
                  record.nativeBindings.filter((item) => item.producerId === original.producerId),
                ),
              });
            }
            yield* store.recordRetirement({
              schema: "jones.workspace-runtime-retirement/v1",
              useId: binding.useId,
              bindingSha256: digest(binding),
              outcome: "producers_exited",
              observationCount: record.producers.length + record.nativeBindings.length,
              producers: proofs,
              nativeBindings: record.nativeBindings,
            });
          }
        }),
      )
      .pipe(Effect.mapError(runtimeUseError));
  });
  const beginProducer = Effect.fn("RuntimeUseLifetime.beginProducer")(function* (
    identity: CodexCapturedProducerIdentity,
  ) {
    // Register synchronously before any asynchronous validation or SQL. Unknown
    // commit/cancellation keeps this ticket unresolved and cannot hide a spawn.
    yield* assertActive;
    const actor = input.binding.actor;
    if (
      actor.kind === "terminal_spawn" ||
      actor.provider.driver !== identity.driver ||
      actor.provider.instanceId !== identity.instanceId ||
      actor.provider.sessionId !== identity.providerSessionId ||
      tickets.has(identity.generation)
    )
      return yield* fail("runtime_use_original_producer_mismatch");
    const ticket = {
      identity: Object.freeze({ ...identity }),
      observation: undefined as RuntimeUseProducer | undefined,
      receipt: undefined as unknown,
      attached: false,
      proof: undefined as Proof | undefined,
    };
    tickets.set(identity.generation, ticket);
    const validation = yield* input.prepareProducer(ticket.identity);
    yield* validation.revalidate;
    yield* mutex.withPermit(
      Effect.gen(function* () {
        yield* assertActive;
        const observation: RuntimeUseProducer = {
          schema: "jones.workspace-runtime-producer/v1",
          useId: input.binding.useId,
          bindingSha256: digest(input.binding),
          producerId: identity.generation,
          generation: identity.generation,
          threadId: identity.threadId,
          coverage: "captured_process_only",
          writerSetSha256: digest(identity),
        };
        yield* sql
          .withTransaction(
            validation.validate.pipe(
              Effect.andThen(
                Effect.gen(function* () {
                  for (const binding of uses.values())
                    yield* store.observe(producerForUse(observation, binding));
                }),
              ),
            ),
          )
          .pipe(Effect.mapError(runtimeUseError));
        ticket.observation = observation;
      }),
    );
    const beforeSpawn = assertActive.pipe(
      Effect.andThen(validation.revalidate),
      Effect.andThen(assertActive),
    );
    const capture = Effect.fn("RuntimeUseLifetime.capture")(function* (receipt: unknown) {
      if (receipt === undefined) return;
      if (ticket.attached) {
        if (ticket.receipt !== receipt) {
          uncertain = true;
          return yield* fail("runtime_use_captured_producer_changed");
        }
        return;
      }
      ticket.attached = true;
      ticket.receipt = receipt;
      return yield* awaitCapturedCodexProducerLifetime(receipt, ticket.identity).pipe(
        Effect.mapError(runtimeUseError),
        Effect.flatMap((proof) =>
          mutex.withPermit(
            Effect.gen(function* () {
              ticket.proof = proof;
              yield* retireIfComplete();
            }),
          ),
        ),
        // Failure/interrupt is absence of retirement, never authority to retire.
        Effect.catchCause((cause) =>
          Effect.logWarning("jones.runtime-use-retirement-unavailable", {
            useId: input.binding.useId,
            generation: identity.generation,
            cause,
          }),
        ),
        Effect.forkIn(input.observationScope, { startImmediately: true }),
      );
    });
    return { beforeSpawn, capture };
  });
  const observeBinding = Effect.fn("RuntimeUseLifetime.observeBinding")(function* (
    binding: ProviderRuntimeBinding,
  ) {
    const observed = Object.freeze({ ...binding });
    const operation = yield* Effect.serviceOption(ActiveRuntimeUseOperation);
    yield* mutex
      .withPermit(
        Effect.gen(function* () {
          // A late binding notification may describe already admitted work. It must
          // belong to the captured producer, and cannot create a new generation.
          const ticket = tickets.get(binding.runtimeGeneration);
          const admittedPublication =
            Option.isSome(operation) &&
            operation.value.owner === operationOwner &&
            operation.value.active();
          if (
            ticket?.observation === undefined ||
            (ticket.proof !== undefined && !admittedPublication)
          )
            return yield* fail("runtime_use_uncovered_native_binding");
          yield* sql
            .withTransaction(
              Effect.gen(function* () {
                for (const use of uses.values())
                  yield* store.observeNativeBinding(nativeForUse(observed, use));
              }),
            )
            .pipe(Effect.mapError(runtimeUseError));
          if (!nativeWriters.some((writer) => canonical(writer) === canonical(observed)))
            nativeWriters.push(observed);
        }),
      )
      .pipe(
        Effect.onError(() =>
          Effect.sync(() => {
            uncertain = true;
          }),
        ),
      );
  });
  const adoptUse = Effect.fn("RuntimeUseLifetime.adoptUse")(function* (binding: RuntimeUseBinding) {
    yield* mutex.withPermit(
      Effect.gen(function* () {
        yield* assertActive;
        const actor = input.binding.actor;
        if (
          binding.actor.kind !== "provider_invoke" ||
          actor.kind === "terminal_spawn" ||
          canonical(binding.actor.provider) !== canonical(actor.provider)
        )
          return yield* fail("runtime_use_additional_scope_owner_mismatch");
        const previous = uses.get(binding.useId);
        if (previous !== undefined) {
          if (canonical(previous) !== canonical(binding))
            return yield* fail("runtime_use_additional_scope_changed");
          return;
        }
        yield* sql
          .withTransaction(
            Effect.gen(function* () {
              const record = yield* store.read(binding.useId);
              if (
                record?.state !== "pending" ||
                canonical(record.binding) !== canonical(binding) ||
                record.lastOrdinal !== 0
              )
                return yield* fail("runtime_use_additional_scope_not_fresh");
              // These observations come from the live captured producer owner, never
              // from an upstream row, an old serialized receipt or another session.
              for (const ticket of tickets.values()) {
                if (ticket.observation !== undefined)
                  yield* store.observe(producerForUse(ticket.observation, binding));
              }
              for (const writer of nativeWriters)
                yield* store.observeNativeBinding(nativeForUse(writer, binding));
            }),
          )
          .pipe(Effect.mapError(runtimeUseError));
        uses.set(binding.useId, binding);
      }),
    );
  });
  const seal = Effect.suspend(() => {
    sealed = true;
    return mutex.withPermit(retireIfComplete());
  });
  const withOperation = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        yield* assertActive;
        operations++;
        let active = true;
        return yield* restore(
          effect.pipe(
            Effect.provideService(
              ActiveRuntimeUseOperation,
              Object.freeze({ owner: operationOwner, active: () => active }),
            ),
          ),
        ).pipe(
          Effect.ensuring(
            Effect.suspend(() => {
              active = false;
              operations--;
              return mutex.withPermit(retireIfComplete()).pipe(
                Effect.catchCause((cause) =>
                  Effect.logWarning("jones.runtime-use-retirement-unavailable", {
                    useId: input.binding.useId,
                    cause,
                  }),
                ),
              );
            }),
          ),
        );
      }),
    );
  return { beginProducer, observeBinding, assertActive, seal, withOperation, adoptUse };
});

export class RuntimeUseLifetime extends Context.Service<
  RuntimeUseLifetime,
  Effect.Success<ReturnType<typeof makeRuntimeUseLifetime>>
>()("t3/jones/cleanup/RuntimeUseLifetime") {}
