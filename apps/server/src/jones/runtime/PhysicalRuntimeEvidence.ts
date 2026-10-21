import { PhysicalProcessObservation, ProviderRuntimeBinding } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { PhysicalProcessLease } from "@t3tools/provider-core/server/jones/runtimeTypes";

const decodeObservation = Schema.decodeUnknownEffect(PhysicalProcessObservation);
const encodeObservation = Schema.encodeEffect(Schema.fromJsonString(PhysicalProcessObservation));
const equivalentBinding = Schema.toEquivalence(ProviderRuntimeBinding);

// Persistence is history only. Restarted owners never hydrate a current lease from stored rows.
export const make = Effect.gen(function* () {
  const storage = yield* Effect.serviceOption(SqlClient.SqlClient);
  const leases = new Map<
    string,
    { binding: ProviderRuntimeBinding; lease: PhysicalProcessLease }
  >();
  const unknown = (
    generation: string,
    reason: Extract<PhysicalProcessObservation, { status: "unknown" }>["reason"],
  ): PhysicalProcessObservation => ({ status: "unknown", runtimeGeneration: generation, reason });
  const persist = Effect.fn("PhysicalRuntimeEvidence.persist")(
    function* (binding: ProviderRuntimeBinding, observation: PhysicalProcessObservation) {
      if (Option.isNone(storage)) return false;
      const sql = storage.value;
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const rows =
            yield* sql`SELECT provider_thread_id FROM orchestration_v2_projection_provider_threads
          WHERE provider_thread_id=${binding.providerThreadId} AND thread_id=${binding.threadId}`;
          if (rows.length !== 1) return false;
          const encoded = yield* encodeObservation(observation);
          yield* sql`INSERT INTO jones_physical_process_evidence
          (provider_thread_id, thread_id, runtime_generation, observed_at, observation_json)
          VALUES (${binding.providerThreadId}, ${binding.threadId}, ${binding.runtimeGeneration},
          ${DateTime.formatIso(yield* DateTime.now)}, ${encoded})`;
          return true;
        }),
      );
    },
    Effect.catchCause(() => Effect.succeed(false)),
  );
  const read: (binding: ProviderRuntimeBinding) => Effect.Effect<PhysicalProcessObservation> =
    Effect.fn("PhysicalRuntimeEvidence.read")(function* (
      binding: ProviderRuntimeBinding,
    ): Effect.fn.Return<PhysicalProcessObservation> {
      const entry = leases.get(binding.providerThreadId);
      if (!entry || !equivalentBinding(entry.binding, binding))
        return unknown(binding.runtimeGeneration, "not_captured");
      const captured = yield* entry.lease.capture;
      const decoded = yield* decodeObservation(captured).pipe(
        Effect.catch(() => Effect.succeed(unknown(binding.runtimeGeneration, "malformed"))),
      );
      const observation =
        decoded.runtimeGeneration === binding.runtimeGeneration
          ? decoded
          : unknown(binding.runtimeGeneration, "retired");
      if (leases.get(binding.providerThreadId) !== entry)
        return unknown(binding.runtimeGeneration, "retired");
      if (!(yield* persist(binding, observation)))
        return unknown(binding.runtimeGeneration, "storage_unavailable");
      if (leases.get(binding.providerThreadId) !== entry)
        return unknown(binding.runtimeGeneration, "retired");
      return observation;
    });
  const invalidate = Effect.fn("PhysicalRuntimeEvidence.invalidate")(function* (
    binding: ProviderRuntimeBinding,
  ) {
    const entry = leases.get(binding.providerThreadId);
    if (!entry || entry.binding.runtimeGeneration !== binding.runtimeGeneration) return;
    leases.delete(binding.providerThreadId);
    yield* persist(binding, unknown(binding.runtimeGeneration, "retired"));
  });
  return {
    bind: Effect.fn("PhysicalRuntimeEvidence.bind")(function* (
      binding: ProviderRuntimeBinding,
      lease: PhysicalProcessLease | undefined,
    ) {
      const previous = leases.get(binding.providerThreadId);
      if (
        previous &&
        previous.lease === lease &&
        previous.binding.runtimeGeneration === binding.runtimeGeneration
      )
        return;
      leases.delete(binding.providerThreadId);
      const entry =
        lease && lease.runtimeGeneration === binding.runtimeGeneration
          ? { binding, lease }
          : undefined;
      if (entry) leases.set(binding.providerThreadId, entry);
      if (previous)
        yield* persist(previous.binding, unknown(previous.binding.runtimeGeneration, "retired"));
      if (entry && leases.get(binding.providerThreadId) === entry) yield* read(binding);
    }),
    read,
    invalidate,
    abandon: Effect.fn("PhysicalRuntimeEvidence.abandon")(function* (generation: string) {
      for (const entry of Array.from(leases.values()))
        if (entry.binding.runtimeGeneration === generation) yield* invalidate(entry.binding);
    }),
  };
});
