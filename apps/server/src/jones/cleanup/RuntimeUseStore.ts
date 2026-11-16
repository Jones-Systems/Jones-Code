import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import {
  canonical,
  decodeRuntimeUseBinding,
  decodeRuntimeUseProducer,
  decodeRuntimeUseNativeBinding,
  decodeRuntimeUseRetirement,
  digest,
  RuntimeUseError,
  runtimeUseError,
  RuntimeUseProducer,
  type RuntimeUseNativeBinding,
  type RuntimeUseRetirement,
} from "./RuntimeUseProtocol.ts";

const rowSchema = Schema.Struct({
  use_id: Schema.String,
  binding_json: Schema.String,
  binding_sha256: Schema.String,
  state: Schema.Literals(["pending", "retired"]),
  last_ordinal: Schema.Int,
  retirement_json: Schema.NullOr(Schema.String),
});
const decodeRow = Schema.decodeUnknownEffect(rowSchema, { onExcessProperty: "error" });
const parseJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const fail = (reason: string) => Effect.fail(new RuntimeUseError({ reason }));
const isProducer = Schema.is(RuntimeUseProducer);

// Persistence only. This module grants no executor, replay, coverage or retirement
// capability. Its production caller must retain the original live authority.
export const makeRuntimeUseStore = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const read = Effect.fn("RuntimeUseStore.read")(function* (useId: string) {
    const rows =
      yield* sql`SELECT use_id,binding_json,binding_sha256,state,last_ordinal,retirement_json
      FROM jones_workspace_runtime_uses WHERE use_id=${useId}`;
    if (rows.length === 0) return null;
    const row = yield* decodeRow(rows[0]);
    const binding = yield* decodeRuntimeUseBinding(yield* parseJson(row.binding_json));
    if (
      binding.useId !== row.use_id ||
      digest(binding) !== row.binding_sha256 ||
      canonical(binding) !== row.binding_json
    )
      return yield* fail("runtime_use_binding_corrupt");
    const events = yield* sql<{
      readonly ordinal: number;
      readonly action: string;
      readonly binding_sha256: string;
      readonly evidence_json: string;
      readonly evidence_sha256: string;
    }>`SELECT ordinal,action,binding_sha256,evidence_json,evidence_sha256
      FROM jones_workspace_runtime_use_events WHERE use_id=${useId} ORDER BY ordinal`;
    if (events.length !== row.last_ordinal + 1)
      return yield* fail("runtime_use_history_incomplete");
    const producers: RuntimeUseProducer[] = [];
    const nativeBindings: RuntimeUseNativeBinding[] = [];
    let retirement: RuntimeUseRetirement | null = null;
    for (const [index, event] of events.entries()) {
      if (event.ordinal !== index || event.binding_sha256 !== row.binding_sha256)
        return yield* fail("runtime_use_history_identity");
      const value = yield* parseJson(event.evidence_json);
      if (digest(value) !== event.evidence_sha256 || canonical(value) !== event.evidence_json)
        return yield* fail("runtime_use_history_digest");
      if (index === 0) {
        if (event.action !== "reserved" || event.evidence_json !== row.binding_json)
          return yield* fail("runtime_use_reservation_history");
      } else if (event.action === "observed") {
        if (!isProducer(value)) {
          const observed = yield* decodeRuntimeUseNativeBinding(value);
          const producer = producers.find(
            (item) =>
              item.producerId === observed.producerId && item.generation === observed.generation,
          );
          const actor = binding.actor;
          if (
            retirement !== null ||
            producer === undefined ||
            observed.useId !== useId ||
            observed.bindingSha256 !== row.binding_sha256 ||
            observed.binding.runtimeGeneration !== observed.generation ||
            actor.kind === "terminal_spawn" ||
            observed.binding.providerSessionId !== actor.provider.sessionId ||
            observed.binding.providerInstanceId !== actor.provider.instanceId ||
            observed.binding.driver !== actor.provider.driver ||
            nativeBindings.some(
              (other) =>
                other.producerId === observed.producerId &&
                (other.binding.providerThreadId === observed.binding.providerThreadId ||
                  other.binding.nativeThreadId === observed.binding.nativeThreadId),
            )
          )
            return yield* fail("runtime_use_native_binding_identity");
          nativeBindings.push(observed);
          continue;
        }
        const producer = yield* decodeRuntimeUseProducer(value);
        if (
          retirement !== null ||
          producer.useId !== useId ||
          producer.bindingSha256 !== row.binding_sha256 ||
          producers.some(
            (other) =>
              other.producerId === producer.producerId || other.generation === producer.generation,
          )
        )
          return yield* fail("runtime_use_producer_identity");
        producers.push(producer);
      } else if (event.action === "retired") {
        if (retirement !== null || index !== row.last_ordinal)
          return yield* fail("runtime_use_retirement_order");
        retirement = yield* decodeRuntimeUseRetirement(value);
        if (
          retirement.useId !== useId ||
          retirement.bindingSha256 !== row.binding_sha256 ||
          retirement.observationCount !== producers.length + nativeBindings.length
        )
          return yield* fail("runtime_use_retirement_identity");
        if (retirement.outcome === "producers_exited") {
          const actor = binding.actor;
          if (
            actor.kind === "terminal_spawn" ||
            producers.length === 0 ||
            retirement.producers.length !== producers.length ||
            canonical(retirement.nativeBindings) !== canonical(nativeBindings)
          )
            return yield* fail("runtime_use_retirement_coverage");
          for (const [producerIndex, original] of producers.entries()) {
            const proof = retirement.producers[producerIndex];
            if (
              proof === undefined ||
              proof.producerId !== original.producerId ||
              proof.generation !== original.generation ||
              proof.threadId !== original.threadId ||
              proof.observationSha256 !== digest(original) ||
              proof.providerSessionId !== actor.provider.sessionId ||
              proof.instanceId !== actor.provider.instanceId ||
              proof.driver !== actor.provider.driver ||
              proof.writerBindingsSha256 !==
                digest(nativeBindings.filter((item) => item.producerId === original.producerId))
            )
              return yield* fail("runtime_use_retirement_original_producer_mismatch");
          }
        }
      } else return yield* fail("runtime_use_history_action");
    }
    if (
      (row.state === "retired") !== (retirement !== null) ||
      (retirement === null
        ? row.retirement_json !== null
        : canonical(retirement) !== row.retirement_json)
    )
      return yield* fail("runtime_use_current_history_mismatch");
    return {
      binding,
      state: row.state,
      lastOrdinal: row.last_ordinal,
      producers,
      nativeBindings,
      retirement,
    };
  }, Effect.mapError(runtimeUseError));

  const reserve = Effect.fn("RuntimeUseStore.reserve")(function* (input: unknown) {
    const binding = yield* decodeRuntimeUseBinding(input);
    if (
      !binding.canonicalPath.startsWith("/") ||
      binding.canonicalPath.includes("//") ||
      binding.canonicalPath.split("/").some((part) => part === "." || part === "..") ||
      (binding.canonicalPath.length > 1 && binding.canonicalPath.endsWith("/"))
    )
      return yield* fail("runtime_use_path_not_canonical");
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const existing = yield* read(binding.useId);
        if (existing !== null) {
          if (canonical(existing.binding) !== canonical(binding))
            return yield* fail("runtime_use_identity_changed");
          // A pending record means possibly effective. It never authorizes replay.
          return { created: false, record: existing };
        }
        const actor = binding.actor;
        yield* sql`INSERT INTO jones_workspace_runtime_uses
        (use_id,canonical_path,thread_id,kind,provider_session_id,terminal_session_id,binding_json,binding_sha256,state,last_ordinal,retirement_json)
        VALUES (${binding.useId},${binding.canonicalPath},${binding.threadId},${actor.kind},
          ${actor.kind === "terminal_spawn" ? null : actor.provider.sessionId},
          ${actor.kind === "terminal_spawn" ? actor.terminalSessionId : null},
          ${canonical(binding)},${digest(binding)},'pending',0,NULL)`;
        const record = yield* read(binding.useId);
        if (record === null) return yield* fail("runtime_use_reservation_missing");
        return { created: true, record };
      }),
    );
  }, Effect.mapError(runtimeUseError));

  const append = Effect.fn("RuntimeUseStore.append")(function* (
    useId: string,
    action: "observed" | "retired",
    evidence: RuntimeUseProducer | RuntimeUseNativeBinding | RuntimeUseRetirement,
  ) {
    const record = yield* read(useId);
    if (record === null) return yield* fail("runtime_use_reservation_missing");
    if (evidence.useId !== useId || evidence.bindingSha256 !== digest(record.binding))
      return yield* fail("runtime_use_evidence_binding_changed");
    if (action === "retired" && record.retirement !== null) {
      if (canonical(record.retirement) !== canonical(evidence))
        return yield* fail("runtime_use_retirement_changed");
      return record;
    }
    if (record.state !== "pending") return yield* fail("runtime_use_already_retired");
    yield* sql`INSERT INTO jones_workspace_runtime_use_events
        (use_id,ordinal,action,binding_sha256,evidence_json,evidence_sha256)
        VALUES (${useId},${record.lastOrdinal + 1},${action},${digest(record.binding)},${canonical(evidence)},${digest(evidence)})`;
    // Decode the entire resulting history before commit. SQL protects physical
    // identity/order; this check also enforces the versioned semantic contract.
    return yield* read(useId);
  });
  const observe = Effect.fn("RuntimeUseStore.observe")(function* (input: unknown) {
    const producer = yield* decodeRuntimeUseProducer(input);
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const record = yield* read(producer.useId);
        if (record === null) return yield* fail("runtime_use_reservation_missing");
        const prior = record.producers.find((other) => other.producerId === producer.producerId);
        if (prior !== undefined) {
          if (canonical(prior) !== canonical(producer))
            return yield* fail("runtime_use_producer_changed");
          return record;
        }
        return yield* append(producer.useId, "observed", producer);
      }),
    );
  }, Effect.mapError(runtimeUseError));
  const recordRetirement = Effect.fn("RuntimeUseStore.recordRetirement")(function* (
    input: unknown,
  ) {
    const retirement = yield* decodeRuntimeUseRetirement(input);
    return yield* sql.withTransaction(append(retirement.useId, "retired", retirement));
  }, Effect.mapError(runtimeUseError));
  const observeNativeBinding = Effect.fn("RuntimeUseStore.observeNativeBinding")(function* (
    input: unknown,
  ) {
    const observed = yield* decodeRuntimeUseNativeBinding(input);
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const record = yield* read(observed.useId);
        if (record === null) return yield* fail("runtime_use_reservation_missing");
        const prior = record.nativeBindings.find(
          (other) =>
            other.producerId === observed.producerId &&
            (other.binding.providerThreadId === observed.binding.providerThreadId ||
              other.binding.nativeThreadId === observed.binding.nativeThreadId),
        );
        if (prior !== undefined) {
          if (canonical(prior) !== canonical(observed))
            return yield* fail("runtime_use_native_binding_changed");
          return record;
        }
        return yield* append(observed.useId, "observed", observed);
      }),
    );
  }, Effect.mapError(runtimeUseError));
  const inventory = Effect.fn("RuntimeUseStore.inventory")(function* () {
    // Decode all rows, including retired history. Corrupt terminal history must
    // not disappear from admission simply because its state text says retired.
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const ids = yield* sql<{
          readonly use_id: string;
        }>`SELECT use_id FROM jones_workspace_runtime_uses ORDER BY use_id`;
        return yield* Effect.forEach(ids, ({ use_id }) =>
          read(use_id).pipe(
            Effect.filterOrFail(
              (record) => record !== null,
              () => new RuntimeUseError({ reason: "runtime_use_inventory_changed" }),
            ),
          ),
        );
      }),
    );
  }, Effect.mapError(runtimeUseError));
  const pending = Effect.fn("RuntimeUseStore.pending")(function* () {
    const records = yield* inventory();
    return records.filter((record) => record.state === "pending").map((record) => record.binding);
  });
  return { reserve, read, observe, observeNativeBinding, recordRetirement, inventory, pending };
});
