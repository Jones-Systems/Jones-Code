import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as RuntimeHistory from "../RuntimeHistory.ts";
import {
  AdmissionError,
  canonical,
  digest,
  failure,
  OwnerReference,
  Participant,
  PhysicalScope,
  ScopeObservation,
  storageError,
} from "./Protocol.ts";

const hash = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
export const InventoryEntry = Schema.Struct({
  itemKey: Schema.NonEmptyString,
  source: Schema.Struct({
    domain: Schema.NonEmptyString,
    referenceId: Schema.NonEmptyString,
    revision: Schema.NonEmptyString,
    sha256: hash,
  }),
  scopes: Schema.Array(PhysicalScope),
  disposition: Schema.Literals(["reference", "pending", "entered_unknown", "validated_terminal"]),
  owner: Schema.NullOr(OwnerReference),
  terminalEvidenceSha256: Schema.NullOr(hash),
});
export type InventoryEntry = typeof InventoryEntry.Type;
const decodeCapturedEntries = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Array(InventoryEntry)),
);

const freezeSnapshot = <A>(value: A): A => {
  if (value !== null && typeof value === "object") {
    for (const child of Object.values(value)) freezeSnapshot(child);
    Object.freeze(value);
  }
  return value;
};
export const decodeEntry = Schema.decodeUnknownEffect(InventoryEntry);
export const itemKey = (storeId: string, source: InventoryEntry["source"]) =>
  canonical({
    storeId,
    domain: source.domain,
    referenceId: source.referenceId,
    revision: source.revision,
  });
export const orderedManifest = (entries: readonly InventoryEntry[]) =>
  entries
    .map((entry) => ({ itemKey: entry.itemKey, sha256: digest(entry) }))
    .sort((a, b) => (a.itemKey < b.itemKey ? -1 : a.itemKey > b.itemKey ? 1 : 0));
export const inventoryDigest = (entries: readonly InventoryEntry[]) =>
  digest(orderedManifest(entries));

export const validateEntries = Effect.fn("EnrollmentInventory.validateEntries")(function* (
  storeId: string,
  entries: readonly InventoryEntry[],
) {
  const keys = new Set<string>();
  for (const raw of entries) {
    const entry = yield* decodeEntry(raw).pipe(Effect.mapError(storageError));
    if (
      entry.itemKey !== itemKey(storeId, entry.source) ||
      keys.has(entry.itemKey) ||
      (entry.scopes.length === 0 && entry.disposition !== "entered_unknown") ||
      (entry.disposition === "validated_terminal") !== (entry.terminalEvidenceSha256 !== null) ||
      (entry.owner !== null && entry.owner.storeId !== storeId)
    )
      return yield* failure("inventory_entry_invalid");
    keys.add(entry.itemKey);
  }
});

/** Each adapter reads and decodes its actual complete domain, including retained terminal history.
 * readLocal/revalidateLocal run in local SQL only; locate runs outside every SQL transaction.
 * Root must supply every required domain; absence is a hold, never an empty successful scan. */
export class InventoryReaders extends Context.Service<
  InventoryReaders,
  {
    readonly coverageContract: string;
    readonly requiredDomains: readonly string[];
    readonly readers: readonly {
      readonly domain: string;
      readonly readLocal: Effect.Effect<
        { readonly snapshotDigest: string; readonly records: readonly unknown[] },
        AdmissionError
      >;
      readonly locate: (
        records: readonly unknown[],
      ) => Effect.Effect<readonly InventoryEntry[], AdmissionError>;
      readonly revalidateLocal: (snapshotDigest: string) => Effect.Effect<void, AdmissionError>;
    }[];
  }
>()("t3/jones/cleanup/sharedAdmission/EnrollmentInventory/InventoryReaders") {}
export class EnrollmentInventory extends Context.Service<
  EnrollmentInventory,
  {
    readonly capture: Effect.Effect<
      {
        readonly entries: readonly InventoryEntry[];
        readonly inventoryDigest: string;
        readonly sourceSnapshotDigest: string;
        readonly coverageContract: string;
        readonly revalidate: Effect.Effect<void, AdmissionError>;
      },
      AdmissionError
    >;
  }
>()("t3/jones/cleanup/sharedAdmission/EnrollmentInventory") {}

const productionDomains = [
  "ordinaryOwners",
  "ordinaryAdmissions",
  "ordinaryUses",
  "preparation",
  "nativeCreation",
  "deletionLifecycleRetirement",
  "legacyLeases",
  "providerJournals",
  "terminalJournals",
  "runtimeJournals",
  "workspaceReferences",
  "pendingEffects",
  "workspaceDuties",
] as const;

const makeWithDomains = Effect.fn("EnrollmentInventory.make")(function* (
  requiredDomains: readonly string[],
) {
  const participant = yield* Participant;
  const adapters = yield* InventoryReaders;
  const observations = yield* ScopeObservation;
  const sql = participant.sql;
  const domains = adapters.readers.map((reader) => reader.domain).sort();
  if (
    !adapters.coverageContract ||
    adapters.requiredDomains.length === 0 ||
    canonical(domains) !== canonical([...new Set(requiredDomains)].sort()) ||
    canonical([...adapters.requiredDomains].sort()) !== canonical([...requiredDomains].sort()) ||
    new Set(domains).size !== domains.length
  )
    return yield* failure("inventory_domain_coverage_incomplete");
  const outside = Effect.gen(function* () {
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
      return yield* failure("inventory_called_inside_local_transaction");
    yield* participant.checkOutside;
  });
  return EnrollmentInventory.of({
    capture: Effect.gen(function* () {
      yield* outside;
      // One consistent short snapshot covers every adapter, not MAX(event sequence).
      const sources = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* participant.validateLocal;
            const rows = [];
            for (const reader of adapters.readers)
              rows.push({ reader, snapshot: yield* reader.readLocal });
            return rows;
          }),
        )
        .pipe(Effect.mapError(storageError));
      const entries: InventoryEntry[] = [];
      for (const source of sources) {
        const located = yield* source.reader.locate(source.snapshot.records);
        if (located.some((entry) => entry.source.domain !== source.reader.domain))
          return yield* failure("inventory_domain_mismatch");
        entries.push(...located);
      }
      const capturedEntries = freezeSnapshot(
        yield* decodeCapturedEntries(canonical(entries)).pipe(Effect.mapError(storageError)),
      );
      yield* validateEntries(participant.identity.storeId, capturedEntries);
      const revalidate = Effect.gen(function* () {
        yield* outside;
        for (const entry of capturedEntries)
          for (const scope of entry.scopes) yield* observations.revalidate(scope);
        yield* sql
          .withTransaction(
            Effect.gen(function* () {
              yield* participant.validateLocal;
              for (const source of sources)
                yield* source.reader.revalidateLocal(source.snapshot.snapshotDigest);
            }),
          )
          .pipe(Effect.mapError(storageError));
      });
      yield* revalidate;
      return {
        entries: capturedEntries,
        inventoryDigest: inventoryDigest(capturedEntries),
        sourceSnapshotDigest: digest(
          sources
            .map((source) => ({
              domain: source.reader.domain,
              digest: source.snapshot.snapshotDigest,
            }))
            .sort((a, b) => (a.domain < b.domain ? -1 : a.domain > b.domain ? 1 : 0)),
        ),
        coverageContract: adapters.coverageContract,
        revalidate,
      };
    }),
  });
});

/** Production cannot declare a partial domain list complete. Root supplies actual read validators for every domain. */
export const make = makeWithDomains(productionDomains);

/** Explicit synthetic-domain boundary; never composed by production startup. */
export const makeForSynthetic = Effect.gen(function* () {
  const readers = yield* InventoryReaders;
  if (!readers.coverageContract.startsWith("synthetic"))
    return yield* failure("inventory_synthetic_coverage_required");
  return yield* makeWithDomains(readers.requiredDomains);
});

/** Concrete read-only adapter over RuntimeHistory's real full-history decoder.
 * Its path-level blocker output is conservative when several uses share a path:
 * all records at a blocked path stay unknown until exact per-use reconciliation.
 * This supplies ONE domain and cannot satisfy productionDomains by itself. */
export const makeRuntimeReader = Effect.gen(function* () {
  const participant = yield* Participant;
  const sql = yield* SqlClient.SqlClient;
  const observations = yield* ScopeObservation;
  if (sql !== participant.sql) return yield* failure("runtime_inventory_wrong_actual_client");
  const history = yield* RuntimeHistory.makeRuntimeHistory;
  type Snapshot = Effect.Success<ReturnType<typeof history.read>>;
  const snapshots = new WeakMap<readonly unknown[], Snapshot>();
  const readLocal = Effect.gen(function* () {
    yield* participant.validateLocal;
    const snapshot = freezeSnapshot(yield* history.read().pipe(Effect.mapError(storageError)));
    const records: readonly unknown[] = Object.freeze([...snapshot.records]);
    snapshots.set(records, snapshot);
    return { records, snapshotDigest: digest(snapshot) };
  });
  const locate = Effect.fn("EnrollmentInventory.locateRuntimeHistory")(function* (
    records: readonly unknown[],
  ) {
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
      return yield* failure("runtime_inventory_location_inside_sql");
    const snapshot = snapshots.get(records);
    if (snapshot === undefined) return yield* failure("runtime_inventory_not_decoded_snapshot");
    const entries: InventoryEntry[] = [];
    for (const record of snapshot.records) {
      const source = {
        domain: "runtimeJournals",
        referenceId: record.binding.useId,
        revision: String(record.lastOrdinal),
        sha256: digest(record),
      };
      const observed = yield* observations.capture(record.binding.canonicalPath).pipe(
        Effect.map((scope) => [scope] as readonly PhysicalScope[]),
        Effect.orElseSucceed(() => [] as readonly PhysicalScope[]),
      );
      const terminal =
        record.retirement !== null &&
        observed.length !== 0 &&
        !snapshot.blockers.some((blocker) => blocker.target === record.binding.canonicalPath);
      const currentIdentity = observed[0]?.ancestors[0].identity;
      const scopes =
        !terminal &&
        canonical(currentIdentity ?? null) !== canonical(record.binding.directoryIdentity)
          ? []
          : observed;
      entries.push({
        itemKey: itemKey(participant.identity.storeId, source),
        source,
        scopes,
        disposition: terminal ? "validated_terminal" : "entered_unknown",
        owner: null,
        terminalEvidenceSha256: terminal ? digest(record) : null,
      });
    }
    return entries;
  });
  return {
    domain: "runtimeJournals",
    readLocal,
    locate,
    revalidateLocal: Effect.fn("EnrollmentInventory.revalidateRuntimeHistory")(function* (
      expected: string,
    ) {
      yield* participant.validateLocal;
      const snapshot = yield* history.read().pipe(Effect.mapError(storageError));
      if (digest(snapshot) !== expected) return yield* failure("runtime_inventory_history_changed");
    }),
  } satisfies InventoryReaders["Service"]["readers"][number];
});
