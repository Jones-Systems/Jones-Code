import * as NodeCrypto from "node:crypto";
import {
  QualifiedQuota,
  type ProviderInstanceId,
  type ProviderQueueInventory,
  type ProviderQueueRefreshResult,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import { ServerConfig } from "../config.ts";
import { writeFileStringAtomically } from "../atomicWrite.ts";
import type { ProviderRegistryShape } from "./Services/ProviderRegistry.ts";
import {
  quotaResetCrossed,
  PrivateQuotaProof,
  readPrivateQuotaProof,
  samePrivateQuotaProof,
} from "./qualifiedQuota.ts";

const PROVIDER_QUEUE_REFRESH_MS = 300_000;
const MAX_STATE_BYTES = 2 * 1024 * 1024;
const LegacyAttempt = Schema.Struct({
  instanceId: Schema.String,
  attemptedAt: Schema.Finite,
  resetRefreshUsed: Schema.Boolean,
  quota: Schema.NullOr(QualifiedQuota),
});
const Attempt = Schema.Struct({
  ...LegacyAttempt.fields,
  proof: Schema.NullOr(PrivateQuotaProof),
});
const QueueState = Schema.Struct({
  version: Schema.Literal(2),
  attempts: Schema.Array(Attempt),
});
const StoredQueueState = Schema.Union([
  QueueState,
  Schema.Struct({
    version: Schema.Literal(1),
    attempts: Schema.Array(LegacyAttempt),
  }),
]);
export const decodeProviderQueueState = Effect.fnUntraced(function* (contents: string) {
  const state = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(StoredQueueState))(
    contents,
  );
  if (state.version === 2) return state;
  return {
    version: 2 as const,
    attempts: state.attempts.map((attempt) => ({
      ...attempt,
      proof: null,
      quota:
        attempt.quota?.status === "qualified"
          ? {
              ...attempt.quota,
              status: "unknown" as const,
              complete: false,
              failureCode: "refresh_not_observed" as const,
            }
          : attempt.quota,
    })),
  };
});
type QueueState = typeof QueueState.Type;
type Attempt = typeof Attempt.Type;
function cachedQuota(
  provider: ServerProvider,
  attempt: Attempt,
  now: number,
): QualifiedQuota | null {
  const source = provider.qualifiedQuota;
  const previous = attempt.quota;
  if (!source && previous?.status === "qualified") {
    return { ...previous, status: "unknown", complete: false, failureCode: "refresh_not_observed" };
  }
  const newer =
    source &&
    source.instanceId === provider.instanceId &&
    Date.parse(source.attemptedAt) >= attempt.attemptedAt &&
    (!previous ||
      (source.probeId !== previous.probeId &&
        Date.parse(source.probeCompletedAt) >= Date.parse(previous.probeCompletedAt)) ||
      previous.failureCode === "interrupted_attempt");
  const quota = newer ? source : previous;
  if (quota?.status === "qualified") {
    const sourceProof = source ? readPrivateQuotaProof(source) : undefined;
    if (!sourceProof || (!newer && !samePrivateQuotaProof(attempt.proof, sourceProof))) {
      return { ...quota, status: "unknown", complete: false, failureCode: "refresh_not_observed" };
    }
  }
  return quota && quotaResetCrossed(quota, now)
    ? { ...quota, status: "unknown", complete: false, failureCode: "reset_crossed" }
    : quota;
}
export class ProviderQueueStorageError extends Schema.TaggedError<ProviderQueueStorageError>()(
  "ProviderQueueStorageError",
  { code: Schema.Literal("state_unavailable") },
) {}
export interface ProviderQueueStorage {
  readonly read: Effect.Effect<QueueState, ProviderQueueStorageError>;
  readonly write: (state: QueueState) => Effect.Effect<void, ProviderQueueStorageError>;
}
const iso = (millis: number) => DateTime.formatIso(DateTime.makeUnsafe(millis));

export const makeProviderQueueStorage = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const config = yield* ServerConfig;
  const directory = path.join(config.stateDir, "provider-queue");
  const filePath = path.join(directory, "refresh-state.json");
  const failure = () => new ProviderQueueStorageError({ code: "state_unavailable" });
  return {
    read: Effect.gen(function* () {
      if (!(yield* fs.exists(filePath))) return { version: 2 as const, attempts: [] };
      const stat = yield* fs.stat(filePath);
      if (Number(stat.size) > MAX_STATE_BYTES) return yield* failure();
      return yield* decodeProviderQueueState(yield* fs.readFileString(filePath));
    }).pipe(Effect.mapError(failure)),
    write: (state: QueueState) =>
      Effect.gen(function* () {
        const contents = yield* Schema.encodeEffect(Schema.fromJsonString(QueueState))(state);
        if (Buffer.byteLength(contents) > MAX_STATE_BYTES) return yield* failure();
        yield* fs.makeDirectory(directory, { recursive: true, mode: 0o700 });
        yield* fs.chmod(directory, 0o700);
        yield* writeFileStringAtomically({ filePath, contents }).pipe(
          Effect.provideService(FileSystem.FileSystem, fs),
          Effect.provideService(Path.Path, path),
        );
        yield* fs.chmod(filePath, 0o600);
      }).pipe(Effect.mapError(failure)),
  } satisfies ProviderQueueStorage;
});

export function projectQueueInventory(
  providers: ReadonlyArray<ServerProvider>,
  observedAt: string,
): ProviderQueueInventory {
  const instances: ProviderQueueInventory["instances"] = providers
    .filter((provider) => provider.driver === "codex")
    .map((provider) => ({
      instanceId: provider.instanceId,
      displayName: provider.displayName ?? "Codex",
      driver: "codex" as const,
      enabled: provider.enabled,
      capabilityRefs:
        provider.qualifiedQuota && readPrivateQuotaProof(provider.qualifiedQuota)
          ? provider.qualifiedQuota.capabilityRefs
          : [],
    }))
    .sort((a, b) => a.instanceId.localeCompare(b.instanceId));
  return {
    schemaVersion: "t3.provider-queue-inventory/v1",
    inventoryRevision: NodeCrypto.createHash("sha256")
      .update(JSON.stringify(instances))
      .digest("hex"),
    observedAt,
    evidence: "configured-provider-registry",
    instances,
  };
}

export const makeProviderQueue = Effect.fn("makeProviderQueue")(function* (
  registry: Pick<ProviderRegistryShape, "getProviders" | "refreshInstance">,
  storage: ProviderQueueStorage,
  currentTime: Effect.Effect<number> = Clock.currentTimeMillis,
) {
  const serial = yield* Semaphore.make(1);
  let state: QueueState | undefined;
  const inventory = Effect.gen(function* () {
    return projectQueueInventory(yield* registry.getProviders, iso(yield* currentTime));
  });
  const usage = (instanceId: ProviderInstanceId) =>
    serial.withPermits(1)(
      Effect.gen(function* () {
        const provider = (yield* registry.getProviders).find(
          (candidate) => candidate.instanceId === instanceId,
        );
        if (!provider)
          return {
            instanceId,
            status: "unknown_instance",
            nextRefreshAt: null,
            quota: null,
          } satisfies ProviderQueueRefreshResult;
        if (provider.driver !== "codex" || !provider.enabled)
          return {
            instanceId,
            status: "unsupported",
            nextRefreshAt: null,
            quota: null,
          } satisfies ProviderQueueRefreshResult;
        state ??= yield* storage.read;
        const attempt = state.attempts.find((entry) => entry.instanceId === instanceId);
        const now = yield* currentTime;
        return {
          instanceId,
          status: "cached",
          nextRefreshAt: attempt ? iso(attempt.attemptedAt + PROVIDER_QUEUE_REFRESH_MS) : null,
          quota: attempt ? cachedQuota(provider, attempt, now) : null,
        } satisfies ProviderQueueRefreshResult;
      }),
    );
  const refresh = (instanceId: ProviderInstanceId) =>
    serial.withPermits(1)(
      Effect.gen(function* () {
        const providers = yield* registry.getProviders;
        const provider = providers.find((candidate) => candidate.instanceId === instanceId);
        if (!provider)
          return {
            instanceId,
            status: "unknown_instance",
            nextRefreshAt: null,
            quota: null,
          } satisfies ProviderQueueRefreshResult;
        if (provider.driver !== "codex" || !provider.enabled)
          return {
            instanceId,
            status: "unsupported",
            nextRefreshAt: null,
            quota: null,
          } satisfies ProviderQueueRefreshResult;
        state ??= yield* storage.read;
        const now = yield* currentTime;
        const previous = state.attempts.find((attempt) => attempt.instanceId === instanceId);
        const crossed = previous?.quota ? quotaResetCrossed(previous.quota, now) : false;
        const due = !previous || now >= previous.attemptedAt + PROVIDER_QUEUE_REFRESH_MS;
        const resetRefresh = crossed && previous?.resetRefreshUsed === false;
        if (previous && !due && !resetRefresh) {
          return {
            instanceId,
            status: "cached",
            nextRefreshAt: iso(previous.attemptedAt + PROVIDER_QUEUE_REFRESH_MS),
            quota: cachedQuota(provider, previous, now),
          } satisfies ProviderQueueRefreshResult;
        }
        const attemptId = NodeCrypto.randomUUID();
        const unknown: QualifiedQuota = {
          schemaVersion: "codex.t3-qualified-quota/v1",
          instanceId,
          probeId: attemptId,
          status: "unknown",
          attemptedAt: iso(now),
          quotaReceivedAt: null,
          probeCompletedAt: iso(now),
          complete: false,
          rateLimitsByLimitId: null,
          windowProvenance: [],
          capabilityRefs: [],
          failureCode: "interrupted_attempt",
        };
        const attempt = {
          instanceId,
          attemptedAt: now,
          resetRefreshUsed: !due && resetRefresh,
          quota: unknown,
          proof: null,
        };
        const configured = new Set(providers.map((item) => item.instanceId));
        const remaining = state.attempts.filter(
          (item) =>
            item.instanceId !== instanceId && configured.has(item.instanceId as ProviderInstanceId),
        );
        const pending: QueueState = { version: 2, attempts: [...remaining, attempt] };
        yield* storage.write(pending);
        state = pending;
        const result = yield* registry
          .refreshInstance(instanceId)
          .pipe(Effect.timeoutOption("20 seconds"));
        const completed = yield* currentTime;
        const refreshed =
          result._tag === "Some"
            ? result.value.find((item) => item.instanceId === instanceId)?.qualifiedQuota
            : undefined;
        const fresh =
          refreshed &&
          refreshed.instanceId === instanceId &&
          refreshed.probeId !== provider.qualifiedQuota?.probeId &&
          Date.parse(refreshed.attemptedAt) >= now &&
          Date.parse(refreshed.probeCompletedAt) >= now &&
          (refreshed.quotaReceivedAt === null || Date.parse(refreshed.quotaReceivedAt) >= now);
        let quota: QualifiedQuota = fresh
          ? refreshed
          : {
              ...unknown,
              status: "failed",
              probeCompletedAt: iso(completed),
              failureCode: result._tag === "None" ? "probe_timeout" : "refresh_not_observed",
            };
        const proof = refreshed ? readPrivateQuotaProof(refreshed) : undefined;
        if (quota.status === "qualified" && !proof) {
          quota = {
            ...quota,
            status: "unknown",
            complete: false,
            failureCode: "refresh_not_observed",
          };
        }
        if (quotaResetCrossed(quota, completed))
          quota = { ...quota, status: "unknown", complete: false, failureCode: "reset_crossed" };
        const finished: QueueState = {
          version: 2,
          attempts: [...remaining, { ...attempt, quota, proof: proof ?? null }],
        };
        yield* storage.write(finished);
        state = finished;
        return {
          instanceId,
          status: "refreshed",
          nextRefreshAt: iso(now + PROVIDER_QUEUE_REFRESH_MS),
          quota,
        } satisfies ProviderQueueRefreshResult;
      }),
    );
  return { inventory, usage, refresh };
});
