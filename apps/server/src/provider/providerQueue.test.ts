import { it } from "@effect/vitest";
import { describe, expect } from "vite-plus/test";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  type QualifiedQuota,
  type ServerProvider,
} from "@t3tools/contracts";
import {
  makeProviderQueue,
  projectQueueInventory,
  decodeProviderQueueState,
  type ProviderQueueStorage,
  ProviderQueueStorageError,
} from "./providerQueue.ts";
import { mergeProviderSnapshot } from "./ProviderRegistry.ts";
import { withInstanceIdentity } from "./Drivers/instanceIdentity.ts";
import {
  makeQualifiedQuota,
  readPrivateQuotaProof,
  beginQuotaProbe,
  resetQuotaBinding,
} from "./qualifiedQuota.ts";

const start = Date.parse("2026-09-30T12:00:00.000Z");
const encodeLegacyState = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const stamp = (n = start) => DateTime.formatIso(DateTime.makeUnsafe(n));
const instanceId = ProviderInstanceId.make("codex-one");
const buckets = {
  codex: {
    primary: { usedPercent: 25, windowDurationMins: 300, resetsAt: start / 1000 + 600 },
    limitName: "Codex",
  },
  other: {
    secondary: { usedPercent: 35, windowDurationMins: 10080, resetsAt: start / 1000 + 604800 },
    credits: { hasCredits: true, unlimited: false, balance: "5" },
  },
};
const quota = (
  now = start,
  probeId = "00000000-0000-4000-8000-000000000001",
  extra: Partial<Parameters<typeof makeQualifiedQuota>[0]> = {},
): QualifiedQuota =>
  makeQualifiedQuota({
    instanceId,
    probeId,
    ordinaryUsageAllowed: true,
    attemptedAt: stamp(now),
    quotaReceivedAt: stamp(now),
    probeCompletedAt: stamp(now),
    rateLimitsByLimitId: buckets,
    ...extra,
  });
const provider = (overrides: Partial<ServerProvider> = {}): ServerProvider => ({
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  displayName: "Same Name",
  enabled: true,
  installed: true,
  version: null,
  status: "ready",
  auth: { status: "authenticated", email: "private@example.com" },
  checkedAt: stamp(),
  models: [],
  slashCommands: [],
  skills: [],
  ...overrides,
});
function fixture() {
  resetQuotaBinding(instanceId);
  let now = start;
  let calls = 0;
  let providers = [provider()];
  let stored: Parameters<ProviderQueueStorage["write"]>[0] = { version: 2, attempts: [] };
  const storage: ProviderQueueStorage = {
    read: Effect.sync(() => structuredClone(stored)),
    write: (state) =>
      Effect.sync(() => {
        stored = structuredClone(state);
      }),
  };
  const registry = {
    getProviders: Effect.sync(() => providers),
    refreshInstance: () =>
      Effect.sync(() => {
        calls++;
        providers = [
          provider({
            qualifiedQuota: quota(
              now,
              `00000000-0000-4000-8000-${String(calls).padStart(12, "0")}`,
            ),
          }),
        ];
        return providers;
      }),
  };
  return {
    storage,
    registry,
    time: Effect.sync(() => now),
    calls: () => calls,
    advance: (ms: number) => {
      now += ms;
    },
    stored: () => stored,
    replace: (value: ServerProvider[]) => {
      providers = value;
    },
  };
}

describe("qualified native quota", () => {
  it("preserves every bucket and exact window provenance without guessing", () => {
    const result = quota();
    expect(result.status).toBe("qualified");
    expect(result.rateLimitsByLimitId).toEqual(buckets);
    expect(result.windowProvenance.map((entry) => [entry.limitId, entry.windowId])).toEqual([
      ["codex", "primary"],
      ["other", "secondary"],
    ]);
    expect(
      makeQualifiedQuota({
        instanceId,
        probeId: "p",
        ordinaryUsageAllowed: true,
        attemptedAt: stamp(),
        probeCompletedAt: stamp(),
      }).status,
    ).toBe("unknown");
    expect(
      makeQualifiedQuota({
        instanceId,
        probeId: "p",
        ordinaryUsageAllowed: true,
        attemptedAt: stamp(),
        probeCompletedAt: stamp(),
        quotaReceivedAt: stamp(),
        rateLimitsByLimitId: { codex: { primary: { usedPercent: 5 } } },
      }).complete,
    ).toBe(false);
  });
  it("exports distinct routing IDs despite duplicate names and excludes private fields", () => {
    const result = projectQueueInventory(
      [
        provider(),
        provider({ instanceId: ProviderInstanceId.make("two"), status: "error" }),
        provider({ driver: ProviderDriverKind.make("opencode") }),
      ],
      stamp(),
    );
    expect(result.instances).toHaveLength(2);
    expect(new Set(result.instances.map((entry) => entry.instanceId)).size).toBe(2);
    expect(JSON.stringify(result)).not.toContain("private@example.com");
    expect(JSON.stringify(result)).not.toContain("auth");
  });
  it.effect(
    "single-flights, preserves receipt timestamps while cached, and resumes cadence after restart",
    () =>
      Effect.gen(function* () {
        const f = fixture();
        const queue = yield* makeProviderQueue(f.registry, f.storage, f.time);
        const results = yield* Effect.all([queue.refresh(instanceId), queue.refresh(instanceId)], {
          concurrency: "unbounded",
        });
        expect(f.calls()).toBe(1);
        expect(results[0]?.quota).toEqual(results[1]?.quota);
        f.advance(299999);
        const restarted = yield* makeProviderQueue(f.registry, f.storage, f.time);
        expect((yield* restarted.refresh(instanceId)).status).toBe("cached");
        expect(f.calls()).toBe(1);
        f.advance(1);
        expect((yield* restarted.refresh(instanceId)).status).toBe("refreshed");
        expect(f.calls()).toBe(2);
      }),
  );
  it.effect("holds interrupted attempts after restart and rejects unknown IDs before probing", () =>
    Effect.gen(function* () {
      const f = fixture();
      yield* f.storage.write({
        version: 2,
        attempts: [
          { instanceId, attemptedAt: start, resetRefreshUsed: false, quota: null, proof: null },
        ],
      });
      const queue = yield* makeProviderQueue(f.registry, f.storage, f.time);
      expect((yield* queue.refresh(instanceId)).quota).toBeNull();
      expect((yield* queue.refresh(ProviderInstanceId.make("missing"))).status).toBe(
        "unknown_instance",
      );
      expect(f.calls()).toBe(0);
    }),
  );
  it.effect("cannot relabel last-good data as a fresh successful probe", () =>
    Effect.gen(function* () {
      const f = fixture();
      const stale = provider({ qualifiedQuota: quota(start - 500000) });
      f.replace([stale]);
      const queue = yield* makeProviderQueue(
        { getProviders: f.registry.getProviders, refreshInstance: () => Effect.succeed([stale]) },
        f.storage,
        f.time,
      );
      const result = yield* queue.refresh(instanceId);
      expect(result.quota?.status).toBe("failed");
      expect(result.quota?.failureCode).toBe("refresh_not_observed");
      expect(result.quota?.quotaReceivedAt).toBeNull();
    }),
  );
  it.effect(
    "cache-only reconciliation observes an exact later native probe without starting one",
    () =>
      Effect.gen(function* () {
        const f = fixture();
        yield* f.storage.write({
          version: 2,
          attempts: [
            { instanceId, attemptedAt: start, resetRefreshUsed: false, quota: null, proof: null },
          ],
        });
        f.replace([provider({ qualifiedQuota: quota(start + 1) })]);
        const queue = yield* makeProviderQueue(f.registry, f.storage, f.time);
        const result = yield* queue.usage(instanceId);
        expect(result.quota?.probeId).toBe("00000000-0000-4000-8000-000000000001");
        expect(result.quota?.quotaReceivedAt).toBe(stamp(start + 1));
        expect(f.calls()).toBe(0);
      }),
  );
  it.effect("permits one reset-crossing refresh and holds an already-expired new response", () =>
    Effect.gen(function* () {
      const f = fixture();
      const short = quota(start, "00000000-0000-4000-8000-000000000001", {
        rateLimitsByLimitId: {
          codex: {
            primary: { usedPercent: 20, resetsAt: start / 1000 + 30, windowDurationMins: 300 },
          },
        },
      });
      f.replace([provider({ qualifiedQuota: short })]);
      yield* f.storage.write({
        version: 2,
        attempts: [
          {
            instanceId,
            attemptedAt: start,
            resetRefreshUsed: false,
            quota: short,
            proof: readPrivateQuotaProof(short) ?? null,
          },
        ],
      });
      let calls = 0;
      const registry = {
        getProviders: f.registry.getProviders,
        refreshInstance: () =>
          Effect.sync(() => {
            calls++;
            return [
              provider({
                qualifiedQuota: quota(start + 30001, "00000000-0000-4000-8000-000000000002", {
                  rateLimitsByLimitId: short.rateLimitsByLimitId,
                }),
              }),
            ];
          }),
      };
      f.advance(30001);
      const queue = yield* makeProviderQueue(registry, f.storage, f.time);
      const result = yield* queue.refresh(instanceId);
      expect(result.quota?.failureCode).toBe("reset_crossed");
      yield* queue.refresh(instanceId);
      const restarted = yield* makeProviderQueue(registry, f.storage, f.time);
      yield* restarted.refresh(instanceId);
      expect(calls).toBe(1);
    }),
  );
  it.effect(
    "invalidates a cached success when the native source reports a newer failed probe",
    () =>
      Effect.gen(function* () {
        const f = fixture();
        const queue = yield* makeProviderQueue(f.registry, f.storage, f.time);
        yield* queue.refresh(instanceId);
        f.replace([
          provider({
            qualifiedQuota: {
              ...quota(start + 1, "00000000-0000-4000-8000-000000000002"),
              status: "failed",
              complete: false,
              rateLimitsByLimitId: null,
              quotaReceivedAt: null,
              failureCode: "probe_failed",
            },
          }),
        ]);
        expect((yield* queue.refresh(instanceId)).quota?.status).toBe("failed");
        expect(f.calls()).toBe(1);
      }),
  );
  it.effect("holds the prior quota after a newer failure loses its native source evidence", () =>
    Effect.gen(function* () {
      const f = fixture();
      const queue = yield* makeProviderQueue(f.registry, f.storage, f.time);
      const good = yield* queue.refresh(instanceId);
      f.replace([
        provider({
          qualifiedQuota: {
            ...quota(start + 1, "00000000-0000-4000-8000-000000000002"),
            status: "failed",
            complete: false,
            rateLimitsByLimitId: null,
            quotaReceivedAt: null,
            failureCode: "probe_failed",
          },
        }),
      ]);
      expect((yield* queue.usage(instanceId)).quota?.status).toBe("failed");
      f.replace([provider()]);
      const restarted = yield* makeProviderQueue(f.registry, f.storage, f.time);
      const held = yield* restarted.usage(instanceId);
      expect(held.quota?.status).toBe("unknown");
      expect(held.quota?.complete).toBe(false);
      expect(held.quota?.failureCode).toBe("refresh_not_observed");
      expect(held.quota?.quotaReceivedAt).toBe(good.quota?.quotaReceivedAt);
      expect(held.quota?.probeCompletedAt).toBe(good.quota?.probeCompletedAt);
      expect((yield* restarted.refresh(instanceId)).quota?.status).toBe("unknown");
      expect(f.calls()).toBe(1);
      expect(f.stored().attempts[0]?.quota).toEqual(good.quota);
    }),
  );
  it.each([
    {
      individualLimit: {
        limit: "100",
        remainingPercent: 80,
        resetsAt: start / 1000 + 300,
        used: "20",
      },
    },
    { spendControlReached: true },
    { rateLimitReachedType: "workspace_owner_credits_depleted" },
  ])("holds additional native budget constraints without guessing a duration: %j", (constraint) => {
    const map = { codex: { ...buckets.codex, ...constraint } };
    const result = makeQualifiedQuota({
      instanceId,
      probeId: "00000000-0000-4000-8000-000000000001",
      ordinaryUsageAllowed: true,
      attemptedAt: stamp(),
      quotaReceivedAt: stamp(),
      probeCompletedAt: stamp(),
      rateLimitsByLimitId: map,
    });
    expect(result.status).toBe("unknown");
    expect(result.complete).toBe(false);
    expect(result.failureCode).toBe("incomplete_windows");
    expect(result.rateLimitsByLimitId).toEqual(map);
  });
  it.effect("never starts a probe when its durable attempt cannot be written", () =>
    Effect.gen(function* () {
      const f = fixture();
      const queue = yield* makeProviderQueue(
        f.registry,
        {
          ...f.storage,
          write: () => Effect.fail(new ProviderQueueStorageError({ code: "state_unavailable" })),
        },
        f.time,
      );
      const result = yield* Effect.result(queue.refresh(instanceId));
      expect(result._tag).toBe("Failure");
      expect(f.calls()).toBe(0);
    }),
  );
  it.each([false, null, undefined])(
    "requires explicit ordinary usage permission, received %s",
    (permission) => {
      const denied = quota(start, "00000000-0000-4000-8000-000000000001", {
        ordinaryUsageAllowed: permission,
      });
      expect(denied.status).toBe("failed");
      expect(denied.complete).toBe(false);
      expect(denied.failureCode).toBe("quota_unavailable");
      expect(readPrivateQuotaProof(denied)).toBeUndefined();
    },
  );
  it("does not recover denied permission from a reset, missing field, or percentages", () => {
    quota(start, "00000000-0000-4000-8000-000000000001", { ordinaryUsageAllowed: false });
    const stillDenied = quota(start + 600000, "00000000-0000-4000-8000-000000000002", {
      ordinaryUsageAllowed: undefined,
      rateLimitsByLimitId: {
        codex: {
          primary: { usedPercent: 0, windowDurationMins: 300, resetsAt: start / 1000 + 1000 },
        },
      },
    });
    expect(stillDenied.failureCode).toBe("quota_unavailable");
    expect(quota(start + 600001, "00000000-0000-4000-8000-000000000003").status).toBe("qualified");
  });
  it("permits missing backend account IDs and keeps optional identity private", () => {
    const absent = quota();
    expect(absent.status).toBe("qualified");
    expect(readPrivateQuotaProof(absent)?.ordinaryUsageAllowed).toBe(true);
    const identified = quota(start, "00000000-0000-4000-8000-000000000002", {
      accountId: "private-account-sentinel",
    });
    expect(identified.status).toBe("qualified");
    const serialized = JSON.stringify(identified);
    expect(serialized).not.toContain("private-account-sentinel");
    expect(serialized).not.toContain("ordinaryUsageAllowed");
    expect(serialized).not.toContain("bindingGeneration");
    expect(JSON.stringify(readPrivateQuotaProof(identified))).not.toContain(
      "private-account-sentinel",
    );
  });
  it.effect("rejects cached same-probe arithmetic when a present backend identity changes", () =>
    Effect.gen(function* () {
      const f = fixture();
      const before = quota(start, "00000000-0000-4000-8000-000000000001", {
        accountId: "account-a",
      });
      yield* f.storage.write({
        version: 2,
        attempts: [
          {
            instanceId,
            attemptedAt: start,
            resetRefreshUsed: false,
            quota: before,
            proof: readPrivateQuotaProof(before) ?? null,
          },
        ],
      });
      const after = quota(start, before.probeId, { accountId: "account-b" });
      f.replace([provider({ qualifiedQuota: after })]);
      const queue = yield* makeProviderQueue(f.registry, f.storage, f.time);
      expect(readPrivateQuotaProof(before)).toBeUndefined();
      expect((yield* queue.usage(instanceId)).quota?.failureCode).toBe("refresh_not_observed");
      expect(f.calls()).toBe(0);
    }),
  );
  it.effect(
    "holds hydrated quota without live private proof and retains cadence across restart",
    () =>
      Effect.gen(function* () {
        const f = fixture();
        const queue = yield* makeProviderQueue(f.registry, f.storage, f.time);
        const good = yield* queue.refresh(instanceId);
        resetQuotaBinding(instanceId);
        if (!good.quota) throw new Error("fixture quota was not produced");
        f.replace([provider({ qualifiedQuota: structuredClone(good.quota) })]);
        const restarted = yield* makeProviderQueue(f.registry, f.storage, f.time);
        const held = yield* restarted.refresh(instanceId);
        expect(held.status).toBe("cached");
        expect(held.quota?.failureCode).toBe("refresh_not_observed");
        expect(held.quota?.quotaReceivedAt).toBe(good.quota?.quotaReceivedAt);
        expect(f.calls()).toBe(1);
      }),
  );
  it.effect(
    "invalidates prior proof as a new whole probe starts, even before a model timeout finishes",
    () =>
      Effect.gen(function* () {
        const f = fixture();
        const queue = yield* makeProviderQueue(f.registry, f.storage, f.time);
        yield* queue.refresh(instanceId);
        beginQuotaProbe(instanceId, "00000000-0000-4000-8000-000000000002");
        expect((yield* queue.usage(instanceId)).quota?.failureCode).toBe("refresh_not_observed");
        expect(f.calls()).toBe(1);
      }),
  );
  it.effect("loads v1 arithmetic as ineligible while retaining the durable attempt deadline", () =>
    Effect.gen(function* () {
      const legacy = {
        version: 1,
        attempts: [{ instanceId, attemptedAt: start, resetRefreshUsed: false, quota: quota() }],
      };
      const upgraded = yield* decodeProviderQueueState(yield* encodeLegacyState(legacy));
      expect(upgraded.version).toBe(2);
      expect(upgraded.attempts[0]?.attemptedAt).toBe(start);
      expect(upgraded.attempts[0]?.quota?.status).toBe("unknown");
      expect(upgraded.attempts[0]?.proof).toBeNull();
    }),
  );

  it("retains the last reported private identity across omission without requiring identity", () => {
    resetQuotaBinding(instanceId);
    const a = quota(start, "00000000-0000-4000-8000-000000000001", { accountId: "account-a" });
    const proofA = readPrivateQuotaProof(a);
    const absent = quota(start + 1, "00000000-0000-4000-8000-000000000002");
    expect(absent.status).toBe("qualified");
    expect(readPrivateQuotaProof(absent)?.bindingGeneration).toBe(proofA?.bindingGeneration);
    const b = quota(start + 2, "00000000-0000-4000-8000-000000000003", { accountId: "account-b" });
    expect(readPrivateQuotaProof(b)?.bindingGeneration).not.toBe(proofA?.bindingGeneration);
  });
  it("cannot grant from a delayed probe after instance reconstruction", () => {
    resetQuotaBinding(instanceId);
    const old = beginQuotaProbe(instanceId, "00000000-0000-4000-8000-000000000001");
    resetQuotaBinding(instanceId);
    const result = quota(start, old.probeId, { probeBinding: old, accountId: "old-account" });
    expect(result.status).toBe("unknown");
    expect(result.failureCode).toBe("refresh_not_observed");
    expect(readPrivateQuotaProof(result)).toBeUndefined();
  });
  it("preserves live proof through the production identity and registry merge boundaries", () => {
    const live = quota();
    const stamped = withInstanceIdentity({
      instanceId,
      driverKind: ProviderDriverKind.make("codex"),
      displayName: "Same Name",
      accentColor: undefined,
      continuationGroupKey: "fixture",
    })(provider({ qualifiedQuota: live }));
    const merged = mergeProviderSnapshot(provider(), stamped);
    expect(merged.qualifiedQuota).toBe(live);
    expect(merged.qualifiedQuota && readPrivateQuotaProof(merged.qualifiedQuota)).toEqual(
      readPrivateQuotaProof(live),
    );
    expect(readPrivateQuotaProof(structuredClone(live))).toBeUndefined();
  });
});
