import type {
  JonesUpdateProvenance,
  JonesUpdateState,
} from "@t3tools/contracts/jones/jonesUpdates";
import { describe, expect, it } from "@effect/vitest";
import { EnvironmentId } from "@t3tools/contracts";
import { normalizeDpopHtu } from "@t3tools/shared/dpopCommon";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as Schema from "effect/Schema";
import * as Layer from "effect/Layer";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";
import { JonesUpdateState as JonesUpdateStateSchema } from "@t3tools/contracts/jones/jonesUpdates";
import {
  compareJonesSource,
  createJonesUpdateAtoms,
  jonesExactSourceResultError,
  jonesInstallRefusal,
  jonesFollowerHold,
  jonesFollowerSummary,
  jonesUpdateActionError,
  jonesCandidatePresentation,
  jonesUpdatePresentation,
  observeJonesUpdateSnapshot,
  observeJonesUpdateState,
  resolveJonesExactSourceAction,
  resolveJonesAlternativeAction,
  sameJonesReplacementStep,
  jonesAlternativePresentation,
  type JonesExactSourceInput,
  type JonesUpdateObservation,
} from "./jonesUpdates.ts";
import { EnvironmentRegistry } from "../../connection/registry.ts";
import { EnvironmentSupervisor } from "../../connection/supervisor.ts";
import {
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
  RelayConnectionTarget,
  type PreparedConnection,
} from "../../connection/model.ts";
import {
  ManagedRelayDpopSigner,
  type ManagedRelayDpopProofInput,
} from "../../relay/managedRelay.ts";
import { RemoteEnvironmentAuthorization } from "../../authorization/service.ts";
import { layerRemoteHttpClient } from "../../rpc/http.ts";
import type * as RpcSession from "../../rpc/session.ts";

const decodeJonesUpdateState = Schema.decodeUnknownSync(JonesUpdateStateSchema);

const staged: JonesUpdateState = {
  source: "jones-actions",
  channel: "jones-main",
  phase: "staged",
  revision: 2,
  capability: { check: true, download: true, install: true },
  stagedHandle: "fixed-stage",
};

it("projects settlement only from reported evidence and preserves old-server status", () => {
  expect(jonesUpdatePresentation(staged)).toMatchObject({
    canReconcile: false,
    settled: false,
    settlementMessage: undefined,
  });
  const failed = {
    ...staged,
    phase: "error" as const,
    updateId: "11111111-1111-4111-8111-111111111111",
    settlement: "none" as const,
    launcherSettlement: "supported" as const,
  };
  expect(jonesUpdatePresentation(failed)).toMatchObject({
    canReconcile: true,
    settled: false,
    settlementMessage: "Previous attempt needs reconciliation",
  });
  expect(
    jonesUpdatePresentation({ ...failed, launcherSettlement: "unsupported" }).canReconcile,
  ).toBe(false);
  for (const settlement of ["launcher-recorded", "legacy-reconciled"] as const) {
    const state = { ...failed, settlement, cause: "backup refused" };
    expect(jonesUpdatePresentation(state)).toMatchObject({
      canReconcile: false,
      settled: true,
      settlementMessage: "Update did not start: backup refused. Nothing changed.",
    });
    expect(jonesUpdateActionError(AsyncResult.success(state))).toBeNull();
    expect(
      jonesUpdateActionError(
        AsyncResult.success({ ...state, phase: "blocked", message: "Build lookup failed." }),
      ),
    ).toBe("Build lookup failed.");
  }
  const refused = { ...failed, settlementRefusal: "restore-pending" as const };
  expect(jonesUpdatePresentation(refused).refusalMessage).toBe("Recovery is still pending.");
  expect(jonesUpdateActionError(AsyncResult.success(refused))).toBe("Recovery is still pending.");
});
it.effect(
  "keeps relay update polling authenticated while transmitting zero and later revision cursors",
  () =>
    Effect.gen(function* () {
      const target = new RelayConnectionTarget({
        environmentId: EnvironmentId.make("update-relay-auth-test"),
        label: "Synthetic relay",
      });
      const origin = "https://updates.example.test";
      const prepared: PreparedConnection = {
        environmentId: target.environmentId,
        label: target.label,
        httpBaseUrl: origin,
        socketUrl: "wss://updates.example.test/ws",
        httpAuthorization: {
          _tag: "Dpop",
          accessToken: "synthetic-token",
          expiresAtEpochMs: 3_600_000,
        },
        target,
      };
      const supervisor = EnvironmentSupervisor.of({
        target,
        state: yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE),
        session: yield* SubscriptionRef.make<Option.Option<RpcSession.RpcSession>>(Option.none()),
        prepared: yield* SubscriptionRef.make<Option.Option<PreparedConnection>>(
          Option.some(prepared),
        ),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      });
      const followStream: EnvironmentRegistry["Service"]["followStream"] = (_id, stream) =>
        Stream.provideService(stream, EnvironmentSupervisor, supervisor);
      const run: EnvironmentRegistry["Service"]["run"] = (_id, effect) =>
        Effect.provideService(effect, EnvironmentSupervisor, supervisor);
      const environmentRegistry = EnvironmentRegistry.of({
        run,
        followStream,
      } as unknown as EnvironmentRegistry["Service"]);
      const authorizations: Array<string | undefined> = [];
      const authorization = RemoteEnvironmentAuthorization.of({
        authorizeBearer: () => Effect.die("Unexpected bearer authorization"),
        authorizeDpop: () => Effect.die("Polling must not replace the socket"),
        authorizeDpopHttp: (input) =>
          Effect.sync(() => {
            authorizations.push(input.rejectedAccessToken);
            return {
              environmentId: target.environmentId,
              label: target.label,
              httpBaseUrl: origin,
              httpAuthorization: {
                _tag: "Dpop" as const,
                accessToken: "synthetic-token",
                expiresAtEpochMs: 3_600_000,
              },
            };
          }),
      });
      const proofs: Array<ManagedRelayDpopProofInput> = [];
      const signer = ManagedRelayDpopSigner.of({
        thumbprint: Effect.succeed("synthetic-thumbprint"),
        createProof: (input) =>
          Effect.sync(() => {
            proofs.push(input);
            return JSON.stringify({ method: input.method, htu: normalizeDpopHtu(input.url) });
          }),
      });
      const calls: Array<{ url: string; proofHtu: string; method: string }> = [];
      const posts: Array<{ url: string; input: unknown }> = [];
      let loseReply = false;
      const polled = yield* Deferred.make<void>();
      const observedRevision = yield* Deferred.make<void>();
      let releaseFetch = () => {};
      const released = new Promise<void>((resolve) => {
        releaseFetch = () => resolve();
      });
      const fetchFn: typeof fetch = async (request, init) => {
        const url = String(request);
        const proof = JSON.parse(new Headers(init?.headers).get("dpop")!);
        if (init?.method === "POST") {
          expect(proof).toEqual({ method: "POST", htu: normalizeDpopHtu(url) });
          posts.push({ url, input: JSON.parse(String(init.body)) });
          if (loseReply) throw new TypeError("Synthetic lost reconciliation response");
          return Response.json({ ...staged, settlementRefusal: "restore-pending" });
        }
        calls.push({ url, proofHtu: proof.htu, method: init?.method ?? "GET" });
        if (calls.length === 3) Deferred.doneUnsafe(polled, Effect.void);
        if (proof.htu !== normalizeDpopHtu(url) || proof.method !== (init?.method ?? "GET"))
          return Response.json(
            {
              _tag: "EnvironmentAuthInvalidError",
              code: "auth_invalid",
              reason: "invalid_credential",
              traceId: "synthetic-rejection",
            },
            { status: 401 },
          );
        if (calls.length === 3) {
          await released;
          return Response.json(null);
        }
        return Response.json({ ...staged, revision: calls.length === 1 ? 0 : 7 });
      };
      const runtime = Atom.runtime(
        Layer.mergeAll(
          Layer.succeed(EnvironmentRegistry, environmentRegistry),
          Layer.succeed(RemoteEnvironmentAuthorization, authorization),
          Layer.succeed(ManagedRelayDpopSigner, signer),
          layerRemoteHttpClient(fetchFn),
        ),
      );
      const registry = AtomRegistry.make();
      const updates = createJonesUpdateAtoms(runtime);
      const value = updates.value(target.environmentId);
      const observed: Array<number> = [];
      let unsubscribe = () => {};
      let unmount = () => {};
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          releaseFetch();
          unsubscribe();
          unmount();
          registry.dispose();
        }),
      );
      unsubscribe = registry.subscribe(value, (state) => {
        if (typeof state?.revision === "number") observed.push(state.revision);
        if (state?.revision === 7) Deferred.doneUnsafe(observedRevision, Effect.void);
      });
      unmount = registry.mount(value);
      yield* Deferred.await(polled);
      expect(calls.map((call) => new URL(call.url).searchParams.get("after"))).toEqual([
        null,
        "0",
        "7",
      ]);
      expect(
        calls.every(
          (call) =>
            !call.url.includes("%3F") &&
            call.proofHtu === normalizeDpopHtu(call.url) &&
            call.method === "GET",
        ),
      ).toBe(true);
      expect(proofs.map((proof) => proof.url)).toEqual(calls.map((call) => call.url));
      expect(proofs.map((proof) => normalizeDpopHtu(proof.url))).toEqual(
        Array(3).fill(`${origin}/api/jones-updates`),
      );
      expect(authorizations).toEqual([undefined, undefined, undefined]);
      yield* Deferred.await(observedRevision);
      expect(observed).toEqual([0, 7]);
      expect(registry.get(value)?.revision).toBe(7);
      const input = {
        operationId: "11111111-1111-4111-8111-111111111111",
        requestId: "22222222-2222-4222-8222-222222222222",
      };
      const result = yield* Effect.promise(() =>
        updates.action.run(registry, {
          environmentId: target.environmentId,
          input: { action: "reconcile", input },
        }),
      );
      expect(result).toMatchObject({
        _tag: "Success",
        value: { settlementRefusal: "restore-pending" },
      });
      expect(posts).toEqual([{ url: `${origin}/api/jones-updates/reconcile`, input }]);
      loseReply = true;
      expect(
        yield* Effect.promise(() =>
          updates.action.run(registry, {
            environmentId: target.environmentId,
            input: { action: "reconcile", input },
          }),
        ),
      ).toMatchObject({ _tag: "Failure" });
      expect(posts).toHaveLength(2);
    }).pipe(Effect.scoped),
);
it.effect("starts observation after an initially disconnected mount prepares its connection", () =>
  Effect.gen(function* () {
    const connections = yield* SubscriptionRef.make(Option.none<string>());
    const disconnected = yield* Deferred.make<void>();
    let reads = 0;
    const observed = yield* observeJonesUpdateState(
      SubscriptionRef.changes(connections),
      (after) =>
        after === undefined
          ? Effect.sync(() => {
              reads++;
              return staged;
            })
          : Effect.never,
    ).pipe(
      Stream.tap((state) =>
        state === null ? Deferred.succeed(disconnected, undefined) : Effect.void,
      ),
      Stream.take(2),
      Stream.runCollect,
      Effect.forkChild({ startImmediately: true }),
    );
    yield* Deferred.await(disconnected);
    expect(reads).toBe(0);
    yield* SubscriptionRef.set(connections, Option.some("prepared"));
    expect(yield* Fiber.join(observed)).toEqual([null, staged]);
    expect(reads).toBe(1);
  }),
);
it.effect("hides Jones controls when an older host has no updater endpoint", () =>
  Effect.gen(function* () {
    const state = yield* observeJonesUpdateState(Stream.succeed(Option.some("older-host")), () =>
      Effect.fail({ _tag: "RemoteEnvironmentAuthUndeclaredStatusError", status: 404 }),
    ).pipe(Stream.runHead);
    expect(state).toEqual(Option.some(null));
  }),
);

it.effect("an ordinary Release host leaves Jones absent and ends HTTP observation", () =>
  Effect.gen(function* () {
    let reads = 0;
    const states = yield* observeJonesUpdateState(Stream.succeed(Option.some("release-host")), () =>
      Effect.sync(() => {
        reads++;
        return null;
      }),
    ).pipe(Stream.runCollect);
    expect(states).toEqual([null]);
    expect(reads).toBe(1);
  }),
);

it.effect("resumes mounted observation with a fresh cursor after connection replacement", () =>
  Effect.gen(function* () {
    const connections = yield* SubscriptionRef.make(Option.some("first"));
    const firstState = yield* Deferred.make<void>();
    const disconnected = yield* Deferred.make<void>();
    const cursors: Array<number | undefined> = [];
    const observed = yield* observeJonesUpdateState(
      SubscriptionRef.changes(connections),
      (after) => {
        cursors.push(after);
        return after === undefined ? Effect.succeed(staged) : Effect.never;
      },
    ).pipe(
      Stream.tap((state) =>
        state === null
          ? Deferred.succeed(disconnected, undefined)
          : Deferred.succeed(firstState, undefined),
      ),
      Stream.take(3),
      Stream.runCollect,
      Effect.forkChild({ startImmediately: true }),
    );
    yield* Deferred.await(firstState);
    yield* SubscriptionRef.set(connections, Option.none());
    yield* Deferred.await(disconnected);
    yield* SubscriptionRef.set(connections, Option.some("replacement"));
    expect(yield* Fiber.join(observed)).toEqual([staged, null, staged]);
    expect(cursors.filter((cursor) => cursor === undefined)).toHaveLength(2);
  }),
);

it.effect("long-polls by the returned revision until the host reports no Jones state", () =>
  Effect.gen(function* () {
    const cursors: Array<number | undefined> = [];
    const newer: JonesUpdateState = {
      ...staged,
      revision: 3,
      phase: "available",
      capability: { check: true, download: false, install: true },
    };
    const states = yield* observeJonesUpdateState(
      Stream.succeed(Option.some("prepared")),
      (after) => {
        cursors.push(after);
        return Effect.succeed(after === undefined ? staged : after === 2 ? newer : null);
      },
    ).pipe(Stream.runCollect);
    expect(cursors).toEqual([undefined, 2, 3]);
    expect(states).toEqual([staged, newer, null]);
  }),
);

it("keeps restart pending until a host outcome arrives and presents rollback reasons", () => {
  expect(
    jonesUpdatePresentation({ ...staged, phase: "installing", updateId: "native-id" }),
  ).toMatchObject({
    busy: true,
    message: "Installing. Server restarting. Waiting for the launcher outcome.",
    outcomeMessage: undefined,
  });
  expect(
    jonesUpdatePresentation({
      ...staged,
      phase: "rolled-back",
      outcome: {
        status: "rolled-back",
        fromVersion: "1.0.0",
        targetVersion: "2.0.0",
        reason: "candidate older than database",
      },
    }),
  ).toMatchObject({
    busy: false,
    outcomeMessage: "Rolled back: 1.0.0 → 2.0.0 · candidate older than database",
  });
});

it("decodes older Jones update payloads without outcome metadata", () => {
  expect(decodeJonesUpdateState(staged)).toEqual(staged);
});

it.effect(
  "retains the cursor and visible state across a transient GET failure, then recovers",
  () =>
    Effect.gen(function* () {
      const cursors: Array<number | undefined> = [];
      const delays: number[] = [];
      const states = yield* observeJonesUpdateSnapshot(
        Stream.succeed(Option.some("prepared")),
        (after) => {
          cursors.push(after);
          if (cursors.length === 1) return Effect.succeed(staged);
          if (cursors.length === 2)
            return Effect.fail({ _tag: "RemoteEnvironmentAuthTimeoutError" });
          if (cursors.length === 3) return Effect.succeed({ ...staged, revision: 3 });
          return Effect.succeed(null);
        },
        (failure) =>
          Effect.sync(() => {
            delays.push(failure);
          }),
      ).pipe(Stream.runCollect);
      expect(states.map((snapshot) => snapshot.freshness)).toEqual([
        "fresh",
        "stale",
        "fresh",
        "unsupported",
      ]);
      expect(states[1]?.state).toEqual(staged);
      expect(states[2]?.state).toEqual({ ...staged, revision: 3 });
      expect(states[3]?.state).toBeNull();
      expect(cursors).toEqual([undefined, 2, 2, 3]);
      expect(delays).toEqual([1]);
    }),
);

it.effect("connection replacement cancels a pending read retry and resets its cursor", () =>
  Effect.gen(function* () {
    const connections = yield* SubscriptionRef.make(Option.some("first"));
    const waiting = yield* Deferred.make<void>();
    const canceled = yield* Deferred.make<void>();
    let reads = 0;
    const cursors: Array<number | undefined> = [];
    const observer = yield* observeJonesUpdateSnapshot(
      SubscriptionRef.changes(connections),
      (after) => {
        reads++;
        cursors.push(after);
        return reads === 1
          ? Effect.fail({ _tag: "RemoteEnvironmentAuthTimeoutError" })
          : Effect.succeed(reads === 2 ? staged : null);
      },
      () =>
        Deferred.succeed(waiting, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.onInterrupt(() => Deferred.succeed(canceled, undefined)),
        ),
    ).pipe(Stream.take(3), Stream.runCollect, Effect.forkChild({ startImmediately: true }));
    yield* Deferred.await(waiting);
    yield* SubscriptionRef.set(connections, Option.some("replacement"));
    yield* Deferred.await(canceled);
    const results = yield* Fiber.join(observer);
    expect(results.map((snapshot) => snapshot.freshness)).toEqual([
      "stale",
      "fresh",
      "unsupported",
    ]);
    expect(results[1]?.state).toEqual(staged);
    expect(results[2]?.state).toBeNull();
    expect(cursors).toEqual([undefined, undefined, 2]);
  }),
);

it.effect("authorization refusal marks retained data stale without retrying", () =>
  Effect.gen(function* () {
    let reads = 0;
    let delays = 0;
    const results = yield* observeJonesUpdateSnapshot(
      Stream.succeed(Option.some("prepared")),
      () =>
        ++reads === 1
          ? Effect.succeed(staged)
          : Effect.fail({ _tag: "EnvironmentAuthInvalidError" }),
      () =>
        Effect.sync(() => {
          delays++;
        }),
    ).pipe(Stream.take(2), Stream.runCollect);
    expect(results[1]).toMatchObject({ state: staged, freshness: "stale" });
    expect(delays).toBe(0);
  }),
);

it("distinguishes fulfilled remote failures from accepted restart progress", () => {
  expect(
    jonesUpdateActionError(
      AsyncResult.success({ ...staged, phase: "blocked", message: "Source changed" }),
    ),
  ).toBe("Source changed");
  expect(
    jonesUpdateActionError(AsyncResult.success({ ...staged, phase: "installing" })),
  ).toBeNull();
});

it.effect("marks a replaced live connection stale before its first status read completes", () =>
  Effect.gen(function* () {
    const connections = yield* SubscriptionRef.make(Option.some("first"));
    const ready = yield* Deferred.make<void>();
    const observer = yield* observeJonesUpdateSnapshot(
      SubscriptionRef.changes(connections),
      (after) => (after === undefined ? Effect.succeed(staged) : Effect.never),
    ).pipe(
      Stream.tap((snapshot) =>
        snapshot.freshness === "fresh" ? Deferred.succeed(ready, undefined) : Effect.void,
      ),
      Stream.take(3),
      Stream.runCollect,
      Effect.forkChild({ startImmediately: true }),
    );
    yield* Deferred.await(ready);
    yield* SubscriptionRef.set(connections, Option.some("replacement"));
    const snapshots = yield* Fiber.join(observer);
    expect(snapshots.map((snapshot) => snapshot.freshness)).toEqual(["fresh", "stale", "fresh"]);
    expect(snapshots[1]?.state).toEqual(staged);
  }),
);

const offered: JonesUpdateProvenance = {
  repository: "Jones-Systems/Jones-Code",
  sourceSha: "b".repeat(40),
  sourceTree: "c".repeat(40),
  workflow: ".github/workflows/artifact-cli-linux.yml",
  runId: 2,
  runAttempt: 1,
  artifactId: 4,
  artifactDigest: "d".repeat(64),
  platform: "linux",
  architecture: "x64",
  version: "0.0.45-preview.20261011.2.1",
};

describe("generic alternative selection", () => {
  const environmentId = EnvironmentId.make("generic-alternative-host");
  const offer = {
    provenance: { ...offered, sourceSha: "f".repeat(40), artifactId: 5 },
    checkedAt: "2026-10-29T00:00:00Z",
    expiresAt: "2099-01-01T00:00:00Z",
  };
  const state: JonesUpdateState = {
    ...staged,
    environmentId,
    currentVersion: "installed",
    installedSource: {
      repository: "Jones-Systems/Jones-Code",
      sha: "a".repeat(40),
      tree: "c".repeat(40),
    },
    provenance: offered,
    capability: {
      check: true,
      download: true,
      install: true,
      exactSource: true,
      replaceStaged: true,
    },
    alternativeCandidate: offer,
    migrationPlan: { pendingUpstream: [42], pendingJones: [7] },
  };
  const resolve = (
    patch: Partial<JonesUpdateState> = {},
    freshness: JonesUpdateObservation["freshness"] = "fresh",
  ) =>
    resolveJonesAlternativeAction({
      environmentId,
      observation: { state: { ...state, ...patch }, freshness },
      canOperate: true,
      alternative: offer,
    });

  it("captures the installed binding, retained handle/source and distinct exact target without offering Install", () => {
    const decision = resolve();
    expect(decision).toEqual({
      status: "ready",
      step: {
        kind: "replace-exact",
        targetSource: offer.provenance.sourceSha,
        replace: {
          stagedHandle: "fixed-stage",
          sourceSha: offered.sourceSha,
          environmentId,
          currentVersion: "installed",
          expectedInstalledSource: "a".repeat(40),
          alternative: offer,
        },
      },
    });
    expect(jonesAlternativePresentation(state)).toEqual({
      label: "Alternative",
      version: offered.version,
      buildStartedAt: undefined,
      sourceSha: offer.provenance.sourceSha,
    });
    const changed = resolve({ stagedHandle: "competing-client-stage" });
    expect(
      decision.status === "ready" &&
        changed.status === "ready" &&
        sameJonesReplacementStep(decision.step, changed.step),
    ).toBe(false);
  });

  it("refuses stale, absent, changed and expired offers, follower policy and unsupported hosts", () => {
    for (const patch of [
      { alternativeCandidate: { ...offer, provenance: { ...offer.provenance, artifactId: 99 } } },
      { alternativeCandidate: { ...offer, expiresAt: "2000-01-01T00:00:00Z" } },
      { environmentId: EnvironmentId.make("other") },
      { policy: { role: "follower", leaderSource: offer.provenance.sourceSha } },
      { capability: { check: true, download: true, install: true, exactSource: true } },
      { phase: "preparing" },
      { phase: "installing" },
    ] satisfies Array<Partial<JonesUpdateState>>)
      expect(resolve(patch).status).toBe("unavailable");
    const { alternativeCandidate: _offer, ...withoutOffer } = state;
    const { stagedHandle: _handle, ...withoutStage } = state;
    const { installedSource: _source, ...withoutInstalled } = state;
    for (const missing of [withoutOffer, withoutStage, withoutInstalled])
      expect(
        resolveJonesAlternativeAction({
          environmentId,
          observation: { state: missing, freshness: "fresh" },
          canOperate: true,
        }).status,
      ).toBe("unavailable");
    expect(resolve({}, "stale").status).toBe("unavailable");
    expect(
      resolveJonesAlternativeAction({
        environmentId,
        observation: { state, freshness: "fresh" },
        canOperate: false,
      }).status,
    ).toBe("unavailable");
  });
});

it("presents an offered build's version before download and leaves an older host's absent", () => {
  const { stagedHandle: _handle, ...unstaged } = staged;
  const available = { ...unstaged, phase: "available" as const };
  expect(jonesCandidatePresentation({ ...available, provenance: offered })).toEqual({
    label: "Available",
    version: offered.version,
    buildStartedAt: undefined,
    sourceSha: offered.sourceSha,
  });
  const { version: _version, ...older } = offered;
  expect(jonesCandidatePresentation({ ...available, provenance: older })).toEqual({
    label: "Available",
    version: undefined,
    buildStartedAt: undefined,
    sourceSha: offered.sourceSha,
  });
  expect(jonesCandidatePresentation({ ...staged, provenance: offered })?.label).toBe("Downloaded");
  expect(jonesCandidatePresentation(staged)).toBeUndefined();
});

it("does not present retained provenance as a fresh offer after another check", () => {
  const { stagedHandle: _handle, ...unstaged } = staged;
  for (const phase of ["building", "no-new", "blocked"] as const) {
    expect(jonesCandidatePresentation({ ...unstaged, phase, provenance: offered })).toBeUndefined();
    expect(jonesCandidatePresentation({ ...staged, phase, provenance: offered })?.label).toBe(
      "Downloaded",
    );
  }
});

it("matches laptop and candidate by full source commit, never by version or run", () => {
  const laptop = { repository: "Jones-Systems/Jones-Code", sha: offered.sourceSha };
  expect(compareJonesSource(laptop, offered)).toBe("matches");
  // Each platform workflow builds the same source under its own run and version.
  expect(
    compareJonesSource(laptop, { ...offered, runId: 9, version: "0.0.45-preview.20261011.9.1" }),
  ).toBe("matches");
  expect(compareJonesSource({ ...laptop, sha: "a".repeat(40) }, offered)).toBe("different");
});

it("keeps the comparison unavailable without both full source identities", () => {
  const laptop = { repository: "Jones-Systems/Jones-Code", sha: offered.sourceSha };
  expect(compareJonesSource(undefined, offered)).toBe("unavailable");
  expect(compareJonesSource(laptop, undefined)).toBe("unavailable");
  expect(compareJonesSource({ ...laptop, sha: offered.sourceSha.slice(0, 7) }, offered)).toBe(
    "unavailable",
  );
  expect(compareJonesSource({ ...laptop, repository: "pingdotgg/t3code" }, offered)).toBe(
    "unavailable",
  );
});

describe("exact-source action resolution", () => {
  const environmentId = EnvironmentId.make("exact-source-host");
  // The running app's own source; the host's installed and offered builds differ from it.
  const target = "a".repeat(40);
  const installed = {
    repository: "Jones-Systems/Jones-Code" as const,
    sha: "e".repeat(40),
    tree: "f".repeat(40),
  };
  const hostState = (overrides: Partial<JonesUpdateState> = {}): JonesUpdateState => ({
    source: "jones-actions",
    channel: "jones-main",
    phase: "no-new",
    revision: 4,
    capability: { check: true, download: true, install: true, exactSource: true },
    currentVersion: "0.1.0-preview.20261019.39341000929.1",
    installedSource: installed,
    ...overrides,
  });
  const targetBuild: JonesUpdateProvenance = { ...offered, sourceSha: target };
  const resolve = (
    state: JonesUpdateState | null,
    overrides: Partial<JonesExactSourceInput> & {
      freshness?: JonesUpdateObservation["freshness"];
    } = {},
  ) => {
    const { freshness = "fresh", ...rest } = overrides;
    return resolveJonesExactSourceAction({
      environmentId,
      observation: { state, freshness },
      targetSource: target,
      canOperate: true,
      ...rest,
    });
  };
  const unavailable = (decision: ReturnType<typeof resolve>) =>
    decision.status === "unavailable" ? decision.reason : decision.step.kind;

  it("lets a host that follows its laptop take only the laptop's installed source", () => {
    const leader = { role: "follower" as const, leaderSource: target };
    expect(resolve(hostState({ policy: leader }))).toMatchObject({ status: "ready" });
    const other = resolve(
      hostState({ policy: { role: "follower", leaderSource: "b".repeat(40) } }),
    );
    expect(unavailable(other)).toBe("follows-laptop");
    expect(other.status === "unavailable" ? other.message : "").toContain("bbbbbbb");
    expect(
      unavailable(resolve(hostState({ policy: { role: "follower", leaderSource: null } }))),
    ).toBe("follows-laptop");
    const staged = hostState({
      phase: "staged",
      stagedHandle: "handle-1",
      provenance: { ...offered, sourceSha: "c".repeat(40) },
      policy: leader,
    });
    expect(jonesFollowerHold(staged)).toContain("aaaaaaa");
    expect(jonesFollowerHold({ ...staged, provenance: targetBuild })).toBeUndefined();
    const { policy: _policy, ...standalone } = staged;
    expect(jonesFollowerHold(standalone)).toBeUndefined();
    expect(jonesFollowerSummary(staged)).toBe("Follows its laptop · takes only source aaaaaaa");
  });

  it("downloads the target by source when the host advertises exact selection, whatever it offers", () => {
    // The host's own latest offer must never decide the target.
    const decision = resolve(hostState({ phase: "available", provenance: offered }));
    expect(decision).toEqual({
      status: "ready",
      step: { kind: "download-exact", targetSource: target },
    });
  });

  it("ignores version strings: another platform's version for the same source is allowed", () => {
    const decision = resolve(
      hostState({
        phase: "available",
        provenance: {
          ...targetBuild,
          platform: "darwin",
          version: "0.1.0-preview.20261019.39341000901.1",
        },
        capability: { check: true, download: true, install: true },
      }),
    );
    expect(decision).toMatchObject({ status: "ready", step: { kind: "download" } });
  });

  it("installs only a staged build whose source is the target, bound to its handle", () => {
    const decision = resolve(
      hostState({ phase: "staged", stagedHandle: "handle-1", provenance: targetBuild }),
    );
    expect(decision).toEqual({
      status: "ready",
      step: {
        kind: "install",
        input: {
          stagedHandle: "handle-1",
          environmentId,
          currentVersion: "0.1.0-preview.20261019.39341000929.1",
        },
      },
    });
    expect(
      unavailable(
        resolve(hostState({ phase: "staged", stagedHandle: "handle-1", provenance: offered })),
      ),
    ).toBe("staged-other");
    expect(unavailable(resolve(hostState({ phase: "staged", stagedHandle: "handle-1" })))).toBe(
      "staged-other",
    );
  });

  it("offers replacement only with explicit opt-in, native capability and a complete host binding", () => {
    const staged = hostState({
      environmentId,
      phase: "staged",
      stagedHandle: "old-handle",
      provenance: offered,
      capability: {
        check: true,
        download: true,
        install: true,
        exactSource: true,
        replaceStaged: true,
      },
    });
    expect(unavailable(resolve(staged))).toBe("staged-other");
    expect(resolve(staged, { allowReplace: true, subject: "this laptop" })).toEqual({
      status: "ready",
      step: {
        kind: "replace-exact",
        targetSource: target,
        replace: {
          environmentId,
          currentVersion: staged.currentVersion,
          expectedInstalledSource: installed.sha,
          stagedHandle: "old-handle",
          sourceSha: offered.sourceSha,
        },
      },
    });
    const { installedSource: _installedSource, ...withoutInstalled } = staged;
    const { provenance: _provenance, ...withoutProvenance } = staged;
    for (const changed of [
      { ...staged, capability: { ...staged.capability, replaceStaged: false } },
      { ...staged, environmentId: EnvironmentId.make("another-host") },
      withoutInstalled,
      withoutProvenance,
    ])
      expect(unavailable(resolve(changed, { allowReplace: true }))).toBe("staged-other");
    expect(unavailable(resolve(staged, { allowReplace: true, freshness: "stale" }))).toBe(
      "waiting",
    );
    expect(
      unavailable(resolve({ ...staged, provenance: targetBuild }, { allowInstall: false })),
    ).toBe("already-downloaded");
  });

  it("rejects missing or competing target provenance throughout preparation", () => {
    const step = { kind: "download-exact", targetSource: target } as const;
    for (const phase of ["downloading", "verifying", "staged"] as const) {
      expect(
        jonesExactSourceResultError(
          step,
          AsyncResult.success(hostState({ phase, provenance: offered })),
          target,
        ),
      ).toContain(offered.sourceSha.slice(0, 7));
      expect(
        jonesExactSourceResultError(step, AsyncResult.success(hostState({ phase })), target),
      ).toContain("did not report");
      expect(
        jonesExactSourceResultError(
          step,
          AsyncResult.success(hostState({ phase, provenance: targetBuild })),
          target,
        ),
      ).toBeNull();
    }
  });

  it("revalidates the complete install selection and follower policy after confirmation", () => {
    const state = hostState({
      environmentId,
      phase: "staged",
      stagedHandle: "handle-1",
      provenance: targetBuild,
    });
    const input = {
      environmentId,
      currentVersion: state.currentVersion!,
      stagedHandle: "handle-1",
    };
    expect(jonesInstallRefusal(input, { state, freshness: "fresh" })).toBeUndefined();
    for (const changed of [
      { ...state, stagedHandle: "competing-handle" },
      { ...state, currentVersion: "changed" },
      { ...state, environmentId: EnvironmentId.make("another-host") },
      { ...state, phase: "installing" as const },
      { ...state, capability: { ...state.capability, install: false } },
      { ...state, policy: { role: "follower" as const, leaderSource: "b".repeat(40) } },
    ])
      expect(jonesInstallRefusal(input, { state: changed, freshness: "fresh" })).toBeDefined();
    expect(jonesInstallRefusal(input, { state, freshness: "stale" })).toBeDefined();
  });

  it("names the other staged source and offers nothing for it", () => {
    const decision = resolve(
      hostState({ phase: "staged", stagedHandle: "handle-1", provenance: offered }),
    );
    expect(decision).toMatchObject({
      status: "unavailable",
      reason: "staged-other",
      message: expect.stringContaining(offered.sourceSha.slice(0, 7)),
    });
    if (decision.status !== "unavailable") throw new Error("Expected a source refusal");
    expect(decision.message).toContain(`this app needs ${target.slice(0, 7)}`);
    expect(decision.message).toContain("will not be installed from this notice");
    expect(decision.message).toContain("Settings → Connections");
    expect(decision.message).toContain("review this host's downloaded update");
    expect(decision.message).toContain("Installing it there will not match this app");
  });

  it("directs an unknown staged source to review without claiming it matches the app", () => {
    const decision = resolve(hostState({ phase: "staged", stagedHandle: "handle-1" }));
    expect(decision).toMatchObject({ status: "unavailable", reason: "staged-other" });
    if (decision.status !== "unavailable") throw new Error("Expected a source refusal");
    expect(decision.message).toContain("no reported source");
    expect(decision.message).toContain("will not be installed from this notice");
    expect(decision.message).toContain("Settings → Connections");
    expect(decision.message).toContain("review this host's downloaded update");
    expect(decision.message).toContain("cannot confirm it matches this app");
  });

  it("lets an older host download only an offer that already is the target", () => {
    const legacy = { check: true, download: true, install: true };
    expect(
      unavailable(
        resolve(hostState({ phase: "available", provenance: offered, capability: legacy })),
      ),
    ).toBe("offered-other");
    expect(unavailable(resolve(hostState({ capability: legacy })))).toBe("offered-other");
    expect(
      resolve(hostState({ phase: "available", provenance: targetBuild, capability: legacy })),
    ).toEqual({
      status: "ready",
      step: {
        kind: "download",
        input: { artifactId: targetBuild.artifactId, sourceSha: target },
      },
    });
    // A retained offer after another check is not a download binding.
    expect(
      unavailable(
        resolve(hostState({ phase: "no-new", provenance: targetBuild, capability: legacy })),
      ),
    ).toBe("offered-other");
  });

  it("offers nothing without a fresh, supported, permitted observation", () => {
    const live = hostState({ phase: "available", provenance: offered });
    expect(unavailable(resolve(live, { freshness: "stale" }))).toBe("waiting");
    expect(unavailable(resolve(live, { freshness: "loading" }))).toBe("waiting");
    expect(unavailable(resolve(null, { freshness: "unsupported" }))).toBe("unsupported");
    expect(unavailable(resolve(null))).toBe("unsupported");
    expect(unavailable(resolve(live, { canOperate: false }))).toBe("no-permission");
    expect(unavailable(resolve(live, { targetSource: undefined }))).toBe("invalid-target");
    expect(unavailable(resolve(live, { targetSource: "a".repeat(7) }))).toBe("invalid-target");
    expect(
      resolveJonesExactSourceAction({
        environmentId,
        observation: {
          state: live,
          freshness: "stale",
          message: "Update status is temporarily unavailable; retrying.",
        },
        targetSource: target,
        canOperate: true,
      }),
    ).toMatchObject({ message: "Update status is temporarily unavailable; retrying." });
  });

  it("offers nothing once the host runs the source, or while it is working", () => {
    expect(
      unavailable(resolve(hostState({ installedSource: { ...installed, sha: target } }))),
    ).toBe("already-installed");
    for (const phase of [
      "checking",
      "downloading",
      "verifying",
      "preparing",
      "installing",
    ] as const)
      expect(
        unavailable(
          resolve(hostState({ phase, stagedHandle: "handle-1", provenance: targetBuild })),
        ),
      ).toBe("busy");
  });

  it("explains a host that cannot download or install", () => {
    expect(
      unavailable(
        resolve(hostState({ capability: { check: true, download: false, install: false } })),
      ),
    ).toBe("unavailable");
    expect(
      resolve(
        hostState({
          phase: "staged",
          stagedHandle: "handle-1",
          provenance: targetBuild,
          capability: { check: true, download: true, install: false, reason: "bootstrap-required" },
        }),
      ),
    ).toMatchObject({
      status: "unavailable",
      message: "Installing needs a qualified launcher on this host first.",
    });
    const { currentVersion: _unreported, ...unversioned } = hostState({
      phase: "staged",
      stagedHandle: "handle-1",
      provenance: targetBuild,
    });
    expect(unavailable(resolve(unversioned))).toBe("unavailable");
  });

  it("holds an install whose response was lost until the host reports movement", () => {
    const stagedHost = hostState({
      phase: "staged",
      stagedHandle: "handle-1",
      provenance: targetBuild,
    });
    const unresolvedInstall = { stagedHandle: "handle-1", revision: stagedHost.revision };
    expect(unavailable(resolve(stagedHost, { unresolvedInstall }))).toBe("reconciling");
    // A new revision with the staged build still present means the host did not start it.
    expect(unavailable(resolve({ ...stagedHost, revision: 5 }, { unresolvedInstall }))).toBe(
      "install",
    );
    // The committed outcome ends the question.
    expect(
      unavailable(
        resolve(hostState({ phase: "committed", installedSource: { ...installed, sha: target } }), {
          unresolvedInstall,
        }),
      ),
    ).toBe("already-installed");
  });

  it("refuses a host that stages another source than the target", () => {
    const step = { kind: "download-exact", targetSource: target } as const;
    const elsewhere = AsyncResult.success(
      hostState({ phase: "staged", stagedHandle: "handle-1", provenance: offered }),
    );
    expect(jonesExactSourceResultError(step, elsewhere, target)).toContain(
      offered.sourceSha.slice(0, 7),
    );
    expect(
      jonesExactSourceResultError(
        step,
        AsyncResult.success(
          hostState({ phase: "staged", stagedHandle: "handle-1", provenance: targetBuild }),
        ),
        target,
      ),
    ).toBeNull();
    expect(
      jonesExactSourceResultError(
        step,
        AsyncResult.success(hostState({ phase: "blocked", message: "Candidate is not newer." })),
        target,
      ),
    ).toBe("Candidate is not newer.");
    expect(jonesExactSourceResultError(step, AsyncResult.success(null), target)).toBe(
      "This host does not support Jones updates.",
    );
    // A download the host did not take is not success, even without an error phase.
    expect(
      jonesExactSourceResultError(
        step,
        AsyncResult.success(
          hostState({ phase: "no-new", message: "This host already runs the requested source." }),
        ),
        target,
      ),
    ).toBe("This host already runs the requested source.");
    expect(
      jonesExactSourceResultError(
        step,
        AsyncResult.success(hostState({ phase: "downloading", provenance: targetBuild })),
        target,
      ),
    ).toBeNull();
  });
});

it("presents same-minute outcomes with exact-version stamps without changing identity", () => {
  const fromVersion = "0.0.45-preview.20261020.100.1";
  const targetVersion = "0.0.45-preview.20261020.101.1";
  const state: JonesUpdateState = {
    ...staged,
    currentVersion: fromVersion,
    currentBuildStartedAt: "2026-10-20T21:13:43Z",
    provenance: { ...offered, version: targetVersion, buildStartedAt: "2026-10-20T21:13:47Z" },
    outcome: { status: "committed", fromVersion, targetVersion },
  };
  const original = JSON.stringify(state);
  expect(jonesUpdatePresentation(state).outcomeMessage).toBe(
    "Installed: 1020·1713:43 → 1020·1713:47",
  );
  expect(JSON.stringify(state)).toBe(original);
  expect(jonesCandidatePresentation(state)).toMatchObject({
    version: targetVersion,
    buildStartedAt: "2026-10-20T21:13:47Z",
    sourceSha: offered.sourceSha,
  });
  expect(
    jonesUpdatePresentation({
      ...state,
      outcome: {
        status: "blocked",
        fromVersion: `${fromVersion}0`,
        targetVersion: `${targetVersion}0`,
      },
    }).outcomeMessage,
  ).toBe(`Blocked: ${fromVersion}0 → ${targetVersion}0`);
});

it.effect(
  "Stop bypasses a long operation request while later operation reads remain serialized",
  () =>
    Effect.gen(function* () {
      const environmentId = EnvironmentId.make("operation-lanes");
      const target = new PrimaryConnectionTarget({
        environmentId,
        label: "Synthetic primary",
        httpBaseUrl: "http://updates.example.test",
        wsBaseUrl: "ws://updates.example.test/ws",
      });
      const prepared: PreparedConnection = {
        environmentId,
        label: target.label,
        target,
        httpBaseUrl: "http://updates.example.test",
        socketUrl: "ws://updates.example.test/ws",
        httpAuthorization: null,
      };
      const supervisor = EnvironmentSupervisor.of({
        target,
        state: yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE),
        session: yield* SubscriptionRef.make<Option.Option<RpcSession.RpcSession>>(Option.none()),
        prepared: yield* SubscriptionRef.make(Option.some(prepared)),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      });
      const run: EnvironmentRegistry["Service"]["run"] = (_id, effect) =>
        Effect.provideService(effect, EnvironmentSupervisor, supervisor);
      const environmentRegistry = EnvironmentRegistry.of({
        run,
      } as unknown as EnvironmentRegistry["Service"]);
      const entered = yield* Deferred.make<void>();
      let release = () => {};
      const released = new Promise<void>((resolve) => {
        release = resolve;
      });
      const calls: string[] = [];
      const payloads: unknown[] = [];
      const operationId = "11111111-1111-4111-8111-111111111111";
      const stopId = "22222222-2222-4222-8222-222222222222";
      const input = {
        operationId,
        environmentId,
        expectedInstalledSource: "a".repeat(40),
        action: { kind: "download-exact" as const, targetSource: "b".repeat(40) },
      };
      const stopInput = { environmentId, operationId, stopId };
      const readInput = { environmentId, operationIds: [operationId] };
      const receipt = {
        ...input,
        protocol: 1,
        phase: "accepted",
        acceptedAt: "2026-10-27T05:00:00Z",
        updatedAt: "2026-10-27T05:00:00Z",
      };
      const stopReceipt = {
        ...receipt,
        phase: "stop-requested",
        stop: { stopId, requestedAt: receipt.updatedAt },
      };
      const fetchFn: typeof fetch = async (request, init) => {
        const path = new URL(String(request)).pathname;
        calls.push(path);
        expect(init?.credentials).toBe("include");
        if (path.includes("/operations/")) payloads.push(JSON.parse(String(init?.body)));
        if (path.endsWith("/operations/start")) {
          Deferred.doneUnsafe(entered, Effect.void);
          await released;
          return Response.json(receipt);
        }
        if (path.endsWith("/operations/stop")) return Response.json(stopReceipt);
        if (path.endsWith("/operations/read"))
          return Response.json({ receipts: [{ operationId, receipt }] });
        return Response.json(staged);
      };
      const runtime = Atom.runtime(
        Layer.mergeAll(
          Layer.succeed(EnvironmentRegistry, environmentRegistry),
          layerRemoteHttpClient(fetchFn),
        ),
      );
      const registry = AtomRegistry.make();
      const pending: Array<Promise<unknown>> = [];
      yield* Effect.addFinalizer(() =>
        Effect.promise(async () => {
          release();
          try {
            await Promise.allSettled(pending);
          } finally {
            registry.dispose();
          }
        }),
      );
      const updates = createJonesUpdateAtoms(runtime);
      const started = updates.operation.run(registry, {
        environmentId,
        input: { action: "operationStart", input },
      });
      pending.push(started);
      yield* Effect.raceFirst(
        Deferred.await(entered),
        Effect.promise(() => started).pipe(
          Effect.flatMap((result) =>
            Effect.die(
              new Error("operationStart settled before entering the held fetch", { cause: result }),
            ),
          ),
        ),
      );
      const read = updates.operation.run(registry, {
        environmentId,
        input: { action: "operationRead", input: readInput },
      });
      pending.push(read);
      const stopped = updates.stop.run(registry, {
        environmentId,
        input: { action: "operationStop", input: stopInput },
      });
      pending.push(stopped);
      expect(yield* Effect.promise(() => stopped)).toMatchObject({
        _tag: "Success",
        value: stopReceipt,
      });
      expect(calls).toEqual([
        "/api/jones-updates/operations/start",
        "/api/jones-updates/operations/stop",
      ]);
      const checked = updates.action.run(registry, { environmentId, input: { action: "check" } });
      pending.push(checked);
      expect(yield* Effect.promise(() => checked)).toMatchObject({
        _tag: "Success",
        value: staged,
      });
      release();
      expect(yield* Effect.promise(() => started)).toMatchObject({
        _tag: "Success",
        value: receipt,
      });
      expect(yield* Effect.promise(() => read)).toMatchObject({
        _tag: "Success",
        value: { receipts: [{ operationId, receipt }] },
      });
      expect(calls).toEqual([
        "/api/jones-updates/operations/start",
        "/api/jones-updates/operations/stop",
        "/api/jones-updates/check",
        "/api/jones-updates/operations/read",
      ]);
      expect(payloads).toEqual([input, stopInput, readInput]);
    }).pipe(Effect.scoped),
);
