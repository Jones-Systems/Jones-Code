import { expect, it } from "@effect/vitest";
import { AuthOrchestrationReadScope, AuthOrchestrationOperateScope } from "@t3tools/contracts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { hash } from "../../operations/testFixtures.ts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as Callers from "./recordedCaller.ts";

const record: Callers.RecordedCallerV1 = {
  version: 1,
  origin: { kind: "agent" },
  auth: {
    kind: "mcp_client",
    session_id: "caller-session-sentinel",
    subject: "mcp-client",
    method: "bearer-access-token",
    approved_access: "full-access",
    requested_limits: { runtimeMode: "auto-accept-edits", interactionMode: "default" },
  },
  source_environment_id: "source",
  registry_principal_id: "principal",
  singleton_binding_version: 2,
  grant_version: 3,
  captured_at: 0,
  source: {
    environment_id: "source",
    authority_namespace: "source-namespace-sentinel",
    store_generation: 4,
    binding_version: 5,
    enrollment_id: "native-enrollment",
    enrollment_session_id: "provider-session",
  },
  destination: {
    registry_instance_id: "registry",
    owner_id: "owner",
    home_environment_id: "home",
    home_generation: 6,
    principal_id: "principal",
    binding_version: 2,
  },
  ceiling: {
    permissions: ["workstreams:read", "workstreams:write"],
    scopes: [AuthOrchestrationReadScope, AuthOrchestrationOperateScope],
    capabilities: ["orchestration"],
    limits: { runtimeMode: "auto-accept-edits", interactionMode: "default" },
  },
};
const current = (admission = record): Callers.CurrentCallerState => ({
  admission,
  registry_active: true,
  singleton_active: true,
  source_binding_active: true,
  grant: {
    active: true,
    version: admission.grant_version,
    permissions: ["workstreams:read", "workstreams:write"],
  },
  enrollment: {
    enrollment_id: admission.source.enrollment_id,
    session_id: admission.source.enrollment_session_id,
    active: true,
    issued_at: 0,
    expires_at: 1_000_000_000,
  },
  session: {
    session_id: "caller-session-sentinel",
    subject: "mcp-client",
    method: "bearer-access-token",
    scopes: [AuthOrchestrationReadScope, AuthOrchestrationOperateScope],
    issued_at: 0,
    expires_at: 1_000_000_000,
    revoked: false,
  },
  credential: null,
  run: null,
  trusted_cli_authority_id: null,
});
const harness = () => {
  const state = {
    capture: record,
    current: current(),
    checks: [] as { ceiling: Callers.RecordedCallerV1["ceiling"]; thread: string | null }[],
  };
  const layer = Callers.layer.pipe(
    Layer.provide(
      Layer.succeed(Callers.CallerAuthority, {
        capture: () => Effect.succeed(state.capture),
        refresh: () => Effect.succeed(state.current),
        checkTarget: (_record, ceiling, _permission, thread) =>
          Effect.sync(() => {
            state.checks.push({ ceiling, thread });
          }),
      }),
    ),
  );
  return { state, layer };
};
it.effect(
  "captures closed non-secret evidence and fences malformed or missing historical admissions",
  () => {
    const h = harness();
    return Effect.gen(function* () {
      const callers = yield* Callers.RecordedCallers;
      expect(yield* callers.captureCaller()).toEqual(record);
      for (const input of [
        null,
        {},
        { ...record, version: 2 },
        { ...record, token: "never-store-this" },
        { ...record, source: { ...record.source, store_generation: 0 } },
      ])
        expect(Exit.isFailure(yield* Effect.exit(callers.readmitRecorded(input)))).toBe(true);
      expect(h.state.checks).toEqual([]);
    }).pipe(Effect.provide(h.layer));
  },
);
it.effect("intersects original, requested and current MCP ceilings on every target check", () => {
  const h = harness();
  return Effect.gen(function* () {
    const callers = yield* Callers.RecordedCallers;
    const caller = yield* callers.readmitRecorded(record);
    h.state.current = {
      ...current(),
      admission: {
        ...record,
        ceiling: {
          ...record.ceiling,
          limits: { runtimeMode: "full-access", interactionMode: "default" },
        },
      },
    };
    yield* caller.check("workstreams:write", "target-thread");
    expect(h.state.checks.at(-1)).toMatchObject({
      thread: "target-thread",
      ceiling: { limits: record.ceiling.limits },
    });
    h.state.current = {
      ...current(),
      admission: {
        ...record,
        ceiling: {
          ...record.ceiling,
          limits: { runtimeMode: "approval-required", interactionMode: "plan" },
        },
      },
    };
    expect(
      Exit.isFailure(yield* Effect.exit(caller.check("workstreams:write", "target-thread"))),
    ).toBe(true);
    yield* caller.check("workstreams:read", "target-thread");
    expect(h.state.checks.at(-1)?.ceiling.limits).toEqual({
      runtimeMode: "approval-required",
      interactionMode: "plan",
    });
  }).pipe(Effect.provide(h.layer));
});
it.effect("revocation, expiry, grant and enrollment fences prevent further target calls", () => {
  const h = harness();
  return Effect.gen(function* () {
    const callers = yield* Callers.RecordedCallers;
    const caller = yield* callers.readmitRecorded(record);
    const baseline = current();
    const variants: Callers.CurrentCallerState[] = [
      { ...baseline, session: null },
      { ...baseline, session: { ...baseline.session!, revoked: true } },
      { ...baseline, session: { ...baseline.session!, expires_at: 0 } },
      { ...baseline, session: { ...baseline.session!, issued_at: 10 } },
      { ...baseline, session: { ...baseline.session!, scopes: [] } },
      { ...baseline, grant: { ...baseline.grant, active: false } },
      { ...baseline, grant: { ...baseline.grant, permissions: ["workstreams:read"] } },
      { ...baseline, enrollment: null },
      {
        ...baseline,
        enrollment: { ...baseline.enrollment!, session_id: "caller-session-sentinel" },
      },
      { ...baseline, registry_active: false },
      { ...baseline, singleton_active: false },
      { ...baseline, source_binding_active: false },
    ];
    for (const value of variants) {
      h.state.current = value;
      expect(Exit.isFailure(yield* Effect.exit(caller.check("workstreams:write", null)))).toBe(
        true,
      );
    }
    expect(h.state.checks).toHaveLength(0);
    h.state.current = baseline;
    yield* TestClock.adjust(1_000_000_000);
    expect(Exit.isFailure(yield* Effect.exit(caller.check("workstreams:read", null)))).toBe(true);
  }).pipe(Effect.provide(h.layer));
});
it.effect(
  "same-kind callers, changed source, binding and home pins never gain visibility or readmission",
  () => {
    const h = harness();
    return Effect.gen(function* () {
      const callers = yield* Callers.RecordedCallers;
      const variants: Callers.RecordedCallerV1[] = [
        {
          ...record,
          auth: {
            ...record.auth,
            session_id: "different-session",
          } as Callers.RecordedCallerV1["auth"],
        },
        { ...record, source: { ...record.source, store_generation: 10 } },
        { ...record, source: { ...record.source, binding_version: 10 } },
        { ...record, source: { ...record.source, authority_namespace: "changed" } },
        { ...record, destination: { ...record.destination, home_generation: 10 } },
        {
          ...record,
          singleton_binding_version: 10,
          destination: { ...record.destination, binding_version: 10 },
        },
      ];
      expect(yield* callers.visible(record, null)).toBe(true);
      for (const value of variants) {
        h.state.capture = value;
        h.state.current = current(value);
        expect(yield* callers.visible(record, null)).toBe(false);
        expect(Exit.isFailure(yield* Effect.exit(callers.readmitRecorded(record)))).toBe(true);
      }
      expect(yield* callers.visible(null, null)).toBe(false);
    }).pipe(Effect.provide(h.layer));
  },
);
it.effect("agent recovery requires the original credential, run and provider association", () => {
  const h = harness();
  const agent: Callers.RecordedCallerV1 = {
    ...record,
    auth: {
      kind: "agent",
      calling_thread_id: "caller-thread",
      provider_instance_id: "provider",
      mcp_credential_id: "credential-id",
      run_id: "original-run",
      provider_thread_id: "provider-thread",
      provider_session_id: "native-session",
    },
  };
  const baseline: Callers.CurrentCallerState = {
    ...current(agent),
    session: null,
    credential: {
      credential_id: "credential-id",
      calling_thread_id: "caller-thread",
      provider_instance_id: "provider",
      active: true,
    },
    run: {
      run_id: "original-run",
      calling_thread_id: "caller-thread",
      provider_instance_id: "provider",
      provider_thread_id: "provider-thread",
      provider_session_id: "native-session",
      active: true,
    },
  };
  return Effect.gen(function* () {
    const callers = yield* Callers.RecordedCallers;
    h.state.current = baseline;
    const caller = yield* callers.readmitRecorded(agent);
    yield* caller.check("workstreams:write", "target-thread");
    for (const value of [
      { ...baseline, credential: null },
      { ...baseline, run: null },
      { ...baseline, run: { ...baseline.run!, run_id: "new-current-run" } },
      { ...baseline, credential: { ...baseline.credential!, calling_thread_id: "target-thread" } },
      { ...baseline, run: { ...baseline.run!, provider_instance_id: "other-provider" } },
    ]) {
      h.state.current = value;
      expect(
        Exit.isFailure(yield* Effect.exit(caller.check("workstreams:write", "target-thread"))),
      ).toBe(true);
    }
    expect(h.state.checks).toHaveLength(1);
  }).pipe(Effect.provide(h.layer));
});

it.effect(
  "raw-effect wrapper preserves original intent and request bytes and never upgrades historical intent",
  () =>
    Effect.gen(function* () {
      const intentBody =
        '{ "command": { "command_id": "exact-command" }, "protected": "intent-sentinel" }';
      const body = '{ "keep": "original request bytes" }';
      const stored = yield* Callers.wrapEffectIntent(intentBody, body, hash(body), record);
      expect(stored.intent_body).toBe(intentBody);
      const decoded = yield* Callers.unwrapEffectIntent(stored);
      expect(decoded.intent).toEqual(JSON.parse(intentBody));
      expect(decoded.recovery?.request_body).toBe(body);
      expect(decoded.recovery?.recorded_caller).toEqual(record);
      expect(yield* Callers.unwrapEffectIntent(JSON.parse(intentBody))).toEqual({
        intent: JSON.parse(intentBody),
        recovery: null,
      });
      for (const value of [
        { ...stored, request_body: body + " " },
        { ...stored, format: "unsupported" },
        { ...stored, intent_body: "invalid" },
      ])
        expect(Exit.isFailure(yield* Effect.exit(Callers.unwrapEffectIntent(value)))).toBe(true);
    }).pipe(Effect.provide(NodeCrypto.layer)),
);
