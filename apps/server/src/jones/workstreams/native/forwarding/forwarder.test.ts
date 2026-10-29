// @effect-diagnostics nodeBuiltinImport:off -- Retained file-backed synthetic SQLite evidence belongs to root.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import { expect, it } from "@effect/vitest";
import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Wire from "@t3tools/contracts/jones/workstreamsForwarding";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as SqlClient from "effect/sql/SqlClient";
import * as Peer from "../../../../peer/PeerMcpClient.ts";
import { canonicalJson } from "../../operations/encoding.ts";
import { RegistryError } from "../registry/session.ts";
import * as Admission from "../runtime/memberAdmission.ts";
import type { MemberWriteTarget } from "../runtime/memberRecordedCaller.ts";
import * as Store from "../store/operationStore.ts";
import * as Bindings from "../store/bindingStore.ts";
import * as Test from "../store/sql.testkit.ts";
import * as Home from "./homeClient.ts";
import * as Forwarder from "./forwarder.ts";

const decodeCaller = Schema.decodeUnknownEffect(Admission.MemberRecordedWriteCallerV2);
const decodeForward = Schema.decodeUnknownEffect(Wire.WorkstreamsForwardRequest);
const sha = (body: string) => NodeCrypto.createHash("sha256").update(body).digest("hex");
const origin = { kind: "agent" as const };
const record = (target: MemberWriteTarget): Admission.MemberRecordedWriteCallerV2 => ({
  schema: "jones-workstreams-member-recorded-write-caller/2",
  local: {
    schema: "jones-workstreams-member-local-write-caller/2",
    environment_id: "member:test",
    auth: {
      kind: "agent",
      calling_thread_id: "caller:test",
      provider_instance_id: "codex:test",
      mcp_credential_id: "credential-caller-sentinel",
      run_id: "run:original",
      provider_thread_id: "provider-thread:test",
      provider_session_id: "provider-session:test",
    },
    origin,
    ceiling: {
      permissions: ["workstreams:read", "workstreams:write"],
      scopes: [],
      capabilities: ["orchestration"],
      limits: { runtimeMode: "auto", interactionMode: "default" },
    },
    session: null,
    request_namespace: "request:test",
    invocation_issued_at: 1,
    link_origin: null,
    link_session: null,
    captured_at: 1,
    target,
  },
  association: {
    enrollment_id: "enrollment:test",
    enrollment_session_id: "enrollment-session:test",
    intent_sha256: "a".repeat(64),
    home: { ...Test.scope, home_environment_id: "home:test", server_generation: 1 },
    canonical_origin: "https://native.workstreams.invalid",
    principal_id: Test.binding.principal_id,
    singleton_binding_version: 1,
    pin_bound_revision: 3,
    source_binding_version: 4,
    source: {
      environment_id: "member:test",
      authority_namespace: "source-store",
      store_generation: 1,
    },
    link_generation: "opaque-link:original",
    member_pin_id: "member-pin:test",
    home_pin_id: "home-pin:test",
    home_pin_bound_revision: 5,
  },
  original_outbound_ceiling: {
    access: "auto",
    runtime_mode: "auto",
    interaction_mode: "default",
    permissions: ["workstreams:read", "workstreams:write"],
    scopes: ["orchestration:read", "orchestration:operate"],
  },
});
const raw = (id = "command:source-00001") =>
  canonicalJson({
    command_id: id,
    expected_server_generation: 1,
    expected_registry_version: 0,
    action: {
      operation: "create_workstream",
      name: "request-body-sentinel",
      lifecycle: "active",
      progress: { state: "unknown" },
      sort_order: 0,
    },
  });
const operation = (
  id = "operation:source-001",
  kind: "set_primary" | "create_workstream" = "create_workstream",
) => ({
  contract: "workstream-operations/1.0.0",
  operation_id: id,
  origin,
  kind,
  ...(kind === "create_workstream"
    ? {
        registry: { registry_id: Test.scope.registry_instance_id, owner_id: Test.scope.owner_id },
        workstream: {
          name: "request-body-sentinel",
          lifecycle: "active",
          progress: { state: "unknown" },
          sort_order: 0,
        },
      }
    : {
        thread: { environment_id: "member:test", thread_id: "thread:target" },
        target: {
          kind: "unassigned",
          registry_id: Test.scope.registry_instance_id,
          owner_id: Test.scope.owner_id,
        },
        expected: { kind: "any" },
      }),
});
const appearance = (id = "appearance:source-01") =>
  canonicalJson({
    command_id: id,
    expected_server_generation: 1,
    expected_version: 0,
    workstream_id: "workstream:test",
    border_color: "#AABBCC",
  });
const resultFor = (request: Wire.WorkstreamsForwardRequest): Wire.WorkstreamsForwardResult =>
  ({
    contract: Wire.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
    identity: request.identity,
    home: {
      registry: {
        ...request.identity.expected_home,
        canonical_origin: request.identity.canonical_origin,
        registry_version: 17,
      },
      principal_id: Test.binding.principal_id,
      authorization_revision: 9,
      source_binding_version: 4,
      incoming_peer_pin_id: "home-pin:test",
      incoming_peer_pin_revision: 5,
    },
    ...(request.kind === "command"
      ? {
          kind: "command" as const,
          result: {
            ...Test.terminal(Test.pending(request.identity.command_ids[0]!, request.request.body))
              .receipt,
            operation: JSON.parse(request.request.body).action.operation,
          },
        }
      : request.kind === "appearance"
        ? {
            kind: "appearance" as const,
            result: {
              workstream_id: request.request.request.workstream_id,
              border_color: request.request.request.border_color,
              version: request.request.request.expected_version + 1,
            },
          }
        : {
            kind: "operation" as const,
            result: {
              contract: "workstream-operations/1.0.0" as const,
              operation_id: request.identity.source_operation_id,
              kind: request.request.request.kind,
              origin: request.request.request.origin,
              request_sha256: sha(canonicalJson(request.request.request)),
              observed: null,
              children: [],
              accepted_at: Test.now,
              updated_at: Test.now,
              state: "committed" as const,
              reason: null,
              result: {
                workstream_id:
                  request.request.request.kind === "set_primary" ? null : "result-value-sentinel",
                membership_id:
                  request.request.request.kind === "set_primary" ? "membership:test" : null,
                registry_version: 17,
              },
              poll_after_ms: null,
              detail: null,
            },
          }),
  }) as Wire.WorkstreamsForwardResult;

const fixture = () => {
  const parent =
    process.env.NW_SOURCE_EVIDENCE_DIR ??
    NodePath.join(NodeOS.tmpdir(), "nw-source-forwarding-evidence");
  NodeFS.mkdirSync(parent, { recursive: true });
  const dir = NodeFS.mkdtempSync(NodePath.join(parent, "sqlite-"));
  const filename = NodePath.join(dir, "forward.sqlite");
  let current = record({ kind: "metadata" }),
    revoked = false,
    offline = false,
    lost = false;
  let calls = 0,
    readbacks = 0;
  let readbackReason: "not_found" | "in_flight" | "unsupported_protocol" = "not_found";
  let invalidResult = false,
    unavailableHome = false;
  let barrier: Effect.Effect<void> = Effect.void;
  const retained = new Map<string, Wire.WorkstreamsForwardResult>();
  const requests: Wire.WorkstreamsForwardRequest[] = [];
  const admit = (input: unknown) =>
    Effect.gen(function* () {
      const captured = yield* decodeCaller(input).pipe(
        Effect.mapError(() => new RegistryError({ code: "invalid_request" })),
      );
      const check = () =>
        Effect.suspend(() =>
          revoked ||
          canonicalJson({
            ...captured,
            local: { ...captured.local, target: current.local.target },
          }) !== canonicalJson(current) ||
          captured.local.link_origin !== null ||
          captured.local.ceiling.limits?.interactionMode === "plan" ||
          !captured.local.ceiling.permissions.includes("workstreams:write")
            ? Effect.fail(new RegistryError({ code: "forbidden" }))
            : Effect.succeed(captured.local.ceiling.limits!),
        );
      const limits = yield* check();
      return { record: captured, ceiling: captured.local.ceiling, limits, checkCurrent: check };
    });
  const ports = Layer.mergeAll(
    Layer.succeed(Admission.MemberAdmission, {
      captureWrite: (target) => {
        current = { ...current, local: { ...current.local, target } };
        return admit(current).pipe(Effect.map((a) => a.record));
      },
      readmitWrite: admit,
      readmitWriteRetained: admit,
    } as Admission.MemberAdmission["Service"]),
    Layer.succeed(Forwarder.MemberCommandTargets, {
      resolve: () => Effect.succeed({ kind: "metadata" as const }),
    }),
    Layer.succeed(Peer.PeerMcpClient, {
      call: (input) =>
        Effect.gen(function* () {
          if (offline)
            return yield* new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "synthetic offline",
            });
          expect(input.environmentId).toBe("home:test");
          expect(input.expectedGeneration).toBe("opaque-link:original");
          expect(input.origin).toBeUndefined();
          const identity = input.arguments.identity as Wire.WorkstreamsForwardIdentity;
          let value: unknown;
          if (input.tool === "jones_workstreams_home_forward") {
            const request = yield* decodeForward(input.arguments).pipe(
              Effect.mapError(
                () =>
                  new OrchestratorMcpFailure({
                    code: "orchestration_error",
                    message: "synthetic decode",
                  }),
              ),
            );
            requests.push(request);
            calls++;
            yield* barrier;
            const normal = resultFor(request);
            const result =
              unavailableHome && normal.kind === "operation"
                ? {
                    ...normal,
                    result: {
                      ...normal.result,
                      state: "unavailable" as const,
                      reason: "backpressure" as const,
                      result: null,
                      poll_after_ms: null,
                      detail: null,
                    },
                  }
                : normal;
            retained.set(identity.forward_id, result);
            if (lost)
              return yield* new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "synthetic lost reply after commit",
              });
            value = invalidResult ? { ...result, contract: "unsupported/9" } : result;
          } else {
            expect(input.tool).toBe("jones_workstreams_home_forward_readback");
            readbacks++;
            const result = retained.get(identity.forward_id);
            value = {
              contract: Wire.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
              identity,
              ...(result
                ? { state: "present", result }
                : { state: "unknown", reason: readbackReason }),
            };
          }
          // oxlint-disable-next-line t3code/no-inline-schema-compile -- Synthetic generic peer must decode the per-call production success schema.
          return yield* Schema.decodeUnknownEffect(input.success)(value).pipe(
            Effect.mapError(
              () =>
                new OrchestratorMcpFailure({
                  code: "orchestration_error",
                  message: "synthetic decode refused",
                }),
            ),
          );
        }),
    }),
  );
  const layer = Forwarder.layer.pipe(
    Layer.provideMerge(Home.layer),
    Layer.provideMerge(ports),
    Layer.provideMerge(Forwarder.locksLayer),
    Layer.provideMerge(Test.testLayer({ filename })),
  );
  const seed = Effect.gen(function* () {
    yield* Test.seed;
    yield* (yield* Bindings.BindingStore).bind(Test.binding, null);
  });
  return {
    layer,
    seed,
    filename,
    requests,
    retained,
    calls: () => calls,
    readbacks: () => readbacks,
    revoke: () => {
      revoked = true;
    },
    offline: () => {
      offline = true;
    },
    lose: () => {
      lost = true;
    },
    change: (
      f: (r: Admission.MemberRecordedWriteCallerV2) => Admission.MemberRecordedWriteCallerV2,
    ) => {
      current = f(current);
    },
    barrier: (effect: Effect.Effect<void>) => {
      barrier = effect;
    },
    unknown: (reason: typeof readbackReason) => {
      retained.clear();
      readbackReason = reason;
    },
    unavailableHome: () => {
      unavailableHome = true;
    },
    invalid: () => {
      invalidResult = true;
    },
  };
};
const refused = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.exit(effect).pipe(Effect.map((e) => e._tag));

it.effect(
  "all three kinds use real encrypted member storage, one home dispatch each and exact same-ID replay",
  () => {
    const f = fixture();
    return Effect.gen(function* () {
      yield* f.seed;
      const owner = yield* Forwarder.MemberForwarder,
        store = yield* Store.OperationStore;
      for (const input of [
        { kind: "command" as const, body: " \n" + raw() + "\n" },
        { kind: "operation" as const, body: canonicalJson(operation()) },
        { kind: "appearance" as const, body: appearance() },
      ]) {
        const result = yield* owner.submit(input);
        expect(result.state).toBe("committed");
        expect(yield* owner.submit(input)).toEqual(result);
        expect((yield* store.readMemberForward(result.operation_id))?.request_body).toBe(
          input.body,
        );
        expect(
          (yield* store.readMemberForward(result.operation_id))?.member_forward
            .forward_request_body,
        ).toBe(JSON.stringify(f.requests[f.requests.length - 1]));
        expect(yield* refused(store.read(result.operation_id))).toBe("Failure");
      }
      expect(f.calls()).toBe(3);
      expect(yield* store.open()).toEqual([]);
      expect(yield* owner.open()).toEqual([]);
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect(
  "reordered operation bytes conflict while canonical public digest remains unchanged",
  () => {
    const f = fixture();
    return Effect.gen(function* () {
      yield* f.seed;
      const owner = yield* Forwarder.MemberForwarder;
      const request = operation(),
        body = JSON.stringify(request, null, 2);
      const view = yield* owner.submit({ kind: "operation", body });
      expect(view.result?.identity.request_sha256).toBe(sha(body));
      expect(view.result?.kind === "operation" && view.result.result.request_sha256).toBe(
        sha(canonicalJson(request)),
      );
      expect(sha(body)).not.toBe(sha(canonicalJson(request)));
      expect(
        yield* refused(owner.submit({ kind: "operation", body: canonicalJson(request) })),
      ).toBe("Failure");
      expect(f.calls()).toBe(1);
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect(
  "raw command whitespace, parent identity and original caller changes never replace accepted intent",
  () => {
    const f = fixture();
    return Effect.gen(function* () {
      yield* f.seed;
      const owner = yield* Forwarder.MemberForwarder;
      yield* owner.submit({ kind: "command", body: raw() });
      expect(yield* refused(owner.submit({ kind: "command", body: " " + raw() }))).toBe("Failure");
      expect(
        yield* refused(
          owner.submit({
            kind: "command",
            body: raw(),
            parent_operation_id: "parent:changed-0001",
          }),
        ),
      ).toBe("Failure");
      f.change((r) => ({
        ...r,
        local: {
          ...r.local,
          auth: { ...r.local.auth, run_id: "run:later" } as typeof r.local.auth,
        },
      }));
      expect(yield* refused(owner.submit({ kind: "command", body: raw() }))).toBe("Failure");
      expect(f.calls()).toBe(1);
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect("home unavailable before preflight creates no accepted mutation", () => {
  const f = fixture();
  f.offline();
  return Effect.gen(function* () {
    yield* f.seed;
    expect(
      yield* refused((yield* Forwarder.MemberForwarder).submit({ kind: "command", body: raw() })),
    ).toBe("Failure");
    expect(yield* (yield* Store.OperationStore).openMemberForwards()).toEqual([]);
    expect(f.calls()).toBe(0);
  }).pipe(Effect.provide(f.layer));
});

it.effect(
  "lost home reply survives SQLite close/reopen and reconciles the original home with one effect",
  () => {
    const f = fixture();
    f.lose();
    return Effect.gen(function* () {
      yield* Effect.gen(function* () {
        yield* f.seed;
        const view = yield* (yield* Forwarder.MemberForwarder).submit({
          kind: "command",
          body: raw(),
        });
        expect(view.state).toBe("reconciling");
        expect(view.result).toBeNull();
      }).pipe(Effect.provide(f.layer));
      yield* Effect.gen(function* () {
        const owner = yield* Forwarder.MemberForwarder;
        expect((yield* owner.read("command:source-00001"))?.state).toBe("reconciling");
        expect(f.readbacks()).toBe(1);
        expect((yield* owner.recover("command:source-00001"))?.state).toBe("committed");
        expect(f.calls()).toBe(1);
      }).pipe(Effect.provide(f.layer));
    });
  },
);

it.effect(
  "dispatch marker precedes network; concurrent duplicates serialize and open-thread uniqueness holds",
  () => {
    const f = fixture();
    return Effect.gen(function* () {
      yield* f.seed;
      const started = yield* Deferred.make<void>(),
        release = yield* Deferred.make<void>();
      f.barrier(Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release))));
      const owner = yield* Forwarder.MemberForwarder;
      const input = {
        kind: "operation" as const,
        body: canonicalJson(operation("operation:thread-001", "set_primary")),
      };
      const first = yield* Effect.forkChild(owner.submit(input));
      yield* Deferred.await(started);
      const row = yield* (yield* Store.OperationStore).readMemberForward("operation:thread-001");
      expect(row?.state).toBe("dispatching");
      expect(row?.thread_id).toBe("thread:target");
      const duplicate = yield* Effect.forkChild(owner.submit(input));
      expect(
        yield* refused(
          owner.submit({
            kind: "operation",
            body: canonicalJson(operation("operation:thread-002", "set_primary")),
          }),
        ),
      ).toBe("Failure");
      yield* Deferred.succeed(release, undefined);
      expect((yield* Fiber.join(first)).state).toBe("committed");
      expect((yield* Fiber.join(duplicate)).state).toBe("committed");
      expect(f.calls()).toBe(1);
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect(
  "crash at dispatch and unknown/not_found retain the fence without retransmission or new IDs",
  () => {
    const f = fixture();
    return Effect.gen(function* () {
      yield* f.seed;
      const started = yield* Deferred.make<void>();
      f.barrier(Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)));
      const owner = yield* Forwarder.MemberForwarder;
      const child = yield* Effect.forkChild(owner.submit({ kind: "command", body: raw() }));
      yield* Deferred.await(started);
      yield* Fiber.interrupt(child);
      f.unknown("not_found");
      const view = yield* owner.recover("command:source-00001");
      expect(view?.state).toBe("reconciling");
      expect(view?.result).toBeNull();
      expect(f.calls()).toBe(1);
      expect(yield* (yield* Store.OperationStore).openMemberForwards()).toHaveLength(1);
      expect(f.requests[0]?.identity.command_ids).toEqual(["command:source-00001"]);
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect("original run, revocation and relink remain fenced", () => {
  const f = fixture();
  return Effect.gen(function* () {
    yield* f.seed;
    const owner = yield* Forwarder.MemberForwarder;
    yield* owner.submit({ kind: "command", body: raw() });
    f.change((r) => ({ ...r, association: { ...r.association, link_generation: "relinked" } }));
    expect(yield* refused(owner.read("command:source-00001"))).toBe("Failure");
    f.revoke();
    expect(yield* refused(owner.recover("command:source-00001"))).toBe("Failure");
    expect(f.calls()).toBe(1);
  }).pipe(Effect.provide(f.layer));
});

it.effect(
  "unsupported result version becomes reconciling and unsupported preflight cannot accept",
  () => {
    const f = fixture();
    f.invalid();
    return Effect.gen(function* () {
      yield* f.seed;
      const owner = yield* Forwarder.MemberForwarder;
      expect((yield* owner.submit({ kind: "command", body: raw() })).state).toBe("reconciling");
      f.unknown("unsupported_protocol");
      expect((yield* owner.recover("command:source-00001"))?.state).toBe("reconciling");
      expect(
        yield* refused(owner.submit({ kind: "command", body: raw("command:new-0000001") })),
      ).toBe("Failure");
      expect(f.calls()).toBe(1);
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect("DB and live WAL contain no plaintext original caller/request/result sentinels", () => {
  const f = fixture();
  return Effect.gen(function* () {
    yield* f.seed;
    const sql = yield* SqlClient.SqlClient;
    yield* sql.unsafe("PRAGMA journal_mode=WAL");
    yield* (yield* Forwarder.MemberForwarder).submit({
      kind: "operation",
      body: canonicalJson(operation()),
    });
    expect(NodeFS.existsSync(f.filename + "-wal")).toBe(true);
    for (const path of [f.filename, f.filename + "-wal"]) {
      if (!NodeFS.existsSync(path)) continue;
      const bytes = NodeFS.readFileSync(path);
      for (const sentinel of [
        "credential-caller-sentinel",
        "request-body-sentinel",
        "result-value-sentinel",
      ])
        expect(bytes.includes(Buffer.from(sentinel))).toBe(false);
    }
    const rows = yield* sql<{ kind: string }>`SELECT kind FROM jones_workstreams_operations`;
    expect(rows).toEqual([{ kind: "create_workstream" }]);
  }).pipe(Effect.provide(f.layer));
});

it.effect(
  "known accepted intent survives reopen without inventing absence or dispatching after unknown readback",
  () => {
    const original = fixture(),
      restarted = fixture();
    return Effect.gen(function* () {
      const saved = yield* Effect.gen(function* () {
        yield* original.seed;
        yield* (yield* Forwarder.MemberForwarder).submit({ kind: "command", body: raw() });
        return (yield* (yield* Store.OperationStore).readMemberForward("command:source-00001"))!;
      }).pipe(Effect.provide(original.layer));
      yield* Effect.gen(function* () {
        yield* restarted.seed;
        const store = yield* Store.OperationStore;
        const accepted = yield* store.acceptMemberForward(saved);
        expect(accepted.state).toBe("accepted");
      }).pipe(Effect.provide(restarted.layer));
      yield* Effect.gen(function* () {
        const owner = yield* Forwarder.MemberForwarder;
        const recovered = yield* owner.recover("command:source-00001");
        expect(recovered?.state).toBe("accepted");
        expect(recovered?.next_poll_at).toBeNull();
        expect(restarted.calls()).toBe(0);
        expect(restarted.readbacks()).toBe(1);
        expect(yield* (yield* Store.OperationStore).openMemberForwards()).toHaveLength(1);
      }).pipe(Effect.provide(restarted.layer));
    });
  },
);

it.effect.each(["linked", "read-only", "plan"] as const)(
  "fresh %s caller cannot accept or dispatch metadata",
  (mode) => {
    const f = fixture();
    f.change((r) =>
      mode === "linked"
        ? { ...r, local: { ...r.local, link_origin: "linked:origin" } }
        : mode === "plan"
          ? {
              ...r,
              local: {
                ...r.local,
                ceiling: {
                  ...r.local.ceiling,
                  limits: { runtimeMode: "auto", interactionMode: "plan" },
                },
              },
            }
          : {
              ...r,
              original_outbound_ceiling: { ...r.original_outbound_ceiling, access: "read-only" },
            },
    );
    return Effect.gen(function* () {
      yield* f.seed;
      expect(
        yield* refused((yield* Forwarder.MemberForwarder).submit({ kind: "command", body: raw() })),
      ).toBe("Failure");
      expect(yield* (yield* Store.OperationStore).openMemberForwards()).toEqual([]);
      expect(f.calls()).toBe(0);
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect(
  "client routing, callback, generic-tool and CP selectors are refused before acceptance",
  () => {
    const f = fixture();
    return Effect.gen(function* () {
      yield* f.seed;
      const owner = yield* Forwarder.MemberForwarder;
      for (const key of ["tool", "url", "callback", "relay", "home", "origin", "control_plane"]) {
        const input = { kind: "command" as const, body: raw(), [key]: "untrusted-selector" };
        expect(yield* refused(owner.submit(input))).toBe("Failure");
      }
      expect(f.calls()).toBe(0);
      expect(f.readbacks()).toBe(0);
      expect(yield* (yield* Store.OperationStore).openMemberForwards()).toEqual([]);
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect(
  "post-dispatch home unavailable remains a reconciling source operation with its original result",
  () => {
    const f = fixture();
    f.unavailableHome();
    return Effect.gen(function* () {
      yield* f.seed;
      const owner = yield* Forwarder.MemberForwarder;
      const view = yield* owner.submit({ kind: "operation", body: canonicalJson(operation()) });
      expect(view.state).toBe("reconciling");
      expect(view.result?.kind === "operation" && view.result.result.state).toBe("unavailable");
      expect(yield* (yield* Store.OperationStore).openMemberForwards()).toHaveLength(1);
      expect((yield* owner.recover(view.operation_id))?.state).toBe("reconciling");
      expect(f.calls()).toBe(1);
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect(
  "terminal replay requires original-home readback and never substitutes unknown for a retained result",
  () => {
    const f = fixture();
    return Effect.gen(function* () {
      yield* f.seed;
      const owner = yield* Forwarder.MemberForwarder;
      const input = { kind: "command" as const, body: raw() };
      const original = yield* owner.submit(input);
      f.unknown("not_found");
      expect(yield* refused(owner.submit(input))).toBe("Failure");
      expect(yield* refused(owner.read(original.operation_id))).toBe("Failure");
      expect(
        (yield* (yield* Store.OperationStore).readMemberForward(original.operation_id))?.state,
      ).toBe("committed");
      expect(f.calls()).toBe(1);
    }).pipe(Effect.provide(f.layer));
  },
);
