import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  OrchestratorMcpFailure,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import {
  WORKSTREAMS_HOME_READ_RESULT_CONTRACT as contract,
  type WorkstreamsHomeReadResultRequest,
} from "@t3tools/contracts/jones/workstreamsHomeReadResult";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import { liveThreadShell } from "../../../../mcp/McpToolAccess.testkit.ts";
import * as Threads from "../../../../orchestration-v2/ThreadManagementService.ts";
import * as Peer from "../../../../peer/PeerMcpClient.ts";
import * as Member from "../read/memberRead.ts";
import { RegistryError } from "../registry/session.ts";
import * as Protocol from "./homeReadProtocol.ts";
import * as Client from "./peerHomeReadClient.ts";

const home = {
  registry_instance_id: "11111111-1111-4111-8111-111111111111",
  owner_id: "owner:test",
  home_environment_id: "home:test",
  server_generation: 1,
};
const counts: WorkstreamsHomeReadResultRequest = {
  contract,
  request: { route: "counts", params: {} },
};
const limits: Peer.PeerCallLimits = {
  runtimeMode: "approval-required",
  interactionMode: "plan",
};
const scope: Mcp.McpInvocationScope = {
  environmentId: EnvironmentId.make("member:test"),
  capabilities: new Set(["orchestration"]),
  issuedAt: 1,
  requestNamespace: "synthetic:test",
  thread: undefined,
  client: undefined,
};

const refused = <A>(effect: Effect.Effect<A, RegistryError>) =>
  effect.pipe(
    Effect.match({
      onFailure: (error) => error,
      onSuccess: () => {
        throw new Error("Expected a RegistryError refusal.");
      },
    }),
  );

// Routing/admission, thread lookup and the peer client are synthetic external ports.
// No member enrollment, native authentication, transport enforcement or physical peer is proved.
const fixture = (
  options: {
    raw?: unknown;
    failure?: OrchestratorMcpFailure;
    admissionFailure?: RegistryError;
    check?: (ordinal: number) => Effect.Effect<void, RegistryError>;
  } = {},
) => {
  const calls: Array<Parameters<Peer.PeerMcpClient["Service"]["call"]>[0]> = [];
  let admissions = 0;
  let checks = 0;
  const raw = options.raw ?? { sentinel: "unvalidated result" };
  const route = {
    home: { ...home },
    link_generation: "qualified-link-generation:7",
    limits: { ...limits },
    checkCurrent: () =>
      Effect.suspend(() => {
        checks++;
        return options.check?.(checks) ?? Effect.void;
      }),
  };
  const ports = Layer.mergeAll(
    Layer.succeed(Client.MemberHomeReadRouting, {
      current: () =>
        Effect.suspend(() => {
          admissions++;
          return options.admissionFailure === undefined
            ? Effect.succeed(route)
            : Effect.fail(options.admissionFailure);
        }),
    }),
    Layer.succeed(Peer.PeerMcpClient, {
      call: (input) =>
        Effect.gen(function* () {
          calls.push(input);
          if (options.failure !== undefined) return yield* options.failure;
          // oxlint-disable-next-line t3code/no-inline-schema-compile -- This synthetic generic client decodes the per-call schema supplied by the real port.
          return yield* Schema.decodeUnknownEffect(input.success)(raw).pipe(
            Effect.mapError(
              () =>
                new OrchestratorMcpFailure({
                  code: "orchestration_error",
                  message: "Synthetic decoder refused.",
                }),
            ),
          );
        }),
    }),
  );
  return {
    calls,
    raw,
    route,
    admissions: () => admissions,
    checks: () => checks,
    layer: Client.layer.pipe(Layer.provide(ports)),
  };
};

it.effect(
  "makes exactly one fixed call with the closed request, admitted target, limits and generation",
  () => {
    const f = fixture();
    return Effect.gen(function* () {
      const result = yield* (yield* Protocol.HomeReadResultClient).read(home, counts);
      expect(result).toBe(f.raw);
      expect(f.calls).toHaveLength(1);
      expect(f.calls[0]).toEqual({
        environmentId: EnvironmentId.make(home.home_environment_id),
        tool: "jones_workstreams_home_read_result",
        arguments: counts,
        limits,
        expectedGeneration: f.route.link_generation,
        success: Schema.Unknown,
      });
      expect(f.calls[0]?.arguments).toBe(counts);
      expect(f.calls[0]?.expectedGeneration).toBe("qualified-link-generation:7");
      expect(f.admissions()).toBe(1);
      expect(f.checks()).toBe(2);
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect.each([
  ["registry_instance_id", "22222222-2222-4222-8222-222222222222", "registry_mismatch"],
  ["owner_id", "owner:other", "registry_mismatch"],
  ["home_environment_id", "home:other", "registry_mismatch"],
  ["server_generation", 2, "generation_mismatch"],
] as const)("refuses expected home mismatch %s without transport", ([field, value, code]) => {
  const f = fixture();
  const expected = { ...home, [field]: value };
  return Effect.gen(function* () {
    expect(
      yield* (yield* Protocol.HomeReadResultClient).read(expected, counts).pipe(refused),
    ).toMatchObject({ code });
    expect(f.calls).toHaveLength(0);
  }).pipe(Effect.provide(f.layer));
});

it.effect.each(["before", "after"] as const)("fences %s-dispatch currentness changes", (when) => {
  const f = fixture({
    check: (ordinal) =>
      ordinal === (when === "before" ? 1 : 2)
        ? Effect.fail(new RegistryError({ code: "generation_conflict" }))
        : Effect.void,
  });
  return Effect.gen(function* () {
    expect(
      yield* (yield* Protocol.HomeReadResultClient).read(home, counts).pipe(refused),
    ).toMatchObject({ code: "generation_conflict" });
    expect(f.calls).toHaveLength(when === "before" ? 0 : 1);
  }).pipe(Effect.provide(f.layer));
});

it.effect("does not invent a route when source admission is unavailable", () => {
  const f = fixture({ admissionFailure: new RegistryError({ code: "member_not_supported" }) });
  return Effect.gen(function* () {
    expect(
      yield* (yield* Protocol.HomeReadResultClient).read(home, counts).pipe(refused),
    ).toMatchObject({ code: "member_not_supported" });
    expect(f.calls).toHaveLength(0);
    expect(f.checks()).toBe(0);
  }).pipe(Effect.provide(f.layer));
});

it.effect.each([
  "empty-generation",
  "blank-generation",
  "runtime-limit",
  "interaction-limit",
  "destination",
] as const)("refuses invalid admitted %s before dispatch", (fault) => {
  const f = fixture();
  if (fault === "empty-generation") f.route.link_generation = "";
  if (fault === "blank-generation") f.route.link_generation = "   ";
  // Deliberately corrupt external port values at runtime without a type assertion.
  if (fault === "runtime-limit")
    Object.defineProperty(f.route.limits, "runtimeMode", { value: "invalid" });
  if (fault === "interaction-limit")
    Object.defineProperty(f.route.limits, "interactionMode", { value: "invalid" });
  if (fault === "destination") f.route.home.home_environment_id = " ";
  const code = fault.endsWith("generation")
    ? "link_generation_required"
    : fault === "destination"
      ? "registry_mismatch"
      : "invalid_limits";
  return Effect.gen(function* () {
    expect(
      yield* (yield* Protocol.HomeReadResultClient)
        .read(fault === "destination" ? f.route.home : home, counts)
        .pipe(refused),
    ).toMatchObject({ code });
    expect(f.calls).toHaveLength(0);
  }).pipe(Effect.provide(f.layer));
});

it.effect.each([
  "linked",
  "full-access-linked",
  "remote-origin",
  "linked-thread",
  "full-access-linked-thread",
] as const)("refuses %s scope before routing or transport", (kind) => {
  const f = fixture();
  const isThread = kind.endsWith("thread");
  const access: Mcp.McpClientCaller["access"] = kind.startsWith("full-access")
    ? "full-access"
    : "read-only";
  const invocation: Mcp.McpInvocationScope = {
    ...scope,
    thread: isThread
      ? {
          threadId: ThreadId.make("thread:test"),
          providerSessionId: "provider-session:test",
          providerInstanceId: ProviderInstanceId.make("codex"),
        }
      : undefined,
    client: isThread
      ? undefined
      : {
          sessionId: "link:test",
          label: "Attribution only",
          access,
          linked: kind !== "remote-origin",
          ...(kind === "remote-origin"
            ? {
                remoteOrigin: {
                  schemaVersion: "t3.linked-message-origin/v1",
                  environmentId: "untrusted-label:test",
                  linkSessionId: "link:test",
                },
              }
            : {}),
        },
  };
  const thread = {
    ...liveThreadShell(ThreadId.make("thread:test")),
    linkOrigin: { sessionId: "link:test", label: "Attribution only", access },
  };
  return Effect.gen(function* () {
    expect(
      yield* (yield* Protocol.HomeReadResultClient).read(home, counts).pipe(refused),
    ).toMatchObject({ code: "home_route_required" });
    expect(f.admissions()).toBe(0);
    expect(f.calls).toHaveLength(0);
  }).pipe(
    Effect.provide(f.layer),
    Effect.provideService(Mcp.McpInvocationContext, invocation),
    Effect.provide(
      Layer.mock(Threads.ThreadManagementService)({
        getThreadShell: () => Effect.succeed(thread),
      }),
    ),
  );
});

it.effect("refuses thread forwarding when caller provenance cannot be checked", () => {
  const f = fixture();
  return Effect.gen(function* () {
    expect(
      yield* (yield* Protocol.HomeReadResultClient).read(home, counts).pipe(refused),
    ).toMatchObject({ code: "caller_unavailable" });
    expect(f.admissions()).toBe(0);
    expect(f.calls).toHaveLength(0);
  }).pipe(
    Effect.provide(f.layer),
    Effect.provideService(Mcp.McpInvocationContext, {
      ...scope,
      thread: {
        threadId: ThreadId.make("thread:test"),
        providerSessionId: "provider-session:test",
        providerInstanceId: ProviderInstanceId.make("codex"),
      },
    }),
  );
});

it.effect.each([
  ["missing-tool", "invalid_request"],
  ["unsupported-version", "capability_denied"],
  ["lost-reply", "orchestration_error"],
] as const)(
  "preserves bounded %s failure without retry or bare-page fallback",
  ([_fault, code]) => {
    const failure = new OrchestratorMcpFailure({
      code,
      message: "Synthetic private body must stay internal.",
    });
    const f = fixture({ failure });
    return Effect.gen(function* () {
      const error = yield* (yield* Protocol.HomeReadResultClient).read(home, counts).pipe(refused);
      expect(error.code).toBe(code);
      expect(error.cause).toBe(failure);
      expect(error.message).toBe("Native Workstreams request refused.");
      expect(f.calls).toHaveLength(1);
      expect(f.checks()).toBe(1);
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect.each([undefined, "workstreams-t3-placement/2.0.0"] as const)(
  "preserves placement selection %s and the original request unchanged",
  (selected) => {
    const f = fixture();
    const request: WorkstreamsHomeReadResultRequest = {
      contract,
      request: {
        route: "placements",
        params: {
          identities: [
            {
              source_instance_id: "source:test",
              native_thread_id: "thread:test",
            },
          ],
        },
      },
      ...(selected === undefined ? {} : { placement_contract: selected }),
    };
    return Effect.gen(function* () {
      yield* (yield* Protocol.HomeReadResultClient).read(home, request);
      expect(f.calls).toHaveLength(1);
      expect(f.calls[0]?.arguments).toBe(request);
      expect(Object.hasOwn(f.calls[0]?.arguments ?? {}, "placement_contract")).toBe(
        selected !== undefined,
      );
      expect(f.calls[0]?.arguments).toEqual(request);
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect.each(["version", "bare", "unknown-field"] as const)(
  "leaves %s results untouched for existing strict homeReadPort decoding",
  (fault) => {
    const raw =
      fault === "version"
        ? { contract: "unsupported/2", route: "counts" }
        : fault === "bare"
          ? { active: 1, total: 1 }
          : { contract, route: "counts", unexpected: true };
    const f = fixture({ raw });
    return Effect.gen(function* () {
      expect(yield* (yield* Protocol.HomeReadResultClient).read(home, counts)).toBe(raw);
      expect(
        yield* (yield* Member.HomeReadPort).read(home, counts.request).pipe(refused),
      ).toMatchObject({ code: "invalid_response" });
      expect(f.calls).toHaveLength(2); // Two explicit reads; each dispatches exactly once.
    }).pipe(
      Effect.provide(Layer.merge(f.layer, Protocol.homeReadPortLayer.pipe(Layer.provide(f.layer)))),
    );
  },
);

it.effect("rejects a non-closed request instead of stripping fields or forwarding", () => {
  const f = fixture();
  const request = { ...counts, environmentId: "untrusted:target" };
  return Effect.gen(function* () {
    expect(
      yield* (yield* Protocol.HomeReadResultClient).read(home, request).pipe(refused),
    ).toMatchObject({ code: "invalid_request" });
    expect(f.calls).toHaveLength(0);
  }).pipe(Effect.provide(f.layer));
});
