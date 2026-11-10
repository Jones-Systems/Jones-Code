import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Dashboard from "./dashboard.ts";
import type { AdmittedCaller } from "../mode/authorityMode.ts";
const now = "2026-01-01T00:00:00.000Z",
  actor = { principal_id: "principal:test" };
const caller: AdmittedCaller = {
  principal_id: actor.principal_id,
  source_environment_id: "home:test",
  binding_version: 1,
  origin: { kind: "app" },
};
const context = { owner_id: "owner:test", server_generation: 1, registry_version: 1 };
const workstream = {
  workstream_id: "ws:test",
  owner_id: context.owner_id,
  name: "Synthetic dashboard",
  lifecycle: "active" as const,
  progress: { state: "progressing" as const },
  delivery: "none-observed" as const,
  freshness: "current" as const,
  sort_order: 0,
  version: 1,
  created_at: now,
  updated_at: now,
  created_by: actor,
  updated_by: actor,
};
const reference = {
  context,
  reference: {
    native_reference_id: "ref:pr",
    owner_id: context.owner_id,
    identity: {
      provider: "github",
      source_instance_id: "source:test",
      resource_kind: "pull_request",
      id_kind: "external" as const,
      native_id: "jones/repo#1",
      account_provenance: { kind: "not_account_scoped" as const },
    },
    pr_locator: {
      host: "github.com" as const,
      repository_owner: "jones",
      repository_name: "repo",
      number: 1,
    },
    registration: {
      state: "quarantined" as const,
      attestation_version: 0,
      attested_at: null,
      expires_at: null,
      evidence: null,
    },
    created_at: now,
    created_by: actor,
    created_registry_version: 1,
  },
  latest_observation: null,
};
const member = {
  membership_id: "membership:pr",
  workstream_id: "ws:test",
  native_reference_id: "ref:pr",
  kind: "secondary" as const,
  opened: { at: now, actor, command_id: "command:dashboard-0001", registry_version: 1 },
  closed: null,
};
const client = {
  listWorkstreams: () => Effect.succeed({ context, items: [workstream], next_cursor: null }),
  getWorkstream: () => Effect.succeed({ context, workstream }),
  listMemberships: () =>
    Effect.succeed({ context, items: [member], next_cursor: "membershipCursor" }),
  listDeclarations: () => Effect.succeed({ context, items: [], next_cursor: null }),
  listEdges: () => Effect.succeed({ context, items: [], next_cursor: null }),
  listHistory: () => Effect.succeed({ context, items: [], next_cursor: "historyCursor" }),
  getReference: () => Effect.succeed(reference),
};
const layers = (provided: typeof client = client) =>
  Dashboard.layer.pipe(
    Layer.provide(
      Layer.succeed(Dashboard.DashboardClient, { forCaller: () => Effect.succeed(provided) }),
    ),
  );
it.effect(
  "ports the frozen CP list/detail projection with linked PRs and honest page continuations",
  () =>
    Effect.gen(function* () {
      const d = yield* Dashboard.Dashboard,
        list = yield* d.list(caller),
        detail = yield* d.detail(caller, "ws:test");
      expect(list.items[0]).not.toHaveProperty("created_by");
      expect(detail.workstream).toEqual(list.items[0]);
      expect(detail.manually_linked_prs[0]?.url).toBe("https://github.com/jones/repo/pull/1");
      expect(detail.memberships[0]?.kind).toBe("secondary");
      expect(detail.continuation).toEqual({
        memberships_cursor: "membershipCursor",
        declarations_cursor: null,
        edges_cursor: null,
        history_cursor: "historyCursor",
      });
    }).pipe(Effect.provide(layers())),
);
it.effect("refuses mixed-revision detail pages instead of presenting them as one snapshot", () =>
  Effect.gen(function* () {
    expect((yield* Effect.flip((yield* Dashboard.Dashboard).detail(caller, "ws:test"))).code).toBe(
      "content_unavailable",
    );
  }).pipe(
    Effect.provide(
      layers({
        ...client,
        listHistory: () =>
          Effect.succeed({
            context: { ...context, registry_version: 2 },
            items: [],
            next_cursor: "historyCursor",
          }),
      }),
    ),
  ),
);
it.effect("rejects cross-owner list rows and invalid page requests", () =>
  Effect.gen(function* () {
    const d = yield* Dashboard.Dashboard;
    expect((yield* Effect.flip(d.list(caller, { limit: 101 }))).code).toBe("invalid_request");
    expect((yield* Effect.flip(d.list(caller))).code).toBe("content_unavailable");
  }).pipe(
    Effect.provide(
      layers({
        ...client,
        listWorkstreams: () =>
          Effect.succeed({
            context,
            items: [{ ...workstream, owner_id: "owner:other" }],
            next_cursor: null,
          }),
      }),
    ),
  ),
);
