import { expect, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import { canonical } from "../ordinaryCheckout/CheckoutProtocol.ts";
import { LifecycleError, type Binding, type InventoryEntry } from "./Protocol.ts";
import * as Lifecycle from "./WorkspaceLifecycle.ts";
import * as Registry from "./RegistryCli.ts";
import * as Store from "./TransitionStore.ts";

const input: Lifecycle.NativeTransition = {
  transitionId: "original-effect",
  threadId: "thread",
  boundary: "start",
  requestDigest: "original-request-digest",
};
const original: Lifecycle.NativeSource = {
  binding: {
    environment_id: "environment",
    thread_id: "thread",
    incarnation_id: "birth:inode:1",
    repository: "Jones-Systems/Synthetic",
    worktree: "/repo-linked",
    git_common_dir: "/repo/.git",
    git_dir: "/repo/.git/worktrees/linked",
    branch: "refs/heads/work",
    head: "a".repeat(40),
    source_id: "native-source",
  },
  ownership: "native-owner:1",
};
const absent = (): InventoryEntry => ({
  worktree: "/repo-linked",
  generation: null,
  binding: null,
  assessment: null,
  disposition: "unknown",
  assessment_status: "absent",
  stored_readiness: null,
  physical_readiness: "unknown",
  binding_evidence: "caller_asserted",
  authority_effect: "none",
});
const fixture = () => {
  const state = {
    source: original,
    view: absent(),
    calls: 0,
    loseResponse: false,
    unavailable: false,
    changeDuringResume: false,
    staleCleanupRead: false,
    cleanupReads: 0,
  };
  const source = Lifecycle.NativeLifecycleSource.of({
    capture: () => Effect.succeed(state.source),
    validateOwnership: (_input, captured) =>
      canonical(captured) === canonical(state.source)
        ? Effect.void
        : Effect.fail(new LifecycleError({ reason: "stale" })),
    revalidate: (_input, captured) =>
      canonical(captured) === canonical(state.source)
        ? Effect.void
        : Effect.fail(new LifecycleError({ reason: "stale" })),
  });
  const registry = Registry.LifecycleRegistry.of({
    show: () =>
      Effect.gen(function* () {
        if (state.unavailable) return yield* new LifecycleError({ reason: "unavailable" });
        if (state.staleCleanupRead && ++state.cleanupReads === 2)
          state.view = {
            ...state.view,
            generation: "racing-generation",
            assessment_status: "stale",
          };
        return state.view;
      }),
    resume: (binding, expected) =>
      Effect.gen(function* () {
        state.calls++;
        if (state.view.generation !== expected)
          return yield* new LifecycleError({ reason: "conflict" });
        state.view = {
          ...state.view,
          binding,
          generation: `cas-${state.calls}`,
          assessment_status: "stale",
        };
        if (state.changeDuringResume)
          state.source = { ...original, binding: { ...original.binding, head: "b".repeat(40) } };
        if (state.loseResponse) return yield* new LifecycleError({ reason: "unknown" });
        return state.view.generation!;
      }),
  });
  const layer = Lifecycle.layer.pipe(
    Layer.provide(Store.layer),
    Layer.provide(Layer.succeed(Lifecycle.NativeLifecycleSource, source)),
    Layer.provide(Layer.succeed(Registry.LifecycleRegistry, registry)),
  );
  return { state, layer };
};
const program = <A, E>(effect: Effect.Effect<A, E, SqlClient.SqlClient>) =>
  effect.pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" })));
const candidate = (binding: Binding, generation: string): InventoryEntry => ({
  ...absent(),
  binding,
  generation,
  disposition: "cleanup_candidate",
  assessment_status: "current",
  assessment: {
    schema: "workspace-assessment-record/v1",
    generation,
    recorded_at: "2026-10-24T00:00:00Z",
    binding_evidence: "caller_asserted",
    authority_effect: "none",
    assessment: {
      schema: "workspace-assessment/v1",
      binding,
      role: "authoring",
      disposition: "cleanup_candidate",
      remaining_work: [],
      preservation_refs: ["preserved-commit"],
      handoff_refs: [],
      artifact_destinations: [],
      unresolved_obligations: [],
    },
  },
});

it.effect(
  "reconciles a lost CAS response after service restart without issuing a replacement transition",
  () =>
    program(
      Effect.gen(function* () {
        yield* Store.migration;
        const { state, layer } = fixture();
        state.loseResponse = true;
        const first = yield* Effect.flatMap(Lifecycle.WorkspaceLifecycle, (s) =>
          s.resume(input),
        ).pipe(Effect.provide(layer), Effect.exit);
        expect(Exit.isFailure(first)).toBe(true);
        expect(state.calls).toBe(1);
        const resumed = yield* Effect.flatMap(Lifecycle.WorkspaceLifecycle, (s) =>
          s.resume(input),
        ).pipe(Effect.provide(layer));
        expect(resumed.state).toBe("confirmed");
        expect(resumed.id).toBe(input.transitionId);
        expect(resumed.generation).toBe("cas-1");
        expect(resumed.binding.source_generation).not.toBe(resumed.generation);
        expect(state.calls).toBe(1);
        const sql = yield* SqlClient.SqlClient;
        expect(
          (yield* sql`SELECT transition_id FROM jones_workspace_lifecycle_transitions`).length,
        ).toBe(1);
      }),
    ),
);

it.effect(
  "retains an uncertain original write and fences a different transition on the same checkout",
  () =>
    program(
      Effect.gen(function* () {
        yield* Store.migration;
        const { state, layer } = fixture();
        state.loseResponse = true;
        yield* Effect.flatMap(Lifecycle.WorkspaceLifecycle, (s) => s.resume(input)).pipe(
          Effect.provide(layer),
          Effect.exit,
        );
        state.view = absent();
        const held = yield* Effect.flatMap(Lifecycle.WorkspaceLifecycle, (s) =>
          s.resume(input),
        ).pipe(Effect.provide(layer), Effect.exit);
        expect(Exit.isFailure(held)).toBe(true);
        const replacement = yield* Effect.flatMap(Lifecycle.WorkspaceLifecycle, (s) =>
          s.resume({ ...input, transitionId: "replacement" }),
        ).pipe(Effect.provide(layer), Effect.exit);
        expect(Exit.isFailure(replacement)).toBe(true);
        expect(state.calls).toBe(1);
      }),
    ),
);

it.effect(
  "revalidates HEAD and incarnation after invalidation and never rebinds the original transition",
  () =>
    program(
      Effect.gen(function* () {
        yield* Store.migration;
        const { state, layer } = fixture();
        state.changeDuringResume = true;
        expect(
          Exit.isFailure(
            yield* Effect.flatMap(Lifecycle.WorkspaceLifecycle, (s) => s.resume(input)).pipe(
              Effect.provide(layer),
              Effect.exit,
            ),
          ),
        ).toBe(true);
        state.source = {
          ...original,
          binding: { ...original.binding, incarnation_id: "recreated-inode" },
        };
        expect(
          Exit.isFailure(
            yield* Effect.flatMap(Lifecycle.WorkspaceLifecycle, (s) => s.resume(input)).pipe(
              Effect.provide(layer),
              Effect.exit,
            ),
          ),
        ).toBe(true);
        expect(state.calls).toBe(1);
      }),
    ),
);

it.effect("unavailable inventory and caller transactions perform no registry write", () =>
  program(
    Effect.gen(function* () {
      yield* Store.migration;
      const { state, layer } = fixture();
      state.unavailable = true;
      const sql = yield* SqlClient.SqlClient;
      const resume = Effect.flatMap(Lifecycle.WorkspaceLifecycle, (s) => s.resume(input)).pipe(
        Effect.provide(layer),
      );
      expect(Exit.isFailure(yield* Effect.exit(resume))).toBe(true);
      state.unavailable = false;
      expect(Exit.isFailure(yield* Effect.exit(sql.withTransaction(resume)))).toBe(true);
      expect(state.calls).toBe(0);
      expect(yield* sql`SELECT * FROM jones_workspace_lifecycle_transitions`).toEqual([]);
    }),
  ),
);

it.effect(
  "a current native-bound cleanup candidate only clears the semantic veto; preserve, unfinished and racing assessments hold",
  () =>
    program(
      Effect.gen(function* () {
        yield* Store.migration;
        const { state, layer } = fixture();
        const run = <A, E>(effect: Effect.Effect<A, E, Lifecycle.WorkspaceLifecycle>) =>
          effect.pipe(Effect.provide(layer));
        const transition = yield* run(
          Effect.flatMap(Lifecycle.WorkspaceLifecycle, (s) => s.resume(input)),
        );
        const cleanup = Effect.flatMap(Lifecycle.WorkspaceLifecycle, (s) =>
          s.cleanupVeto({ ...input, boundary: "cleanup" }),
        );
        state.view = candidate(transition.binding, "assessment-generation");
        yield* run(cleanup);
        for (const disposition of ["preserve", "unknown", "active", "needs_handoff"] as const) {
          state.view = { ...candidate(transition.binding, "assessment-generation"), disposition };
          expect(Exit.isFailure(yield* run(Effect.exit(cleanup)))).toBe(true);
        }
        state.view = candidate(transition.binding, "assessment-generation");
        state.view = {
          ...state.view,
          assessment: {
            ...state.view.assessment!,
            assessment: { ...state.view.assessment!.assessment, remaining_work: ["unfinished"] },
          },
        };
        expect(Exit.isFailure(yield* run(Effect.exit(cleanup)))).toBe(true);
        state.view = candidate(transition.binding, "assessment-generation");
        state.staleCleanupRead = true;
        expect(Exit.isFailure(yield* run(Effect.exit(cleanup)))).toBe(true);
      }),
    ),
);
