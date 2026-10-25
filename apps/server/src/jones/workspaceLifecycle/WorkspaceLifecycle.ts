import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { canonical, digest } from "../ordinaryCheckout/CheckoutProtocol.ts";
import { Binding, NativeBinding, LifecycleError, type InventoryEntry } from "./Protocol.ts";
import * as Registry from "./RegistryCli.ts";
import * as Store from "./TransitionStore.ts";

export interface NativeTransition {
  readonly transitionId: string;
  readonly threadId: string;
  readonly requestDigest: string;
  readonly boundary:
    | "start"
    | "resume"
    | "recreate"
    | "unsettle"
    | "restore"
    | "cleanup"
    | "settle-script";
}
export interface NativeSource {
  readonly binding: Omit<Binding, "source_generation">;
  readonly ownership: string;
}

// Only a native owner may supply this service. Registry strings and persisted Jones metadata cannot issue it.
export class NativeLifecycleSource extends Context.Service<
  NativeLifecycleSource,
  {
    readonly capture: (transition: NativeTransition) => Effect.Effect<NativeSource, LifecycleError>;
    readonly revalidate: (
      transition: NativeTransition,
      source: NativeSource,
    ) => Effect.Effect<void, LifecycleError>;
    readonly validateOwnership: (
      transition: NativeTransition,
      source: NativeSource,
    ) => Effect.Effect<void, LifecycleError>;
  }
>()("t3/jones/workspaceLifecycle/WorkspaceLifecycle/NativeLifecycleSource") {}

export class WorkspaceLifecycle extends Context.Service<
  WorkspaceLifecycle,
  {
    readonly resume: (
      transition: NativeTransition,
    ) => Effect.Effect<Store.Transition, LifecycleError>;
    readonly cleanupVeto: (input: NativeTransition) => Effect.Effect<void, LifecycleError>;
    readonly cleanupPathVeto: (worktree: string) => Effect.Effect<void, LifecycleError>;
    readonly validate: (transition: NativeTransition) => Effect.Effect<void, LifecycleError>;
  }
>()("t3/jones/workspaceLifecycle/WorkspaceLifecycle") {}

const same = (left: unknown, right: unknown) => canonical(left) === canonical(right);
const invalidated = (view: InventoryEntry) =>
  view.stored_readiness === null || view.stored_readiness.outcome === "invalidated";
const decodeBinding = Schema.decodeUnknownEffect(Binding, { onExcessProperty: "error" });
const decodeSource = Schema.decodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      input: Schema.Struct({
        transitionId: Schema.String,
        threadId: Schema.String,
        requestDigest: Schema.String,
        boundary: Schema.String,
      }),
      captured: Schema.Struct({ binding: NativeBinding, ownership: Schema.String }),
    }),
  ),
);
const make = Effect.gen(function* () {
  const source = yield* NativeLifecycleSource;
  const registry = yield* Registry.LifecycleRegistry;
  const store = yield* Store.TransitionStore;
  const resume = Effect.fn("WorkspaceLifecycle.resume")(function* (input: NativeTransition) {
    yield* store.outsideTransaction;
    const captured = yield* source.capture(input);
    const sourceJson = canonical({ input, captured });
    const binding = yield* decodeBinding({
      ...captured.binding,
      source_generation: `jones-transition:${digest({ input, captured })}`,
    }).pipe(Effect.mapError((cause) => new LifecycleError({ reason: "identity", cause })));
    let transition = yield* store.read(input.transitionId);
    if (
      transition !== null &&
      (transition.source !== sourceJson || !same(transition.binding, binding))
    )
      return yield* new LifecycleError({ reason: "stale" });
    if (transition === null) {
      const before = yield* registry.show(binding.worktree);
      if (
        before.binding !== null &&
        (before.binding.environment_id !== binding.environment_id ||
          before.binding.repository !== binding.repository)
      )
        return yield* new LifecycleError({ reason: "identity" });
      yield* source.revalidate(input, captured);
      transition = yield* store.prepare({
        id: input.transitionId,
        source: sourceJson,
        binding,
        expectedGeneration: before.generation,
        generation: null,
        state: "prepared",
      });
    }
    if (transition.state === "prepared") {
      yield* source.revalidate(input, captured);
      yield* store.begin(transition);
      // The durable invoking fence precedes the external write. Even interruption leaves the original CAS identity intact.
      const generation = yield* registry.resume(binding, transition.expectedGeneration);
      transition = yield* store.confirm({ ...transition, state: "invoking" }, generation);
    }
    const current = yield* registry.show(binding.worktree);
    if (
      !same(current.binding, binding) ||
      !invalidated(current) ||
      current.generation === null ||
      current.generation === transition.expectedGeneration ||
      (transition.generation !== null && current.generation !== transition.generation) ||
      current.assessment_status === "current"
    )
      return yield* new LifecycleError({ reason: "unknown" });
    if (transition.state === "invoking")
      transition = yield* store.confirm(transition, current.generation);
    yield* source.revalidate(input, captured);
    return transition;
  });
  const cleanupVeto = Effect.fn("WorkspaceLifecycle.cleanupVeto")(function* (
    input: NativeTransition,
  ) {
    yield* store.outsideTransaction;
    const captured = yield* source.capture(input);
    const local = yield* store.current(captured.binding.worktree);
    const current = yield* registry.show(captured.binding.worktree);
    const binding = current.binding;
    const assessment = current.assessment;
    if (
      local === null ||
      binding === null ||
      !same(local.binding, binding) ||
      assessment === null ||
      current.generation === null ||
      current.assessment_status !== "current" ||
      current.disposition !== "cleanup_candidate" ||
      assessment.assessment.disposition !== "cleanup_candidate" ||
      assessment.generation !== current.generation ||
      !same(assessment.assessment.binding, binding) ||
      !same(
        { ...binding, source_generation: undefined },
        { ...captured.binding, source_generation: undefined },
      ) ||
      assessment.assessment.remaining_work.length > 0 ||
      assessment.assessment.unresolved_obligations.length > 0
    )
      return yield* new LifecycleError({ reason: "preserve" });
    yield* source.revalidate(input, captured);
    const again = yield* registry.show(binding.worktree);
    if (!same(current, again)) return yield* new LifecycleError({ reason: "stale" });
    yield* source.revalidate(input, captured);
    // Passing this veto grants nothing: physical readiness, preservation and native deletion policy still decide removal.
  });
  return WorkspaceLifecycle.of({
    resume,
    cleanupVeto,
    cleanupPathVeto: Effect.fn("WorkspaceLifecycle.cleanupPathVeto")(function* (worktree) {
      const local = yield* store.current(worktree);
      if (!local) return yield* new LifecycleError({ reason: "preserve" });
      yield* cleanupVeto({
        transitionId: `cleanup:${worktree}`,
        threadId: local.binding.thread_id,
        requestDigest: digest(worktree),
        boundary: "cleanup",
      });
    }),
    validate: Effect.fn("WorkspaceLifecycle.validate")(function* (input) {
      const original = yield* store.read(input.transitionId);
      if (original === null || original.state !== "confirmed")
        return yield* new LifecycleError({ reason: "unknown" });
      const current = yield* store.current(original.binding.worktree);
      if (current?.id !== original.id || current.generation !== original.generation)
        return yield* new LifecycleError({ reason: "stale" });
      const captured = yield* decodeSource(original.source).pipe(
        Effect.mapError((cause) => new LifecycleError({ reason: "storage", cause })),
      );
      if (!same(captured.input, input)) return yield* new LifecycleError({ reason: "identity" });
      yield* source.validateOwnership(input, captured.captured);
    }),
  });
});
export const layer = Layer.effect(WorkspaceLifecycle, make);
