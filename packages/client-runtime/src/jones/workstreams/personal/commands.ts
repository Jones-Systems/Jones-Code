import type { EnvironmentRegistry } from "../../../connection/registry.ts";
import { createEnvironmentCommand } from "../../../state/runtime.ts";
import * as Effect from "effect/Effect";
import type { Atom } from "effect/reactivity";
import type { PersonalWorkstreamsController } from "./types.ts";

/** Shared command binding; create once beside the client-owned controller. */
export function createPersonalWorkstreamsCommands<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | R, E>,
  controller: PersonalWorkstreamsController,
) {
  const command = <I, A>(label: string, execute: (input: I) => Promise<A>) =>
    createEnvironmentCommand(runtime, {
      label: `jones:personal-workstreams:${label}`,
      execute: (input: I) => Effect.promise(() => execute(input)),
    });
  return {
    create: command("create", (input: Parameters<typeof controller.create>[0]) =>
      controller.create(input),
    ),
    rename: command("rename", (input: { id: string; name: string }) =>
      controller.rename(input.id, input.name),
    ),
    setColor: command("color", (input: { id: string; color: string | null }) =>
      controller.setColor(input.id, input.color),
    ),
    setLifecycle: command(
      "lifecycle",
      (input: { id: string; lifecycle: Parameters<typeof controller.setLifecycle>[1] }) =>
        controller.setLifecycle(input.id, input.lifecycle),
    ),
    reorder: command(
      "reorder",
      (input: { id: string; neighbors: Parameters<typeof controller.reorder>[1] }) =>
        controller.reorder(input.id, input.neighbors),
    ),
    remove: command("delete", (input: { id: string }) => controller.remove(input.id)),
    assign: command(
      "assign",
      (input: { thread: Parameters<typeof controller.assign>[0]; workstreamId: string | null }) =>
        controller.assign(input.thread, input.workstreamId),
    ),
    createAndAssign: command(
      "create-and-assign",
      (input: { thread: Parameters<typeof controller.assign>[0]; name: string }) =>
        controller.createAndAssign(input.thread, input.name),
    ),
  };
}
