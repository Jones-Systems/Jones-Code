import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import type { Atom } from "effect/reactivity";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { createPendingThreadOrder } from "../../features/threads/threadOrder";
import { makeThreadShellFixture } from "../../test-fixtures";
import { appAtomRegistry } from "../../state/atom-registry";
import {
  beginPendingThreadOrder,
  getPendingThreadOrder,
  pendingThreadOrderAtom,
} from "../../state/thread-order";
import { environmentThreadShells } from "../../state/threads";

vi.mock("../../state/atom-registry", async () => {
  const { AtomRegistry } = await import("effect/reactivity");
  return { appAtomRegistry: AtomRegistry.make() };
});
vi.mock("../../state/threads", async () => {
  const { Atom } = await import("effect/reactivity");
  return {
    environmentThreadShells: { navigationThreadShellsAtom: Atom.make([]).pipe(Atom.keepAlive) },
  };
});
vi.mock("../../state/server", async () => {
  const { Atom } = await import("effect/reactivity");
  return { environmentServerConfigsAtom: Atom.make(new Map()).pipe(Atom.keepAlive) };
});
vi.mock("../../state/use-thread-outbox", async () => {
  const { Atom } = await import("effect/reactivity");
  return { queuedThreadKeysAtom: Atom.make(new Set<string>()).pipe(Atom.keepAlive) };
});

const shellsAtom = environmentThreadShells.navigationThreadShellsAtom as Atom.Writable<
  readonly EnvironmentThreadShell[],
  readonly EnvironmentThreadShell[]
>;
afterEach(() => appAtomRegistry.reset());

describe("delegated request changes during a mobile move", () => {
  it.each(["revealed", "re-hidden"] as const)(
    "invalidates the pending move when a delegate is %s",
    (change) => {
      const environmentId = EnvironmentId.make("destination");
      const rows = ["a", "b"].map((id) =>
        makeThreadShellFixture({ id: ThreadId.make(id), environmentId }),
      );
      const delegate = makeThreadShellFixture({
        id: ThreadId.make("delegate"),
        environmentId,
        hasPendingUserInput: true,
        delegatedFrom: {
          environmentId: EnvironmentId.make("origin"),
          threadId: ThreadId.make("parent"),
          title: "Parent",
        },
      });
      const before = change === "revealed" ? rows : [...rows, delegate];
      const after = change === "revealed" ? [...rows, delegate] : rows;
      appAtomRegistry.set(shellsAtom, before);
      const move = beginPendingThreadOrder(
        createPendingThreadOrder({
          section: "active",
          ordered: before,
          movedId: `${environmentId}:b`,
          direction: "up",
          assignments: [
            { id: `${environmentId}:b`, orderKey: "aa" },
            { id: `${environmentId}:a`, orderKey: "bb" },
          ],
        }),
      );
      expect(move.isPending()).toBe(true);
      appAtomRegistry.set(shellsAtom, after);
      expect(move.isPending()).toBe(false);
      expect(appAtomRegistry.get(pendingThreadOrderAtom)).toBeNull();
      move.complete();
      appAtomRegistry.set(shellsAtom, before);
      expect(getPendingThreadOrder()).toBeNull();
    },
  );
});
