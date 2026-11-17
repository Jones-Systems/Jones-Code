import "~/index.css";
import { RegistryContext } from "@effect/atom-react";
import {
  personalThreadKey,
  type PersonalWorkstreamsController,
  type PersonalWorkstreamsState,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { Atom, AtomRegistry } from "effect/reactivity";
import { expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";
import { act, render } from "../../../test/render";
import { WorkstreamPicker } from "./WorkstreamPicker";

it("describes automatic assignment only while the thread has that provenance", async () => {
  const thread = {
    environmentId: EnvironmentId.make("picker-test"),
    id: ThreadId.make("picker-thread"),
  };
  const key = personalThreadKey(thread);
  const timestamp = "2026-11-17T00:00:00.000Z";
  const initial: PersonalWorkstreamsState = {
    workstreams: [
      {
        id: "ui-work",
        name: "UI work",
        lifecycle: "active",
        color: null,
        orderKey: "n",
        createdAt: timestamp,
        updatedAt: timestamp,
      },
    ],
    environments: [],
    assignmentsByThreadKey: new Map([
      [
        key,
        {
          threadId: thread.id,
          workstreamId: "ui-work",
          revision: 1,
          updatedAt: timestamp,
        },
      ],
    ]),
    pendingThreadKeys: new Set(),
    canCreate: true,
  };
  const registry = AtomRegistry.make();
  const state = Atom.make(initial);
  const controller: PersonalWorkstreamsController = {
    state,
    canAssign: () => true,
    create: vi.fn(),
    rename: vi.fn(),
    setColor: vi.fn(),
    setLifecycle: vi.fn(),
    reorder: vi.fn(),
    remove: vi.fn(),
    assign: vi.fn(),
    createAndAssign: vi.fn(),
  };
  try {
    await using _screen = await render(
      <RegistryContext.Provider value={registry}>
        <WorkstreamPicker controller={controller} thread={thread} onClose={vi.fn()} />
      </RegistryContext.Provider>,
    );
    const dialog = page.getByRole("dialog", { name: "Move to Workstream" });
    const provenance = dialog.getByText(
      "Assigned automatically to UI work from a queued prompt. Choose another Workstream or remove it.",
    );
    await expect.element(provenance).not.toBeInTheDocument();
    await act(() => registry.set(state, { ...initial, autoAssignedThreadKeys: new Set([key]) }));
    await expect.element(provenance).toBeVisible();
    await act(() => registry.set(state, { ...initial, autoAssignedThreadKeys: new Set<string>() }));
    await expect.element(provenance).not.toBeInTheDocument();
  } finally {
    registry.dispose();
  }
});
