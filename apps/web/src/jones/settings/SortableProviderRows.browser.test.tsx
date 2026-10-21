import { userEvent } from "vite-plus/test/browser";
import { act, render as renderTest, type Screen } from "../../../test/render";
import {
  EnvironmentId,
  ProviderDriverKind,
  ProviderInstanceId,
  defaultInstanceIdForDriver,
  type ProviderInstanceConfig,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { AsyncResult } from "effect/reactivity";

import { expect, it, vi } from "vite-plus/test";
import "~/index.css";
import { SortableProviderRows } from "./SortableProviderRows";

const state = vi.hoisted(() => ({
  instances: {} as Readonly<Record<string, ProviderInstanceConfig>>,
  canManage: true,
  persist: vi.fn(),
  toast: vi.fn(),
}));
vi.mock("~/hooks/useSettings", () => ({
  useEnvironmentSettings: () => ({ providerInstances: state.instances }),
}));
vi.mock("~/state/session", () => ({ useEnvironmentScope: () => state.canManage }));
vi.mock("~/state/server", () => ({ serverEnvironment: { updateSettings: {} } }));
vi.mock("~/state/use-atom-command", () => ({ useAtomCommand: () => state.persist }));
vi.mock("~/components/ui/toast", () => ({ toastManager: { add: state.toast } }));

const driver = ProviderDriverKind.make("codex");
const defaultId = defaultInstanceIdForDriver(driver);
let root: Screen;
let container: HTMLDivElement;
let selected: string;
async function setupTest(): Promise<AsyncDisposable> {
  let initialized = false;
  let ownedContainer: HTMLDivElement | undefined;
  async function cleanup() {
    const screen = initialized ? root : undefined;
    try {
  await act(async () => screen?.unmount());
    } finally {
      try {
        await screen?.[Symbol.asyncDispose]();
      } finally {
        ownedContainer?.remove();
        vi.unstubAllGlobals();
      }
    }
  }
  try {
  state.instances = {
    [defaultId]: { driver, displayName: "Default" },
    malcolm: { driver, displayName: "Malcolm" },
    jace: { driver, displayName: "Jace" },
    wallace: { driver, displayName: "Wallace" },
    claude: { driver: ProviderDriverKind.make("claudeAgent"), displayName: "Claude" },
  };
  state.canManage = true;
  state.persist.mockReset().mockResolvedValue({ _tag: "Success", value: undefined });
  state.toast.mockReset();
  selected = "";
  ownedContainer = container = document.createElement("div");
  container.style.width = "272px";
  document.body.append(container);
  root = await renderTest(null, { container: container, router: false });
  initialized = true;

    return { [Symbol.asyncDispose]: cleanup };
  } catch (error) {
    await cleanup();
    throw error;
  }
}

async function render(readOnly = false, environment = "test") {
  const rows = Object.entries(state.instances).map(([id, instance]) => ({
    instanceId: ProviderInstanceId.make(id),
    driver: instance.driver,
    explicitInstance: instance,
    isDefault: id === defaultId,
  }));
  await act(async () =>
    await root.rerender(
      <SortableProviderRows
        environmentId={EnvironmentId.make(environment)}
        rows={rows}
        readOnly={readOnly}
        renderRow={(row) => (
          <div style={{ height: 72, paddingLeft: 16 }}>
            <button
              onClick={() => {
                selected = row.instanceId;
              }}
              data-provider={row.instanceId}
            >
              {row.explicitInstance.displayName}
            </button>
            <input type="checkbox" aria-label={`Enable ${row.instanceId}`} />
          </div>
        )}
      />,
    ),
  );
}
const order = () =>
  [...container.querySelectorAll("[data-provider]")].map((node) =>
    node.getAttribute("data-provider"),
  );
const grip = () => container.querySelector<HTMLButtonElement>('[aria-label="Reorder Malcolm"]')!;
async function moveMalcolm() {
  grip().focus();
  await userEvent.keyboard(" ");
  await userEvent.keyboard("{ArrowDown}");
  await vi.waitFor(() =>
    expect(document.querySelector('[role="status"]')?.textContent).toContain(
      "was moved over droppable area jace.",
    ),
  );
  await userEvent.keyboard(" ");
}

it("reorders with the keyboard while keeping default and other types pinned", async () => {
    await using _fixture = await setupTest();
  await render();
  expect(container.querySelector('[aria-label="Reorder Default"]')).toBeNull();
  expect(container.querySelector('[aria-label="Reorder Claude"]')).toBeNull();
  await moveMalcolm();
  expect(state.persist).toHaveBeenCalledTimes(1);
  expect(Object.keys(state.persist.mock.calls[0]![0].input.patch.providerInstances)).toEqual([
    defaultId,
    "jace",
    "malcolm",
    "wallace",
    "claude",
  ]);
  expect(order()).toEqual([defaultId, "jace", "malcolm", "wallace", "claude"]);
  state.instances = { ...state.instances };
  await render();
  expect(order()).toEqual([defaultId, "malcolm", "jace", "wallace", "claude"]);
});

it("keeps selection and enablement independent and cancels a keyboard drag", async () => {
    await using _fixture = await setupTest();
  await render();
  await act(async () =>
    container.querySelector<HTMLButtonElement>('[data-provider="malcolm"]')!.click(),
  );
  expect(selected).toBe("malcolm");
  const checkbox = container.querySelector<HTMLInputElement>('[aria-label="Enable malcolm"]')!;
  await act(async () => checkbox.click());
  expect(checkbox.checked).toBe(true);
  grip().focus();
  await userEvent.keyboard(" ");
  await userEvent.keyboard("{ArrowDown}");
  await vi.waitFor(() =>
    expect(document.querySelector('[role="status"]')?.textContent).toContain(
      "was moved over droppable area jace.",
    ),
  );
  await userEvent.keyboard("{Escape}");
  expect(state.persist).not.toHaveBeenCalled();
  expect(order()[1]).toBe("malcolm");
});

it("removes drag activators for read-only or unauthorized sessions", async () => {
    await using _fixture = await setupTest();
  await render(true);
  expect(container.querySelector('[aria-label^="Reorder"]')).toBeNull();
  state.canManage = false;
  await render();
  expect(container.querySelector('[aria-label^="Reorder"]')).toBeNull();
  expect(state.persist).not.toHaveBeenCalled();
});

it("rolls back a failed save and permits another attempt", async () => {
    await using _fixture = await setupTest();
  state.persist.mockResolvedValueOnce(
    AsyncResult.failure(Cause.fail(new Error("Synthetic failure"))),
  );
  await render();
  await moveMalcolm();
  expect(order()[1]).toBe("malcolm");
  expect(state.toast).toHaveBeenCalledWith(
    expect.objectContaining({ title: "Could not reorder providers" }),
  );
  await moveMalcolm();
  expect(state.persist).toHaveBeenCalledTimes(2);
  expect(order()[1]).toBe("jace");
});

it("does not carry a pending save into a different environment", async () => {
    await using _fixture = await setupTest();
  let reject!: (error: Error) => void;
  state.persist.mockImplementationOnce(
    () =>
      new Promise((_resolve, onReject) => {
        reject = onReject;
      }),
  );
  await render();
  await moveMalcolm();
  expect(container.firstElementChild?.getAttribute("aria-busy")).toBe("true");
  await render(false, "other");
  expect(container.firstElementChild?.getAttribute("aria-busy")).toBe("false");
  expect(order()[1]).toBe("malcolm");
  await act(async () => reject(new Error("Old environment failure")));
  expect(state.toast).not.toHaveBeenCalled();
  expect(grip()).not.toBeNull();
});

it("clears an interrupted save without a toast and permits retry", async () => {
    await using _fixture = await setupTest();
  state.persist.mockResolvedValueOnce(AsyncResult.failure(Cause.interrupt()));
  await render();
  await moveMalcolm();
  expect(order()[1]).toBe("malcolm");
  expect(state.toast).not.toHaveBeenCalled();
  expect(container.firstElementChild?.getAttribute("aria-busy")).toBe("false");
  await moveMalcolm();
  expect(state.persist).toHaveBeenCalledTimes(2);
});
