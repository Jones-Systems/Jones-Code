import {
  EnvironmentId,
  ProviderInstanceId,
  type RemoteAdmissionSnapshot,
} from "@t3tools/contracts";
import { act, render } from "../../../test/render";
import { expect, it, vi } from "vite-plus/test";
import { RemoteAdmissionSettings } from "./RemoteAdmissionSettings";

const request = vi.hoisted(() => vi.fn());
vi.mock("./runtime", () => ({ remoteAdmissionRequest: request }));
vi.mock("../../state/environments", () => ({ useEnvironmentProviders: () => new Map() }));
vi.mock("../../state/server", () => ({ EMPTY_SERVER_PROVIDERS: [] }));
vi.mock("../../hooks/useSettings", () => ({ useEnvironmentSettings: () => ({}) }));
vi.mock("../../components/chat/ProviderModelPicker", () => ({
  ProviderModelPicker: ({
    model,
    disabled,
    onInstanceModelChange,
  }: {
    model: string;
    disabled: boolean;
    onInstanceModelChange: (instanceId: ProviderInstanceId, model: string) => void;
  }) => (
    <button
      disabled={disabled}
      aria-label="Evaluator model"
      onClick={() =>
        onInstanceModelChange(ProviderInstanceId.make("synthetic-claude"), "draft-model")
      }
    >
      {model}
    </button>
  ),
}));

const environmentId = EnvironmentId.make("synthetic-environment");
const snapshot: RemoteAdmissionSnapshot = {
  eligible: true,
  policy: {
    enabled: true,
    revision: "synthetic-policy",
    modelSelection: {
      instanceId: ProviderInstanceId.make("synthetic-claude"),
      model: "saved-model",
    },
  },
  pending: [],
};
function setupPolling() {
  let tick!: () => void;
  const visibility = vi.spyOn(document, "visibilityState", "get").mockReturnValue("visible");
  const interval = vi.spyOn(window, "setInterval").mockImplementation((callback, delay) => {
    expect(delay).toBe(10_000);
    tick = callback as () => void;
    return 123 as unknown as ReturnType<typeof window.setInterval>;
  });
  const clearInterval = vi.spyOn(window, "clearInterval").mockImplementation(() => undefined);
  request.mockReset().mockResolvedValue(snapshot);
  return {
    visibility,
    poll: () => act(async () => tick()),
    [Symbol.dispose]() {
      try {
        expect(clearInterval).toHaveBeenCalledWith(123);
      } finally {
        clearInterval.mockRestore();
        interval.mockRestore();
        visibility.mockRestore();
      }
    },
  };
}

function controls(container: HTMLElement) {
  return {
    alert: () => container.querySelector('[role="alert"]'),
    model: () => container.querySelector<HTMLButtonElement>('[aria-label="Evaluator model"]')!,
    toggle: () => container.querySelector<HTMLElement>('[aria-label="Evaluate remote requests"]')!,
  };
}

it("keeps the review heading and a safe load error visible, then recovers on the next poll", async () => {
  using polling = setupPolling();
  request.mockRejectedValueOnce(new Error("private backend detail: synthetic-secret"));
  await using screen = await render(<RemoteAdmissionSettings environmentId={environmentId} />, {
    router: false,
  });
  const { container } = screen;
  const { alert, model, toggle } = controls(container);
  expect(container.querySelector("h3")?.textContent).toBe("Review remote requests on this laptop");
  expect(alert()?.textContent).toBe(
    "Remote request review could not be loaded. Retrying automatically.",
  );
  expect(container.textContent).not.toContain("synthetic-secret");
  expect(container.querySelector('[aria-label="Evaluate remote requests"]')).toBeNull();
  expect(container.querySelector("article")).toBeNull();
  await polling.poll();
  expect(alert()).toBeNull();
  expect(toggle().getAttribute("aria-checked")).toBe("true");
  expect(model().textContent).toBe("saved-model");
  expect(request.mock.calls.every(([, input]) => input.action === "snapshot")).toBe(true);
});

it("preserves policy and an unsaved model through a failed refresh and recovery", async () => {
  using polling = setupPolling();
  await using screen = await render(<RemoteAdmissionSettings environmentId={environmentId} />, {
    router: false,
  });
  const { container } = screen;
  const { alert, model, toggle } = controls(container);
  await act(async () => model().click());
  expect(model().textContent).toBe("draft-model");
  request.mockRejectedValueOnce(new Error("private backend failure"));
  await polling.poll();
  expect(alert()?.textContent).toContain("could not be loaded");
  expect(toggle().getAttribute("aria-checked")).toBe("true");
  expect(toggle().getAttribute("aria-disabled")).toBe("true");
  expect(model().textContent).toBe("draft-model");
  expect(model().disabled).toBe(true);
  await polling.poll();
  expect(alert()).toBeNull();
  expect(toggle().getAttribute("aria-disabled")).not.toBe("true");
  expect(model().textContent).toBe("draft-model");
  const save = [...container.querySelectorAll("button")].find(
    (button) => button.textContent === "Save model",
  )!;
  await act(async () => save.click());
  expect(request).toHaveBeenCalledWith(environmentId, {
    action: "policy",
    input: {
      enabled: true,
      expectedRevision: "synthetic-policy",
      modelSelection: {
        instanceId: "synthetic-claude",
        model: "draft-model",
      },
    },
  });
});

it("keeps hidden and overlapping polls suppressed while a failed load can recover", async () => {
  using polling = setupPolling();
  let resolve!: (value: RemoteAdmissionSnapshot) => void;
  request.mockImplementationOnce(
    () =>
      new Promise<RemoteAdmissionSnapshot>((done) => {
        resolve = done;
      }),
  );
  await using screen = await render(<RemoteAdmissionSettings environmentId={environmentId} />, {
    router: false,
  });
  const { container } = screen;
  const { alert } = controls(container);
  await polling.poll();
  expect(request).toHaveBeenCalledTimes(1);
  await act(async () => resolve(snapshot));
  polling.visibility.mockReturnValue("hidden");
  await polling.poll();
  expect(request).toHaveBeenCalledTimes(1);
  polling.visibility.mockReturnValue("visible");
  request.mockRejectedValueOnce(new Error("offline"));
  await polling.poll();
  expect(alert()).not.toBeNull();
  await polling.poll();
  expect(alert()).toBeNull();
  expect(request).toHaveBeenCalledTimes(3);
});
