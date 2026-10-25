import { act, render } from "../../../test/render";
import { expect, it, vi } from "vite-plus/test";
import {
  EnvironmentId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ModelSelection,
  type ServerProvider,
} from "@t3tools/contracts";
import { DEFAULT_UNIFIED_SETTINGS } from "@t3tools/contracts/settings";
import { admissionSnapshot } from "./testFixtures";
import { RemoteAdmissionSettings } from "./RemoteAdmissionSettings";

const request = vi.hoisted(() => vi.fn());
vi.mock("./runtime", () => ({ remoteAdmissionRequest: request }));
vi.mock("../../state/environments", () => ({
  useEnvironmentProviders: () => new Map([[environmentId, providers]]),
  useEnvironment: () => ({ label: "Synthetic receiver" }),
  useEnvironmentIds: () => [],
  useEnvironments: () => ({ environments: [], isReady: true }),
}));
vi.mock("../../state/server", () => ({ EMPTY_SERVER_PROVIDERS: [] }));
vi.mock("../../hooks/useSettings", () => ({
  useEnvironmentSettings: () => ({
    ...DEFAULT_UNIFIED_SETTINGS,
    providerInstances: {
      [instanceId]: { driver: ProviderDriverKind.make("codex"), enabled: true },
    },
  }),
}));
vi.mock("../../components/chat/ProviderModelPicker", () => ({
  ProviderModelPicker: ({
    model,
    disabled,
    triggerLabel,
    instanceEntries,
    onInstanceModelChange,
  }: {
    model: string;
    disabled: boolean;
    triggerLabel?: string;
    instanceEntries: readonly { instanceId: ProviderInstanceId }[];
    onInstanceModelChange: (instanceId: ProviderInstanceId, model: string) => void;
  }) => (
    <button
      disabled={disabled}
      aria-label="Evaluator model"
      data-instances={instanceEntries.map((entry) => entry.instanceId).join(",")}
      onClick={() => onInstanceModelChange(instanceId, model)}
    >
      {triggerLabel ?? model}
    </button>
  ),
}));
vi.mock("../../components/chat/TraitsPicker", () => ({
  TraitsMenuContent: () => null,
  getReasoningTraitStep: () => undefined,
  shouldRenderTraitsControls: () => false,
  readPromptUltrathink: () => "none",
  buildTraitsOptionSelections: () => undefined,
  useTraitsSelection: () => null,
  TraitsPicker: ({
    onModelOptionsChange,
  }: {
    onModelOptionsChange: (options: ModelSelection["options"]) => void;
  }) => (
    <button
      aria-label="Evaluator effort"
      onClick={() =>
        onModelOptionsChange([
          { id: "reasoningEffort", value: "medium" },
          { id: "serviceTier", value: "default" },
        ])
      }
    >
      Medium
    </button>
  ),
}));

const instanceId = ProviderInstanceId.make("synthetic-codex");
let environmentId: EnvironmentId;
let providers: readonly (ServerProvider & { supportsRemoteRequestEvaluation?: boolean })[];
const provider: ServerProvider & { supportsRemoteRequestEvaluation?: boolean } = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  displayName: "Synthetic Codex account",
  enabled: true,
  installed: true,
  version: null,
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: "2026-10-24T12:00:00Z",
  supportsRemoteRequestEvaluation: true,
  models: [
    { slug: "gpt-6.1-sol", name: "Sol", isCustom: false, capabilities: {}, isDefault: true },
  ],
  slashCommands: [],
  skills: [],
};
const selection: ModelSelection = {
  instanceId,
  model: "gpt-6.1-sol",
  options: [
    { id: "reasoningEffort", value: "high" },
    { id: "serviceTier", value: "default" },
  ],
};
function setup(name: string) {
  environmentId = EnvironmentId.make(name);
  const { supportsRemoteRequestEvaluation: _supports, ...olderProvider } = provider;
  providers = [
    provider,
    {
      ...provider,
      instanceId: ProviderInstanceId.make("unsupported"),
      supportsRemoteRequestEvaluation: false,
    },
    {
      ...olderProvider,
      instanceId: ProviderInstanceId.make("older-server"),
    },
  ];
  const snapshot = {
    ...admissionSnapshot(),
    policy: { enabled: true, revision: "synthetic-policy", modelSelection: selection },
  };
  request.mockReset().mockResolvedValue(snapshot);
  let tick!: () => void;
  const visibility = vi.spyOn(document, "visibilityState", "get").mockReturnValue("visible");
  const interval = vi.spyOn(window, "setInterval").mockImplementation((callback) => {
    tick = callback as () => void;
    return 123 as unknown as ReturnType<typeof window.setInterval>;
  });
  const clear = vi.spyOn(window, "clearInterval").mockImplementation(() => undefined);
  return {
    snapshot,
    visibility,
    poll: () => act(async () => tick()),
    [Symbol.dispose]() {
      clear.mockRestore();
      interval.mockRestore();
      visibility.mockRestore();
    },
  };
}
const button = (container: HTMLElement, text: string) =>
  [...container.querySelectorAll("button")].find((entry) => entry.textContent === text)!;

it("saves the complete exact Codex account selection and keeps request articles and decisions out of Settings", async () => {
  using _fixture = setup("settings-exact");
  await using screen = await render(<RemoteAdmissionSettings environmentId={environmentId} />);
  expect(screen.container.textContent).toContain("Receiving environment: Synthetic receiver");
  expect(screen.container.querySelector("article")).toBeNull();
  expect(screen.container.textContent).not.toContain("Inspect synthetic project");
  const picker = screen.container.querySelector<HTMLButtonElement>(
    '[aria-label="Evaluator model"]',
  )!;
  expect(picker.dataset.instances).toBe(instanceId);
  await act(async () => picker.click());
  await act(async () => button(screen.container, "Save model").click());
  expect(request).toHaveBeenCalledWith(environmentId, {
    action: "policy",
    input: {
      enabled: true,
      expectedRevision: "synthetic-policy",
      modelSelection: selection,
    },
  });
  await act(async () =>
    screen.container.querySelector<HTMLButtonElement>('[aria-label="Evaluator effort"]')!.click(),
  );
  await act(async () => button(screen.container, "Save model").click());
  expect(request).toHaveBeenLastCalledWith(environmentId, { action: "snapshot" });
  expect(request).toHaveBeenCalledWith(environmentId, {
    action: "policy",
    input: {
      enabled: true,
      expectedRevision: "synthetic-policy",
      modelSelection: {
        ...selection,
        options: [
          { id: "reasoningEffort", value: "medium" },
          { id: "serviceTier", value: "default" },
        ],
      },
    },
  });
});

it("shows a saved unavailable selection without replacing it or permitting Save", async () => {
  using fixture = setup("settings-unavailable");
  request.mockResolvedValue({
    ...fixture.snapshot,
    policy: {
      ...fixture.snapshot.policy,
      modelSelection: {
        ...selection,
        instanceId: ProviderInstanceId.make("missing-account"),
        model: "retired-model",
      },
    },
  });
  await using screen = await render(<RemoteAdmissionSettings environmentId={environmentId} />);
  expect(screen.container.textContent).toContain("retired-model (Unavailable)");
  expect(screen.container.textContent).toContain("missing-account / retired-model");
  const save = button(screen.container, "Save model");
  expect(save.getAttribute("aria-disabled")).toBe("true");
  await act(async () => save.click());
  expect(request.mock.calls.every(([, input]) => input.action === "snapshot")).toBe(true);
});

it("keeps a safe load error and unsaved effort, disables writes, then recovers without hidden or overlapping polls", async () => {
  using fixture = setup("settings-recovery");
  await using screen = await render(<RemoteAdmissionSettings environmentId={environmentId} />);
  await act(async () =>
    screen.container.querySelector<HTMLButtonElement>('[aria-label="Evaluator effort"]')!.click(),
  );
  fixture.visibility.mockReturnValue("hidden");
  await fixture.poll();
  expect(request).toHaveBeenCalledTimes(1);
  fixture.visibility.mockReturnValue("visible");
  request.mockRejectedValueOnce(new Error("private synthetic-secret"));
  await fixture.poll();
  expect(screen.container.textContent).toContain("Owner review is unavailable");
  expect(screen.container.textContent).not.toContain("synthetic-secret");
  await act(async () => button(screen.container, "Save model").click());
  expect(request).toHaveBeenCalledTimes(2);
  let resolve!: (value: typeof fixture.snapshot) => void;
  request.mockImplementationOnce(
    () =>
      new Promise((done) => {
        resolve = done;
      }),
  );
  await fixture.poll();
  await fixture.poll();
  expect(request).toHaveBeenCalledTimes(3);
  await act(async () => resolve(fixture.snapshot));
  expect(screen.container.querySelector('[role="alert"]')).toBeNull();
  await act(async () => button(screen.container, "Save model").click());
  expect(request).toHaveBeenCalledWith(environmentId, {
    action: "policy",
    input: {
      enabled: true,
      expectedRevision: "synthetic-policy",
      modelSelection: {
        ...selection,
        options: [
          { id: "reasoningEffort", value: "medium" },
          { id: "serviceTier", value: "default" },
        ],
      },
    },
  });
});
