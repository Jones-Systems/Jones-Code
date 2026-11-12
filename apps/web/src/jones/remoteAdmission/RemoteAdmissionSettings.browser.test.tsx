import { act, render } from "../../../test/render";
import { expect, it, vi } from "vite-plus/test";
import { userEvent } from "vite-plus/test/browser";
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
import { TooltipProvider } from "../../components/ui/tooltip";
import "../../index.css";

const request = vi.hoisted(() => vi.fn());
vi.mock("./runtime", () => ({ remoteAdmissionRequest: request }));
vi.mock("../../state/environments", () => ({
  useEnvironmentProviders: () => new Map([[environmentId, providers]]),
  useEnvironment: () => ({ label: "Synthetic receiver", connection: { phase: "connected" } }),
  useEnvironmentIds: () => [],
  useEnvironments: () => ({ environments: [], isReady: true }),
}));
vi.mock("./useEvaluatorProviders", () => ({
  useEvaluatorProviders: () => (providerDataKnown ? providers : null),
}));
vi.mock("../../components/settings/settingsLayout", () => import("./settingsLayoutTestStub"));
vi.mock("../../state/server", () => ({ EMPTY_SERVER_PROVIDERS: [] }));
vi.mock("../../hooks/useSettings", () => ({
  useEnvironmentSettings: () => ({
    ...DEFAULT_UNIFIED_SETTINGS,
    providerInstances: Object.fromEntries(
      providers.map((entry) => [
        entry.instanceId,
        { driver: entry.driver, enabled: entry.enabled },
      ]),
    ),
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
let providerDataKnown: boolean;
let providers: readonly ServerProvider[];
const provider: ServerProvider = {
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
  providerDataKnown = true;
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
  let now = 1_800_000_000_000;
  const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
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
    poll: () =>
      act(async () => {
        now += 10_000;
        tick();
      }),
    [Symbol.dispose]() {
      clock.mockRestore();
      clear.mockRestore();
      interval.mockRestore();
      visibility.mockRestore();
    },
  };
}
const button = (container: HTMLElement, text: string) =>
  [...container.querySelectorAll("button")].find(
    (entry) => entry.textContent === text || entry.getAttribute("aria-label") === text,
  )!;
const saveButton = (container: HTMLElement) =>
  [...container.querySelectorAll("button")].find((entry) => entry.textContent === "Save model");
const effort = (container: HTMLElement) =>
  container.querySelector<HTMLButtonElement>('[aria-label="Evaluator effort"]')!;
const mediumSelection: ModelSelection = {
  ...selection,
  options: [
    { id: "reasoningEffort", value: "medium" },
    { id: "serviceTier", value: "default" },
  ],
};

it("saves the complete exact Codex account selection and keeps request articles and decisions out of Settings", async () => {
  using _fixture = setup("settings-exact");
  await using screen = await render(<RemoteAdmissionSettings environmentId={environmentId} />);
  expect(screen.container.textContent).toContain(
    "Screens incoming tasks on Synthetic receiver before they start",
  );
  expect(screen.container.textContent).toContain("Uses Synthetic Codex account.");
  expect(screen.container.querySelector("article")).toBeNull();
  expect(screen.container.textContent).not.toContain("Inspect synthetic project");
  expect(screen.container.textContent).not.toContain("Evaluator policy is enabled");
  const picker = screen.container.querySelector<HTMLButtonElement>(
    '[aria-label="Evaluator model"]',
  )!;
  expect(picker.dataset.instances).toBe(instanceId);
  await act(async () => picker.click());
  expect(saveButton(screen.container)).toBeUndefined();
  await act(async () => effort(screen.container).click());
  await act(async () => button(screen.container, "Save model").click());
  expect(request).toHaveBeenLastCalledWith(environmentId, { action: "snapshot" });
  expect(request).toHaveBeenCalledWith(environmentId, {
    action: "policy",
    input: {
      enabled: true,
      expectedRevision: "synthetic-policy",
      modelSelection: mediumSelection,
    },
  });
});

it("offers Save for an unsaved default selection", async () => {
  using fixture = setup("settings-default");
  request.mockResolvedValue({
    ...fixture.snapshot,
    policy: { ...fixture.snapshot.policy, enabled: false, modelSelection: null },
  });
  await using screen = await render(<RemoteAdmissionSettings environmentId={environmentId} />);
  await act(async () => button(screen.container, "Save model").click());
  expect(request).toHaveBeenCalledWith(environmentId, {
    action: "policy",
    input: expect.objectContaining({
      enabled: false,
      modelSelection: expect.objectContaining({ instanceId, model: "gpt-6.1-sol" }),
    }),
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
  expect(screen.container.textContent).toContain("missing-account / retired-model is unavailable");
  expect(saveButton(screen.container)).toBeUndefined();
  await act(async () => button(screen.container, "retired-model (Unavailable)").click());
  const save = button(screen.container, "Save model");
  expect(save.getAttribute("aria-disabled")).toBe("true");
  await act(async () => save.click());
  expect(request.mock.calls.every(([, input]) => input.action === "snapshot")).toBe(true);
});

it("keeps a safe load error and unsaved effort, disables writes, then recovers without hidden or overlapping polls", async () => {
  using fixture = setup("settings-recovery");
  await using screen = await render(<RemoteAdmissionSettings environmentId={environmentId} />);
  await act(async () => effort(screen.container).click());
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

it.each([
  [403, "Owner access is unavailable"],
  [404, "unsupported on Synthetic receiver"],
])(
  "pauses polling after HTTP %s and recovers only after explicit Refresh",
  async (status, message) => {
    using fixture = setup(`settings-terminal-${status}`);
    providers = [];
    request.mockRejectedValue({
      _tag: "RemoteEnvironmentAuthUndeclaredStatusError",
      status,
      message: "private synthetic-detail",
    });
    await using screen = await render(<RemoteAdmissionSettings environmentId={environmentId} />);
    expect(screen.container.textContent).toContain(message);
    expect(screen.container.textContent).toContain("Unknown");
    expect(screen.container.textContent).not.toContain("No supported providers");
    expect(screen.container.textContent).not.toContain("synthetic-detail");
    expect(screen.container.textContent).not.toContain("Retrying automatically");
    expect(screen.container.querySelector('[aria-label="Evaluate remote requests"]')).toBeNull();
    expect(screen.container.querySelector('[aria-label="Evaluator model"]')).toBeNull();
    if (status === 403) {
      expect(screen.container.textContent).toContain(
        "from Synthetic receiver's own Jones Code app or signed-in browser",
      );
      expect(screen.container.textContent).toContain("Settings → Connections");
    }
    await fixture.poll();
    await act(async () => document.dispatchEvent(new Event("visibilitychange")));
    expect(request).toHaveBeenCalledTimes(1);
    providers = [provider];
    request.mockResolvedValue({
      ...fixture.snapshot,
      policy: { ...fixture.snapshot.policy, enabled: false },
    });
    await act(async () => button(screen.container, "Refresh").click());
    const toggle = screen.container.querySelector<HTMLElement>(
      '[aria-label="Evaluate remote requests"]',
    )!;
    expect(toggle.getAttribute("aria-checked")).toBe("false");
    expect(screen.container.querySelector('[role="alert"]')).toBeNull();
    expect(saveButton(screen.container)).toBeUndefined();
    await act(async () => toggle.click());
    expect(request).toHaveBeenCalledWith(environmentId, {
      action: "policy",
      input: { enabled: true, expectedRevision: "synthetic-policy", modelSelection: selection },
    });
    const beforePoll = request.mock.calls.length;
    await fixture.poll();
    expect(request).toHaveBeenCalledTimes(beforePoll + 1);
    expect(request).toHaveBeenLastCalledWith(environmentId, { action: "snapshot" });
  },
);

it("keeps provider absence unknown until a current provider source succeeds", async () => {
  using fixture = setup("settings-provider-source");
  providerDataKnown = false;
  const emptyPolicy = {
    ...fixture.snapshot,
    policy: { ...fixture.snapshot.policy, enabled: false, modelSelection: null },
  };
  request.mockResolvedValue(emptyPolicy);
  await using screen = await render(<RemoteAdmissionSettings environmentId={environmentId} />);
  expect(screen.container.textContent).toContain("Provider availability is unknown");
  expect(screen.container.textContent).not.toContain("No supported providers");
  expect(saveButton(screen.container)).toBeUndefined();
  expect(request).toHaveBeenCalledTimes(1);
  providers = [];
  providerDataKnown = true;
  await fixture.poll();
  expect(screen.container.textContent).toContain("No supported providers");
});

it("shows retained policy as last known and blocks settings during a current read", async () => {
  using fixture = setup("settings-last-known");
  await using screen = await render(<RemoteAdmissionSettings environmentId={environmentId} />);
  await act(async () => effort(screen.container).click());
  let resolve!: (value: typeof fixture.snapshot) => void;
  request.mockImplementationOnce(
    () =>
      new Promise((done) => {
        resolve = done;
      }),
  );
  await fixture.poll();
  expect(screen.container.textContent).toContain("Last known: enabled");
  expect(button(screen.container, "Save model").getAttribute("aria-disabled")).toBe("true");
  await act(async () => button(screen.container, "Save model").click());
  expect(request).toHaveBeenCalledTimes(2);
  await act(async () => resolve(fixture.snapshot));
  expect(screen.container.textContent).not.toContain("Last known");
  expect(button(screen.container, "Save model").getAttribute("aria-disabled")).toBe("false");
});

it("explains unavailable owner controls on hover and focus and guards keyboard activation", async () => {
  using _fixture = setup("settings-owner-help");
  request.mockRejectedValue({ _tag: "RemoteEnvironmentAuthFetchError", message: "private detail" });
  await using screen = await render(
    <TooltipProvider delay={0}>
      <RemoteAdmissionSettings environmentId={environmentId} />
    </TooltipProvider>,
  );
  const control = screen.container.querySelector<HTMLElement>(
    '[role="group"][aria-label="Evaluate remote requests"]',
  )!;
  await userEvent.hover(control);
  await expect
    .poll(() => document.querySelector('[data-slot="tooltip-popup"]')?.textContent)
    .toContain("Refresh to retry now");
  await userEvent.unhover(control);
  await act(async () => control.focus());
  await expect
    .poll(() => document.querySelector('[data-slot="tooltip-popup"]')?.textContent)
    .toContain("Owner review is unavailable");
  expect(document.activeElement).toBe(control);
  await userEvent.keyboard("{Enter}");
  await userEvent.keyboard(" ");
  expect(saveButton(screen.container)).toBeUndefined();
  expect(request).toHaveBeenCalledTimes(1);
});

it("explains missing remote evaluation support without inferring an integration or offering it", async () => {
  using fixture = setup("settings-cli-explanation");
  providers = [{ ...provider, supportsRemoteRequestEvaluation: false }];
  request.mockResolvedValue({
    ...fixture.snapshot,
    policy: { ...fixture.snapshot.policy, enabled: false, modelSelection: null },
  });
  await using screen = await render(<RemoteAdmissionSettings environmentId={environmentId} />);
  const disclosure = button(screen.container, "1 provider unavailable for remote evaluation");
  await act(async () => disclosure.click());
  expect(screen.container.textContent).toContain("Not supported.");
  expect(screen.container.textContent).not.toContain("This Codex CLI integration");
  expect(screen.container.textContent).not.toContain("Managed Codex (ChatGPT authorization)");
  expect(screen.container.textContent).not.toContain("qualified Claude integrations");
  expect(screen.container.textContent).toContain("No supported providers");
  expect(screen.container.querySelector('[aria-label="Evaluator model"]')).toBeNull();
  expect(saveButton(screen.container)).toBeUndefined();
  expect(request).toHaveBeenCalledTimes(1);
});

it("lists only rejected instances with their first snapshot-supported reason once provider data is known", async () => {
  using fixture = setup("settings-instance-reasons");
  const cases: readonly [string, Partial<ServerProvider>, string][] = [
    [
      "Plain Codex",
      {
        supportsRemoteRequestEvaluation: false,
        availability: "unavailable",
        unavailableReason: "Lower priority unavailable reason",
        enabled: false,
        installed: false,
      },
      "Not supported.",
    ],
    [
      "Unqualified Claude",
      { driver: ProviderDriverKind.make("claudeAgent"), supportsRemoteRequestEvaluation: false },
      "Not supported.",
    ],
    ["Older instance", { supportsRemoteRequestEvaluation: undefined }, "Not supported."],
    [
      "Unavailable instance",
      {
        availability: "unavailable",
        unavailableReason: "Reported instance is unavailable.",
        installed: false,
        enabled: false,
      },
      "Unavailable. Reported instance is unavailable.",
    ],
    [
      "Unavailable without detail",
      { availability: "unavailable", installed: false, enabled: false },
      "Unavailable.",
    ],
    [
      "Disabled instance",
      {
        enabled: false,
        auth: { status: "unauthenticated" },
        status: "disabled",
        message: "Lower priority sign-in reason",
      },
      "Not enabled.",
    ],
    [
      "Managed Codex",
      {
        auth: { status: "unauthenticated" },
        status: "warning",
        message: "Sign in with ChatGPT to use Codex.",
      },
      "Not signed in. Sign in with ChatGPT to use Codex.",
    ],
    [
      "Signed out without detail",
      { auth: { status: "unauthenticated" }, status: "warning" },
      "Not signed in.",
    ],
    [
      "Not ready instance",
      { status: "error", message: "Reported probe failed.", models: [] },
      "Status: error. Reported probe failed.",
    ],
    ["Not ready without detail", { status: "warning" }, "Status: warning."],
    ["No model instance", { models: [] }, "No available models reported."],
  ];
  providers = [
    provider,
    ...cases.map(([name, overrides]) => ({
      ...provider,
      instanceId: ProviderInstanceId.make(name.toLowerCase().replaceAll(" ", "-")),
      displayName: name,
      ...overrides,
    })),
  ];
  providerDataKnown = false;
  await using screen = await render(<RemoteAdmissionSettings environmentId={environmentId} />);
  const list = () =>
    screen.container.querySelector(
      '[aria-label="Provider instances unavailable for remote evaluation"]',
    );
  expect(list()).toBeNull();
  providerDataKnown = true;
  await fixture.poll();
  expect(list()).toBeNull();
  await act(async () =>
    button(screen.container, `${cases.length} providers unavailable for remote evaluation`).click(),
  );
  const rows = [...list()!.querySelectorAll("li")].map((row) => row.textContent);
  expect(rows).toHaveLength(cases.length);
  for (const [name, , reason] of cases) {
    expect(rows).toContain(`${name} · ${name.toLowerCase().replaceAll(" ", "-")}: ${reason}`);
  }
  expect(list()!.textContent).not.toContain("Synthetic Codex account");
  expect(screen.container.textContent).not.toContain("This Codex CLI integration");
  providerDataKnown = false;
  await fixture.poll();
  expect(list()).toBeNull();
  expect(screen.container.textContent).not.toContain("providers unavailable");
});
