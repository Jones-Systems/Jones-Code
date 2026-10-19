import { act, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import { userEvent } from "vite-plus/test/browser";
import type { VoiceReviewSelfIdentity } from "@t3tools/contracts";

const fixture = vi.hoisted(() => ({
  environments: [
    {
      environmentId: "blocked",
      label: "Blocked server",
      entry: { enabled: true },
      connection: { phase: "connected" },
      serverConfig: { environment: { capabilities: { workQueueMetadata: false } } },
    },
    {
      environmentId: "ready",
      label: "Ready server",
      entry: { enabled: true },
      connection: { phase: "connected" },
      serverConfig: { environment: { capabilities: { workQueueMetadata: false } } },
    },
  ],
  blockedList: vi.fn(),
  readyList: vi.fn(),
  identity: vi.fn<() => Promise<VoiceReviewSelfIdentity | null>>(),
  loadQueue: vi.fn(),
}));
vi.mock("../../state/environments", () => ({
  useEnvironments: () => ({ environments: fixture.environments }),
}));
vi.mock("@tanstack/react-router", () => ({ useBlocker: () => ({ status: "idle" }) }));
vi.mock("../../env", () => ({ isElectron: false }));
vi.mock("../ui/sidebar", () => ({
  SidebarInset: ({ children }: { children: ReactNode }) => <main>{children}</main>,
}));
vi.mock("../../jones/workQueue/useWorkQueueMetadata", () => ({
  useWorkQueueMetadata: () => fixture.loadQueue,
}));
vi.mock("./useVoiceReview", () => ({
  useVoiceReview: (id: string) => ({
    fetchList: id === "blocked" ? fixture.blockedList : fixture.readyList,
    fetchSelfIdentity: fixture.identity,
  }),
}));
vi.mock("./VoiceReviewPage", () => ({
  EnvironmentVoiceReview: () => (
    <textarea aria-label="Existing prompt draft" defaultValue="Unsaved prompt" />
  ),
}));

import { UnifiedQueuePage } from "../../jones/workQueue/UnifiedQueuePage";

let root: Root;
let container: HTMLDivElement;
let originals: typeof fixture.environments;
let clipboard: ReturnType<typeof vi.spyOn>;
const identity: VoiceReviewSelfIdentity = {
  subject: "cloud-connect",
  method: "dpop-access-token",
  proof_key_thumbprint: "own-device-key",
  enrollment: "not-enrolled",
};

beforeEach(() => {
  originals = structuredClone(fixture.environments);
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  fixture.blockedList.mockReset().mockRejectedValue({ _tag: "VoiceReviewForbiddenError" });
  fixture.readyList.mockReset().mockResolvedValue({ drafts: [] });
  fixture.identity.mockReset().mockResolvedValue(identity);
  fixture.loadQueue.mockReset();
  clipboard = vi.spyOn(navigator.clipboard, "writeText").mockResolvedValue(undefined);
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});
afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  fixture.environments = originals;
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});
const render = async () => {
  await act(async () => root.render(<UnifiedQueuePage />));
};
const click = async (label: string) => {
  const button = [...container.querySelectorAll("button")].find(
    (item) => item.textContent === label,
  );
  if (!button) throw new Error(`Missing button: ${label}`);
  await act(async () => userEvent.click(button));
};

it.each(["VoiceReviewForbiddenError", "VoiceReviewNotConfiguredError"])(
  "reaches setup from %s discovery without blocking a ready source or changing its draft",
  async (tag) => {
    fixture.blockedList.mockRejectedValue({ _tag: tag });
    await render();
    expect(fixture.identity).not.toHaveBeenCalled();
    expect(container.querySelector('[aria-label="Blocked server prompts"]')).toBeNull();
    expect(container.querySelector('[aria-label="Ready server prompts"]')).not.toBeNull();
    const editor = container.querySelector<HTMLTextAreaElement>("textarea")!;
    await act(async () => userEvent.fill(editor, "Keep this unsaved edit"));
    await click("Set up this device");
    expect(fixture.identity).toHaveBeenCalledOnce();
    expect(container.textContent).toContain("This device is not enrolled");
    await click("Copy enrollment entry");
    expect(clipboard).toHaveBeenCalledExactlyOnceWith(
      '{\n  "subject": "cloud-connect",\n  "proof_key_thumbprint": "own-device-key"\n}',
    );
    expect(container.textContent).toContain("Enrollment entry copied.");
    expect(container.querySelector("textarea")).toBe(editor);
    expect(editor.value).toBe("Keep this unsaved edit");
    expect(fixture.blockedList).toHaveBeenCalledOnce();
    expect(fixture.readyList).toHaveBeenCalledOnce();
    await click("Hide device setup");
    expect(container.textContent).not.toContain("own-device-key");
    expect(fixture.identity).toHaveBeenCalledOnce();
  },
);

it("reports clipboard failure without claiming a copy succeeded", async () => {
  clipboard.mockRejectedValue(new Error("denied"));
  await render();
  await click("Set up this device");
  await click("Copy enrollment entry");
  expect(container.textContent).toContain("Could not copy.");
  expect(container.textContent).not.toContain("Enrollment entry copied.");
  expect(container.querySelector("pre")?.textContent).toContain("own-device-key");
});

it("degrades an older server without disturbing other prompt sources", async () => {
  fixture.identity.mockResolvedValue(null);
  await render();
  await click("Set up this device");
  expect(container.textContent).toContain("Device setup details are unavailable on this server.");
  expect(container.textContent).not.toContain("Copy enrollment entry");
  expect(container.querySelector('[aria-label="Ready server prompts"]')).not.toBeNull();
  expect(fixture.identity).toHaveBeenCalledOnce();
});

it.each(["bearer-access-token", "browser-session-cookie"] as const)(
  "does not offer a device entry for %s",
  async (method) => {
    fixture.identity.mockResolvedValue({ ...identity, method, proof_key_thumbprint: null });
    await render();
    await click("Set up this device");
    expect(container.textContent).toContain("does not provide a verified device key");
    expect(container.textContent).not.toContain("Copy enrollment entry");
  },
);

it.each(["disabled", "disconnected"])(
  "performs no reads for a %s source and removes open details on disconnect",
  async (state) => {
    await render();
    await click("Set up this device");
    fixture.blockedList.mockClear();
    fixture.identity.mockClear();
    fixture.environments = fixture.environments.map((environment) =>
      environment.environmentId !== "blocked"
        ? environment
        : {
            ...environment,
            entry: { enabled: state !== "disabled" },
            connection: { phase: state === "disconnected" ? "disconnected" : "connected" },
          },
    );
    await render();
    expect(container.textContent).not.toContain("Set up this device");
    expect(container.textContent).not.toContain("own-device-key");
    expect(fixture.blockedList).not.toHaveBeenCalled();
    expect(fixture.identity).not.toHaveBeenCalled();
  },
);
