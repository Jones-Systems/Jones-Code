// @vitest-environment jsdom

import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import type { WorkstreamListView } from "../../state/workstreams";
import { data, reference } from "./nativeWorkstreamActions.fixtures";
import { WorkstreamAddPrDialog } from "./WorkstreamAddPrDialog";
import { prepareWorkstreamPr } from "./workstreamReferenceActions";

vi.mock("./workstreamReferenceActions", async (original) => ({
  ...(await original<typeof import("./workstreamReferenceActions")>()),
  prepareWorkstreamPr: vi.fn(),
}));

let root: Root;
let container: HTMLDivElement;
let controller: WorkstreamListView;
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
  controller = {
    data,
    placements: null,
    references: null,
    registrationContext: {
      protocol: "workstreams-registration-context/1.0.0",
      state: "ready",
      owner_id: "owner",
      principal_id: "principal",
      grant_id: "grant",
      authorization_revision: 1,
      server_generation: 7,
      registry_version: 11,
      sources: [
        {
          provider: "github",
          source_instance_id: "github",
          resource_kind: "pull_request",
          id_kind: "external",
          account_provenance: { kind: "not_account_scoped" },
          authority_namespace: "github-authority",
          store_generation: 1,
        },
      ],
    },
    error: null,
    loading: false,
    placementInventory: { coverage: "complete", identities: [], json: "[]", totalIdentities: 0 },
    refresh: vi.fn(),
    retry: vi.fn(async () => {}),
    submit: vi.fn(),
    observeCommand: vi.fn(),
    runBindingOperation: vi.fn(),
    loadActionSnapshot: vi.fn(),
    loadDetail: vi.fn(),
    loadReference: vi.fn(),
  };
  vi.mocked(prepareWorkstreamPr).mockReset();
});
afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});

async function render() {
  await act(async () =>
    root.render(
      <WorkstreamAddPrDialog
        controller={controller}
        workstreamId="beta"
        open
        onOpenChange={vi.fn()}
        onLinked={vi.fn()}
        commandId={async () => "command"}
      />,
    ),
  );
}
async function enterUrl(value: string) {
  const input = document.querySelector<HTMLInputElement>('[aria-label="GitHub pull request URL"]')!;
  await act(async () => {
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!.call(input, value);
    input.dispatchEvent(new Event("input", { bubbles: true }));
  });
}
const button = (text: string) =>
  [...document.querySelectorAll<HTMLButtonElement>("button")].find(
    (item) => item.textContent === text,
  )!;

describe("Add PR reference dialog", () => {
  it("blocks duplicate preparation synchronously and requires a separate Verify PR action", async () => {
    let resolve!: (value: typeof reference) => void;
    const pending = new Promise<typeof reference>((finish) => {
      resolve = finish;
    });
    vi.mocked(prepareWorkstreamPr).mockReturnValueOnce(pending).mockResolvedValue(reference);
    await render();
    await enterUrl("https://github.com/Jones-Systems/Jones-Code/pull/42");
    const add = button("Add reference");
    await act(async () => {
      add.click();
      add.click();
    });
    expect(prepareWorkstreamPr).toHaveBeenCalledOnce();
    expect(document.querySelector('[role="status"]')?.textContent).toContain("Registering");
    expect(button("Cancel").disabled).toBe(true);
    await act(async () => {
      resolve(reference);
      await pending;
    });
    expect(button("Verify PR")).toBeDefined();
    expect(document.querySelector<HTMLInputElement>("input")?.disabled).toBe(true);
    await act(async () => button("Verify PR").click());
    expect(vi.mocked(prepareWorkstreamPr).mock.calls.map(([input]) => input.verify)).toEqual([
      false,
      true,
    ]);
  });
  it("keeps GitHub preparation usable without T3 and rejects incomplete input", async () => {
    await render();
    await enterUrl("#42");
    expect(button("Add reference").disabled).toBe(true);
    await enterUrl("https://github.com/a/b/pull/42");
    expect(button("Add reference").disabled).toBe(false);
    expect(document.body.textContent).not.toContain("unavailable");
  });
  it("renders a fixed unknown-effect message and Retry only observes metadata", async () => {
    vi.mocked(prepareWorkstreamPr).mockRejectedValue(new Error("arbitrary private exception"));
    await render();
    await enterUrl("https://github.com/a/b/pull/42");
    await act(async () => button("Add reference").click());
    expect(document.body.textContent).toContain("effect is unknown");
    expect(document.body.textContent).not.toContain("arbitrary private exception");
    await act(async () => button("Retry").click());
    expect(controller.retry).toHaveBeenCalledOnce();
    expect(prepareWorkstreamPr).toHaveBeenCalledOnce();
  });
});
