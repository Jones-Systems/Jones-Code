// @vitest-environment jsdom
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { SetupStatusContent, sourceReasonLabels } from "./SetupStatusContent";
import { setupFixture } from "./operations.fixtures";
let root: Root;
let container: HTMLDivElement;
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});
afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});
describe("read-only setup diagnosis", () => {
  for (const reason of Object.keys(sourceReasonLabels) as (keyof typeof sourceReasonLabels)[]) {
    it(`shows ${reason} with its documented next step`, async () => {
      await act(async () =>
        root.render(
          <SetupStatusContent
            status={{
              ...setupFixture,
              t3_source: { state: "unavailable", reason },
              next_steps: [
                {
                  code: "enroll_native_source",
                  doc: "docs/operations/workstreams-native-enrollment.md",
                },
              ],
            }}
          />,
        ),
      );
      expect(container.textContent).toContain(reason);
      expect(container.textContent).toContain(sourceReasonLabels[reason]);
      expect(container.querySelector("a")?.href).toContain("workstreams-native-enrollment.md");
      expect(container.querySelector("button,input,form")).toBeNull();
    });
  }
  it("distinguishes not configured from offline without exposing values", async () => {
    await act(async () =>
      root.render(
        <SetupStatusContent
          status={{
            ...setupFixture,
            activation: { state: "disabled", missing_keys: ["T3_WORKSTREAM_SIGNING_SECRET"] },
            registry: {
              state: "not_configured",
              registry_id: null,
              owner_id: null,
              can_write: false,
            },
          }}
        />,
      ),
    );
    expect(container.textContent).toContain("Not configured");
    expect(container.textContent).toContain("T3_WORKSTREAM_SIGNING_SECRET");
    await act(async () =>
      root.render(
        <SetupStatusContent
          status={{
            ...setupFixture,
            registry: { state: "unavailable", registry_id: null, owner_id: null, can_write: false },
          }}
        />,
      ),
    );
    expect(container.textContent).toContain("Offline");
  });
});
