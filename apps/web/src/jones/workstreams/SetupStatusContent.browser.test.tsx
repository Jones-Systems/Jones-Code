import { describe, expect, it } from "vite-plus/test";
import { render } from "../../../test/render";
import { SetupStatusContent, sourceReasonLabels } from "./SetupStatusContent";
import { setupFixture } from "./operations.fixtures";
describe("read-only setup diagnosis", () => {
  it.each(Object.keys(sourceReasonLabels) as (keyof typeof sourceReasonLabels)[])(
    "shows %s with its documented next step",
    async (reason) => {
      await using screen = await render(
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
      );
      expect(screen.container.textContent).toContain(reason);
      expect(screen.container.textContent).toContain(sourceReasonLabels[reason]);
      expect(screen.container.querySelector("a")?.href).toContain(
        "workstreams-native-enrollment.md",
      );
      expect(screen.container.querySelector("button,input,form")).toBeNull();
    },
  );
  it("distinguishes not configured from offline without exposing values", async () => {
    await using screen = await render(
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
    );
    expect(screen.container.textContent).toContain("Not configured");
    expect(screen.container.textContent).toContain("T3_WORKSTREAM_SIGNING_SECRET");
    await screen.rerender(
      <SetupStatusContent
        status={{
          ...setupFixture,
          registry: { state: "unavailable", registry_id: null, owner_id: null, can_write: false },
        }}
      />,
    );
    expect(screen.container.textContent).toContain("Offline");
  });
});
