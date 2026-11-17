import { EnvironmentId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import { render } from "../../../test/render";
import { PromptWorkstreams } from "./PromptWorkstreamDetails";
import type { PromptWorkstreamLine } from "./promptWorkstreams";

const line: PromptWorkstreamLine = {
  key: "from_thread:mini:docs",
  filterKey: "mini:docs",
  id: "docs",
  name: "Docs",
  color: "#123456",
  provenance: "from_thread",
  summary: null,
  threadId: null,
  assignedAutomatically: true,
};

describe("Prompt Workstream details", () => {
  it("shows automatic assignment evidence beside distinct queue provenance", async () => {
    await using screen = await render(
      <PromptWorkstreams
        environmentId={EnvironmentId.make("mini")}
        namesUnavailable
        lines={[
          line,
          {
            ...line,
            key: "queue_id:mini:docs",
            name: null,
            color: null,
            provenance: "queue_id",
            assignedAutomatically: false,
          },
        ]}
      />,
      { router: false },
    );
    expect(screen.container.textContent).toContain("Workstream names unavailable.");
    expect(screen.container.textContent).toContain("from thread · assigned automatically");
    expect(screen.container.textContent).toContain("queue ID");
    expect(screen.container.querySelectorAll("li")).toHaveLength(2);
    expect(screen.container.querySelector("li .font-mono")?.textContent).toBe("docs");
  });
});
