import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vite-plus/test";

vi.mock("../../state/workstreams", () => ({
  useWorkstreams: () => ({
    placementInventory: {
      coverage: "partial",
      identities: Array.from({ length: 1_000 }, (_, index) => ({
        source_instance_id: "environment",
        native_thread_id: String(index),
      })),
      json: "[]",
      totalIdentities: 20_000,
    },
    data: {
      binding: {
        registryId: "registry",
        ownerId: "owner",
        principalId: "principal",
        authorizationRevision: 1,
        serverGeneration: 7,
        registryVersion: 11,
        permissions: ["workstreams:read", "workstreams:write"],
        contractVersion: "workstreams/1.0.0",
        contractManifest: "6d8da23d51c1bba024dddc4b8d4dd9a594d2f474affd73e4cc7de508b797f557",
      },
      items: [
        {
          workstreamId: "ws-a",
          name: "Alpha",
          lifecycle: "active",
          progress: { state: "progressing" },
          delivery: "pr-open",
          freshness: "current",
          sortOrder: 0,
          version: 1,
          updatedAt: "2026-09-12T12:00:00Z",
        },
      ],
      nextCursor: null,
      source: "live",
      stale: false,
    },
    submit: vi.fn(),
    runBindingOperation: vi.fn(),
    loadDetail: vi.fn(),
    loadReference: vi.fn(),
  }),
}));

import { WorkstreamSidebarSection } from "./WorkstreamSidebarSection";
import { useWorkstreams } from "../../state/workstreams";

describe("mounted Workstream sidebar", () => {
  it("renders owner groups ahead of native content with menu and drag alternatives", () => {
    const html = renderToStaticMarkup(<WorkstreamSidebarSection controller={useWorkstreams()} />);
    expect(html).toContain('aria-label="Owner Workstreams"');
    expect(html).toContain("Alpha");
    expect(html).toContain("Actions for Alpha");
    expect(html).toContain('draggable="true"');
    expect(html).toContain(
      "Thread placement lookup scope is partial (1,000 of 20,000 identities selected).",
    );
  });
});
