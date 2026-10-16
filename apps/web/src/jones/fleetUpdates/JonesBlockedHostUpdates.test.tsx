// @vitest-environment jsdom
import { act } from "react";
import { createRoot } from "react-dom/client";
import { describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import { JonesBlockedHostUpdates } from "./JonesBlockedHostUpdates";

const { execute } = vi.hoisted(() => ({ execute: vi.fn() }));
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: () => execute }));
vi.mock("../../connection/runtime", () => ({ connectionAtomRuntime: {} }));
vi.mock("@t3tools/client-runtime/state/runtime", () => ({ createRuntimeCommand: () => ({}) }));
vi.mock("./blockedHostCommand", () => ({ executeBlockedHostUpdate: {} }));

describe("historical host update management", () => {
  it("labels history separately and requests fresh state before offering an update action", async () => {
    let settle!: (value: unknown) => void;
    execute.mockImplementation(
      () =>
        new Promise((resolve) => {
          settle = resolve;
        }),
    );
    vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
    const container = document.createElement("div");
    document.body.append(container);
    const root = createRoot(container);
    try {
      const environmentId = EnvironmentId.make("historical-off-host");
      await act(async () => {
        root.render(<JonesBlockedHostUpdates environmentId={environmentId} historical />);
      });
      expect(container.textContent).toContain("Previously required an update.");
      expect(container.textContent).toContain("The saved connection remains switched off.");
      expect(execute).toHaveBeenCalledExactlyOnceWith({
        environmentId,
        request: { action: "state" },
      });
      expect(container.textContent).not.toContain("Check for builds");
      expect(container.textContent).not.toContain("Install");
      await act(async () => {
        settle({
          _tag: "Success",
          value: {
            currency: "unknown",
            state: {
              phase: "blocked",
              message: "Fresh authenticated status",
              capability: { check: true, download: false, install: false },
            },
          },
        });
      });
      expect(container.textContent).toContain("Fresh authenticated status");
      expect(container.textContent).toContain("Source relationship has not been verified.");
      expect(container.textContent).toContain("Check for builds");
      expect(container.textContent).not.toContain("Install");
      expect(execute).toHaveBeenCalledTimes(1);
    } finally {
      await act(async () => root.unmount());
      container.remove();
      vi.unstubAllGlobals();
      execute.mockReset();
    }
  });
});
