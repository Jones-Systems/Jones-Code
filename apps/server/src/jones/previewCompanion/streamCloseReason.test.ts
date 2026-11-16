import { expect, it } from "vite-plus/test";
import { CompanionHostUnavailable } from "./CompanionHostRegistry.ts";
import { companionCloseReason } from "./streamCloseReason.ts";

it.each(["offline", "timeout", "unknown"] as const)(
  "bounds complete JSON for %s without losing host identity",
  (state) => {
    for (const label of [
      "a".repeat(64),
      "漢".repeat(64),
      "😀".repeat(32),
      '"\\'.repeat(32),
      "\n\t\u0000".repeat(20),
      "\ud800".repeat(64),
      "👩‍💻".repeat(12),
      "e\u0301".repeat(32),
    ]) {
      const cause = new CompanionHostUnavailable({ hostId: "h".repeat(64), label, state });
      const reason = companionCloseReason(cause)!;
      expect(new TextEncoder().encode(reason).byteLength).toBeLessThanOrEqual(123);
      const decoded = JSON.parse(reason);
      expect(decoded.hostId).toBe(cause.hostId);
      expect(decoded.state).toBe(state);
      if (state === "unknown") expect(decoded.label).toBe("");
      else if (decoded.label !== "") {
        const prefix = decoded.label.endsWith("…") ? decoded.label.slice(0, -1) : decoded.label;
        expect(label.trim().startsWith(prefix)).toBe(true);
        expect(Array.from(label.trim()).slice(0, Array.from(prefix).length).join("")).toBe(prefix);
      }
    }
  },
);
it("preserves fitting labels and ignores other launch failures", () => {
  expect(
    JSON.parse(
      companionCloseReason(
        new CompanionHostUnavailable({ hostId: "mini", label: " Mini ", state: "offline" }),
      )!,
    ).label,
  ).toBe("Mini");
  expect(companionCloseReason(new Error("browser launch"))).toBeUndefined();
});
