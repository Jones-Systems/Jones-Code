import { describe, expect, it } from "vite-plus/test";
import {
  collectionPosition,
  compareCollectionPositions,
  historyPage,
  validCollectionPosition,
  lifecycleCounts,
  type SequencedAuditEvent,
} from "./collections.ts";
import { createWorkstreamDomainState, type AuditEvent, type Workstream } from "./state.ts";
const event = (id: string, version: number): AuditEvent => ({
  event_id: id,
  command_id: "command:0000000000000001",
  actor: { principal_id: "principal-one" },
  operation: "update_workstream",
  occurred_at: "2026-09-12T00:00:00.000Z",
  registry_version: version,
  changed: false,
  workstream_versions: [{ workstream_id: "workstream:one", version: 1 }],
  native_reference_id: null,
  membership_ids: [],
  declaration_id: null,
  declaration_revision: null,
  edge_id: null,
  lifecycle_declaration: null,
});
describe("complete native keyset collections", () => {
  it.each([1, 7, 100])(
    "pages every equal-version event exactly once at limit %i, excluding later no-ops",
    (limit) => {
      const rows: SequencedAuditEvent[] = Array.from({ length: 203 }, (_, i) => ({
        event_sequence: i * 3 + 1,
        event: event("event:" + String(202 - i).padStart(4, "0"), Math.floor(i / 7)),
      }));
      const highWater = rows.at(-1)!.event_sequence;
      rows.push({ event_sequence: highWater + 7, event: event("event:late", 28) });
      const all: AuditEvent[] = [];
      let after: readonly (number | string)[] | null = null;
      for (let page = 0; page <= 203; page++) {
        const items = historyPage(rows, null, highWater, after, limit);
        if (items.length === 0) break;
        all.push(...items);
        after = collectionPosition("history", items.at(-1)!);
      }
      expect(all).toHaveLength(203);
      expect(new Set(all.map((e) => e.event_id)).size).toBe(203);
      expect(all.some((e) => e.event_id === "event:late")).toBe(false);
      expect(all).toEqual(
        rows
          .slice(0, -1)
          .map((r) => r.event)
          .toSorted((a, b) =>
            compareCollectionPositions(
              collectionPosition("history", a),
              collectionPosition("history", b),
            ),
          ),
      );
      expect(historyPage(rows, "workstream:absent", highWater, null, limit)).toEqual([]);
    },
  );
  it("uses byte order for mixed-case IDs and validates route-specific key widths", () => {
    expect(compareCollectionPositions([7, "event:Z"], [7, "event:a"])).toBe(-1);
    expect(compareCollectionPositions([7, "event:a"], [8, "event:Z"])).toBe(-1);
    expect(validCollectionPosition("history", [7, "event:a"])).toBe(true);
    expect(validCollectionPosition("history", [7])).toBe(false);
    expect(validCollectionPosition("declarations", [7, "declaration:a", 1])).toBe(true);
    expect(validCollectionPosition("references", ["reference:a", 1])).toBe(false);
    expect(() => historyPage([], null, -1, null, 1)).toThrow("invalid_request");
  });
  it("counts latest lifecycle declarations and preserves honest unknowns", () => {
    const state = createWorkstreamDomainState("owner-one");
    state.workstreams.set("one", {} as Workstream);
    state.workstreams.set("two", {} as Workstream);
    expect(lifecycleCounts(state)).toEqual({ total: 2, active: 0, unknown_lifecycle: 2 });
    const declaration = {
      lifecycle_declaration_id: "declaration:one",
      workstream_id: "one",
      revision: 1,
      prior_lifecycle: null,
      new_lifecycle: "active" as const,
      recorded_at: "2026-09-12T00:00:00.000Z",
      actor: { principal_id: "principal-one" },
      command_id: "command:0000000000000001",
      supersedes_lifecycle_declaration_id: null,
      registry_version: 1,
    };
    state.lifecycle_declarations.set("one", [
      { ...declaration, revision: 2, new_lifecycle: "completed" },
      declaration,
    ]);
    expect(lifecycleCounts(state)).toEqual({ total: 2, active: 0, unknown_lifecycle: 1 });
  });
});
