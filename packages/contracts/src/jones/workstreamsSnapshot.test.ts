import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  canonicalWorkstreamsSnapshotJson as canonical,
  WorkstreamsSnapshotRow,
  WorkstreamsSnapshotHeader,
  WorkstreamsSnapshotTrailer,
} from "./workstreamsSnapshot.ts";
describe("lossless snapshot framing", () => {
  it("uses UTF-16 key order and preserves private JSON fractions as opaque bytes", () => {
    expect(canonical({ "2": 2, "10": 10, a: ["é", true, null] })).toBe(
      '{"10":10,"2":2,"a":["é",true,null]}',
    );
    const payload = '{"confidence":0.75,"outcome":"synthetic"}';
    const row = {
      class: "workstream_rollup_summaries",
      key: ["owner-1", "ws-1"],
      row: { payload, revision: 1 },
    };
    expect(decodeWorkstreamsSnapshotRow(row)).toEqual(row);
    expect(JSON.parse(canonical(row)).row.payload).toBe(payload);
    expect(JSON.parse(JSON.parse(canonical(row)).row.payload).confidence).toBe(0.75);
  });
  it("preserves legacy singleton and history predecessor keys without coercion", () => {
    const rows = [
      {
        class: "workstream_registry_identity",
        key: [true],
        row: { singleton: true, registry_instance_id: "12345678-1234-1234-1234-123456789abc" },
      },
      {
        class: "workstream_history_links",
        key: ["owner-1", "global", -1, "", 1],
        row: {
          owner_id: "owner-1",
          scope_key: "global",
          predecessor_registry_version: -1,
          predecessor_event_id: "",
          valid_from_event_sequence: 1,
        },
      },
    ];
    for (const row of rows) {
      expect(decodeWorkstreamsSnapshotRow(row)).toEqual(row);
      expect(decodeWorkstreamsSnapshotRow(JSON.parse(canonical(row)))).toEqual(row);
    }
    for (const key of [[-2], [0.5], [Number.MAX_SAFE_INTEGER + 1], [null], [{}]]) {
      expect(() => decodeWorkstreamsSnapshotRow({ ...rows[1], key })).toThrow();
    }
  });
  it("refuses lossy numbers and structures rather than silently normalizing them", () => {
    const cycle: Record<string, unknown> = {};
    cycle.self = cycle;
    const extra = [1];
    Object.defineProperty(extra, "hidden", { value: true });
    const sparse: unknown[] = [];
    sparse.length = 2;
    sparse[1] = 1;
    for (const value of [
      0.75,
      -0,
      Number.MAX_SAFE_INTEGER + 1,
      NaN,
      undefined,
      new Map(),
      sparse,
      cycle,
      extra,
      { bad: undefined },
      { [Symbol("key")]: 1 },
    ])
      expect(() => canonical(value)).toThrow();
    expect(() => decodeWorkstreamsSnapshotRow({ class: "unknown", key: ["x"], row: {} })).toThrow();
    expect(() =>
      decodeWorkstreamsSnapshotRow({ class: "workstreams", key: [], row: {} }),
    ).toThrow();
  });
  it("requires unique owner scopes and an explicit complete trailer", () => {
    const header = {
      contract: "workstreams-snapshot/1.0.0",
      snapshot_id: "12345678-1234-1234-1234-123456789abc",
      producer: { system: "control-plane", revision: "a".repeat(40), schema_version: 15 },
      registry: {
        registry_instance_id: "12345678-1234-1234-1234-123456789abc",
        canonical_origin: "https://registry.workstreams.invalid",
      },
      owners: [
        { owner_id: "owner-1", server_generation: 1, registry_version: 0, audit_high_water: 0 },
      ],
      fence: { epoch: 1, fenced_at: "2026-10-25T12:00:00Z" },
      exported_at: "2026-10-25T12:01:00Z",
      open_effects: 0,
      encryption: { mode: "stream", key_id: null },
    };
    expect(decodeWorkstreamsSnapshotHeader(header)).toEqual(header);
    expect(() =>
      decodeWorkstreamsSnapshotHeader({
        ...header,
        owners: [...header.owners, ...header.owners],
      }),
    ).toThrow();
    const decode = decodeWorkstreamsSnapshotTrailer;
    expect(decode({ snapshot_sha256: "a".repeat(64), complete: true }).complete).toBe(true);
    expect(() => decode({ snapshot_sha256: "a".repeat(64), complete: false })).toThrow();
  });
});

const decodeWorkstreamsSnapshotHeader = Schema.decodeUnknownSync(WorkstreamsSnapshotHeader);
const decodeWorkstreamsSnapshotRow = Schema.decodeUnknownSync(WorkstreamsSnapshotRow);
const decodeWorkstreamsSnapshotTrailer = Schema.decodeUnknownSync(WorkstreamsSnapshotTrailer);
