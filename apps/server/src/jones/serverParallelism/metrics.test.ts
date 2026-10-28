import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Metric from "effect/Metric";
import { makePoolObservers } from "./metrics.ts";

it.effect("records execution without relabeling, interval uncertainty, exact RSS and bounded slot generations", () =>
  Effect.gen(function* () {
    const observer = yield* makePoolObservers("vps");
    const base = { operation: "search" as const, slotId: 2, generation: 123, queueDepth: 4, outcome: "ok" as const };
    observer.compute({ ...base, phase: "execute", durationMs: 17 });
    observer.compute({ ...base, phase: "request_round_trip", durationMs: 29 });
    observer.compute({ ...base, phase: "reply_delivery", durationMs: 4, estimated: true,
      lowerMs: 0, upperMs: 8, uncertaintyMs: 4 });
    observer.compute({ phase: "resources", operation: "pool", slotId: 2, generation: 124,
      outcome: "ok", rssBytes: 123_456_789 });
    const snapshots = (yield* Metric.snapshot).filter((sample) => sample.id.startsWith("jones_server_") &&
      sample.attributes?.profile === "vps" && sample.attributes?.slot === "2");
    const phase = (name: string) => snapshots.find((sample) => sample.id === "jones_server_pool_phase_duration" &&
      sample.attributes?.phase === name);
    expect(phase("execute")?.type === "Histogram" ? phase("execute")!.state : undefined).toMatchObject({ sum: 17 });
    expect(phase("request_round_trip")?.type === "Histogram" ? phase("request_round_trip")!.state : undefined).toMatchObject({ sum: 29 });
    expect(snapshots.find((sample) => sample.id === "jones_server_pool_reply_uncertainty_ms")?.state).toMatchObject({ value: 4 });
    expect(snapshots.find((sample) => sample.id === "jones_server_pool_reply_bound_ms" && sample.attributes?.bound === "upper")?.state)
      .toMatchObject({ value: 8 });
    expect(snapshots.find((sample) => sample.id === "jones_server_compute_resident_bytes")?.state).toMatchObject({ value: 123_456_789 });
    expect(snapshots.find((sample) => sample.id === "jones_server_pool_generation")?.state).toMatchObject({ value: 124 });
    const allowed = new Set(["profile", "pool", "phase", "slot", "outcome", "priority", "operation", "estimated", "bound", "unit"]);
    for (const sample of snapshots) {
      expect(Object.keys(sample.attributes ?? {}).every((key) => allowed.has(key))).toBe(true);
      expect(sample.attributes).not.toHaveProperty("generation");
    }
  }));

it.effect("exports explicit unavailable timing without inventing zero duration or RSS samples", () =>
  Effect.gen(function* () {
    const observer = yield* makePoolObservers("laptop");
    for (const outcome of ["failed", "cancelled", "timeout", "unavailable"] as const) {
      observer.compute({ phase: "execute", operation: "close", slotId: 1, generation: 3,
        queueDepth: 0, durationMs: null, outcome });
    }
    const snapshots = (yield* Metric.snapshot).filter((sample) => sample.attributes?.profile === "laptop" &&
      sample.attributes?.slot === "1" && sample.attributes?.operation === "close");
    expect(snapshots.filter((sample) => sample.id === "jones_server_pool_timing_available")).toHaveLength(4);
    expect(snapshots.filter((sample) => sample.id === "jones_server_pool_timing_available").every((sample) =>
      sample.type === "Gauge" && sample.state.value === 0)).toBe(true);
    expect(snapshots.some((sample) => sample.id === "jones_server_pool_phase_duration")).toBe(false);
  }));
