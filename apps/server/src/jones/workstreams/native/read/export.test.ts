// @effect-diagnostics nodeBuiltinImport:off -- Synthetic in-memory encrypted stores and exact emitted byte assertions.
import { expect, it } from "@effect/vitest";
import * as Wire from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as Fixture from "../store/sql.testkit.ts";
import migration from "../../../../jones/persistence/Migrations/125_JonesWorkstreamsNative.ts";
import * as Admin from "../cli.ts";
import * as Registry from "../registry/service.ts";
import * as Session from "../registry/session.ts";
import * as Store from "../store/registryStore.ts";
import * as Projection from "./projection.ts";
import * as Appearance from "../registry/appearance.ts";
import * as Admission from "../runtime/admission.ts";
import { NativeHome, type AdmittedCaller } from "../mode/authorityMode.ts";
const home = {
  ...Fixture.scope,
  canonical_origin: "https://11111111-1111-4111-8111-111111111111.workstreams.invalid",
  home_environment_id: "home:test",
  served_by_environment_id: "home:test",
  server_generation: 1,
};
const caller: AdmittedCaller = {
  principal_id: "principal:test",
  source_environment_id: "home:test",
  binding_version: 1,
  origin: { kind: "app" },
};
const base = (keys = Fixture.secrets()) => {
  const registry = Registry.layer.pipe(
    Layer.provideMerge(Session.layer),
    Layer.provideMerge(Fixture.testLayer({ keys })),
  );
  return Layer.mergeAll(Projection.layer, Appearance.layer, Admission.layer).pipe(
    Layer.provideMerge(registry),
    Layer.provideMerge(Layer.succeed(NativeHome, home)),
  );
};
const initialize = Effect.gen(function* () {
  yield* TestClock.setTime(Date.parse(Fixture.now));
  yield* migration;
  const admin = yield* Admin.NativeAdmin.pipe(Effect.provide(Admin.layer));
  const { served_by_environment_id: _served, ...identity } = home;
  yield* admin.initialize({
    registry: identity,
    principal_id: caller.principal_id,
    content_key_id: "test-home",
    local_key_id: "test-local",
  });
});
const commands = Effect.gen(function* () {
  const registry = yield* Registry.Registry;
  let sequence = 0;
  const submit = Effect.fn("ReadTest.submit")(function* (action: Wire.WorkstreamCommand["action"]) {
    const s = yield* registry.snapshot(caller);
    return yield* registry.submit(
      caller,
      JSON.stringify({
        command_id: "read-command-" + String(++sequence).padStart(20, "0"),
        expected_server_generation: s.context.server_generation,
        expected_registry_version: s.context.registry_version,
        action,
      }),
    );
  });
  const create = (name = "Synthetic private name") =>
    submit({
      operation: "create_workstream",
      name,
      lifecycle: "active",
      progress: { state: "unknown" },
      sort_order: 0,
    }).pipe(
      Effect.map((accepted) => {
        if (accepted.receipt.state !== "committed") throw new Error("create rejected");
        return accepted.receipt.effects.workstream_versions[0]!.workstream_id;
      }),
    );
  return { registry, submit, create };
});
import * as NodeCrypto from "node:crypto";
import * as Stream from "effect/Stream";
import * as ReadExport from "./export.ts";
const evidence = { origin: { kind: "app" } as const, check: () => Effect.void };
const services = ReadExport.layer.pipe(
  Layer.provideMerge(base()),
  Layer.provide(
    Layer.succeed(Admission.CallerBoundary, { current: () => Effect.succeed(evidence) }),
  ),
);
it.effect(
  "exports deterministic bounded pages and hashes the exact pre-trailer bytes from one live snapshot",
  () =>
    Effect.gen(function* () {
      yield* initialize;
      const h = yield* commands;
      yield* h.create("α");
      yield* h.create("β");
      const exporter = yield* ReadExport.ReadExport;
      const first = yield* Stream.runCollect(exporter.stream(1));
      const second = yield* Stream.runCollect(exporter.stream(1));
      expect(first).toEqual(second);
      expect(first).toHaveLength(3);
      const trailer = JSON.parse(first.at(-1)!);
      expect(trailer).toEqual({
        complete: true,
        registry_version: 3,
        sha256: NodeCrypto.createHash("sha256")
          .update(first.slice(0, -1).join(""), "utf8")
          .digest("hex"),
      });
      const pages = first.slice(0, -1).map((line) => JSON.parse(line));
      expect(pages.map((p) => p.offset)).toEqual([0, 1]);
      expect(
        pages.every((p) => p.items.length === 1 && p.envelope.coverage.registry === "complete"),
      ).toBe(true);
      expect(yield* Stream.runCollect(exporter.stream(101)).pipe(Effect.flip)).toMatchObject({
        code: "invalid_request",
      });
    }).pipe(Effect.provide(services)),
);

it.effect.each(["version", "revocation", "key"] as const)(
  "does not emit a complete trailer after midstream %s change",
  (change) => {
    const keys = Fixture.secrets();
    const runtime = ReadExport.layer.pipe(
      Layer.provideMerge(base(keys)),
      Layer.provide(
        Layer.succeed(Admission.CallerBoundary, { current: () => Effect.succeed(evidence) }),
      ),
    );
    return Effect.gen(function* () {
      yield* initialize;
      const h = yield* commands;
      yield* h.create("one");
      yield* h.create("two");
      const exporter = yield* ReadExport.ReadExport,
        store = yield* Store.RegistryStore;
      const seen: string[] = [];
      const result = yield* exporter.stream(1).pipe(
        Stream.runForEach((line) =>
          Effect.gen(function* () {
            seen.push(line);
            if (seen.length !== 1) return;
            if (change === "version") yield* h.create("third");
            else if (change === "key") keys.delete("workstreams-content-key-test-home");
            else
              yield* store.appendPlain(Fixture.scope, "grants", {
                principal_id: caller.principal_id,
                grant_id: "grant:principal:test",
                grant_version: 2,
                permissions: '["workstreams:read"]',
                state: "revoked",
              });
          }),
        ),
        Effect.flip,
      );
      expect(result).toMatchObject({
        code:
          change === "version"
            ? "cursor_stale"
            : change === "key"
              ? "key_unavailable"
              : "forbidden",
      });
      expect(seen).toHaveLength(1);
      expect(seen.every((line) => !("complete" in JSON.parse(line)))).toBe(true);
    }).pipe(Effect.provide(runtime));
  },
);

it.effect(
  "cancellation yields no trailer and appearance mutations cannot mix independently versioned pages",
  () =>
    Effect.gen(function* () {
      yield* initialize;
      const h = yield* commands,
        id = yield* h.create("one");
      yield* h.create("two");
      const exporter = yield* ReadExport.ReadExport,
        appearance = yield* Appearance.Appearance;
      const cancelled = yield* exporter.stream(1).pipe(Stream.take(1), Stream.runCollect);
      expect(cancelled).toHaveLength(1);
      expect(JSON.parse(cancelled[0]!)).not.toHaveProperty("complete");
      const lines: string[] = [];
      yield* exporter.stream(1).pipe(
        Stream.runForEach((line) =>
          Effect.gen(function* () {
            lines.push(line);
            if (lines.length === 1)
              yield* appearance.save(caller, {
                command_id: "appearance-midstream-0000000001",
                expected_server_generation: 1,
                expected_version: 0,
                workstream_id: id,
                border_color: "#123456",
              });
          }),
        ),
      );
      expect(lines).toHaveLength(3);
      expect(JSON.parse(lines[2]!).complete).toBe(true);
      expect(
        lines
          .slice(0, 2)
          .flatMap((l) => JSON.parse(l).items)
          .every((i: Projection.ProjectionItem) => i.appearance.border_color === null),
      ).toBe(true);
    }).pipe(Effect.provide(services)),
);
