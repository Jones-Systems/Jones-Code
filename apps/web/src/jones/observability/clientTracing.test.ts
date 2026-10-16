import { describe, expect, it } from "@effect/vitest";
import { afterEach, beforeEach, vi } from "vite-plus/test";
import * as Effect from "effect/Effect";
import * as Tracer from "effect/Tracer";
import { OtlpTracer } from "effect/observability";

vi.mock("../../environments/primary", () => ({
  resolvePrimaryEnvironmentHttpUrl: (path: string) => `https://environment.example.test${path}`,
}));
vi.mock("../../environments/primary/httpLayer", async () => ({
  layer: (await import("effect/Layer")).empty,
}));
vi.mock("../../env", () => ({ isElectron: false }));
vi.mock("../../branding", () => ({ APP_VERSION: "0.0.0-test" }));
vi.mock("effect/observability", async (importOriginal) => {
  const original = await importOriginal<typeof import("effect/observability")>();
  return {
    ...original,
    OtlpExporter: {
      ...original.OtlpExporter,
      layerFlusher: (await import("effect/Layer")).empty,
    },
    OtlpTracer: { ...original.OtlpTracer, make: vi.fn() },
  };
});

describe("client tracing recovery", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.mocked(OtlpTracer.make).mockReset();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it.effect(
    "retries a failed exporter on default configuration and shares the recovered delegate",
    () =>
      Effect.gen(function* () {
        const { configureClientTracing } = yield* Effect.promise(
          () => import("../../observability/clientTracing"),
        );
        const ClientTracer = yield* Effect.promise(
          () => import("../../observability/clientTracer"),
        );
        const warning = vi.spyOn(console, "warn").mockImplementation(() => {});
        const releaseFailedExporter = vi.fn();
        const releaseRecoveredExporter = vi.fn();
        const spans: string[] = [];
        const delegate = Tracer.make({
          span(options) {
            spans.push(options.name);
            return new Tracer.NativeSpan(options);
          },
        });
        vi.mocked(OtlpTracer.make)
          .mockImplementationOnce(() =>
            Effect.addFinalizer(() => Effect.sync(releaseFailedExporter)).pipe(
              Effect.andThen(Effect.die(new Error("Exporter initialization failed"))),
            ),
          )
          .mockImplementation(() =>
            Effect.acquireRelease(Effect.succeed(delegate), () =>
              Effect.sync(releaseRecoveredExporter),
            ),
          );

        try {
          yield* Effect.promise(() => configureClientTracing());
          expect(ClientTracer.hasDelegate()).toBe(false);
          expect(releaseFailedExporter).toHaveBeenCalledTimes(1);
          expect(warning).toHaveBeenCalledTimes(1);

          yield* Effect.promise(() =>
            Promise.all([
              configureClientTracing(),
              configureClientTracing(),
              configureClientTracing(),
            ]),
          );
          expect(ClientTracer.hasDelegate()).toBe(true);
          expect(OtlpTracer.make).toHaveBeenCalledTimes(2);
          expect(releaseFailedExporter).toHaveBeenCalledTimes(1);
          expect(releaseRecoveredExporter).not.toHaveBeenCalled();

          yield* Effect.void.pipe(
            Effect.withSpan("recovered.client.work"),
            Effect.provide(ClientTracer.layer),
          );
          expect(spans).toEqual(["recovered.client.work"]);

          yield* Effect.promise(() => configureClientTracing());
          expect(OtlpTracer.make).toHaveBeenCalledTimes(2);
          expect(releaseRecoveredExporter).not.toHaveBeenCalled();
        } finally {
          vi.mocked(OtlpTracer.make).mockImplementationOnce(() =>
            Effect.die(new Error("Test exporter cleanup")),
          );
          yield* Effect.promise(() => configureClientTracing({ exportIntervalMs: 2_000 }));
        }
        expect(ClientTracer.hasDelegate()).toBe(false);
        expect(releaseRecoveredExporter).toHaveBeenCalledTimes(1);
      }),
  );
});
