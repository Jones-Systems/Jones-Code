import * as ClientTracer from "../observability/clientTracer";
import { runtime, runPrimaryHttp, __setPrimaryHttpRunnerForTests } from "./runtime";
import { afterEach, describe, expect, it, vi } from "@effect/vitest";
import { withRelayClientTracing } from "@t3tools/shared/relayTracing";
import * as Effect from "effect/Effect";
import * as Tracer from "effect/Tracer";

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
});

describe("web runtime relay telemetry policy", () => {
  it.each([
    {
      surface: "desktop opt-out",
      desktopBridge: { getRelayTelemetryEnabled: () => false },
      exports: false,
    },
    {
      surface: "desktop enabled",
      desktopBridge: { getRelayTelemetryEnabled: () => true },
      exports: true,
    },
    { surface: "older desktop bridge", desktopBridge: {}, exports: true },
    {
      surface: "ordinary browser ignores host opt-out",
      desktopBridge: undefined,
      exports: true,
    },
  ])("applies $surface before the first relay span", async ({ desktopBridge, exports }) => {
    vi.resetModules();
    vi.stubEnv("T3CODE_TELEMETRY_ENABLED", "false");
    vi.stubEnv("T3CODE_OTEL_SDK_DISABLED", "true");
    vi.stubEnv("OTEL_SDK_DISABLED", "true");
    vi.stubEnv("VITE_RELAY_OTLP_TRACES_URL", "https://telemetry.example.test/v1/traces");
    vi.stubEnv("VITE_RELAY_OTLP_TRACES_DATASET", "relay-test");
    vi.stubEnv("VITE_RELAY_OTLP_TRACES_TOKEN", "test-token");
    vi.stubGlobal("window", { desktopBridge });
    const fetchFn = vi.fn<typeof fetch>(async () => new Response(null, { status: 202 }));
    vi.stubGlobal("fetch", fetchFn);
    // Re-import after setting runtime policy: static initialization creates the exporter layer.
    const { runtime: freshRuntime } = await import("./runtime");
    const { setDelegate } = await import("../observability/clientTracer");
    const userSpans: Array<string> = [];

    try {
      expect(
        await freshRuntime.runPromise(
          Effect.succeed("relay operation completed").pipe(
            Effect.withSpan("relay.first-operation"),
            withRelayClientTracing,
          ),
        ),
      ).toBe("relay operation completed");

      setDelegate(
        Tracer.make({
          span(options) {
            userSpans.push(options.name);
            return new Tracer.NativeSpan(options);
          },
        }),
      );
      await freshRuntime.runPromise(Effect.void.pipe(Effect.withSpan("user.work")));
      expect(userSpans).toEqual(["user.work"]);
    } finally {
      setDelegate(null);
      // Closing the runtime flushes the exporter without waiting for its batch timer.
      await freshRuntime.dispose();
    }

    if (exports) {
      expect(fetchFn).toHaveBeenCalledOnce();
      expect(String(fetchFn.mock.calls[0]?.[0])).toBe("https://telemetry.example.test/v1/traces");
      const payload = new TextDecoder().decode(fetchFn.mock.calls[0]?.[1]?.body as Uint8Array);
      expect(payload).toContain('"name":"relay.first-operation"');
    } else {
      expect(fetchFn).not.toHaveBeenCalled();
    }
  });
});

describe("web runtime", () => {
  it("interrupts the primary HTTP runner when its caller aborts", async () => {
    const controller = new AbortController();
    const cleanup = new AbortController();
    let start!: () => void;
    const started = new Promise<void>((resolve) => {
      start = resolve;
    });
    let finalized = false;
    let settled = false;
    __setPrimaryHttpRunnerForTests((_effect, options) =>
      runtime.runPromise(
        Effect.sync(start).pipe(
          Effect.andThen(Effect.never),
          Effect.ensuring(
            Effect.sync(() => {
              finalized = true;
            }),
          ),
        ),
        options ?? { signal: cleanup.signal },
      ),
    );
    const pending = runPrimaryHttp(Effect.never, { signal: controller.signal }).then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );
    try {
      await started;
      controller.abort();
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(settled).toBe(true);
      expect(finalized).toBe(true);
    } finally {
      cleanup.abort();
      await pending;
      __setPrimaryHttpRunnerForTests();
    }
  });

  it("routes client spans to the exporter client tracing configured", async () => {
    const exported: Array<string> = [];
    ClientTracer.setDelegate(
      Tracer.make({
        span(options) {
          exported.push(options.name);
          return new Tracer.NativeSpan(options);
        },
      }),
    );

    try {
      await runtime.runPromise(Effect.void.pipe(Effect.withSpan("client.work")));
    } finally {
      ClientTracer.setDelegate(null);
    }

    expect(exported).toEqual(["client.work"]);
  });
});
