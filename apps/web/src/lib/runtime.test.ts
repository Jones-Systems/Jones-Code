import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Tracer from "effect/Tracer";

import * as ClientTracer from "../observability/clientTracer";
import { runtime, runPrimaryHttp, __setPrimaryHttpRunnerForTests } from "./runtime";

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
