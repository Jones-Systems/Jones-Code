import { assert, describe, it } from "@effect/vitest";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcessSpawner } from "effect/process";

import { readTailscaleServeConfigJson, TAILSCALE_STATUS_TIMEOUT } from "../tailscale.ts";
import {
  acquireServeMapping,
  classifyServeConfig,
  decidePairWrite,
  queryServeMapping,
  releaseServeMapping,
} from "./serveOwnership.ts";

const claim = { servePort: 8443, expectedTarget: "http://127.0.0.1:3773" };
const config = (target = claim.expectedTarget) => ({
  TCP: { "8443": { HTTPS: true } },
  Web: { "node.tail.ts.net:8443": { Handlers: { "/": { Proxy: target } } } },
});
const exact = JSON.stringify(config());
const statusArgs = ["serve", "status", "--json"];
const writeArgs = ["serve", "--bg", "--https=8443", claim.expectedTarget];
const offArgs = ["serve", "--https=8443", "off"];
const encoder = new TextEncoder();
const encodeErrorJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

function recordingLayer(
  results: ReadonlyArray<{ stdout?: string; stderr?: string; code?: number; never?: boolean }>,
) {
  const commands: Array<ReadonlyArray<string>> = [];
  const layer = Layer.merge(
    Layer.succeed(HostProcessPlatform, "linux"),
    Layer.succeed(
      ChildProcessSpawner.ChildProcessSpawner,
      ChildProcessSpawner.make((command) => {
        const process = command as unknown as { readonly args: ReadonlyArray<string> };
        commands.push(process.args);
        const result = results[commands.length - 1];
        if (result === undefined) return Effect.die(new Error("Unexpected command"));
        return Effect.succeed(
          ChildProcessSpawner.makeHandle({
            pid: ChildProcessSpawner.ProcessId(1),
            exitCode: result.never
              ? Effect.never
              : Effect.succeed(ChildProcessSpawner.ExitCode(result.code ?? 0)),
            isRunning: Effect.succeed(false),
            kill: () => Effect.void,
            unref: Effect.succeed(Effect.void),
            stdin: Sink.drain,
            stdout: Stream.make(encoder.encode(result.stdout ?? "")),
            stderr: Stream.make(encoder.encode(result.stderr ?? "")),
            all: Stream.empty,
            getInputFd: () => Sink.drain,
            getOutputFd: () => Stream.empty,
          }),
        );
      }),
    ),
  );
  return { commands, layer };
}

describe("Serve configuration ownership", () => {
  it("classifies empty, exact, malformed and unfamiliar configurations", () => {
    assert.deepEqual(classifyServeConfig("{}", claim), { _tag: "absent" });
    assert.deepEqual(classifyServeConfig("null", claim), { _tag: "absent" });
    assert.deepEqual(classifyServeConfig(exact, claim), {
      _tag: "exact",
      target: claim.expectedTarget,
    });
    assert.deepEqual(classifyServeConfig(" ", claim), { _tag: "unknown", reason: "empty-output" });
    assert.deepEqual(classifyServeConfig("{", claim), { _tag: "unknown", reason: "decode-failed" });
    assert.deepEqual(classifyServeConfig('{"Future":{"setting":true}}', claim), {
      _tag: "unknown",
      reason: "unrecognized-shape",
    });
    assert.deepEqual(classifyServeConfig('{"TCP":[]}', claim), {
      _tag: "unknown",
      reason: "unrecognized-shape",
    });
  });

  it("refuses each occupied port shape and ignores other ports", () => {
    const fixtures = [
      [config("http://127.0.0.1:9999"), "other-target"],
      [
        {
          TCP: config().TCP,
          Web: {
            "node:8443": {
              Handlers: {
                "/": { Proxy: claim.expectedTarget },
                "/api": { Proxy: claim.expectedTarget },
              },
            },
          },
        },
        "extra-handlers",
      ],
      [{ TCP: { "8443": { TCPForward: "localhost:9999" } } }, "tcp-forward"],
      [{ ...config(), AllowFunnel: { "node:8443": true } }, "funnel"],
      [{ Foreground: { session: config() } }, "foreground"],
      [{ TCP: { "8443": { HTTP: true } } }, "non-web-tcp"],
    ] as const;
    for (const [fixture, reason] of fixtures) {
      const state = classifyServeConfig(JSON.stringify(fixture), claim);
      assert.equal(state._tag, "conflicting");
      if (state._tag === "conflicting") assert.equal(state.reason, reason);
    }
    assert.deepEqual(
      classifyServeConfig(
        JSON.stringify({
          TCP: { "443": { TCPForward: "localhost:1" } },
          Web: { "node:443": { Handlers: { "/": { Proxy: "http://localhost:1" } } } },
          AllowFunnel: { "node:443": true },
          Foreground: { session: { TCP: { "443": { HTTPS: true } } } },
        }),
        claim,
      ),
      { _tag: "absent" },
    );
  });

  it("permits only absent, exact, and proven target-only dev replacements", () => {
    assert.equal(decidePairWrite({ _tag: "absent" }, false), "write");
    assert.equal(decidePairWrite({ _tag: "exact", target: claim.expectedTarget }, false), "reuse");
    const other = classifyServeConfig(JSON.stringify(config("http://127.0.0.1:1")), claim);
    assert.deepEqual(decidePairWrite(other, false), { refuse: other });
    assert.equal(decidePairWrite(other, true), "write");
    const unknown = { _tag: "unknown", reason: "status-failed" } as const;
    assert.deepEqual(decidePairWrite(unknown, true), { refuse: unknown });
    const funnel = { _tag: "conflicting", reason: "funnel" } as const;
    assert.deepEqual(decidePairWrite(funnel, true), { refuse: funnel });
  });

  it.effect("reads with exact argv and drops stderr secrets on failure", () => {
    const { commands, layer } = recordingLayer([
      { stdout: exact },
      { stderr: "permission denied tskey-auth-secret", code: 1 },
    ]);
    return Effect.gen(function* () {
      assert.equal(yield* readTailscaleServeConfigJson, exact);
      const error = yield* readTailscaleServeConfigJson.pipe(Effect.flip);
      assert.notInclude(yield* encodeErrorJson(error), "tskey-auth-secret");
      assert.notInclude(error.message, "tskey-auth-secret");
      assert.deepEqual(commands, [statusArgs, statusArgs]);
    }).pipe(Effect.provide(layer));
  });

  it.effect("bounds status timeout and maps read failure to unknown", () => {
    const { commands, layer } = recordingLayer([{ never: true }]);
    return Effect.gen(function* () {
      const fiber = yield* queryServeMapping(claim).pipe(Effect.forkChild);
      yield* TestClock.adjust(TAILSCALE_STATUS_TIMEOUT);
      assert.deepEqual(yield* Fiber.join(fiber), { _tag: "unknown", reason: "status-failed" });
      assert.deepEqual(commands, [statusArgs]);
    }).pipe(Effect.provide(layer));
  });

  it.effect.each([
    { label: "conflicting", stdout: JSON.stringify(config("http://127.0.0.1:1")) },
    { label: "unknown", stdout: "" },
  ])("does not acquire a $label mapping", ({ stdout }) => {
    const { commands, layer } = recordingLayer([{ stdout }]);
    return Effect.gen(function* () {
      const acquired = yield* acquireServeMapping(claim);
      assert.isTrue("skipped" in acquired);
      assert.deepEqual(commands, [statusArgs]);
    }).pipe(Effect.provide(layer));
  });

  it.effect("creates an absent route, verifies it, and removes only its exact claim", () => {
    const { commands, layer } = recordingLayer([
      { stdout: "{}" },
      {},
      { stdout: exact },
      { stdout: exact },
      {},
      { stdout: "{}" },
    ]);
    return Effect.gen(function* () {
      const acquired = yield* acquireServeMapping(claim);
      assert.deepEqual(acquired, { created: true });
      assert.equal(yield* releaseServeMapping({ ...claim, created: true }), "disabled");
      assert.deepEqual(commands, [
        statusArgs,
        writeArgs,
        statusArgs,
        statusArgs,
        offArgs,
        statusArgs,
      ]);
    }).pipe(Effect.provide(layer));
  });

  it.effect("never adopts an exact reused route", () => {
    const { commands, layer } = recordingLayer([{ stdout: exact }]);
    return Effect.gen(function* () {
      assert.deepEqual(yield* acquireServeMapping(claim), { created: false });
      assert.equal(yield* releaseServeMapping({ ...claim, created: false }), "skipped");
      assert.deepEqual(commands, [statusArgs]);
    }).pipe(Effect.provide(layer));
  });

  it.effect("does not remove a route reassigned during server lifetime", () => {
    const { commands, layer } = recordingLayer([
      { stdout: JSON.stringify(config("http://127.0.0.1:1")) },
    ]);
    return Effect.gen(function* () {
      assert.equal(yield* releaseServeMapping({ ...claim, created: true }), "skipped");
      assert.deepEqual(commands, [statusArgs]);
    }).pipe(Effect.provide(layer));
  });

  it.effect("does not remove on unknown state or retry unknown removal effects", () => {
    const { commands, layer } = recordingLayer([
      { stdout: "" },
      { stdout: exact },
      { code: 1 },
      { stdout: exact },
    ]);
    return Effect.gen(function* () {
      assert.equal(yield* releaseServeMapping({ ...claim, created: true }), "unknown");
      assert.equal(yield* releaseServeMapping({ ...claim, created: true }), "unknown");
      assert.deepEqual(commands, [statusArgs, statusArgs, offArgs, statusArgs]);
    }).pipe(Effect.provide(layer));
  });

  it.effect("does not adopt a write whose readback differs", () => {
    const { commands, layer } = recordingLayer([
      { stdout: "{}" },
      {},
      { stdout: JSON.stringify(config("http://127.0.0.1:1")) },
    ]);
    return Effect.gen(function* () {
      assert.deepEqual(yield* acquireServeMapping(claim), {
        skipped: { _tag: "unknown", reason: "status-failed" },
      });
      assert.deepEqual(commands, [statusArgs, writeArgs, statusArgs]);
    }).pipe(Effect.provide(layer));
  });
});
