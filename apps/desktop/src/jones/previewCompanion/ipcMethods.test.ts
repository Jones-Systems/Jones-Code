import { describe, expect, it } from "@effect/vitest";
import { DesktopCompanionState } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Cause from "effect/Cause";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as ElectronWindow from "../../electron/ElectronWindow.ts";
import * as Uplink from "./CompanionUplink.ts";
import * as Ipc from "./ipcMethods.ts";

function fixture(available = true, destroyed = false) {
  const calls: string[] = [];
  const state = Schema.decodeUnknownSync(DesktopCompanionState)({
    config: {
      enabled: true,
      environmentId: "env",
      hostId: "mini",
      label: "Mini",
      browserOnly: false,
    },
    status: "awaiting_ticket",
    connectionGeneration: null,
    assignments: [],
  });
  const renderer = { id: 7, isDestroyed: () => destroyed } as Electron.WebContents;
  const window = { webContents: renderer } as Electron.BrowserWindow;
  const windows = {
    main: Effect.succeed(available ? Option.some(window) : Option.none()),
  } as ElectronWindow.ElectronWindow["Service"];
  const uplink = Uplink.CompanionUplink.of({
    getState: Effect.sync(() => {
      calls.push("get");
      return state;
    }),
    configure: () => Effect.void,
    setReady: (ready, actualRenderer) =>
      Effect.sync(() => {
        expect(actualRenderer).toBe(renderer);
        calls.push(`ready:${ready}`);
      }),
    completeTicket: () =>
      Effect.sync(() => {
        calls.push("ticket");
      }),
    retry: Effect.sync(() => {
      calls.push("retry");
    }),
  });
  return {
    calls,
    state,
    layer: Layer.mergeAll(
      Layer.succeed(ElectronWindow.ElectronWindow, windows),
      Layer.succeed(Uplink.CompanionUplink, uplink),
    ),
  };
}

describe("companion IPC sender boundary", () => {
  it.effect.each([undefined, { sender: { id: 8 } }])(
    "rejects absent or guest sender before reading state or accepting a ticket",
    (event) =>
      Effect.gen(function* () {
        const f = fixture();
        yield* Effect.gen(function* () {
          expect(Exit.isFailure(yield* Effect.exit(Ipc.getState.handler(undefined, event)))).toBe(
            true,
          );
          expect(
            Exit.isFailure(
              yield* Effect.exit(
                Ipc.completeTicket.handler(
                  {
                    requestId: "request",
                    environmentId: "env",
                    result: {
                      _tag: "ready",
                      url: "wss://example.invalid/api/jones/preview-companion/ws?wsTicket=test",
                    },
                  },
                  event,
                ),
              ),
            ),
          ).toBe(true);
          expect(
            Exit.isFailure(yield* Effect.exit(Ipc.setTicketProviderReady.handler(true, event))),
          ).toBe(true);
        }).pipe(Effect.provide(f.layer));
        expect(f.calls).toEqual([]);
      }),
  );

  it.effect.each([
    [false, false],
    [true, true],
  ])("rejects unavailable or destroyed main renderer", ([available, destroyed]) =>
    Effect.gen(function* () {
      const f = fixture(available, destroyed);
      const result = yield* Effect.exit(
        Ipc.getState.handler(undefined, { sender: { id: 7 } }).pipe(Effect.provide(f.layer)),
      );
      expect(Exit.isFailure(result)).toBe(true);
      expect(f.calls).toEqual([]);
    }),
  );

  it.effect("accepts the current main renderer and binds readiness to its lifetime", () =>
    Effect.gen(function* () {
      const f = fixture();
      const event = { sender: { id: 7 } };
      yield* Effect.gen(function* () {
        expect(yield* Ipc.getState.handler(undefined, event)).toEqual(f.state);
        yield* Ipc.setTicketProviderReady.handler(true, event);
        yield* Ipc.retry.handler(undefined, event);
      }).pipe(Effect.provide(f.layer));
      expect(f.calls).toEqual(["get", "ready:true", "retry"]);
    }),
  );
  it.effect("redacts malformed ticket-bearing input from IPC errors", () =>
    Effect.gen(function* () {
      const f = fixture();
      const secret = "synthetic-ticket-private";
      const result = yield* Effect.exit(
        Ipc.completeTicket
          .handler(
            {
              requestId: "request",
              environmentId: "env",
              result: { _tag: "ready", url: `${secret}${"x".repeat(17000)}` },
            },
            { sender: { id: 7 } },
          )
          .pipe(Effect.provide(f.layer)),
      );
      expect(Exit.isFailure(result)).toBe(true);
      if (Exit.isFailure(result)) {
        expect(Cause.pretty(result.cause)).toContain("Invalid preview companion ticket response.");
        expect(Cause.pretty(result.cause)).not.toContain(secret);
      }
      expect(f.calls).toEqual([]);
    }),
  );
});
