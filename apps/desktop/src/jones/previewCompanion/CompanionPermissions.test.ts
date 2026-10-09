// @effect-diagnostics nodeBuiltinImport:off - Synthetic Electron guests only.
import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as NodeEvents from "node:events";
import { vi } from "vite-plus/test";
import { guardCompanionGuest } from "./CompanionIsolation.ts";

const { requestHandler, checkHandler } = vi.hoisted(() => ({
  requestHandler: vi.fn(),
  checkHandler: vi.fn(),
}));
vi.mock("electron", () => ({
  session: {
    fromPartition: () => ({
      setPermissionRequestHandler: requestHandler,
      setPermissionCheckHandler: checkHandler,
    }),
  },
}));
import * as BrowserSession from "../../preview/BrowserSession.ts";

it.effect(
  "denies clipboard reads on companion guests through both existing permission handlers",
  () =>
    Effect.gen(function* () {
      requestHandler.mockClear();
      checkHandler.mockClear();
      const service = yield* BrowserSession.make;
      yield* service.getSession("synthetic-companion-permissions", false);
      const request = requestHandler.mock.calls[0]![0] as (
        contents: Electron.WebContents,
        permission: string,
        reply: (allow: boolean) => void,
      ) => void;
      const check = checkHandler.mock.calls[0]![0] as (
        contents: Electron.WebContents,
        permission: string,
      ) => boolean;
      const companion = new NodeEvents.EventEmitter() as unknown as Electron.WebContents;
      const ordinary = new NodeEvents.EventEmitter() as unknown as Electron.WebContents;
      guardCompanionGuest(companion);
      for (const contents of [ordinary, companion]) {
        for (const permission of ["clipboard-read", "clipboard-sanitized-write", "local-fonts"]) {
          const expected =
            permission === "clipboard-sanitized-write" ||
            (permission === "clipboard-read" && contents === ordinary);
          const reply = vi.fn();
          request(contents, permission, reply);
          expect(reply).toHaveBeenCalledWith(expected);
          expect(check(contents, permission)).toBe(expected);
        }
      }
    }).pipe(Effect.provide(NodeServices.layer)),
);
