import * as Layer from "effect/Layer";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
// @effect-diagnostics nodeBuiltinImport:off - Synthetic Electron guests only.
import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as NodeEvents from "node:events";
import { vi } from "vite-plus/test";
import { guardCompanionGuest } from "./CompanionIsolation.ts";

const { requestHandler, checkHandler } = vi.hoisted(() => ({
  requestHandler: vi.fn(),
  checkHandler: vi.fn(),
}));
vi.mock("electron", () => ({
  BrowserWindow: { fromWebContents: () => undefined },
  session: {
    fromPartition: () => ({
      setPermissionRequestHandler: requestHandler,
      setPermissionCheckHandler: checkHandler,
      webRequest: { onBeforeSendHeaders: vi.fn() },
    }),
  },
}));
import * as BrowserSession from "../../preview/BrowserSession.ts";
import * as ElectronDialog from "../../electron/ElectronDialog.ts";

it.effect(
  "denies clipboard reads on companion guests through both existing permission handlers",
  () =>
    Effect.gen(function* () {
      requestHandler.mockClear();
      checkHandler.mockClear();
      const unexpectedDialog = vi.fn(() =>
        Effect.die("unexpected dialog for clipboard permissions"),
      );
      const clipboardDialog = vi.fn(() => Effect.succeed({ response: 0, checkboxChecked: false }));
      const service = yield* BrowserSession.make.pipe(
        Effect.provideService(ElectronDialog.ElectronDialog, {
          pickFolder: unexpectedDialog,
          pickFiles: unexpectedDialog,
          showMessageBox: clipboardDialog,
          showErrorBox: unexpectedDialog,
        }),
      );
      yield* service.getSession("synthetic-companion-permissions", false);
      const request = requestHandler.mock.calls[0]![0] as (
        contents: Electron.WebContents,
        permission: string,
        reply: (allow: boolean) => void,
        details: { requestingUrl: string },
      ) => void;
      const check = checkHandler.mock.calls[0]![0] as (
        contents: Electron.WebContents,
        permission: string,
        origin: string,
      ) => boolean;
      const companion = new NodeEvents.EventEmitter() as unknown as Electron.WebContents;
      const ordinary = new NodeEvents.EventEmitter() as unknown as Electron.WebContents;
      ordinary.isDestroyed = () => false;
      guardCompanionGuest(companion);
      expect(check(ordinary, "clipboard-read", "https://example.test")).toBe(false);
      for (const contents of [ordinary, companion]) {
        for (const permission of ["clipboard-read", "clipboard-sanitized-write", "local-fonts"]) {
          const expected =
            permission === "clipboard-sanitized-write" ||
            (permission === "clipboard-read" && contents === ordinary);
          const receipt = yield* Deferred.make<boolean>();
          const reply = vi.fn((allowed: boolean) =>
            Deferred.doneUnsafe(receipt, Effect.succeed(allowed)),
          );
          const promptsBefore = clipboardDialog.mock.calls.length;
          request(contents, permission, reply, { requestingUrl: "https://example.test/page" });
          expect(yield* Deferred.await(receipt)).toBe(expected);
          expect(reply).toHaveBeenCalledWith(expected);
          expect(check(contents, permission, "https://example.test")).toBe(expected);
          if (contents === companion) expect(clipboardDialog).toHaveBeenCalledTimes(promptsBefore);
        }
      }
      const remembered = yield* Deferred.make<boolean>();
      request(
        ordinary,
        "clipboard-read",
        (allowed) => Deferred.doneUnsafe(remembered, Effect.succeed(allowed)),
        { requestingUrl: "https://example.test/another-page" },
      );
      expect(yield* Deferred.await(remembered)).toBe(true);
      expect(check(companion, "clipboard-read", "https://example.test")).toBe(false);
      expect(clipboardDialog).toHaveBeenCalledTimes(1);
      expect(unexpectedDialog).not.toHaveBeenCalled();
    }).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);
