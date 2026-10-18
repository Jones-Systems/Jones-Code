import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as DesktopUpdates from "../../updates/DesktopUpdates.ts";
import { makeHarness } from "../../updates/updatesTestHarness.ts";
import { MANUAL_UPDATE_REASON } from "./CompanionProductUpdates.ts";

describe("dedicated companion manual updates", () => {
  it.effect.each(["darwin", "linux", "win32"] as const)(
    "disables every updater action on %s even with mock feed enabled",
    (platform) =>
      Effect.gen(function* () {
        const f = makeHarness({
          platform,
          env: { JONES_PREVIEW_COMPANION_PRODUCT: "true", T3CODE_DESKTOP_MOCK_UPDATES: "true" },
        });
        yield* Effect.gen(function* () {
          const updates = yield* DesktopUpdates.DesktopUpdates;
          yield* updates.configure;
          expect(yield* updates.getState).toMatchObject({
            enabled: false,
            status: "disabled",
            message: MANUAL_UPDATE_REASON,
          });
          expect(Option.getOrThrow(yield* updates.disabledReason)).toBe(MANUAL_UPDATE_REASON);
          expect((yield* updates.check("user")).checked).toBe(false);
          expect((yield* updates.download).accepted).toBe(false);
          expect((yield* updates.install).accepted).toBe(false);
          expect((yield* updates.installPrepared("9.9.9", "synthetic-handle")).accepted).toBe(
            false,
          );
          expect((yield* updates.setChannel("nightly")).enabled).toBe(false);
          expect((yield* updates.subscribe).latest.enabled).toBe(false);
          expect(yield* updates.isActionActive).toBe(false);
          expect(yield* updates.isInstallActive).toBe(false);
        }).pipe(Effect.provide(f.layer), Effect.scoped);
        expect(f.feedUrls()).toEqual([]);
        expect(f.listenerCount()).toBe(0);
        expect(f.checkCount()).toBe(0);
        expect(f.downloadCount()).toBe(0);
        expect(f.quitAndInstalls()).toBe(0);
        expect(f.installSteps).toEqual([]);
        expect([...f.updateRestartMarkers]).toEqual([]);
      }),
  );
});
