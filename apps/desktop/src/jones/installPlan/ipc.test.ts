import { it } from "@effect/vitest";
import { expect, vi } from "vite-plus/test";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Stream from "effect/Stream";
import type { JonesInstallPlanResult } from "@t3tools/contracts/jones/installPlan";
import { DesktopUpdates } from "../../updates/DesktopUpdates.ts";
import { createInitialDesktopUpdateState } from "../../updates/updateMachine.ts";
import { installPlan, installPlanAvailable } from "./ipc.ts";

// Exercise the real decoder and service tag without loading the external Electron runtime.
vi.mock("electron", () => ({}));
vi.mock("electron-updater", () => ({ autoUpdater: {} }));

it.effect(
  "decodes plan requests at the real IPC boundary and never accepts renderer gate/outcome setters",
  () =>
    Effect.gen(function* () {
      const state = createInitialDesktopUpdateState(
        "1.0.0",
        { hostArch: "arm64", appArch: "arm64", runningUnderArm64Translation: false },
        "latest",
      );
      const action = { accepted: false, completed: false, state };
      const result: JonesInstallPlanResult = {
        state: {
          schema: 1,
          current: null,
          last: null,
          laptop: { observedAt: "2026-10-27T00:00:00Z", activationBlocked: false },
          view: { complete: false },
        },
      };
      const request = vi.fn(() => Effect.succeed(result));
      const service = DesktopUpdates.of({
        getState: Effect.succeed(state),
        isActionActive: Effect.succeed(false),
        isInstallActive: Effect.succeed(false),
        subscribe: Effect.succeed({ latest: state, changes: Stream.empty }),
        emitState: Effect.void,
        disabledReason: Effect.succeedNone,
        configure: Effect.void,
        setChannel: () => Effect.succeed(state),
        check: () => Effect.succeed({ checked: false, state }),
        download: Effect.succeed(action),
        install: Effect.succeed(action),
        installPrepared: () => Effect.succeed({ ...action, failed: false }),
        installPlan: request,
      });
      expect(
        yield* installPlanAvailable.handler().pipe(Effect.provideService(DesktopUpdates, service)),
      ).toBe(true);
      const { installPlan: _installPlan, ...unsupported } = service;
      expect(
        yield* installPlanAvailable
          .handler()
          .pipe(Effect.provideService(DesktopUpdates, DesktopUpdates.of(unsupported))),
      ).toBe(false);
      const invoke = (raw: unknown) =>
        installPlan.handler(raw).pipe(Effect.provideService(DesktopUpdates, service));
      expect(yield* invoke({ type: "read" })).toEqual(result);
      expect(request).toHaveBeenCalledTimes(1);
      for (const raw of [
        { type: "verify", transactionId: "fake" },
        { type: "record", outcome: "committed" },
        {
          type: "claim",
          planId: "invalid",
          environmentId: "host",
          claimant: "a",
          step: "download",
          explicit: false,
        },
      ])
        expect(Exit.isFailure(yield* Effect.exit(invoke(raw)))).toBe(true);
      expect(request).toHaveBeenCalledTimes(1);
    }),
);
