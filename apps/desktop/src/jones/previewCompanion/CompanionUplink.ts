import {
  DesktopBrowserEvent,
  DesktopBrowserCommand,
  type DesktopCompanionConfig,
  type DesktopCompanionState,
  type DesktopCompanionPopupNotice,
  type DesktopCompanionTicketRequest,
  type DesktopCompanionTicketResponse,
} from "@t3tools/contracts";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { NodeWS } from "@effect/platform-node/NodeSocket";
import * as Context from "effect/Context";
import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Random from "effect/Random";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { powerSaveBlocker } from "electron";
import * as DesktopEnvironment from "../../app/DesktopEnvironment.ts";
import * as DesktopAppIdentity from "../../app/DesktopAppIdentity.ts";
import * as DesktopBrowserHost from "../../preview/DesktopBrowserHost.ts";
import * as ElectronWindow from "../../electron/ElectronWindow.ts";
import * as CompanionConfig from "./CompanionConfig.ts";
import { createCompanionUplink, type CompanionSocket } from "./UplinkController.ts";
import * as Channels from "./channels.ts";
import { onCompanionPopupNotice } from "./CompanionPopup.ts";
import { MAX_MESSAGE_BYTES } from "@t3tools/shared/jones/previewCompanionFraming";

export class CompanionUplink extends Context.Service<
  CompanionUplink,
  {
    readonly getState: Effect.Effect<DesktopCompanionState>;
    readonly configure: (
      config: DesktopCompanionConfig,
      restartRequired?: boolean,
    ) => Effect.Effect<void>;
    readonly setReady: (ready: boolean, renderer?: Electron.WebContents) => Effect.Effect<void>;
    readonly completeTicket: (response: DesktopCompanionTicketResponse) => Effect.Effect<void>;
    readonly retry: Effect.Effect<void>;
  }
>()("@t3tools/desktop/jones/previewCompanion/CompanionUplink") {}

function openSocket(url: string): CompanionSocket {
  const socket = new NodeWS.WebSocket(url, {
    maxPayload: MAX_MESSAGE_BYTES,
    perMessageDeflate: false,
    followRedirects: false,
  });
  // Terminating a connecting socket emits an error after its per-attempt listeners are removed.
  socket.on("error", () => {});
  return {
    get bufferedAmount() {
      return socket.bufferedAmount;
    },
    send: (frame) => socket.send(frame),
    close: (code) => {
      if (socket.readyState === NodeWS.WebSocket.CONNECTING) socket.terminate();
      else socket.close(code);
    },
    onOpen: (listener) => {
      socket.on("open", listener);
      return () => {
        socket.off("open", listener);
      };
    },
    onMessage: (listener) => {
      const handler = (data: NodeWS.RawData, binary: boolean) =>
        listener(binary ? null : data.toString());
      socket.on("message", handler);
      return () => {
        socket.off("message", handler);
      };
    },
    onClose: (listener) => {
      socket.on("close", listener);
      return () => {
        socket.off("close", listener);
      };
    },
    onError: (listener) => {
      const error = () => listener();
      const response = (_request: unknown, response: { statusCode?: number }) =>
        listener(response.statusCode);
      socket.on("error", error);
      socket.on("unexpected-response", response);
      return () => {
        socket.off("error", error);
        socket.off("unexpected-response", response);
      };
    },
  };
}

const make = Effect.gen(function* () {
  const config = yield* CompanionConfig.CompanionConfig;
  const environment = yield* DesktopEnvironment.DesktopEnvironment;
  const host = yield* DesktopBrowserHost.DesktopBrowserHost;
  const identity = yield* DesktopAppIdentity.DesktopAppIdentity;
  const windows = yield* ElectronWindow.ElectronWindow;
  const platform = yield* HostProcessPlatform;
  const clock = yield* Clock.Clock;
  const random = yield* Random.Random;
  const runtimeIdentity = yield* identity.previewAutomationRuntimeIdentity;
  const changes = yield* Queue.sliding<
    | { readonly channel: typeof Channels.NOTICE; readonly value: DesktopCompanionPopupNotice }
    | { readonly channel: typeof Channels.STATE; readonly value: DesktopCompanionState }
    | {
        readonly channel: typeof Channels.TICKET_REQUEST;
        readonly value: DesktopCompanionTicketRequest;
      }
  >(64);
  const runFork = Effect.runForkWith(yield* Effect.context<never>());
  const decodeEvent = Schema.decodeUnknownOption(Schema.fromJsonString(DesktopBrowserEvent));
  const controller = createCompanionUplink(yield* config.get, {
    browserOnlyLocked: environment.previewCompanionProduct === true,
    runtimeIdentity,
    platform,
    now: () => clock.currentTimeMillisUnsafe(),
    random: () => random.nextDoubleUnsafe(),
    schedule: (delay, task) => {
      const fiber = runFork(
        clock.sleep(Duration.millis(delay)).pipe(Effect.andThen(Effect.sync(task))),
      );
      return () => fiber.interruptUnsafe();
    },
    connect: openSocket,
    state: (value) => {
      Queue.offerUnsafe(changes, { channel: Channels.STATE, value });
    },
    requestTicket: (value) => {
      Queue.offerUnsafe(changes, { channel: Channels.TICKET_REQUEST, value });
    },
    replaceAssignments: host.replaceCompanionAssignments,
    command: (command, isCurrent) => {
      runFork(
        Effect.suspend(() =>
          isCurrent()
            ? Schema.encodeEffect(Schema.fromJsonString(DesktopBrowserCommand))(command).pipe(
                Effect.flatMap((line) =>
                  host.handleCommandLine(
                    line,
                    (key) => isCurrent() && host.isCompanionAssigned(key),
                  ),
                ),
                Effect.catch(() =>
                  Effect.logWarning("Could not encode preview companion command."),
                ),
              )
            : Effect.void,
        ),
      );
    },
    subscribeBrowser: (listener, isAssigned) => {
      const decoder = new TextDecoder();
      const fiber = runFork(
        Stream.runForEach(host.eventsMatching(isAssigned), (bytes) =>
          Effect.sync(() => {
            const event = decodeEvent(decoder.decode(bytes));
            if (Option.isSome(event) && isAssigned(event.value)) listener(event.value);
          }),
        ),
      );
      return () => fiber.interruptUnsafe();
    },
    preventSuspension: () => {
      const id = powerSaveBlocker.start("prevent-app-suspension");
      return () => {
        if (powerSaveBlocker.isStarted(id)) powerSaveBlocker.stop(id);
      };
    },
    unknownMessage: () => {
      runFork(Effect.logWarning("Ignored unknown preview companion message."));
    },
  });
  let unwatchRenderer: (() => void) | undefined;
  const setReady = (ready: boolean, renderer?: Electron.WebContents) => {
    unwatchRenderer?.();
    unwatchRenderer = undefined;
    if (ready && renderer) {
      const stop = () => {
        unwatchRenderer?.();
        unwatchRenderer = undefined;
        controller.setReady(false);
      };
      const navigate = (
        _event: Electron.Event,
        _url: string,
        _inPlace: boolean,
        mainFrame: boolean,
      ) => {
        if (mainFrame) stop();
      };
      renderer.once("destroyed", stop);
      renderer.once("render-process-gone", stop);
      renderer.on("did-start-navigation", navigate);
      unwatchRenderer = () => {
        renderer.removeListener("destroyed", stop);
        renderer.removeListener("render-process-gone", stop);
        renderer.removeListener("did-start-navigation", navigate);
      };
    }
    controller.setReady(ready);
  };
  const unsubscribeNotices = onCompanionPopupNotice((value) => {
    Queue.offerUnsafe(changes, { channel: Channels.NOTICE, value });
  });
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      unsubscribeNotices();
      unwatchRenderer?.();
      controller.dispose();
    }),
  );
  yield* Stream.runForEach(Stream.fromQueue(changes), ({ channel, value }) =>
    Effect.gen(function* () {
      const main = yield* windows.main;
      if (Option.isSome(main) && !main.value.webContents.isDestroyed())
        main.value.webContents.send(channel, value);
    }),
  ).pipe(Effect.forkScoped);
  return CompanionUplink.of({
    getState: Effect.sync(controller.getState),
    configure: (value, restart) => Effect.sync(() => controller.configure(value, restart)),
    setReady: (ready, renderer) => Effect.sync(() => setReady(ready, renderer)),
    completeTicket: (response) => Effect.sync(() => controller.completeTicket(response)),
    retry: Effect.sync(controller.retry),
  });
});
export const layer = Layer.effect(CompanionUplink, make);
