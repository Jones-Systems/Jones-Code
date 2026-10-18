import {
  PreviewRenderHostSelection,
  type PreviewCompanionThreadSelectionResponse,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as ServerConfig from "../../config.ts";
import type { DesktopTabKey } from "../../preview/DesktopBrowserChannel.ts";

const Stored = Schema.Struct({
  version: Schema.Literal(1),
  environmentDefault: PreviewRenderHostSelection,
  threads: Schema.Record(Schema.String, PreviewRenderHostSelection),
});
const empty = () => ({
  version: 1 as const,
  environmentDefault: { _tag: "server" as const },
  threads: {},
});
const keyOf = (key: DesktopTabKey) => `${key.threadId}\u0000${key.tabId}`;

export class RenderHostSelectionReadError extends Schema.TaggedError<RenderHostSelectionReadError>()(
  "RenderHostSelectionReadError",
  { reason: Schema.Literals(["invalid", "unreadable"]), cause: Schema.optional(Schema.Defect()) },
) {
  override get message() {
    return `Stored preview render host selection is ${this.reason}; no render host was selected.`;
  }
}

export class RenderHostSelectionWriteError extends Schema.TaggedError<RenderHostSelectionWriteError>()(
  "RenderHostSelectionWriteError",
  { cause: Schema.Defect() },
) {
  override get message() {
    return "Could not save the preview render host selection.";
  }
}

export class RenderHostSelection extends Context.Service<
  RenderHostSelection,
  {
    readonly environmentDefault: Effect.Effect<PreviewRenderHostSelection>;
    readonly thread: (threadId: string) => Effect.Effect<PreviewCompanionThreadSelectionResponse>;
    readonly setDefault: (
      selection: PreviewRenderHostSelection,
    ) => Effect.Effect<void, RenderHostSelectionWriteError>;
    readonly setThread: (
      threadId: string,
      selection: PreviewRenderHostSelection | null,
    ) => Effect.Effect<void, RenderHostSelectionWriteError>;
    readonly bind: (key: DesktopTabKey) => Effect.Effect<PreviewRenderHostSelection>;
    readonly binding: (key: DesktopTabKey) => Effect.Effect<PreviewRenderHostSelection | undefined>;
    readonly release: (key: DesktopTabKey) => Effect.Effect<void>;
  }
>()("t3/jones/previewCompanion/RenderHostSelection") {}

const make = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const config = yield* ServerConfig.ServerConfig;
  const directory = path.join(config.stateDir, "jones");
  const destination = path.join(directory, "preview-render-hosts.json");
  const lock = yield* Semaphore.make(1);
  const stored = yield* fs
    .readFileString(destination)
    .pipe(
      Effect.catch((cause) =>
        cause.reason._tag === "NotFound"
          ? Effect.succeed(undefined)
          : Effect.fail(new RenderHostSelectionReadError({ reason: "unreadable", cause })),
      ),
    );
  const decoded =
    stored === undefined
      ? Option.some(empty())
      : Schema.decodeUnknownOption(Schema.fromJsonString(Stored))(stored);
  if (Option.isNone(decoded)) return yield* new RenderHostSelectionReadError({ reason: "invalid" });
  let state: typeof Stored.Type = decoded.value;
  const bindings = new Map<string, { key: DesktopTabKey; selection: PreviewRenderHostSelection }>();
  const effective = (threadId: string) =>
    Object.hasOwn(state.threads, threadId) ? state.threads[threadId]! : state.environmentDefault;
  const update = (next: () => typeof Stored.Type) =>
    lock.withPermits(1)(
      Effect.scoped(
        Effect.gen(function* () {
          const value = next();
          yield* fs.makeDirectory(directory, { recursive: true });
          const temporary = yield* fs.makeTempFileScoped({
            directory,
            prefix: ".preview-render-hosts-",
          });
          yield* fs.writeFileString(
            temporary,
            yield* Schema.encodeEffect(Schema.fromJsonString(Stored))(value),
          );
          yield* fs.rename(temporary, destination);
          state = value;
        }),
      ).pipe(Effect.mapError((cause) => new RenderHostSelectionWriteError({ cause }))),
    );
  return RenderHostSelection.of({
    environmentDefault: Effect.sync(() => state.environmentDefault),
    thread: (threadId) =>
      Effect.sync(() => ({
        selection: Object.hasOwn(state.threads, threadId) ? state.threads[threadId]! : null,
        effective: effective(threadId),
        tabs: [...bindings.values()]
          .filter((entry) => entry.key.threadId === threadId)
          .map(({ key, selection }) => ({
            tabId: key.tabId,
            hostId: selection._tag === "companion" ? selection.hostId : null,
          })),
      })),
    setDefault: (selection) => update(() => ({ ...state, environmentDefault: selection })),
    setThread: (threadId, selection) =>
      update(() => {
        const threads = { ...state.threads };
        if (selection === null) delete threads[threadId];
        else
          Object.defineProperty(threads, threadId, {
            value: selection,
            enumerable: true,
            configurable: true,
            writable: true,
          });
        return { ...state, threads };
      }),
    bind: (key) =>
      Effect.sync(() => {
        const existing = bindings.get(keyOf(key));
        if (existing) return existing.selection;
        const selection = effective(key.threadId);
        bindings.set(keyOf(key), { key, selection });
        return selection;
      }),
    binding: (key) => Effect.sync(() => bindings.get(keyOf(key))?.selection),
    release: (key) =>
      Effect.sync(() => {
        bindings.delete(keyOf(key));
      }),
  });
});
export const layer = Layer.effect(RenderHostSelection, make);
