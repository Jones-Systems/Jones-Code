import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as ServerConfig from "../../config.ts";
import * as Selection from "./RenderHostSelection.ts";

it.effect("persists default/overrides and keeps existing tab bindings until explicit release", () =>
  Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const create = () =>
      Layer.build(Selection.layer).pipe(
        Effect.map((context) => Context.get(context, Selection.RenderHostSelection)),
      );
    const first = yield* create();
    const key = { threadId: "thread", tabId: "tab" };
    expect(yield* first.bind(key)).toEqual({ _tag: "server" });
    yield* first.setDefault({ _tag: "companion", hostId: "mini" });
    yield* first.setThread("thread", { _tag: "companion", hostId: "other" });
    expect(yield* first.bind(key)).toEqual({ _tag: "server" });
    yield* first.release(key);
    expect(yield* first.bind(key)).toEqual({ _tag: "companion", hostId: "other" });
    const restarted = yield* create();
    expect((yield* restarted.thread("thread")).effective).toEqual({
      _tag: "companion",
      hostId: "other",
    });
    yield* restarted.setThread("thread", null);
    expect((yield* restarted.thread("thread")).effective).toEqual({
      _tag: "companion",
      hostId: "mini",
    });
    yield* fs.writeFileString(
      path.join(config.stateDir, "jones", "preview-render-hosts.json"),
      "malformed",
    );
    const corrupted = yield* create().pipe(
      Effect.match({
        onSuccess: () => ({ _tag: "UnexpectedSuccess" }),
        onFailure: (error) => error,
      }),
    );
    expect(corrupted).toMatchObject({ _tag: "RenderHostSelectionReadError", reason: "invalid" });
  }).pipe(
    Effect.scoped,
    Effect.provide(
      ServerConfig.layerTest(process.cwd(), { prefix: "preview-companion-selection-" }).pipe(
        Layer.provideMerge(NodeServices.layer),
      ),
    ),
  ),
);

it.effect("fails closed when an existing selection file cannot be read", () =>
  Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = path.join(config.stateDir, "jones");
    const file = path.join(directory, "preview-render-hosts.json");
    yield* fs.makeDirectory(directory, { recursive: true });
    yield* fs.writeFileString(
      file,
      '{"version":1,"environmentDefault":{"_tag":"companion","hostId":"mini"},"threads":{}}',
    );
    yield* fs.chmod(file, 0);
    yield* Effect.addFinalizer(() => fs.chmod(file, 0o600).pipe(Effect.orDie));
    const result = yield* Layer.build(Selection.layer).pipe(
      Effect.match({
        onSuccess: () => ({ _tag: "UnexpectedSuccess" }),
        onFailure: (error) => error,
      }),
    );
    expect(result).toMatchObject({ _tag: "RenderHostSelectionReadError", reason: "unreadable" });
  }).pipe(
    Effect.scoped,
    Effect.provide(
      ServerConfig.layerTest(process.cwd(), { prefix: "preview-companion-unreadable-" }).pipe(
        Layer.provideMerge(NodeServices.layer),
      ),
    ),
  ),
);

it.effect("popup inheritance is authoritative and rejects a conflicting generic prebind", () =>
  Effect.gen(function* () {
    const selection = yield* Selection.RenderHostSelection;
    yield* selection.setDefault({ _tag: "companion", hostId: "later-default" });
    const key = { threadId: "thread", tabId: "popup" };
    expect(yield* selection.bind(key, { _tag: "server" })).toEqual({ _tag: "server" });
    expect(yield* selection.bind(key, { _tag: "server" })).toEqual({ _tag: "server" });
    yield* selection.release(key);
    yield* selection.bind(key);
    expect(yield* Effect.exit(selection.bind(key, { _tag: "server" }))).toMatchObject({
      _tag: "Failure",
    });
    expect(yield* selection.binding(key)).toEqual({ _tag: "companion", hostId: "later-default" });
    expect(
      yield* Effect.exit(selection.bind(key, { _tag: "companion", hostId: "different" })),
    ).toMatchObject({ _tag: "Failure" });
  }).pipe(
    Effect.scoped,
    Effect.provide(
      Selection.layer.pipe(
        Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "popup-selection-" })),
        Layer.provide(NodeServices.layer),
      ),
    ),
  ),
);
