import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ServerConfig from "../../config.ts";
import * as Manager from "../../preview/Manager.ts";
import * as Selection from "./RenderHostSelection.ts";
import { resolveOpenBinding } from "./openBinding.ts";

it.effect("leaves legacy compositions without render-host selection compatible", () =>
  Effect.gen(function* () {
    assert.isUndefined(yield* resolveOpenBinding());
  }),
);

it.effect("skips non-server opens and preserves saved thread choices at admission", () =>
  Effect.gen(function* () {
    const manager = yield* Manager.PreviewManager;
    const selection = yield* Selection.RenderHostSelection;
    const beforePublish = yield* resolveOpenBinding();
    if (beforePublish === undefined) return yield* Effect.die("Missing open binding");
    const threadId = ThreadId.make("thread");
    yield* selection.setThread(threadId, { _tag: "companion", hostId: "chosen" });
    for (const runtime of [undefined, "desktop"] as const) {
      const snapshot = yield* manager.open({ threadId, runtime, beforePublish });
      assert.isUndefined(yield* selection.binding(snapshot));
    }
    const snapshot = yield* manager.open({ threadId, runtime: "server", beforePublish });
    assert.deepEqual(yield* selection.binding(snapshot), { _tag: "companion", hostId: "chosen" });
    yield* selection.setThread(threadId, { _tag: "server" });
    beforePublish(snapshot);
    assert.deepEqual(yield* selection.binding(snapshot), { _tag: "companion", hostId: "chosen" });
  }).pipe(
    Effect.scoped,
    Effect.provide(
      Layer.merge(Manager.layer, Selection.layer).pipe(
        Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "preview-open-binding-" })),
        Layer.provide(NodeServices.layer),
        Layer.provideMerge(NodeCrypto.layer),
      ),
    ),
  ),
);
