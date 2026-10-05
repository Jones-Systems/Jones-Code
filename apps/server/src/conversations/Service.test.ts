import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as ServerConfig from "../config.ts";
import * as ConversationLibrary from "./Service.ts";

const TestLayer = ConversationLibrary.layer.pipe(
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-library-service-" })),
  Layer.provideMerge(NodeServices.layer),
);

it.effect(
  "direct library service rejects writes before creating storage and remains readable",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const library = yield* ConversationLibrary.ConversationLibrary;
        const config = yield* ServerConfig.ServerConfig;
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const failure = yield* library
          .execute(
            {
              kind: "createAccount",
              label: "Synthetic",
              workspace: "Task fixture",
            },
            false,
          )
          .pipe(Effect.flip);
        expect(failure.code).toBe("forbidden");
        expect(yield* fs.exists(path.join(config.stateDir, "conversation-library"))).toBe(false);
        expect(yield* library.execute({ kind: "accounts" }, false)).toEqual({
          kind: "accounts",
          accounts: [],
        });
        expect(yield* fs.exists(path.join(config.stateDir, "conversation-library"))).toBe(false);
      }).pipe(Effect.provide(TestLayer)),
    ),
);
