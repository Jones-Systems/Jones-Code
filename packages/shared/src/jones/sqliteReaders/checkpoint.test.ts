import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { makeCheckpointReaderAdmission } from "./admission.ts";

it.effect("rejects stale reopening evidence and permits verified same-generation fallback", () =>
  Effect.gen(function* () {
    const bridge = makeCheckpointReaderAdmission();
    yield* bridge.update({ generation: 8, state: "truncate" });
    assert.isFalse(bridge.isOpen());
    yield* bridge.update({ generation: 7, state: "open" });
    assert.isFalse(bridge.isOpen());
    yield* bridge.update({ generation: 9, state: "uncertain" });
    assert.isFalse(bridge.isOpen());
    yield* bridge.update({ generation: 9, state: "restored" });
    assert.isTrue(bridge.isOpen());
    yield* bridge.update({ generation: 8, state: "pressure" });
    assert.isTrue(bridge.isOpen());
  }),
);
