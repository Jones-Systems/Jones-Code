import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import { resolveMessageModelSelection } from "./threadSelfConfiguration.ts";
import { ProviderInstanceId } from "../providerInstance.ts";
import { OrchestrationV2Command } from "../orchestrationV2.ts";

describe("composer selection intent compatibility", () => {
  const message = {
    type: "message.dispatch",
    commandId: "ordinary-follow-up",
    threadId: "thread",
    messageId: "follow-up",
    text: "continue",
    attachments: [],
    createdBy: "user",
    creationSource: "web",
    modelSelection: { instanceId: "compatible-b", model: "gpt-6-astra" },
    deliveryIntent: "auto",
    dispatchMode: { type: "start_immediately" },
  };
  it.each([false, true, undefined])(
    "round-trips explicit intent %s without changing old messages",
    (explicit) => {
      const input = {
        ...message,
        ...(explicit === undefined ? {} : { modelSelectionExplicit: explicit }),
      };
      const decoded = Schema.decodeUnknownSync(OrchestrationV2Command)(input);
      expect(Schema.encodeSync(OrchestrationV2Command)(decoded)).toEqual(input);
    },
  );
  it("rejects malformed selection intent", () => {
    expect(() =>
      Schema.decodeUnknownSync(OrchestrationV2Command)({
        ...message,
        modelSelectionExplicit: "false",
      }),
    ).toThrow();
  });
});

describe("inherited selection resolution", () => {
  const saved = { instanceId: ProviderInstanceId.make("compatible-b"), model: "saved" };
  const snapshot = { instanceId: ProviderInstanceId.make("old-a"), model: "snapshot" };
  it.each([true, undefined])("honors explicit and legacy commands %s", (explicit) => {
    expect(
      resolveMessageModelSelection(
        { modelSelection: snapshot, modelSelectionExplicit: explicit },
        saved,
      ),
    ).toEqual(snapshot);
  });
  it("uses saved defaults for an inherited snapshot and omitted selection", () => {
    expect(
      resolveMessageModelSelection(
        { modelSelection: snapshot, modelSelectionExplicit: false },
        saved,
      ),
    ).toEqual(saved);
    expect(resolveMessageModelSelection({}, saved)).toEqual(saved);
  });
});
