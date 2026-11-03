import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";

import {
  OrchestrationV2DispatchCommandError,
  OrchestrationV2ThreadLaunchError,
} from "../orchestrationV2.ts";
import { LaunchDeliveryEvidence } from "./deliveryEvidence.ts";

const dispatchCodec = Schema.toCodecJson(OrchestrationV2DispatchCommandError);
const launchCodec = Schema.toCodecJson(OrchestrationV2ThreadLaunchError);
const dispatchError = {
  _tag: "OrchestrationV2DispatchCommandError",
  commandId: "send",
  commandType: "message.dispatch",
  message: "Failed to dispatch command",
};
const launchError = {
  _tag: "OrchestrationV2ThreadLaunchError",
  commandId: "launch",
  projectId: "project",
  message: "Failed to launch thread",
};
const dispatchEvidence = {
  kind: "dispatch",
  commandId: "send",
  commandType: "message.dispatch",
  threadId: "thread",
  status: "accepted",
};
const launchEvidence = {
  kind: "launch",
  commandId: "launch",
  projectId: "project",
  creation: { status: "accepted", threadId: "thread" },
  initialMessage: {
    status: "rejected",
    commandId: "launch:initial-message",
    messageId: "message",
  },
};

describe("optional command delivery evidence", () => {
  it("round-trips old errors and new facts through the actual RPC error codecs", () => {
    const decodeDispatch = Schema.decodeUnknownSync(dispatchCodec);
    const decodeLaunch = Schema.decodeUnknownSync(launchCodec);
    expect(Schema.encodeSync(dispatchCodec)(decodeDispatch(dispatchError))).toEqual(dispatchError);
    expect(Schema.encodeSync(launchCodec)(decodeLaunch(launchError))).toEqual(launchError);
    const dispatch = { ...dispatchError, deliveryEvidence: dispatchEvidence };
    const launch = { ...launchError, deliveryEvidence: launchEvidence };
    expect(Schema.encodeSync(dispatchCodec)(decodeDispatch(dispatch))).toEqual(dispatch);
    expect(Schema.encodeSync(launchCodec)(decodeLaunch(launch))).toEqual(launch);
    expect(decodeLaunch(launch).deliveryEvidence).toEqual(launchEvidence);
  });

  it("discards malformed or future optional evidence while retaining the original error", () => {
    for (const deliveryEvidence of [null, "rejected", { status: "rejected" }, { kind: "future" }]) {
      const dispatch = Schema.decodeSync(dispatchCodec)({
        ...dispatchError,
        deliveryEvidence,
      });
      const launch = Schema.decodeSync(launchCodec)({ ...launchError, deliveryEvidence });
      expect(dispatch.deliveryEvidence).toBeUndefined();
      expect(dispatch.message).toBe(dispatchError.message);
      expect(launch.deliveryEvidence).toBeUndefined();
      expect(launch.message).toBe(launchError.message);
    }
  });

  it("requires a proven target for accepted creation and keeps message admission separate", () => {
    const decode = Schema.decodeUnknownSync(LaunchDeliveryEvidence);
    for (const status of ["accepted", "rejected", "unknown"]) {
      expect(
        decode({
          ...launchEvidence,
          initialMessage: { ...launchEvidence.initialMessage, status },
        }).initialMessage.status,
      ).toBe(status);
    }
    const omitted = decode({
      ...launchEvidence,
      initialMessage: { status: "not-requested" },
    });
    expect(omitted.initialMessage.status).toBe("not-requested");
    for (const evidence of [
      { ...launchEvidence, creation: { status: "accepted" } },
      { ...launchEvidence, creation: { status: "rejected", threadId: "thread" } },
      {
        ...launchEvidence,
        creation: { status: "unknown" },
        initialMessage: { ...launchEvidence.initialMessage, status: "accepted" },
      },
      {
        ...launchEvidence,
        initialMessage: { ...launchEvidence.initialMessage, commandId: "another" },
      },
      {
        ...launchEvidence,
        initialMessage: { status: "accepted", commandId: "launch:initial-message" },
      },
      { ...launchEvidence, initialMessage: { status: "not-requested", messageId: "message" } },
    ]) {
      expect(() => decode(evidence)).toThrow();
    }
  });
});
