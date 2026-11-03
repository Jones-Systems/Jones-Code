import { describe, expect, it } from "@effect/vitest";
import { CommandId, MessageId, ProjectId, ThreadId } from "@t3tools/contracts";
import {
  readDispatchDeliveryEvidence,
  readLaunchDeliveryEvidence,
  threadTurnDeliveryStatus,
} from "./deliveryEvidence.ts";

const request = {
  commandId: CommandId.make("launch"),
  projectId: ProjectId.make("project"),
  threadId: ThreadId.make("thread"),
  initialMessage: {
    messageId: MessageId.make("message"),
    text: "Work",
    attachments: [],
  },
};
const acceptedCreation = {
  kind: "launch",
  commandId: "launch",
  projectId: "project",
  creation: { status: "accepted", threadId: "thread" },
  initialMessage: {
    status: "unknown",
    commandId: "launch:initial-message",
    messageId: "message",
  },
};

describe("request-bound delivery facts", () => {
  it("keeps committed creation identity when message delivery is rejected or unknown", () => {
    for (const status of ["accepted", "rejected", "unknown"] as const) {
      const evidence = readLaunchDeliveryEvidence(
        {
          deliveryEvidence: {
            ...acceptedCreation,
            initialMessage: { ...acceptedCreation.initialMessage, status },
          },
        },
        request,
      );
      expect(evidence.creation).toEqual({ status: "accepted", threadId: "thread" });
      expect(threadTurnDeliveryStatus(evidence)).toBe(status);
    }
    const rejected = readLaunchDeliveryEvidence(
      { deliveryEvidence: { ...acceptedCreation, creation: { status: "rejected" } } },
      request,
    );
    expect(threadTurnDeliveryStatus(rejected)).toBe("rejected");
    expect(rejected.creation).not.toHaveProperty("threadId");
    const uncertain = readLaunchDeliveryEvidence(
      {
        deliveryEvidence: {
          ...acceptedCreation,
          creation: { status: "unknown" },
          initialMessage: { ...acceptedCreation.initialMessage, status: "rejected" },
        },
      },
      request,
    );
    expect(threadTurnDeliveryStatus(uncertain)).toBe("unknown");
  });

  it("never infers rejection from V2 tags, error text, absence or unreadable evidence", () => {
    const unreadable = Object.defineProperty({}, "deliveryEvidence", {
      get() {
        throw new Error("Unreadable optional evidence");
      },
    });
    for (const error of [
      undefined,
      { _tag: "OrchestrationV2ThreadLaunchError", message: "Provider rejected" },
      { _tag: "OrchestrationV2DispatchCommandError" },
      { deliveryEvidence: null },
      unreadable,
    ]) {
      const evidence = readLaunchDeliveryEvidence(error, request);
      expect(threadTurnDeliveryStatus(evidence)).toBe("unknown");
      expect(evidence.creation).toEqual({ status: "unknown" });
      expect(evidence.initialMessage).toEqual({
        status: "unknown",
        commandId: "launch:initial-message",
        messageId: "message",
      });
    }
  });

  it("rejects foreign identities while keeping a separately proven shell", () => {
    for (const patch of [
      { commandId: "other" },
      { projectId: "other" },
      { creation: { status: "accepted", threadId: "other" } },
    ]) {
      expect(
        threadTurnDeliveryStatus(readLaunchDeliveryEvidence(
          { deliveryEvidence: { ...acceptedCreation, ...patch } },
          request,
        )),
      ).toBe("unknown");
    }
    const foreignMessage = readLaunchDeliveryEvidence(
      {
        deliveryEvidence: {
          ...acceptedCreation,
          initialMessage: {
            ...acceptedCreation.initialMessage,
            status: "accepted",
            messageId: "other",
          },
        },
      },
      request,
    );
    expect(foreignMessage.creation).toEqual({ status: "accepted", threadId: "thread" });
    expect(threadTurnDeliveryStatus(foreignMessage)).toBe("unknown");
  });

  it("distinguishes an omitted prompt from a requested message still lacking acceptance", () => {
    const { initialMessage: _message, ...withoutMessage } = request;
    const omitted = readLaunchDeliveryEvidence(
      { deliveryEvidence: { ...acceptedCreation, initialMessage: { status: "not-requested" } } },
      withoutMessage,
    );
    expect(threadTurnDeliveryStatus(omitted)).toBe("accepted");
    const requested = readLaunchDeliveryEvidence(
      { deliveryEvidence: { ...acceptedCreation, initialMessage: { status: "not-requested" } } },
      request,
    );
    expect(threadTurnDeliveryStatus(requested)).toBe("unknown");
    expect(readLaunchDeliveryEvidence(undefined, withoutMessage).initialMessage).toEqual({
      status: "not-requested",
    });
  });

  it("requires exact dispatch command type and target for accepted and rejected facts", () => {
    const dispatch = {
      commandId: CommandId.make("send"),
      commandType: "message.dispatch",
      threadId: request.threadId,
    };
    for (const status of ["accepted", "rejected"] as const) {
      const deliveryEvidence = { kind: "dispatch", ...dispatch, status };
      expect(readDispatchDeliveryEvidence({ deliveryEvidence }, dispatch).status).toBe(status);
      for (const patch of [
        { commandId: "other" },
        { commandType: "thread.create" },
        { threadId: "other" },
      ]) {
        expect(
          readDispatchDeliveryEvidence(
            { deliveryEvidence: { ...deliveryEvidence, ...patch } },
            dispatch,
          ).status,
        ).toBe("unknown");
      }
    }
    expect(
      readDispatchDeliveryEvidence(
        { _tag: "OrchestrationV2DispatchCommandError" },
        dispatch,
      ).status,
    ).toBe("unknown");
  });
});
