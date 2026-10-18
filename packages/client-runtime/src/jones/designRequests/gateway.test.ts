import { describe, expect, it } from "vite-plus/test";

import type { StartThreadTurnInput } from "../../operations/commands.ts";
import { buildDesignRequestMessage, designRequestIds, parseDesignRequestIds } from "./dispatch.ts";
import golden from "./fixtures/golden-packets.json" with { type: "json" };
import { createDesignRequestGateway, type DesignRequestGatewayPorts } from "./gateway.ts";
import { parsePacketV1Text } from "./packetV1.ts";
import { resolveDesignRequestRoute, type DesignRequestResolvedRoute } from "./route.ts";
import { binding, placement, placements, routeInput } from "./testFixtures.ts";

const withImages = parsePacketV1Text(golden.packets.withImages);
const noImages = parsePacketV1Text(golden.packets.noImages);
const routable = resolveDesignRequestRoute(routeInput());
const token = routable.state === "routable" ? routable.routeToken : "";

/**
 * Synthetic stand-in for the server's durable command receipts: one receipt per command ID, bound
 * to the thread it first ran on, replayed for the same thread and refused for another.
 */
function syntheticServer() {
  const receipts = new Map<string, { threadId: string; status: "accepted" | "rejected" }>();
  const dispatched: StartThreadTurnInput[] = [];
  const control = {
    rejectNext: false,
    loseResponse: false,
    failUnrecorded: false,
    readbackDown: false,
    turnState: null as null | "pending" | "running",
  };
  const ports = (
    route: () => DesignRequestResolvedRoute = () => routable,
  ): DesignRequestGatewayPorts => ({
    jonesOrigin: "http://jones.example.ts.net",
    resolveRoute: async () => route(),
    observe: async ({ threadId, commandId }) => {
      if (control.readbackDown) throw new Error("offline");
      const receipt = receipts.get(commandId);
      const commandStatus =
        receipt === undefined || receipt.threadId !== threadId ? "not_found" : receipt.status;
      return {
        commandStatus,
        turn:
          commandStatus === "accepted" && control.turnState !== null
            ? {
                turnId: null,
                state: control.turnState,
                requestedAt: "2026-10-09T12:00:00.000Z",
                startedAt: null,
                completedAt: null,
                assistantMessageId: null,
              }
            : null,
      } as never;
    },
    dispatch: async (input) => {
      const commandId = input.commandId!;
      if (control.failUnrecorded) {
        control.failUnrecorded = false;
        throw new Error("network down before the server recorded anything");
      }
      const existing = receipts.get(commandId);
      if (existing && existing.threadId !== input.threadId)
        return {
          kind: "failed",
          message: `Command ${commandId} was already handled for thread ${existing.threadId} and cannot be replayed for ${input.threadId}.`,
        };
      if (existing?.status === "rejected")
        return { kind: "failed", message: `Command ${commandId} was previously rejected: no.` };
      if (!existing) {
        dispatched.push(input);
        receipts.set(commandId, {
          threadId: input.threadId,
          status: control.rejectNext ? "rejected" : "accepted",
        });
      }
      const rejected = control.rejectNext;
      control.rejectNext = false;
      if (control.loseResponse) {
        control.loseResponse = false;
        throw new Error("socket closed");
      }
      return rejected ? { kind: "failed", message: "rejected" } : { kind: "dispatched" };
    },
    isReadableThread: (threadId) => threadId === "thread-1" || threadId === "thread-2",
  });
  return { receipts, dispatched, control, ports };
}

const submission = (
  packet = withImages,
  expectedRouteToken: string | null = token,
  attempt = 1,
) => ({
  binding,
  projectKey: binding.projectKey,
  packet: packet.packet,
  attachmentData: packet.attachmentData,
  expectedRouteToken,
  attempt,
});
const reconcileInput = (
  packet = noImages,
  attempt = 1,
  workstreamId: string | null = "ws-design",
  threadId = "thread-1",
) => ({
  threadId,
  ...designRequestIds(packet.packet.packetId, packet.packet.digest, attempt),
  packetId: packet.packet.packetId,
  digest: packet.packet.digest,
  workstreamId,
});

describe("design request gateway (synthetic transport)", () => {
  it("queues once with deterministic IDs, the thread's modes and verified images", async () => {
    const server = syntheticServer();
    const gateway = createDesignRequestGateway(server.ports());
    const outcome = await gateway.submit(submission());
    const ids = designRequestIds(withImages.packet.packetId, withImages.packet.digest, 1);
    expect(outcome).toEqual({
      status: "accepted",
      packetId: withImages.packet.packetId,
      digest: withImages.packet.digest,
      delivery: "queued",
      destination: {
        jonesOrigin: "http://jones.example.ts.net",
        workstreamId: "ws-design",
        threadId: "thread-1",
        commandId: ids.commandId,
        messageId: ids.messageId,
      },
    });
    expect(server.dispatched).toHaveLength(1);
    const input = server.dispatched[0]!;
    expect(input.dispatchMode).toBe("queue");
    expect(input.modelSelection).toBeUndefined();
    expect(input.runtimeMode).toBe("approval-required");
    expect(input.interactionMode).toBe("default");
    expect(
      input.message.attachments.map((item) => ("sizeBytes" in item ? item.sizeBytes : 0)),
    ).toEqual([300, 77]);
    expect(input.message.text).not.toContain("attachmentData");
    expect(input.message.text).toContain(withImages.packet.digest);
  });

  it("never creates a second message for double clicks, retries or replays", async () => {
    const server = syntheticServer();
    const gateway = createDesignRequestGateway(server.ports());
    const [first, second] = await Promise.all([
      gateway.submit(submission()),
      gateway.submit(submission()),
    ]);
    expect(second).toEqual(first);
    server.control.turnState = "running";
    const replay = await gateway.submit(submission());
    expect(replay.status).toBe("accepted");
    expect(replay.delivery).toBe("started");
    // A fresh page (new gateway) replays from the server receipt as well.
    expect((await createDesignRequestGateway(server.ports()).submit(submission())).status).toBe(
      "accepted",
    );
    expect(server.dispatched).toHaveLength(1);
  });

  it("holds route changes and invalid packets without dispatching", async () => {
    const server = syntheticServer();
    const gateway = createDesignRequestGateway(server.ports());
    expect((await gateway.submit(submission(withImages, "f".repeat(32)))).reason).toBe(
      "route-changed",
    );
    const tampered = { ...submission(), attachmentData: withImages.attachmentData!.slice(0, 1) };
    expect((await gateway.submit(tampered)).reason).toBe("attachment-mismatch");
    const edited = {
      ...submission(),
      packet: { ...withImages.packet, request: { ...withImages.packet.request, note: "Changed" } },
    };
    expect((await gateway.submit(edited)).reason).toBe("digest-mismatch");
    expect((await gateway.submit({ ...submission(), projectKey: "other" })).reason).toBe(
      "no-binding",
    );
    for (const attempt of [0, 21, 1.5])
      expect(await gateway.submit(submission(withImages, token, attempt))).toMatchObject({
        status: "held",
        reason: "packet-invalid",
      });
    const held = createDesignRequestGateway(
      server.ports(() => resolveDesignRequestRoute(routeInput({ placements: placements([]) }))),
    );
    expect(await held.submit(submission())).toMatchObject({ status: "held", reason: "no-primary" });
    expect(server.dispatched).toHaveLength(0);
  });

  it("reports a lost response as unknown and reconciles before any resend", async () => {
    const server = syntheticServer();
    const gateway = createDesignRequestGateway(server.ports());
    server.control.loseResponse = true;
    server.control.readbackDown = true;
    // Readback is down before dispatch: the effect is unknown and nothing is sent.
    expect(await gateway.submit(submission(noImages))).toMatchObject({
      status: "unknown",
      reason: "readback-unavailable",
      digest: noImages.packet.digest,
    });
    expect(server.dispatched).toHaveLength(0);
    server.control.readbackDown = false;
    const lost = await gateway.submit(submission(noImages));
    // The command landed even though the response was lost; readback proves it.
    expect(lost.status).toBe("accepted");
    // A fresh Jones page never saw the submit, yet echoes the full bound digest and destination.
    const fresh = createDesignRequestGateway(server.ports());
    const ids = designRequestIds(noImages.packet.packetId, noImages.packet.digest, 1);
    expect(await fresh.reconcile(reconcileInput())).toEqual({
      status: "accepted",
      packetId: noImages.packet.packetId,
      digest: noImages.packet.digest,
      delivery: "queued",
      destination: {
        jonesOrigin: "http://jones.example.ts.net",
        workstreamId: "ws-design",
        threadId: "thread-1",
        commandId: ids.commandId,
        messageId: ids.messageId,
      },
    });
    // Authoritative not-found is held without destination or delivery.
    expect(await fresh.reconcile(reconcileInput(noImages, 2))).toEqual({
      status: "held",
      packetId: noImages.packet.packetId,
      digest: noImages.packet.digest,
      reason: "not-found",
    });
    // Accepted readback without the paired workstream cannot name the destination: unknown.
    expect(await fresh.reconcile(reconcileInput(noImages, 1, null))).toMatchObject({
      status: "unknown",
      reason: "no-binding",
    });
    server.control.readbackDown = true;
    expect(await fresh.reconcile(reconcileInput())).toEqual({
      status: "unknown",
      packetId: noImages.packet.packetId,
      digest: noImages.packet.digest,
      reason: "readback-unavailable",
    });
    expect(server.dispatched).toHaveLength(1);
  });

  it("uses the gallery's exact attempt and never advances it", async () => {
    const server = syntheticServer();
    const gateway = createDesignRequestGateway(server.ports());
    server.control.rejectNext = true;
    expect(await gateway.submit(submission(noImages))).toMatchObject({
      status: "rejected",
      digest: noImages.packet.digest,
    });
    // Resending the rejected attempt observes it first and dispatches nothing.
    expect(await gateway.submit(submission(noImages))).toMatchObject({ status: "rejected" });
    expect(server.dispatched).toHaveLength(1);
    // Only an explicit gallery Retry with attempt 2 sends, with the attempt-2 IDs.
    const retried = await gateway.submit(submission(noImages, token, 2));
    expect(retried.status).toBe("accepted");
    expect(
      parseDesignRequestIds(retried.destination!.commandId, retried.destination!.messageId)
        ?.attempt,
    ).toBe(2);
    expect(server.dispatched.map((input) => input.commandId)).toEqual([
      designRequestIds(noImages.packet.packetId, noImages.packet.digest, 1).commandId,
      designRequestIds(noImages.packet.packetId, noImages.packet.digest, 2).commandId,
    ]);
  });

  it("leaves a failed dispatch with no record unknown, then resends the same IDs", async () => {
    const server = syntheticServer();
    const gateway = createDesignRequestGateway(server.ports());
    server.control.failUnrecorded = true;
    const failed = await gateway.submit(submission(noImages));
    expect(failed).toMatchObject({ status: "unknown", reason: "dispatch-failed" });
    expect(server.dispatched).toHaveLength(0);
    expect(await gateway.reconcile(reconcileInput())).toMatchObject({
      status: "held",
      reason: "not-found",
    });
    const resent = await gateway.submit(submission(noImages));
    expect(resent.status).toBe("accepted");
    expect(resent.destination?.commandId).toBe(failed.destination?.commandId);
    expect(server.dispatched).toHaveLength(1);
  });

  it("holds IDs already used on another thread", async () => {
    const server = syntheticServer();
    await createDesignRequestGateway(server.ports()).submit(submission(noImages));
    const otherThread = resolveDesignRequestRoute(
      routeInput({ placements: placements([placement({ native_thread_id: "thread-2" })]) }),
    );
    const moved = createDesignRequestGateway(server.ports(() => otherThread));
    const outcome = await moved.submit(
      submission(noImages, otherThread.state === "routable" ? otherThread.routeToken : null),
    );
    expect(outcome).toMatchObject({ status: "held", reason: "already-sent-elsewhere" });
    expect(server.dispatched).toHaveLength(1);
  });

  it("reconciles only IDs derived from the bound packet on readable threads", async () => {
    const server = syntheticServer();
    const gateway = createDesignRequestGateway(server.ports());
    const input = reconcileInput();
    expect(await gateway.reconcile({ ...input, commandId: "user-command" })).toMatchObject({
      status: "held",
      reason: "packet-invalid",
    });
    expect(
      await gateway.reconcile({ ...input, packetId: withImages.packet.packetId }),
    ).toMatchObject({ status: "held", reason: "packet-invalid" });
    expect(await gateway.reconcile({ ...input, digest: withImages.packet.digest })).toMatchObject({
      status: "held",
      reason: "packet-invalid",
    });
    expect(await gateway.reconcile(reconcileInput(noImages, 21))).toMatchObject({
      status: "held",
      reason: "packet-invalid",
    });
    expect(
      await gateway.reconcile(reconcileInput(noImages, 1, "ws-design", "thread-9")),
    ).toMatchObject({ status: "held", reason: "thread-not-local" });
  });
});

describe("design request message", () => {
  it("holds a message over the configured limit", () => {
    const context = { projectKey: "form2", workstreamName: "Form 2 design" };
    expect(buildDesignRequestMessage(withImages.packet, context)).not.toBeNull();
    expect(
      buildDesignRequestMessage(withImages.packet, context, {
        images: 3,
        imageBytes: 1,
        totalBytes: 1,
        messageChars: 100,
      }),
    ).toBeNull();
  });

  it("builds IDs from the packet ID, digest prefix and attempt", () => {
    const ids = designRequestIds("p-1", `sha256:${"ab".repeat(32)}`, 3);
    expect(ids.commandId).toBe(`gallery-req:p-1:${"ab".repeat(16)}:a3`);
    expect(ids.messageId).toBe(`gallery-msg:p-1:${"ab".repeat(16)}:a3`);
    expect(parseDesignRequestIds(ids.commandId, ids.messageId)).toEqual({
      packetId: "p-1",
      digestPrefix: "ab".repeat(16),
      attempt: 3,
    });
    expect(parseDesignRequestIds(ids.commandId, ids.messageId.replace("a3", "a4"))).toBeNull();
    expect(() => designRequestIds("p-1", "sha256:zz", 1)).toThrow();
  });
});
