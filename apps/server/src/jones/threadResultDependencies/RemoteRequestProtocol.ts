import type { RemoteAdmissionEnvelope } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { CommandId, MessageId, ThreadResultRequestDescriptor } from "@t3tools/contracts";

/** The send and its admission observer use the original immutable MCP namespace and key. */
export function threadSendIdentity(requestNamespace: string, requestKey: string) {
  const suffix = [requestNamespace, "thread-send", requestKey].map(encodeURIComponent).join(":");
  return { commandId: CommandId.make(`command:mcp:${suffix}`), messageId: MessageId.make(`message:mcp:${suffix}`) };
}

/** Admission envelopes are assembled from the authenticated invocation, never request IDs alone. */
export function admissionOperation(envelope: RemoteAdmissionEnvelope): { origin: string; operation: string } | undefined {
  if (envelope.operation !== "t3_thread_send" || !envelope.identity.linkId) return undefined;
  if (envelope.payload === null || typeof envelope.payload !== "object") return undefined;
  const value = envelope.payload as { params?: { resultRequest?: unknown }; invocation?: { client?: { sessionId?: string; linked?: boolean; remoteOrigin?: { environmentId?: string; linkSessionId?: string } } } };
  const client = value.invocation?.client;
  const descriptor = value.params?.resultRequest;
  if (!client?.linked || client.sessionId !== client.remoteOrigin?.linkSessionId || typeof client.remoteOrigin?.environmentId !== "string" || !Schema.is(ThreadResultRequestDescriptor)(descriptor)) return undefined;
  return { origin: client.remoteOrigin.environmentId, operation: descriptor.operationId };
}
