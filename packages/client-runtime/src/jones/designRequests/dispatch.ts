import {
  DESIGN_REQUEST_LIMITS,
  type DesignRequestLimits,
} from "@t3tools/contracts/jones/designRequests";
import { CommandId, MessageId, ThreadId, type UploadChatImageAttachment } from "@t3tools/contracts";

import type { StartThreadTurnInput } from "../../operations/commands.ts";
import {
  canonicalJson,
  PACKET_V1_UPDATE_FORMAT,
  type PacketV1,
  type VerifiedImage,
} from "./packetV1.ts";
import type { DesignRequestRoutable } from "./route.ts";

export interface DesignRequestIds {
  readonly commandId: string;
  readonly messageId: string;
  readonly attempt: number;
}

const IDS =
  /^gallery-(req|msg):([a-zA-Z0-9][a-zA-Z0-9_-]{0,79}):([a-f0-9]{32}):a([1-9][0-9]{0,3})$/;

/** `gallery-req|msg:<packetId>:<digestHex first 32>:a<attempt>`; stable across retries of one attempt. */
export function designRequestIds(
  packetId: string,
  digest: string,
  attempt: number,
): DesignRequestIds {
  const hex = /^sha256:([a-f0-9]{64})$/.exec(digest)?.[1];
  if (hex === undefined || !Number.isSafeInteger(attempt) || attempt < 1 || attempt > 9999)
    throw new Error("Invalid design request identity.");
  const suffix = `${packetId}:${hex.slice(0, 32)}:a${attempt}`;
  return { commandId: `gallery-req:${suffix}`, messageId: `gallery-msg:${suffix}`, attempt };
}

/**
 * Parse a command/message pair the gallery asks to reconcile. Only matching gallery-namespaced
 * IDs are readable through the connector, so a gallery cannot probe arbitrary commands.
 */
export function parseDesignRequestIds(
  commandId: string,
  messageId: string,
): { readonly packetId: string; readonly digestPrefix: string; readonly attempt: number } | null {
  const command = IDS.exec(commandId);
  const message = IDS.exec(messageId);
  if (
    command === null ||
    message === null ||
    command[1] !== "req" ||
    message[1] !== "msg" ||
    command[2] !== message[2] ||
    command[3] !== message[3] ||
    command[4] !== message[4]
  )
    return null;
  return { packetId: command[2]!, digestPrefix: command[3]!, attempt: Number(command[4]) };
}

const oneLine = (value: string, max = 120) => {
  const flat = value.replace(/\s+/g, " ").trim();
  return flat.length > max ? `${flat.slice(0, max - 1)}…` : flat;
};
const letter = (ordinal: number) => {
  let value = "";
  for (let number = ordinal; number > 0; number = Math.floor((number - 1) / 26))
    value = String.fromCharCode(65 + ((number - 1) % 26)) + value;
  return value;
};

/**
 * Markdown for the queued message: notices first, the request summary, then the canonical packet
 * (without `attachmentData`) and the update format the agent should reply with. Returns null when
 * it would exceed the configured message limit.
 */
export function buildDesignRequestMessage(
  packet: PacketV1,
  context: { readonly projectKey: string; readonly workstreamName: string },
  limits: DesignRequestLimits = DESIGN_REQUEST_LIMITS,
): string | null {
  const { request, collection } = packet;
  const lines = [
    `# Design request — ${oneLine(collection.title, 300)}`,
    "",
    ...packet.notices.map((notice) => `> ${notice}`),
    "",
    `Project: ${oneLine(context.projectKey, 300)}`,
    `Workstream: ${oneLine(context.workstreamName, 300)}`,
    `Packet: ${packet.packetId}`,
    `Digest: ${packet.digest}`,
    `Saved: ${packet.savedAt}`,
    `Reviewer (self-reported): ${oneLine(request.reviewer, 200)}`,
    `Source generation: ${collection.generation ?? "not indexed"}`,
    "",
    "## Request note",
    "",
    ...request.note.split(/\r\n?|\n/).map((line) => `> ${line}`),
    "",
    "## References",
    "",
  ];
  if (request.references.length === 0) lines.push("- None (collection-level request).");
  request.references.forEach((reference, index) => {
    const link = packet.reopen[index]!;
    const kind = typeof reference.target.kind === "string" ? reference.target.kind : "reference";
    lines.push(
      `- ${link.letter} · ${oneLine(reference.variation, 100)} · ${kind}`,
      `  Reopen: ${link.url}`,
    );
  });
  if (request.attachments.length > 0) {
    lines.push("", "## Attached images", "");
    for (const image of [...request.attachments].sort(
      (left, right) => left.ordinal - right.ordinal,
    ))
      lines.push(
        `- ${letter(image.ordinal)} · ${oneLine(image.name, 200)} · ${image.mediaType} · ${image.bytes} bytes · sha256 ${image.sha256}`,
      );
  }
  lines.push(
    "",
    "## Packet JSON (canonical; recompute the digest before use)",
    "",
    "```json",
    canonicalJson(packet),
    "```",
    "",
    "## Reply",
    "",
    `When you act on this request, reply with one fenced \`${PACKET_V1_UPDATE_FORMAT}\` JSON block naming packetId \`${packet.packetId}\` and digest \`${packet.digest}\`.`,
    "",
  );
  const text = lines.join("\n");
  return text.length > limits.messageChars ? null : text;
}

export function designRequestUploads(
  images: readonly VerifiedImage[],
): UploadChatImageAttachment[] {
  return images.map((image) => ({
    type: "image",
    name: image.name,
    mimeType: image.mediaType,
    sizeBytes: image.bytes,
    dataUrl: image.dataUrl,
  }));
}

/**
 * Queue-mode turn input. No model selection is sent, and runtime and interaction modes are the
 * target thread's current values, so the connector can never raise them.
 */
export function designRequestTurnInput(
  route: DesignRequestRoutable,
  ids: DesignRequestIds,
  text: string,
  images: readonly VerifiedImage[],
): StartThreadTurnInput {
  return {
    threadId: ThreadId.make(route.thread.id),
    commandId: CommandId.make(ids.commandId),
    message: {
      messageId: MessageId.make(ids.messageId),
      role: "user",
      text,
      attachments: designRequestUploads(images),
    },
    runtimeMode: route.thread.runtimeMode,
    interactionMode: route.thread.interactionMode,
    dispatchMode: "queue",
  };
}
