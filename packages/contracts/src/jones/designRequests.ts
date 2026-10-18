import * as Schema from "effect/Schema";

/**
 * `jones-design-request/1`: the window.open/postMessage handshake between a design gallery
 * (opener) and the authenticated Jones web client. The gallery never holds a Jones credential;
 * Jones verifies the packet, resolves the destination and queues the message itself.
 */
export const DESIGN_REQUEST_PROTOCOL = "jones-design-request/1" as const;
const DESIGN_REQUEST_GALLERY_VERSION = 1;

export interface DesignRequestLimits {
  readonly images: number;
  readonly imageBytes: number;
  readonly totalBytes: number;
  readonly messageChars: number;
}

export const DESIGN_REQUEST_LIMITS: DesignRequestLimits = Object.freeze({
  images: 3,
  imageBytes: 2 * 1024 * 1024,
  totalBytes: 8 * 1024 * 1024,
  messageChars: 100_000,
});

/** The gallery numbers attempts explicitly; Jones derives IDs from exactly that attempt. */
export const DESIGN_REQUEST_MAX_ATTEMPTS = 20;

/** Every reason a request or route is held. Held states are always shown, never retried silently. */
export const DESIGN_REQUEST_HELD_REASONS = [
  "no-binding",
  "origin-not-allowed",
  "not-authenticated",
  "runtime-unsupported",
  "registry-stale",
  "workstream-not-found",
  "no-primary",
  "multiple-primary",
  "primary-not-attested",
  "placement-expired",
  "thread-not-local",
  "thread-archived",
  "thread-config-unknown",
  "route-changed",
  "packet-invalid",
  "digest-mismatch",
  "attachment-mismatch",
  "too-large",
  "already-sent-elsewhere",
  "readback-unavailable",
] as const;
export const DesignRequestHeldReason = Schema.Literals(DESIGN_REQUEST_HELD_REASONS);
export type DesignRequestHeldReason = typeof DesignRequestHeldReason.Type;

const closed = <Fields extends Schema.Struct.Fields>(fields: Fields) => {
  const schema = Schema.Struct(fields);
  return Schema.flip(
    Schema.flip(schema).check(
      Schema.makeFilter((value) =>
        Reflect.ownKeys(value).every((key) => Object.hasOwn(fields, key)),
      ),
    ),
  );
};

const Nonce = Schema.String.check(Schema.isPattern(/^[a-f0-9]{32,128}$/));
const Protocol = Schema.Literal(DESIGN_REQUEST_PROTOCOL);
const Text = (max: number) => Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(max));
const ProjectKey = Text(2000);
const Id = Text(512);
const RouteToken = Schema.String.check(Schema.isPattern(/^[a-f0-9]{32}$/));
const Digest = Schema.String.check(Schema.isPattern(/^sha256:[a-f0-9]{64}$/));
const PacketId = Schema.String.check(Schema.isPattern(/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}$/));
const Attempt = Schema.Int.check(
  Schema.isBetween({ minimum: 1, maximum: DESIGN_REQUEST_MAX_ATTEMPTS }),
);

export const DesignRequestReady = closed({
  type: Schema.Literal("jones.design-request.ready"),
  protocol: Protocol,
  nonceJ: Nonce,
});
export type DesignRequestReady = typeof DesignRequestReady.Type;

export const DesignRequestHello = closed({
  type: Schema.Literal("jones.design-request.hello"),
  protocol: Protocol,
  nonceJ: Nonce,
  nonceG: Nonce,
  projectKey: ProjectKey,
  galleryVersion: Schema.Literal(DESIGN_REQUEST_GALLERY_VERSION),
});
export type DesignRequestHello = typeof DesignRequestHello.Type;

export const DesignRequestRoute = closed({
  type: Schema.Literal("jones.design-request.route"),
  protocol: Protocol,
  nonceJ: Nonce,
  nonceG: Nonce,
  state: Schema.Literals(["routable", "held"]),
  routeToken: Schema.optional(RouteToken),
  workstream: Schema.optional(closed({ id: Id, name: Text(500) })),
  thread: Schema.optional(closed({ id: Id, title: Schema.String.check(Schema.isMaxLength(1000)) })),
  reason: Schema.optional(DesignRequestHeldReason),
});
export type DesignRequestRoute = typeof DesignRequestRoute.Type;

/**
 * `attachmentData` keeps the downloaded packet's item shape: one `{id, dataUrl}` per declared image.
 * A submit always carries the array (empty without images); a downloaded file may omit it.
 */
export const DesignRequestAttachmentData = Schema.Array(
  closed({ id: Text(80), dataUrl: Schema.String.check(Schema.isMaxLength(3_000_000)) }),
).check(Schema.isMaxLength(DESIGN_REQUEST_LIMITS.images));
export type DesignRequestAttachmentData = typeof DesignRequestAttachmentData.Type;

export const DesignRequestSubmit = closed({
  type: Schema.Literal("jones.design-request.submit"),
  protocol: Protocol,
  nonceJ: Nonce,
  nonceG: Nonce,
  projectKey: ProjectKey,
  requestNonce: Nonce,
  // Verified by the packet v1 verifier, which rejects unknown members and recomputes the digest.
  packet: Schema.Unknown,
  attachmentData: DesignRequestAttachmentData,
  expectedRouteToken: RouteToken,
  attempt: Attempt,
});
export type DesignRequestSubmit = typeof DesignRequestSubmit.Type;

export const DesignRequestDestination = closed({
  jonesOrigin: Text(2000),
  workstreamId: Id,
  threadId: Id,
  commandId: Id,
  messageId: Id,
});
export type DesignRequestDestination = typeof DesignRequestDestination.Type;

export const DesignRequestReceiptStatus = Schema.Literals([
  "accepted",
  "held",
  "rejected",
  "unknown",
]);
export type DesignRequestReceiptStatus = typeof DesignRequestReceiptStatus.Type;

export const DesignRequestReceipt = closed({
  type: Schema.Literal("jones.design-request.receipt"),
  protocol: Protocol,
  nonceJ: Nonce,
  nonceG: Nonce,
  requestNonce: Nonce,
  packetId: PacketId,
  // The full digest the gallery bound to this request, echoed on every receipt including reconcile.
  digest: Digest,
  status: DesignRequestReceiptStatus,
  reason: Schema.optional(Text(200)),
  destination: Schema.optional(DesignRequestDestination),
  delivery: Schema.optional(Schema.Literals(["started", "queued"])),
});
export type DesignRequestReceipt = typeof DesignRequestReceipt.Type;

export const DesignRequestReconcile = closed({
  type: Schema.Literal("jones.design-request.reconcile"),
  protocol: Protocol,
  nonceJ: Nonce,
  nonceG: Nonce,
  requestNonce: Nonce,
  threadId: Id,
  commandId: Id,
  messageId: Id,
  packetId: PacketId,
  digest: Digest,
});
export type DesignRequestReconcile = typeof DesignRequestReconcile.Type;

/** Messages Jones accepts from the paired gallery. */
export const DesignRequestGalleryMessage = Schema.Union([
  DesignRequestHello,
  DesignRequestSubmit,
  DesignRequestReconcile,
]);
export type DesignRequestGalleryMessage = typeof DesignRequestGalleryMessage.Type;

/** Client-local pairing of one gallery origin and project to an existing workstream. */
export const DesignRequestBinding = closed({
  bindingId: Schema.String.check(Schema.isPattern(/^[a-f0-9]{32}$/)),
  galleryOrigin: Text(2000),
  projectKey: ProjectKey,
  workstreamId: Id,
});
export type DesignRequestBinding = typeof DesignRequestBinding.Type;

export const DesignRequestBindings = closed({
  version: Schema.Literal(1),
  bindings: Schema.Array(DesignRequestBinding).check(Schema.isMaxLength(200)),
});
export type DesignRequestBindings = typeof DesignRequestBindings.Type;
