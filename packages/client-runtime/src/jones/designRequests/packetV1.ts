import {
  DESIGN_REQUEST_LIMITS,
  type DesignRequestAttachmentData,
  type DesignRequestHeldReason,
} from "@t3tools/contracts/jones/designRequests";

/**
 * Port of the design gallery's packet v1 (`tools/design-gallery/request-packet.mjs`, Frontend
 * d7be667). The digest is SHA-256 over canonical JSON of every packet member except `digest`
 * and `attachmentData`. Packet-level members are checked exactly as the gallery does; reference
 * targets and capture contexts are only checked structurally because the digest already binds
 * them and Jones only quotes them as review data.
 */
export const PACKET_V1_FORMAT = "design-gallery-request-packet";
export const PACKET_V1_VERSION = 1;
export const PACKET_V1_UPDATE_FORMAT = "design-gallery-request-update";

const PACKET_KEYS = [
  "format",
  "version",
  "packetId",
  "savedAt",
  "collection",
  "request",
  "reopen",
  "notices",
  "digest",
];
const REFERENCE_KEYS = [
  "id",
  "ordinal",
  "collection",
  "variation",
  "view",
  "source",
  "target",
  "viewport",
  "scroll",
  "state",
  "capturedAt",
  "context",
];
const REQUIRED_REFERENCE_KEYS = [
  "id",
  "ordinal",
  "collection",
  "variation",
  "view",
  "source",
  "target",
  "capturedAt",
  "context",
];
const MEDIA_TYPES = new Set(["image/png", "image/jpeg", "image/webp"]);
const HEX = /^[a-f0-9]{64}$/;
const DIGEST = /^sha256:[a-f0-9]{64}$/;
const REQUEST_ID = /^[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}$/;
const SLUG = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;
// oxlint-disable-next-line no-control-regex -- Mirrors the gallery's bounded-text rule.
const CONTROL = /[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/;
const encoder = new TextEncoder();

export interface PacketV1Attachment {
  readonly id: string;
  readonly ordinal: number;
  readonly name: string;
  readonly mediaType: "image/png" | "image/jpeg" | "image/webp";
  readonly bytes: number;
  readonly sha256: string;
}

export interface PacketV1Reference {
  readonly id: string;
  readonly ordinal: number;
  readonly variation: string;
  readonly view: string;
  readonly state?: unknown;
  readonly target: { readonly kind?: unknown };
  readonly context: unknown;
}

export interface PacketV1 {
  readonly format: typeof PACKET_V1_FORMAT;
  readonly version: typeof PACKET_V1_VERSION;
  readonly packetId: string;
  readonly savedAt: string;
  readonly collection: {
    readonly title: string;
    readonly key: string;
    readonly generation: string | null;
  };
  readonly request: {
    readonly originContext: {
      readonly collection: string;
      readonly variation: string | null;
      readonly view: string;
    };
    readonly reviewer: string;
    readonly note: string;
    readonly references: readonly PacketV1Reference[];
    readonly attachments: readonly PacketV1Attachment[];
  };
  readonly reopen: readonly {
    readonly ordinal: number;
    readonly letter: string;
    readonly url: string;
    readonly fragment: "included" | "omitted-over-limit";
  }[];
  readonly notices: readonly string[];
  readonly digest: string;
}

export interface VerifiedImage extends PacketV1Attachment {
  readonly dataUrl: string;
}

export type PacketV1ErrorReason = Extract<
  DesignRequestHeldReason,
  "packet-invalid" | "digest-mismatch" | "attachment-mismatch" | "too-large"
>;

export class PacketV1Error extends Error {
  readonly reason: PacketV1ErrorReason;
  readonly field: string;
  constructor(reason: PacketV1ErrorReason, field: string) {
    super(`${field}: ${reason}`);
    this.name = "PacketV1Error";
    this.reason = reason;
    this.field = field;
  }
}

const invalid = (field: string): never => {
  throw new PacketV1Error("packet-invalid", field);
};

const K = Uint32Array.of(
  0x428a2f98,
  0x71374491,
  0xb5c0fbcf,
  0xe9b5dba5,
  0x3956c25b,
  0x59f111f1,
  0x923f82a4,
  0xab1c5ed5,
  0xd807aa98,
  0x12835b01,
  0x243185be,
  0x550c7dc3,
  0x72be5d74,
  0x80deb1fe,
  0x9bdc06a7,
  0xc19bf174,
  0xe49b69c1,
  0xefbe4786,
  0x0fc19dc6,
  0x240ca1cc,
  0x2de92c6f,
  0x4a7484aa,
  0x5cb0a9dc,
  0x76f988da,
  0x983e5152,
  0xa831c66d,
  0xb00327c8,
  0xbf597fc7,
  0xc6e00bf3,
  0xd5a79147,
  0x06ca6351,
  0x14292967,
  0x27b70a85,
  0x2e1b2138,
  0x4d2c6dfc,
  0x53380d13,
  0x650a7354,
  0x766a0abb,
  0x81c2c92e,
  0x92722c85,
  0xa2bfe8a1,
  0xa81a664b,
  0xc24b8b70,
  0xc76c51a3,
  0xd192e819,
  0xd6990624,
  0xf40e3585,
  0x106aa070,
  0x19a4c116,
  0x1e376c08,
  0x2748774c,
  0x34b0bcb5,
  0x391c0cb3,
  0x4ed8aa4a,
  0x5b9cca4f,
  0x682e6ff3,
  0x748f82ee,
  0x78a5636f,
  0x84c87814,
  0x8cc70208,
  0x90befffa,
  0xa4506ceb,
  0xbef9a3f7,
  0xc67178f2,
);
const ror = (x: number, n: number) => (x >>> n) | (x << (32 - n));

/**
 * Synchronous FIPS 180-4 SHA-256 as lowercase hex. WebCrypto exists only in secure contexts and a
 * tailnet Jones origin may be plain HTTP, so verification never depends on it.
 */
export function sha256Hex(input: string | Uint8Array): string {
  const bytes = typeof input === "string" ? encoder.encode(input) : input;
  const length = bytes.length;
  const padded = new Uint8Array(Math.ceil((length + 9) / 64) * 64);
  padded.set(bytes);
  padded[length] = 0x80;
  const view = new DataView(padded.buffer);
  view.setUint32(padded.length - 8, Math.floor(length / 0x20000000));
  view.setUint32(padded.length - 4, (length * 8) >>> 0);
  const H = Uint32Array.of(
    0x6a09e667,
    0xbb67ae85,
    0x3c6ef372,
    0xa54ff53a,
    0x510e527f,
    0x9b05688c,
    0x1f83d9ab,
    0x5be0cd19,
  );
  const W = new Uint32Array(64);
  for (let offset = 0; offset < padded.length; offset += 64) {
    for (let t = 0; t < 16; t++) W[t] = view.getUint32(offset + t * 4);
    for (let t = 16; t < 64; t++) {
      const x = W[t - 15]!;
      const y = W[t - 2]!;
      W[t] =
        (W[t - 16]! +
          (ror(x, 7) ^ ror(x, 18) ^ (x >>> 3)) +
          W[t - 7]! +
          (ror(y, 17) ^ ror(y, 19) ^ (y >>> 10))) >>>
        0;
    }
    let a = H[0]!;
    let b = H[1]!;
    let c = H[2]!;
    let d = H[3]!;
    let e = H[4]!;
    let f = H[5]!;
    let g = H[6]!;
    let h = H[7]!;
    for (let t = 0; t < 64; t++) {
      const t1 =
        (h + (ror(e, 6) ^ ror(e, 11) ^ ror(e, 25)) + ((e & f) ^ (~e & g)) + K[t]! + W[t]!) >>> 0;
      const t2 = ((ror(a, 2) ^ ror(a, 13) ^ ror(a, 22)) + ((a & b) ^ (a & c) ^ (b & c))) >>> 0;
      h = g;
      g = f;
      f = e;
      e = (d + t1) >>> 0;
      d = c;
      c = b;
      b = a;
      a = (t1 + t2) >>> 0;
    }
    [a, b, c, d, e, f, g, h].forEach((word, index) => {
      H[index] = H[index]! + word;
    });
  }
  return [...H].map((word) => word.toString(16).padStart(8, "0")).join("");
}

/** Canonical JSON: keys sorted by UTF-16 code unit, no whitespace, finite numbers only. */
export function canonicalJson(value: unknown): string {
  if (value === null || typeof value === "boolean" || typeof value === "string")
    return JSON.stringify(value);
  if (typeof value === "number") {
    if (!Number.isFinite(value)) invalid("canonical JSON");
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (
    typeof value === "object" &&
    [Object.prototype, null].includes(Object.getPrototypeOf(value))
  ) {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`)
      .join(",")}}`;
  }
  return invalid("canonical JSON");
}

export const packetV1Digest = (body: unknown): string => `sha256:${sha256Hex(canonicalJson(body))}`;

const letter = (ordinal: number) => {
  let value = "";
  for (let number = ordinal; number > 0; number = Math.floor((number - 1) / 26))
    value = String.fromCharCode(65 + ((number - 1) % 26)) + value;
  return value;
};

function record(value: unknown, field: string, keys?: readonly string[]) {
  if (!value || typeof value !== "object" || Array.isArray(value)) invalid(field);
  if (keys && Object.keys(value as object).some((key) => !keys.includes(key))) invalid(field);
  return value as Record<string, unknown>;
}
function text(value: unknown, field: string, max: number, empty = false): string {
  if (
    typeof value !== "string" ||
    value.length > max ||
    (!empty && !value.trim()) ||
    CONTROL.test(value)
  )
    invalid(field);
  return value as string;
}
function ordinal(value: unknown, field: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 1) invalid(field);
  return value as number;
}
function timestamp(value: unknown, field: string) {
  if (
    typeof value !== "string" ||
    !/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/.test(value) ||
    !Number.isFinite(Date.parse(value)) ||
    new Date(value).toISOString() !== value
  )
    invalid(field);
}

function verifyOrigin(value: unknown) {
  const origin = record(value, "originContext", ["collection", "variation", "view"]);
  text(origin.collection, "originContext.collection", 300);
  if (!["gallery", "system", "presentation"].includes(origin.view as string))
    invalid("originContext.view");
  if (origin.view === "gallery") {
    if (origin.variation !== null) invalid("originContext.variation");
  } else if (
    typeof origin.variation !== "string" ||
    origin.variation.length > 100 ||
    !SLUG.test(origin.variation)
  )
    invalid("originContext.variation");
}

function verifyReference(value: unknown, ids: Set<string>, ordinals: Set<number>) {
  const reference = record(value, "reference", REFERENCE_KEYS);
  if (!REQUIRED_REFERENCE_KEYS.every((key) => Object.hasOwn(reference, key))) invalid("reference");
  if (typeof reference.id !== "string" || !REQUEST_ID.test(reference.id)) invalid("reference.id");
  const number = ordinal(reference.ordinal, "reference.ordinal");
  if (ids.has(reference.id as string) || ordinals.has(number)) invalid("reference");
  ids.add(reference.id as string);
  ordinals.add(number);
  timestamp(reference.capturedAt, "reference.capturedAt");
  if (reference.view !== "system" && reference.view !== "presentation") invalid("reference.view");
  if (
    typeof reference.variation !== "string" ||
    reference.variation.length > 100 ||
    !SLUG.test(reference.variation)
  )
    invalid("reference.variation");
  text(reference.collection, "reference.collection", 300);
  const source = record(reference.source, "reference.source", ["generation", "path"]);
  if (typeof source.generation !== "string" || !HEX.test(source.generation))
    invalid("reference.source.generation");
  text(source.path, "reference.source.path", 1000);
  const target = record(reference.target, "reference.target");
  if (!["element", "text", "screen", "video", "annotation"].includes(target.kind as string))
    invalid("reference.target.kind");
  if (reference.context !== null) record(reference.context, "reference.context");
}

function verifyAttachment(value: unknown, ids: Set<string>, ordinals: Set<number>): number {
  const item = record(value, "packet attachment", [
    "id",
    "ordinal",
    "name",
    "mediaType",
    "bytes",
    "sha256",
  ]);
  if (
    typeof item.id !== "string" ||
    !REQUEST_ID.test(item.id) ||
    !Number.isSafeInteger(item.ordinal) ||
    (item.ordinal as number) < 1 ||
    ids.has(item.id) ||
    ordinals.has(item.ordinal as number)
  )
    invalid("packet attachment");
  ids.add(item.id as string);
  ordinals.add(item.ordinal as number);
  text(item.name, "packet attachment.name", 200);
  if (!MEDIA_TYPES.has(item.mediaType as string)) invalid("packet attachment.mediaType");
  if (
    !Number.isSafeInteger(item.bytes) ||
    (item.bytes as number) < 1 ||
    (item.bytes as number) > DESIGN_REQUEST_LIMITS.imageBytes
  )
    invalid("packet attachment.bytes");
  if (typeof item.sha256 !== "string" || !HEX.test(item.sha256))
    invalid("packet attachment.sha256");
  return item.bytes as number;
}

/**
 * Validate a packet v1 object (without `attachmentData`) and recompute its digest. Throws
 * `PacketV1Error` with `packet-invalid` or `digest-mismatch`; never mutates the input.
 */
export function verifyPacketV1(value: unknown): PacketV1 {
  const packet = record(value, "packet", PACKET_KEYS);
  if (!PACKET_KEYS.every((key) => Object.hasOwn(packet, key))) invalid("packet");
  if (packet.format !== PACKET_V1_FORMAT || packet.version !== PACKET_V1_VERSION)
    invalid("packet.format");
  if (typeof packet.packetId !== "string" || !REQUEST_ID.test(packet.packetId))
    invalid("packet.packetId");
  timestamp(packet.savedAt, "packet.savedAt");
  const collection = record(packet.collection, "collection", ["title", "key", "generation"]);
  text(collection.title, "collection.title", 300);
  text(collection.key, "collection.key", 2000);
  if (
    collection.generation !== null &&
    (typeof collection.generation !== "string" || !HEX.test(collection.generation))
  )
    invalid("collection.generation");
  const request = record(packet.request, "packet.request", [
    "originContext",
    "reviewer",
    "note",
    "references",
    "attachments",
  ]);
  verifyOrigin(request.originContext);
  text(request.reviewer, "request.reviewer", 200);
  text(request.note, "request.note", 4000);
  if (!Array.isArray(request.references) || request.references.length > 12)
    invalid("request.references");
  const ids = new Set<string>();
  const ordinals = new Set<number>();
  for (const reference of request.references as unknown[])
    verifyReference(reference, ids, ordinals);
  if (
    !Array.isArray(request.attachments) ||
    request.attachments.length > DESIGN_REQUEST_LIMITS.images
  )
    invalid("request.attachments");
  const total = (request.attachments as unknown[]).reduce<number>(
    (sum, item) => sum + verifyAttachment(item, ids, ordinals),
    0,
  );
  if (total > DESIGN_REQUEST_LIMITS.totalBytes) invalid("request.attachments");
  const references = request.references as { ordinal: number }[];
  if (!Array.isArray(packet.reopen) || packet.reopen.length !== references.length)
    invalid("packet.reopen");
  (packet.reopen as unknown[]).forEach((value, index) => {
    const item = record(value, "packet.reopen", ["ordinal", "letter", "url", "fragment"]);
    const expected = references[index]!.ordinal;
    if (item.ordinal !== expected || item.letter !== letter(expected)) invalid("packet.reopen");
    text(item.url, "packet.reopen.url", 16384);
    if (!/^https?:\/\//.test(item.url as string)) invalid("packet.reopen.url");
    if (item.fragment !== "included" && item.fragment !== "omitted-over-limit")
      invalid("packet.reopen.fragment");
  });
  if (!Array.isArray(packet.notices) || packet.notices.length > 10) invalid("packet.notices");
  for (const notice of packet.notices as unknown[]) text(notice, "packet.notice", 500);
  const { digest, ...body } = packet;
  if (typeof digest !== "string" || !DIGEST.test(digest)) invalid("packet.digest");
  if (packetV1Digest(body) !== digest) throw new PacketV1Error("digest-mismatch", "packet.digest");
  return packet as unknown as PacketV1;
}

function decodeBase64(value: string): Uint8Array {
  const binary = atob(value);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index++) bytes[index] = binary.charCodeAt(index);
  return bytes;
}

/**
 * Match `attachmentData` (the downloaded packet's shape) to the verified descriptors: each declared
 * image exactly once, same media type, byte length and SHA-256. Returns images in ordinal order.
 */
export function verifyPacketV1Images(
  packet: PacketV1,
  attachmentData: DesignRequestAttachmentData | undefined,
): readonly VerifiedImage[] {
  const declared = packet.request.attachments;
  if (declared.length > DESIGN_REQUEST_LIMITS.images)
    throw new PacketV1Error("too-large", "attachments");
  if (attachmentData === undefined) {
    if (declared.length === 0) return [];
    throw new PacketV1Error("attachment-mismatch", "attachmentData");
  }
  if (attachmentData.length !== declared.length)
    throw new PacketV1Error("attachment-mismatch", "attachmentData");
  const data = new Map<string, string>();
  let total = 0;
  for (const item of attachmentData) {
    const expected = declared.find((image) => image.id === item.id);
    const match =
      expected === undefined
        ? null
        : /^data:(image\/(?:png|jpeg|webp));base64,([A-Za-z0-9+/]+={0,2})$/.exec(item.dataUrl);
    if (!expected || !match || data.has(item.id) || match[1] !== expected.mediaType)
      throw new PacketV1Error("attachment-mismatch", "attachmentData");
    let bytes: Uint8Array;
    try {
      bytes = decodeBase64(match[2]!);
    } catch {
      throw new PacketV1Error("attachment-mismatch", "attachmentData");
    }
    if (bytes.length > DESIGN_REQUEST_LIMITS.imageBytes)
      throw new PacketV1Error("too-large", "attachmentData");
    if (bytes.length !== expected.bytes || sha256Hex(bytes) !== expected.sha256)
      throw new PacketV1Error("attachment-mismatch", "attachmentData");
    total += bytes.length;
    data.set(item.id, item.dataUrl);
  }
  if (total > DESIGN_REQUEST_LIMITS.totalBytes)
    throw new PacketV1Error("too-large", "attachmentData");
  return [...declared]
    .sort((left, right) => left.ordinal - right.ordinal)
    .map((image) => ({ ...image, dataUrl: data.get(image.id)! }));
}

/**
 * Parse a downloaded packet file or a copied Markdown packet (A′ import without an opener).
 * Returns the verified packet and its `attachmentData`, if present.
 */
export function parsePacketV1Text(input: string): {
  readonly packet: PacketV1;
  readonly attachmentData: DesignRequestAttachmentData | undefined;
} {
  if (encoder.encode(input).byteLength > 12 * 1024 * 1024)
    throw new PacketV1Error("too-large", "packet");
  let json = input.trim();
  if (!json.startsWith("{")) {
    const match = [...input.matchAll(/^```json\r?\n(\{.*\})\r?\n```[ \t]*$/gm)].at(-1);
    if (!match) invalid("packet");
    json = match![1]!;
  }
  let value: unknown;
  try {
    value = JSON.parse(json);
  } catch {
    return invalid("packet");
  }
  const { attachmentData, ...packet } = record(value, "packet");
  if (
    attachmentData !== undefined &&
    (!Array.isArray(attachmentData) ||
      attachmentData.some(
        (item) =>
          !item ||
          typeof item !== "object" ||
          Object.keys(item).some((key) => key !== "id" && key !== "dataUrl") ||
          typeof item.id !== "string" ||
          typeof item.dataUrl !== "string",
      ))
  )
    throw new PacketV1Error("attachment-mismatch", "attachmentData");
  return {
    packet: verifyPacketV1(packet),
    attachmentData: attachmentData as DesignRequestAttachmentData | undefined,
  };
}
