import {
  DESIGN_REQUEST_PROTOCOL,
  DesignRequestGalleryMessage,
  type DesignRequestReady,
} from "@t3tools/contracts/jones/designRequests";
import * as Schema from "effect/Schema";

const decodeGalleryMessage = Schema.decodeUnknownOption(DesignRequestGalleryMessage, {
  onExcessProperty: "error",
});

/** 16 random bytes as hex. `getRandomValues` works on plain-HTTP origins; `randomUUID` does not. */
export function designRequestNonce(
  random: (bytes: Uint8Array<ArrayBuffer>) => Uint8Array = (bytes) => crypto.getRandomValues(bytes),
): string {
  return Array.from(random(new Uint8Array(16)), (value) =>
    value.toString(16).padStart(2, "0"),
  ).join("");
}

/**
 * The gallery origin named by `?origin=`. It must already be an exact serialized http(s) origin;
 * anything with a path, credentials or a different serialization is refused.
 */
export function parseGalleryOrigin(value: string | null): string | null {
  if (value === null || value.length > 2000) return null;
  try {
    const url = new URL(value);
    if ((url.protocol !== "http:" && url.protocol !== "https:") || url.origin !== value)
      return null;
    return url.origin;
  } catch {
    return null;
  }
}

const TAILNET_IPV4 = /^100\.(6[4-9]|[7-9][0-9]|1[01][0-9]|12[0-7])\.\d{1,3}\.\d{1,3}$/;

/** Origins offered for pairing by default: loopback, Tailscale CGNAT addresses and `*.ts.net`. */
export function isPairableGalleryOrigin(origin: string): boolean {
  const { hostname } = new URL(origin);
  return (
    hostname === "localhost" ||
    hostname === "127.0.0.1" ||
    hostname === "[::1]" ||
    TAILNET_IPV4.test(hostname) ||
    hostname.endsWith(".ts.net")
  );
}

export const designRequestReady = (nonceJ: string): DesignRequestReady => ({
  type: "jones.design-request.ready",
  protocol: DESIGN_REQUEST_PROTOCOL,
  nonceJ,
});

export interface DesignRequestChannelState {
  readonly galleryOrigin: string;
  readonly opener: unknown;
  readonly nonceJ: string;
  /** Set by the first valid hello; later messages must repeat it. */
  readonly nonceG: string | null;
}

/**
 * Accept one postMessage event only from the exact opener window and origin with both nonces.
 * Everything else is ignored silently so a wrong sender learns nothing beyond `ready`.
 */
export function acceptGalleryMessage(
  state: DesignRequestChannelState,
  event: { readonly origin: string; readonly source: unknown; readonly data: unknown },
): DesignRequestGalleryMessage | null {
  if (state.opener === null || state.opener === undefined) return null;
  if (event.origin !== state.galleryOrigin || event.source !== state.opener) return null;
  const decoded = decodeGalleryMessage(event.data);
  if (decoded._tag !== "Some") return null;
  const message = decoded.value;
  if (message.nonceJ !== state.nonceJ) return null;
  if (message.type === "jones.design-request.hello")
    return state.nonceG === null || state.nonceG === message.nonceG ? message : null;
  return state.nonceG !== null && message.nonceG === state.nonceG ? message : null;
}
