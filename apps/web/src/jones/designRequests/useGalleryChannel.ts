import type {
  DesignRequestGalleryMessage,
  DesignRequestReceipt,
  DesignRequestRoute,
} from "@t3tools/contracts/jones/designRequests";
import {
  acceptGalleryMessage,
  designRequestNonce,
  designRequestReady,
  parseGalleryOrigin,
  type DesignRequestChannelState,
} from "@t3tools/client-runtime/jones/design-requests";
import { useCallback, useEffect, useRef, useState } from "react";

export type GalleryChannelPhase =
  /** Opened directly or from the desktop app: only file/paste import is available. */
  "no-opener" | "invalid-origin" | "waiting" | "connected";

export interface GalleryChannel {
  readonly phase: GalleryChannelPhase;
  readonly galleryOrigin: string | null;
  readonly projectKey: string | null;
  /** Post one reply to the exact opener origin; the nonces are filled in here. */
  readonly send: (
    message:
      | Omit<DesignRequestRoute, "nonceJ" | "nonceG">
      | Omit<DesignRequestReceipt, "nonceJ" | "nonceG">,
  ) => void;
}

interface OpenerWindow {
  postMessage(message: unknown, targetOrigin: string): void;
}

export interface GalleryChannelEnvironment {
  readonly search: string;
  readonly opener: OpenerWindow | null;
  readonly target: Pick<Window, "addEventListener" | "removeEventListener">;
}

const browserEnvironment = (): GalleryChannelEnvironment => ({
  search: window.location.search,
  opener: window.opener as OpenerWindow | null,
  target: window,
});

/**
 * The Jones side of the opener handshake. Jones announces `ready` only to the origin named in
 * the query string, then accepts messages only from that exact opener window, origin and nonces.
 */
export function useGalleryChannel(
  onMessage: (message: DesignRequestGalleryMessage) => void,
  environment: () => GalleryChannelEnvironment = browserEnvironment,
): GalleryChannel {
  const [phase, setPhase] = useState<GalleryChannelPhase>("waiting");
  const [galleryOrigin, setGalleryOrigin] = useState<string | null>(null);
  const [projectKey, setProjectKey] = useState<string | null>(null);
  const state = useRef<(DesignRequestChannelState & { opener: OpenerWindow }) | null>(null);
  const handler = useRef(onMessage);
  handler.current = onMessage;

  useEffect(() => {
    const env = environment();
    if (env.opener === null) {
      setPhase("no-opener");
      return;
    }
    const origin = parseGalleryOrigin(new URLSearchParams(env.search).get("origin"));
    if (origin === null) {
      setPhase("invalid-origin");
      return;
    }
    const nonceJ = designRequestNonce();
    state.current = { galleryOrigin: origin, opener: env.opener, nonceJ, nonceG: null };
    setGalleryOrigin(origin);
    setPhase("waiting");
    const listener = (event: Event) => {
      const current = state.current;
      if (current === null) return;
      const { origin: eventOrigin, source, data } = event as MessageEvent;
      const message = acceptGalleryMessage(current, { origin: eventOrigin, source, data });
      if (message === null) return;
      if (message.type === "jones.design-request.hello") {
        state.current = { ...current, nonceG: message.nonceG };
        setProjectKey(message.projectKey);
        setPhase("connected");
      }
      handler.current(message);
    };
    env.target.addEventListener("message", listener);
    env.opener.postMessage(designRequestReady(nonceJ), origin);
    return () => {
      env.target.removeEventListener("message", listener);
      state.current = null;
    };
    // The handshake runs once per page load; a new gallery tab opens a new Jones window.
  }, []);

  const send = useCallback<GalleryChannel["send"]>((message) => {
    const current = state.current;
    if (current === null || current.nonceG === null) return;
    current.opener.postMessage(
      { ...message, nonceJ: current.nonceJ, nonceG: current.nonceG },
      current.galleryOrigin,
    );
  }, []);

  return { phase, galleryOrigin, projectKey, send };
}
