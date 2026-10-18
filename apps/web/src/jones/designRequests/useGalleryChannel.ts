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
import { useEffect, useEffectEvent, useRef, useState } from "react";

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
  const [env] = useState(environment);
  const galleryOrigin =
    env.opener === null ? null : parseGalleryOrigin(new URLSearchParams(env.search).get("origin"));
  const [projectKey, setProjectKey] = useState<string | null>(null);
  const phase: GalleryChannelPhase =
    env.opener === null
      ? "no-opener"
      : galleryOrigin === null
        ? "invalid-origin"
        : projectKey === null
          ? "waiting"
          : "connected";
  const state = useRef<(DesignRequestChannelState & { opener: OpenerWindow }) | null>(null);
  const handleMessage = useEffectEvent(onMessage);

  useEffect(() => {
    if (env.opener === null || galleryOrigin === null) return;
    const origin = galleryOrigin;
    const nonceJ = designRequestNonce();
    state.current = { galleryOrigin: origin, opener: env.opener, nonceJ, nonceG: null };
    const listener = (event: Event) => {
      const current = state.current;
      if (current === null) return;
      const { origin: eventOrigin, source, data } = event as MessageEvent;
      const message = acceptGalleryMessage(current, { origin: eventOrigin, source, data });
      if (message === null) return;
      if (message.type === "jones.design-request.hello") {
        state.current = { ...current, nonceG: message.nonceG };
        setProjectKey(message.projectKey);
      }
      handleMessage(message);
    };
    env.target.addEventListener("message", listener);
    env.opener.postMessage(designRequestReady(nonceJ), origin);
    return () => {
      env.target.removeEventListener("message", listener);
      state.current = null;
    };
    // The captured environment binds this handshake to one page load and exact opener.
  }, [env, galleryOrigin]);

  const send: GalleryChannel["send"] = (message) => {
    const current = state.current;
    if (current === null || current.nonceG === null) return;
    current.opener.postMessage(
      { ...message, nonceJ: current.nonceJ, nonceG: current.nonceG },
      current.galleryOrigin,
    );
  };

  return { phase, galleryOrigin, projectKey, send };
}
