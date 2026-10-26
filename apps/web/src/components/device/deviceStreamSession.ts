import { createCanvasFrameSink } from "@t3tools/client-runtime/device/frame";
import type { DuoControlState } from "@t3tools/client-runtime/device/duo-control";
import {
  createDeviceStreamClient,
  type DeviceScreenSize,
  type DeviceStreamClient,
  type DeviceStreamStatus,
} from "@t3tools/client-runtime/device/stream";
import type { DeviceHubAccess } from "@t3tools/client-runtime/state/deviceHubAccess";
import type { DevicePlatform, EnvironmentId } from "@t3tools/contracts";
import { Atom } from "effect/reactivity";

import { refreshDeviceHubAccess } from "~/state/device";

export interface DeviceStreamState {
  readonly status: DeviceStreamStatus;
  readonly detail: string | undefined;
  /** A frame arrived during this session, so a reconnect can keep showing it. */
  readonly hasFrame: boolean;
  readonly screen: DeviceScreenSize | null;
  readonly mjpegUrl: string | null;
  readonly mjpegGeneration: number;
  readonly input: { readonly connected: boolean; readonly detail?: string };
  readonly duoControl: DuoControlState;
  readonly phoneUnavailable: boolean;
}

export interface DeviceStreamSession {
  readonly stream: DeviceStreamClient;
  /** The stream draws here; the view mounts it and the 3D viewers sample it. */
  readonly canvas: HTMLCanvasElement;
  readonly setFrameListener: (listener: (() => void) | null) => void;
  /** Called when the hub rejects a Duo control, so the 3D viewer drops its optimistic pose. */
  readonly setControlRejectedListener: (listener: (() => void) | null) => void;
  readonly markPhoneUnavailable: () => void;
  readonly reconnect: () => void;
  readonly state: DeviceStreamState;
}

export interface DeviceStreamLifecycle {
  readonly bind: (retire: () => void) => () => void;
  readonly report: (channel: "video" | "input", connected: boolean, detail?: string) => void;
  readonly unauthorized: () => void;
}

export interface DeviceStreamKey {
  readonly environmentId: EnvironmentId;
  readonly hostId: string;
  readonly platform: DevicePlatform;
  readonly deviceId: string;
  readonly access: DeviceHubAccess;
  /** Each mounted view owns its own client: one canvas cannot be in two places. */
  readonly viewId: string;
  readonly lifecycle?: DeviceStreamLifecycle;
}

export const IDLE_DEVICE_STREAM_STATE: DeviceStreamState = {
  status: "connecting",
  detail: undefined,
  hasFrame: false,
  screen: null,
  mjpegUrl: null,
  mjpegGeneration: 0,
  input: { connected: false },
  duoControl: { pending: false, requested: null, error: null },
  phoneUnavailable: false,
};

export const deviceScreenKey = (environmentId: EnvironmentId, hostId: string, deviceId: string) =>
  `${environmentId}\u0000${hostId}\u0000${deviceId}`;

/**
 * The screen one view's stream last reported, null while that view streams nothing.
 * Keyed by view, not device: two views of one device stream independently, and one
 * ending must not clear the other's size. The mini player lays out its frame from it.
 */
export const deviceStreamScreenAtom = Atom.family((viewId: string) =>
  Atom.make<DeviceScreenSize | null>(null).pipe(Atom.withLabel(`device-stream-screen:${viewId}`)),
);

/** A device's video and input connection, live while a view reads it. */
export const deviceStreamSessionAtom = Atom.family((key: DeviceStreamKey) =>
  Atom.make((get): DeviceStreamSession => {
    const screenAtom = deviceStreamScreenAtom(key.viewId);
    const canvas = document.createElement("canvas");
    canvas.style.display = "block";
    canvas.style.width = "100%";
    canvas.style.height = "100%";
    let frameListener: (() => void) | null = null;
    let controlRejectedListener: (() => void) | null = null;
    let state = IDLE_DEVICE_STREAM_STATE;
    let session: DeviceStreamSession;
    let active = true;
    const update = (patch: Partial<DeviceStreamState>) => {
      if (!active) return;
      state = { ...state, ...patch };
      session = { ...session, state };
      get.setSelf(session);
    };
    const client = createDeviceStreamClient(
      { platform: key.platform, deviceId: key.deviceId, access: key.access },
      createCanvasFrameSink(canvas, () => {
        if (active) frameListener?.();
      }),
      {
        onDuoControl: (duoControl) => {
          if (!active) return;
          if (duoControl.error && duoControl.error !== state.duoControl.error)
            controlRejectedListener?.();
          update({ duoControl });
        },
        onDuoUnavailable: () => update({ phoneUnavailable: true }),
        onStatus: (status, detail) => {
          if (!active) return;
          key.lifecycle?.report(
            "video",
            status === "streaming",
            status === "error" ? (detail ?? "Video failed") : detail,
          );
          update({ status, detail, ...(status === "streaming" ? { hasFrame: true } : {}) });
        },
        onScreen: (screen) => {
          if (!active) return;
          get.set(screenAtom, screen);
          update({ screen });
        },
        onUnauthorized: () => {
          if (!active) return;
          if (key.lifecycle) key.lifecycle.unauthorized();
          else refreshDeviceHubAccess(key.environmentId);
        },
        onMjpegFallback: (mjpegUrl) =>
          update({ mjpegUrl, mjpegGeneration: state.mjpegGeneration + 1 }),
        onCanvasVideo: () => update({ mjpegUrl: null }),
        onInputConnected: (connected, detail) => {
          if (!active) return;
          key.lifecycle?.report("input", connected, detail);
          update({ input: { connected, ...(detail ? { detail } : {}) } });
        },
      },
    );
    session = {
      stream: client,
      canvas,
      setFrameListener: (listener) => {
        frameListener = active ? listener : null;
      },
      setControlRejectedListener: (listener) => {
        controlRejectedListener = active ? listener : null;
      },
      markPhoneUnavailable: () => update({ phoneUnavailable: true }),
      reconnect: () => {
        if (!active) return;
        client.stop();
        update({ phoneUnavailable: false });
        client.start();
      },
      state,
    };
    const retire = () => {
      if (!active) return;
      active = false;
      frameListener = null;
      controlRejectedListener = null;
      state = IDLE_DEVICE_STREAM_STATE;
      session = { ...session, state };
      get.setSelf(session);
      client.stop();
      // Registry disposal removes all nodes before finalizers; no screen remains to clear.
      if (get.registry.getNodes().has(screenAtom)) get.registry.set(screenAtom, null);
    };
    const unbind = key.lifecycle?.bind(retire);
    get.addFinalizer(() => {
      retire();
      unbind?.();
    });
    if (active) client.start();
    return session;
  }).pipe(Atom.withLabel(`device-stream:${key.platform}:${key.deviceId}`)),
);
