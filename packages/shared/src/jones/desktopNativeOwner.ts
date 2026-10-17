import type { JonesNativeWriterAdmission } from "./nativeWriterFence.ts";

export const DESKTOP_NATIVE_OWNER_INPUT_FD = 8;
export const DESKTOP_NATIVE_OWNER_OUTPUT_FD = 9;
export const DESKTOP_NATIVE_OWNER_MAX_BYTES = 8192;
export type DesktopNativeOwnerBinding = JonesNativeWriterAdmission["binding"];
export interface DesktopNativeOwnerChallenge {
  readonly protocol: 1;
  readonly sequence: number;
  readonly nonce: string;
  readonly childPid: number;
  readonly binding: DesktopNativeOwnerBinding;
}
export interface DesktopNativeOwnerResponse extends DesktopNativeOwnerChallenge {
  readonly runId: string;
  readonly parentPid: number;
}

export function sameDesktopNativeOwnerBinding(
  left: DesktopNativeOwnerBinding,
  right: DesktopNativeOwnerBinding,
): boolean {
  return [
    "home",
    "databasePath",
    "profile",
    "environmentId",
    "version",
    "sourceSha",
    "sourceTree",
    "manifestSha256",
  ].every(
    (key) =>
      left[key as keyof DesktopNativeOwnerBinding] ===
      right[key as keyof DesktopNativeOwnerBinding],
  );
}

export function decodeDesktopNativeOwnerMessage(
  line: string,
  response: false,
): DesktopNativeOwnerChallenge;
export function decodeDesktopNativeOwnerMessage(
  line: string,
  response: true,
): DesktopNativeOwnerResponse;
export function decodeDesktopNativeOwnerMessage(
  line: string,
  response: boolean,
): DesktopNativeOwnerChallenge | DesktopNativeOwnerResponse {
  if (new TextEncoder().encode(line).byteLength > DESKTOP_NATIVE_OWNER_MAX_BYTES)
    throw new Error("Desktop owner message exceeds its bound.");
  const value = JSON.parse(line) as DesktopNativeOwnerResponse;
  const keys = [
    "protocol",
    "sequence",
    "nonce",
    "childPid",
    "binding",
    ...(response ? ["runId", "parentPid"] : []),
  ];
  const bindingKeys = [
    "home",
    "databasePath",
    "profile",
    "environmentId",
    "version",
    "sourceSha",
    "sourceTree",
    "manifestSha256",
  ];
  if (
    value === null ||
    typeof value !== "object" ||
    Object.keys(value).length !== keys.length ||
    !keys.every((key) => Object.hasOwn(value, key)) ||
    value.protocol !== 1 ||
    !Number.isSafeInteger(value.sequence) ||
    value.sequence < 1 ||
    !Number.isSafeInteger(value.childPid) ||
    value.childPid < 1 ||
    typeof value.nonce !== "string" ||
    !/^[a-f0-9]{64}$/.test(value.nonce) ||
    value.binding === null ||
    typeof value.binding !== "object" ||
    Object.keys(value.binding).length !== bindingKeys.length ||
    !bindingKeys.every(
      (key) =>
        typeof value.binding[key as keyof DesktopNativeOwnerBinding] === "string" &&
        value.binding[key as keyof DesktopNativeOwnerBinding].length > 0,
    ) ||
    (response &&
      (typeof value.runId !== "string" ||
        !/^[a-f0-9]{64}$/.test(value.runId) ||
        !Number.isSafeInteger(value.parentPid) ||
        value.parentPid < 1))
  )
    throw new Error("Desktop owner message is invalid.");
  return value;
}

export function encodeDesktopNativeOwnerMessage(
  message: DesktopNativeOwnerChallenge | DesktopNativeOwnerResponse,
): Uint8Array {
  return new TextEncoder().encode(`${JSON.stringify(message)}\n`);
}
