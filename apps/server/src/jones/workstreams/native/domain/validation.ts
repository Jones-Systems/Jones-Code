import * as Schema from "effect/Schema";
import {
  WorkstreamCommand,
  WorkstreamRegistrationVerification,
  WorkstreamPrObservation,
} from "@t3tools/contracts";
import type { Command, NativeIdentity, PrLocator, AuthorizedOwner, OwnerGrant } from "./state.ts";
import { requireDomain, WorkstreamDomainError } from "./errors.ts";

const strict = { onExcessProperty: "error" as const };
const commandDecoder = Schema.decodeUnknownSync(WorkstreamCommand);
const registrationDecoder = Schema.decodeUnknownSync(WorkstreamRegistrationVerification);
const observationDecoder = Schema.decodeUnknownSync(WorkstreamPrObservation);

export function requireAuthorization(
  authorization: AuthorizedOwner,
  current: OwnerGrant,
  permission: "workstreams:read" | "workstreams:write",
  ownerId: string,
): void {
  const grant = authorization.grant;
  requireDomain(
    authorization.owner_id === ownerId &&
      grant.owner_id === ownerId &&
      grant.principal_id === authorization.actor.principal_id &&
      grant.state === "active" &&
      grant.permissions.includes(permission) &&
      current.owner_id === ownerId &&
      current.principal_id === grant.principal_id &&
      current.grant_id === grant.grant_id &&
      current.grant_version === grant.grant_version &&
      current.state === "active" &&
      current.permissions.includes(permission),
    "forbidden",
  );
}

export function validTimestamp(value: unknown): value is string {
  if (typeof value !== "string") return false;
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d{1,6})?Z$/.exec(value);
  if (!match) return false;
  const [year, month, day, hour, minute, second] = match.slice(1, 7).map(Number);
  const leap = year! % 4 === 0 && (year! % 100 !== 0 || year! % 400 === 0);
  const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  return (
    month! >= 1 &&
    month! <= 12 &&
    day! >= 1 &&
    day! <= days[month! - 1]! &&
    hour! <= 23 &&
    minute! <= 59 &&
    second! <= 59
  );
}

export function validateExternalValue(kind: "registration" | "observation", value: unknown): void {
  try {
    (kind === "registration" ? registrationDecoder : observationDecoder)(value, strict);
  } catch {
    throw new WorkstreamDomainError("invalid_request");
  }
  const timestamps = (item: unknown): void => {
    if (item && typeof item === "object")
      for (const [key, field] of Object.entries(item)) {
        if (field !== null && key.endsWith("_at"))
          requireDomain(validTimestamp(field), "invalid_request");
        else if (field && typeof field === "object") timestamps(field);
      }
  };
  timestamps(value);
}

export function validText(value: unknown, maximum: number): value is string {
  return (
    typeof value === "string" &&
    [...value].length >= 1 &&
    [...value].length <= maximum &&
    !/[\uD800-\uDFFF]/u.test(value)
  );
}

export function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value !== null && typeof value === "object")
    return `{${Object.keys(value)
      .sort()
      .map(
        (key) => `${JSON.stringify(key)}:${canonicalJson((value as Record<string, unknown>)[key])}`,
      )
      .join(",")}}`;
  return JSON.stringify(value);
}

export function validateCommand(command: Command): void {
  try {
    commandDecoder(command, strict);
  } catch {
    throw new WorkstreamDomainError("invalid_request");
  }
  const validateStrings = (value: unknown): void => {
    if (typeof value === "string")
      requireDomain(!/[\uD800-\uDFFF]/u.test(value), "invalid_request");
    else if (value && typeof value === "object")
      for (const item of Object.values(value)) validateStrings(item);
  };
  validateStrings(command);
}

export function validatePrIdentity(identity: NativeIdentity, locator: PrLocator | null): void {
  const isPr = identity.provider === "github" && identity.resource_kind === "pull_request";
  requireDomain(isPr === (locator !== null), "identity_conflict");
  if (locator === null) return;
  requireDomain(
    identity.id_kind === "external" &&
      locator.host === "github.com" &&
      /^[a-z0-9](?:[a-z0-9-]{0,37}[a-z0-9])?$/.test(locator.repository_owner) &&
      /^[a-z0-9._-]{1,100}$/.test(locator.repository_name) &&
      locator.repository_name !== "." &&
      locator.repository_name !== ".." &&
      Number.isSafeInteger(locator.number) &&
      locator.number >= 1 &&
      locator.number <= 2_147_483_647 &&
      identity.native_id ===
        `${locator.repository_owner}/${locator.repository_name}#${locator.number}`,
    "identity_conflict",
  );
}
