import type { WorkstreamsRegistrationContextAcceptedResponse } from "@t3tools/contracts";
import {
  parseWorkstreamsT3Availability,
  WORKSTREAMS_T3_AVAILABILITY_HEADER,
  type T3RegistrationAvailability,
} from "@t3tools/contracts/jones/workstreamsRegistrationAvailability";

export interface AssignmentAvailability {
  readonly state: "ready" | "unknown" | "unavailable";
  readonly reason: string | null;
}

const reasons: Record<
  Extract<T3RegistrationAvailability, { state: "unavailable" }>["reason"],
  string
> = {
  descriptor_absent: "source descriptor unavailable",
  build_unavailable: "build identity unavailable",
  contract_mismatch: "source contract differs",
  source_mismatch: "enrolled source or build differs",
  authority_mismatch: "source authority differs",
  generation_mismatch: "source generation differs",
  authority_fenced: "source authority is fenced",
  authority_unavailable: "source authority unavailable",
};

export function registrationAvailability(
  headers: Readonly<Record<string, string | undefined>>,
): AssignmentAvailability {
  const parsed = parseWorkstreamsT3Availability(headers[WORKSTREAMS_T3_AVAILABILITY_HEADER]);
  if (parsed === null) return { state: "unknown", reason: null };
  return parsed.state === "ready"
    ? { state: "ready", reason: null }
    : { state: "unavailable", reason: reasons[parsed.reason] };
}

export function registrationContextWithAvailability([context, response]: readonly [
  WorkstreamsRegistrationContextAcceptedResponse,
  { readonly headers: Readonly<Record<string, string | undefined>> },
]) {
  return { context, availability: registrationAvailability(response.headers) };
}
