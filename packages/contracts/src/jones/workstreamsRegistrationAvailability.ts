export const WORKSTREAMS_T3_AVAILABILITY_HEADER = "x-jones-workstreams-t3-availability";

const unavailableReasons = [
  "descriptor_absent",
  "build_unavailable",
  "contract_mismatch",
  "source_mismatch",
  "authority_mismatch",
  "generation_mismatch",
  "authority_fenced",
  "authority_unavailable",
] as const;

export type T3RegistrationAvailability =
  | { readonly state: "ready" }
  | { readonly state: "unavailable"; readonly reason: (typeof unavailableReasons)[number] };

export const parseWorkstreamsT3Availability = (
  value: string | null | undefined,
): T3RegistrationAvailability | null => {
  if (value === "ready") return { state: "ready" };
  for (const reason of unavailableReasons) {
    if (value === `unavailable;reason=${reason}`) return { state: "unavailable", reason };
  }
  return null;
};

export const formatWorkstreamsT3Availability = (value: T3RegistrationAvailability): string =>
  value.state === "ready" ? "ready" : `unavailable;reason=${value.reason}`;
