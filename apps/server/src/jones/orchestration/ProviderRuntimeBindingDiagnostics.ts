import * as NodeUtil from "node:util";

function reasonForText(value: unknown) {
  switch (value) {
    case "The launch reservation was lost.":
      return "reservation_lost";
    case "The launch reservation or native ownership was lost.":
      return "reservation_or_ownership_lost";
    case "Another producer already owns this native conversation.":
      return "previous_generation_mismatch";
    case "The native binding changed before its boundary committed.":
      return "boundary_commit_empty";
    case "Native execution has no committed physical runtime binding.":
      return "physical_binding_absent";
    default:
      return undefined;
  }
}

function dataProperty(value: object, key: string): unknown {
  const descriptor = Object.getOwnPropertyDescriptor(value, key);
  return descriptor !== undefined && "value" in descriptor ? descriptor.value : undefined;
}

export function classifyProviderRuntimeBindingFailure(input: unknown) {
  const seen = new Set<object>();
  let current = input;
  for (let depth = 0; depth < 16; depth += 1) {
    const textReason = reasonForText(current);
    if (textReason !== undefined) return textReason;
    if (
      typeof current !== "object" ||
      current === null ||
      NodeUtil.types.isProxy(current) ||
      seen.has(current)
    ) {
      break;
    }
    seen.add(current);
    if (dataProperty(current, "_tag") === "ProviderEventOriginStaleError") {
      return "producer_origin_stale";
    }
    const reason =
      reasonForText(dataProperty(current, "detail")) ??
      reasonForText(dataProperty(current, "message"));
    if (reason !== undefined) return reason;
    current = dataProperty(current, "cause");
  }
  return "unclassified";
}
