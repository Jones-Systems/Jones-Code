import * as NodeUtil from "node:util";
import * as Cause from "effect/Cause";

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
    case "Codex cannot admit a thread to an unconfirmed or replaced runtime.":
      return "runtime_unconfirmed_or_replaced";
    case "Codex cannot admit a thread while a native start is unconfirmed.":
      return "native_start_unconfirmed";
    case "Codex cannot admit a thread to an inactive or replaced runtime producer.":
      return "runtime_producer_not_current";
    case "Codex resumed a different native conversation; continuation is unconfirmed.":
      return "resume_native_thread_mismatch";
    case "Codex rollback resumed another native conversation; its effect is unconfirmed.":
      return "rollback_native_thread_mismatch";
    case "Codex has an unconfirmed native effect; starting a new conversation is not safe.":
      return "native_effect_unconfirmed_new_thread";
    case "Codex has an unconfirmed native effect; another resume is not safe.":
      return "native_effect_unconfirmed_resume";
    default:
      return undefined;
  }
}

function fallbackReasonForText(value: unknown) {
  switch (value) {
    case "The native thread opened but its runtime binding is unconfirmed; do not replay it.":
      return "native_open_binding_unconfirmed";
    case "Failed to establish the actual provider runtime binding; native effects must not be replayed.":
      return "binding_establishment_failed";
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
  let fallback: ReturnType<typeof fallbackReasonForText>;
  for (let depth = 0; depth < 16; depth += 1) {
    const textReason = reasonForText(current);
    if (textReason !== undefined) return textReason;
    fallback = fallbackReasonForText(current) ?? fallback;
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
    fallback =
      fallbackReasonForText(dataProperty(current, "detail")) ??
      fallbackReasonForText(dataProperty(current, "message")) ??
      fallback;
    current = dataProperty(current, "cause");
  }
  return fallback ?? "unclassified";
}

export function providerRuntimeBindingFailureCode(input: unknown): string | undefined {
  const seen = new Set<object>();
  let current = input;
  for (let depth = 0; depth < 16; depth += 1) {
    if (
      typeof current !== "object" ||
      current === null ||
      NodeUtil.types.isProxy(current) ||
      seen.has(current)
    ) {
      break;
    }
    seen.add(current);
    if (dataProperty(current, "_tag") === "ProviderRuntimeBindingError") {
      return `provider_runtime_binding_${classifyProviderRuntimeBindingFailure(current)}`;
    }
    current = dataProperty(current, "cause");
  }
  return undefined;
}

function effectCauseValue(value: object): unknown {
  const reasons = dataProperty(value, "reasons");
  if (
    typeof reasons !== "object" ||
    reasons === null ||
    NodeUtil.types.isProxy(reasons) ||
    !Array.isArray(reasons)
  )
    return undefined;
  const count = dataProperty(reasons, "length");
  if (typeof count !== "number" || count > 16) return undefined;
  let firstDie: object | undefined;
  for (let index = 0; index < count; index += 1) {
    const reason = dataProperty(reasons, String(index));
    if (typeof reason !== "object" || reason === null || NodeUtil.types.isProxy(reason))
      return undefined;
    switch (dataProperty(reason, "_tag")) {
      case "Fail":
        return dataProperty(reason, "error");
      case "Die":
        firstDie ??= reason;
        break;
      case "Interrupt":
        break;
      default:
        return undefined;
    }
  }
  return firstDie === undefined ? undefined : dataProperty(firstDie, "defect");
}

export function providerRuntimeBindingFailureCodeFromCause(input: unknown): string | undefined {
  const seen = new Set<object>();
  let current = input;
  for (let depth = 0; depth < 16; depth += 1) {
    if (
      typeof current !== "object" ||
      current === null ||
      NodeUtil.types.isProxy(current) ||
      seen.has(current)
    ) {
      break;
    }
    seen.add(current);
    const code = providerRuntimeBindingFailureCode(current);
    if (code !== undefined) return code;
    current =
      dataProperty(current, Cause.TypeId) === Cause.TypeId
        ? effectCauseValue(current)
        : dataProperty(current, "cause");
  }
  return undefined;
}
