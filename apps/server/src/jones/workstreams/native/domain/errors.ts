export const REGISTRY_ERROR_SHAPES = {
  unauthorized: { status: 401, recovery: "stop" },
  forbidden: { status: 403, recovery: "stop" },
  not_found: { status: 404, recovery: "stop" },
  invalid_request: { status: 400, recovery: "correct_request" },
  contract_mismatch: { status: 409, recovery: "stop" },
  version_conflict: { status: 409, recovery: "reload" },
  registration_unverified: { status: 409, recovery: "reverify" },
  generation_conflict: { status: 409, recovery: "reconcile" },
  identity_conflict: { status: 409, recovery: "correct_request" },
  membership_conflict: { status: 409, recovery: "reload" },
  relationship_conflict: { status: 409, recovery: "correct_request" },
  cycle_conflict: { status: 409, recovery: "correct_request" },
  idempotency_conflict: { status: 409, recovery: "reconcile" },
  cursor_invalid: { status: 400, recovery: "restart_page" },
  cursor_stale: { status: 409, recovery: "restart_page" },
  payload_too_large: { status: 413, recovery: "correct_request" },
  unsupported_media_type: { status: 415, recovery: "correct_request" },
  backpressure: { status: 429, recovery: "retry_after" },
  provider_unconfigured: { status: 503, recovery: "stop" },
  unavailable: { status: 503, recovery: "reconcile" },
  unknown_effect: { status: 503, recovery: "reconcile" },
} as const;

export type RegistryErrorCode = keyof typeof REGISTRY_ERROR_SHAPES;

export type RegistryError = {
  readonly [Code in RegistryErrorCode]: Readonly<
    { readonly code: Code; readonly request_id: string } & (typeof REGISTRY_ERROR_SHAPES)[Code] &
      (Code extends "backpressure" ? { readonly retry_after_seconds: number } : object)
  >;
}[RegistryErrorCode];

export type DomainErrorCode = RegistryErrorCode;

export class WorkstreamDomainError extends Error {
  readonly code: DomainErrorCode;

  constructor(code: DomainErrorCode, message: string = code) {
    super(message);
    this.name = "WorkstreamDomainError";
    this.code = code;
  }
}

export function requireDomain(
  condition: unknown,
  code: DomainErrorCode,
  message?: string,
): asserts condition {
  if (!condition) throw new WorkstreamDomainError(code, message);
}

export function isDomainErrorCode(value: string): value is DomainErrorCode {
  return Object.hasOwn(REGISTRY_ERROR_SHAPES, value);
}

export function registryError(
  code: DomainErrorCode,
  request_id: string,
  retryAfterSeconds?: number,
): RegistryError {
  if (code === "backpressure") {
    const retry_after_seconds =
      Number.isSafeInteger(retryAfterSeconds) &&
      retryAfterSeconds! >= 1 &&
      retryAfterSeconds! <= 3_600
        ? retryAfterSeconds!
        : 1;
    return { code, ...REGISTRY_ERROR_SHAPES[code], retry_after_seconds, request_id };
  }
  return { code, ...REGISTRY_ERROR_SHAPES[code], request_id } as RegistryError;
}
