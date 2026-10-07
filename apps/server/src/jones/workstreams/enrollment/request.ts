// @effect-diagnostics nodeBuiltinImport:off -- SHA-256 binds the closed enrollment request, not authority.
import * as NodeCrypto from "node:crypto";
import { AuthSessionId, WorkstreamsNativeContext } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type { CreateAuthSessionInput } from "../../../persistence/AuthSessions.ts";
import {
  NATIVE_PROVIDER_SCOPES,
  type NativeProviderEnrollmentBinding,
} from "../nativeProvider/enrollment.ts";

const closed = <Fields extends Schema.Struct.Fields>(schema: Schema.Struct<Fields>) =>
  Schema.flip(
    Schema.flip(schema).check(
      Schema.makeFilter(
        (value) =>
          typeof value === "object" &&
          value !== null &&
          !Array.isArray(value) &&
          Reflect.ownKeys(value).every((key) => Object.hasOwn(schema.fields, key)),
      ),
    ),
  );
const UtcMillis = Schema.String.check(
  Schema.isPattern(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/),
  Schema.makeFilter((value) => {
    const parsed = DateTime.make(value);
    return Option.isSome(parsed) && DateTime.formatIso(parsed.value) === value;
  }),
);
const RegistryOrigin = Schema.String.check(
  Schema.isMaxLength(2048),
  Schema.makeFilter((value) => {
    try {
      const url = new URL(value);
      return (
        url.protocol === "https:" &&
        url.origin === value &&
        url.username === "" &&
        url.password === ""
      );
    } catch {
      return false;
    }
  }),
);

export const NativeEnrollmentRequest = closed(
  Schema.Struct({
    schema: Schema.Literal("jones-code.workstreams-native-enrollment/v1"),
    enrollment_id: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/)),
    registry_origin: RegistryOrigin,
    context: WorkstreamsNativeContext,
    session: closed(
      Schema.Struct({ session_id: AuthSessionId, issued_at: UtcMillis, expires_at: UtcMillis }),
    ),
  }),
).check(
  Schema.makeFilter(
    (value) =>
      value.enrollment_id === value.context.enrollment_id &&
      Date.parse(value.session.expires_at) > Date.parse(value.session.issued_at) &&
      Date.parse(value.session.expires_at) - Date.parse(value.session.issued_at) <=
        30 * 24 * 60 * 60 * 1000,
  ),
);
export type NativeEnrollmentRequest = typeof NativeEnrollmentRequest.Type;

const canonicalJson = (value: unknown): string => {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (typeof value === "object" && value !== null)
    return `{${Object.keys(value)
      .sort()
      .map(
        (key) => `${JSON.stringify(key)}:${canonicalJson((value as Record<string, unknown>)[key])}`,
      )
      .join(",")}}`;
  return JSON.stringify(value);
};

// UTF-8 JSON, recursively sorted object keys, no whitespace or trailing newline; all request fields are closed.
export const canonicalNativeEnrollmentRequest = (request: NativeEnrollmentRequest): string =>
  canonicalJson(Schema.decodeUnknownSync(NativeEnrollmentRequest)(request));
const sha256EnrollmentBytes = (bytes: string | Uint8Array): string =>
  NodeCrypto.createHash("sha256").update(bytes).digest("hex");
export const nativeEnrollmentRequestSha256 = (request: NativeEnrollmentRequest): string =>
  sha256EnrollmentBytes(canonicalNativeEnrollmentRequest(request));

export const deriveNativeEnrollmentSession = (
  request: NativeEnrollmentRequest,
): CreateAuthSessionInput => ({
  sessionId: request.session.session_id,
  subject: `workstreams-native:${request.enrollment_id}`,
  scopes: Object.values(NATIVE_PROVIDER_SCOPES),
  method: "bearer-access-token",
  client: {
    label: `Workstreams native ${request.enrollment_id}`,
    deviceType: "bot",
    ipAddress: null,
    userAgent: null,
    os: null,
    browser: null,
  },
  issuedAt: DateTime.makeUnsafe(request.session.issued_at),
  expiresAt: DateTime.makeUnsafe(request.session.expires_at),
});

export const deriveNativeEnrollmentBinding = (
  request: NativeEnrollmentRequest,
): NativeProviderEnrollmentBinding => ({
  ...request.context,
  session_id: request.session.session_id,
  registry_origin: request.registry_origin,
  scopes: Object.values(NATIVE_PROVIDER_SCOPES),
});
