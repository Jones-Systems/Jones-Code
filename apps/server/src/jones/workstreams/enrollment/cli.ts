// @effect-diagnostics nodeBuiltinImport:off -- The standalone CLI reads and writes only explicitly named enrollment files.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import { AuthSessionId, WORKSTREAMS_T3_PROVIDER_PROTOCOL } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Console from "effect/Console";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { Argument, Command, Flag } from "effect/cli";
import * as SqlClient from "effect/sql/SqlClient";
import {
  makeReservedBearerSessionMaterializer,
  SessionCredentialIssueError,
} from "../../../auth/SessionStore.ts";
import { ServerSecretStore } from "../../../auth/ServerSecretStore.ts";
import * as AuthSessions from "../../../persistence/AuthSessions.ts";
import { makeNativeCredentialWriter } from "./credentialFile.ts";
import { makeNativeEnrollmentOperations, type NativeEnrollmentReceipt } from "./operations.ts";
import {
  canonicalNativeEnrollmentRequest,
  NativeEnrollmentRequest,
  nativeEnrollmentRequestSha256,
} from "./request.ts";
import { makeExistingEnrollmentSecretStore } from "./secretStore.ts";
import { makeNativeEnrollments } from "./service.ts";
import {
  makeTrustedNativeEnrollmentCliContext,
  NativeEnrollmentCliContextError,
  type NativeEnrollmentCliLocation,
} from "./trustedCliContext.ts";

export class WorkstreamProviderCliRequestError extends Schema.TaggedError<WorkstreamProviderCliRequestError>()(
  "WorkstreamProviderCliRequestError",
  {
    code: Schema.Literals([
      "request_unavailable",
      "invalid_request",
      "request_exists",
      "qualification_failed",
    ]),
  },
) {
  override get message(): string {
    return `Native provider enrollment request failed (${this.code}).`;
  }
}
const MAX_REQUEST_BYTES = 32768;
const MAX_SESSION_LIFETIME_DAYS = 30;
const RequestFile = Argument.String("request-file").pipe(
  Argument.withDescription(
    "Existing closed native enrollment request JSON; retains its reserved session ID on retry.",
  ),
);
const Sha256 = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
const decodeRequestJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(NativeEnrollmentRequest),
);
const decodeRequest = Schema.decodeUnknownEffect(NativeEnrollmentRequest);
const isRequestError = Schema.is(WorkstreamProviderCliRequestError);
const isContextError = Schema.is(NativeEnrollmentCliContextError);
const authorityLocationFlags = {
  baseDir: Flag.String("base-dir").pipe(
    Flag.withDescription("Explicit existing T3 home; never initialized by this command."),
  ),
  authorityDir: Flag.String("authority-dir").pipe(
    Flag.withDescription("Explicit existing native authority directory."),
  ),
};
const locationFlags = {
  ...authorityLocationFlags,
  credentialPath: Flag.String("credential-path").pipe(
    Flag.withDescription("Explicit private credential file under the existing T3 home."),
  ),
  requestFile: RequestFile,
};
const sqliteSidecarEffects = Flag.Literals("sqlite-sidecar-effects", ["deny", "allow"]).pipe(
  Flag.withDefault("deny"),
  Flag.withDescription(
    "deny requires a separately quiesced rollback store; allow is operational inspection that may write WAL/SHM sidecars and grants no operation authority.",
  ),
);
export type WorkstreamProviderCliInput = NativeEnrollmentCliLocation & {
  readonly credentialPath: string;
  readonly requestFile: string;
} & (
    | { readonly operation: "plan"; readonly sqliteSidecarEffects?: "deny" | "allow" }
    | { readonly operation: "apply"; readonly expectedPriorSha256: string | null }
    | {
        readonly operation: "readback";
        readonly expectedCredentialSha256: string;
        readonly sqliteSidecarEffects?: "deny" | "allow";
      }
  );
export interface WorkstreamProviderPrepareInput extends NativeEnrollmentCliLocation {
  readonly requestFile: string;
  readonly ownerId: string;
  readonly principalId: string;
  readonly enrollmentId: string;
  readonly registryOrigin: string;
  readonly lifetimeDays: number;
}
export interface NativeEnrollmentPrepared {
  readonly schema: "jones-code.workstreams-native-enrollment-prepared/v1";
  readonly enrollment_id: string;
  readonly request_sha256: string;
  readonly session_id: string;
  readonly expires_at: string;
  readonly native_context: NativeEnrollmentRequest["context"];
}

const readRequest = (file: string) =>
  Effect.try({
    try: () => {
      const fd = NodeFS.openSync(
        NodePath.resolve(file),
        NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
      );
      try {
        const stat = NodeFS.fstatSync(fd);
        if (
          !stat.isFile() ||
          stat.uid !== process.getuid?.() ||
          (stat.mode & 0o022) !== 0 ||
          stat.size > MAX_REQUEST_BYTES
        )
          throw new WorkstreamProviderCliRequestError({ code: "request_unavailable" });
        const bytes = Buffer.alloc(MAX_REQUEST_BYTES + 1);
        const count = NodeFS.readSync(fd, bytes, 0, bytes.length, 0);
        if (count > MAX_REQUEST_BYTES)
          throw new WorkstreamProviderCliRequestError({ code: "request_unavailable" });
        return new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, count));
      } finally {
        NodeFS.closeSync(fd);
      }
    },
    catch: () => new WorkstreamProviderCliRequestError({ code: "request_unavailable" }),
  }).pipe(
    Effect.flatMap(decodeRequestJson),
    Effect.mapError((error) =>
      isRequestError(error)
        ? error
        : new WorkstreamProviderCliRequestError({ code: "invalid_request" }),
    ),
  );
const unavailableReceipt = (
  request: NativeEnrollmentRequest,
  reason: NativeEnrollmentReceipt["reason"],
): NativeEnrollmentReceipt => ({
  schema: "jones-code.workstreams-native-enrollment-receipt/v1",
  enrollment_id: request.enrollment_id,
  request_sha256: nativeEnrollmentRequestSha256(request),
  state: "unknown",
  completed_phases: [],
  session_id: request.session.session_id,
  native_context: request.context,
  file_pre_sha256: null,
  file_post_sha256: null,
  reason,
});

/**
 * Builds a closed request from persisted qualification and owner-supplied control-plane
 * identity, then publishes it no-clobber. It opens no database and reads no secret.
 */
export const prepareWorkstreamProviderRequest = Effect.fn("prepareWorkstreamProviderRequest")(
  function* (input: WorkstreamProviderPrepareInput) {
    if (
      !Number.isInteger(input.lifetimeDays) ||
      input.lifetimeDays < 1 ||
      input.lifetimeDays > MAX_SESSION_LIFETIME_DAYS ||
      !NodePath.isAbsolute(input.requestFile) ||
      NodePath.resolve(input.requestFile) !== input.requestFile
    )
      return yield* new WorkstreamProviderCliRequestError({ code: "invalid_request" });
    // Operational inspection only selects metadata checks that tolerate a live WAL store;
    // no SQL client is constructed here.
    const trusted = yield* makeTrustedNativeEnrollmentCliContext(
      input,
      "operational_inspection",
    ).pipe(
      Effect.mapError(
        () => new WorkstreamProviderCliRequestError({ code: "qualification_failed" }),
      ),
    );
    const authority = yield* trusted.authority.readCurrent.pipe(
      Effect.mapError(
        () => new WorkstreamProviderCliRequestError({ code: "qualification_failed" }),
      ),
    );
    const now = yield* DateTime.now;
    const issuedAt = DateTime.makeUnsafe(now.epochMilliseconds);
    const request = yield* decodeRequest({
      schema: "jones-code.workstreams-native-enrollment/v1",
      enrollment_id: input.enrollmentId,
      registry_origin: input.registryOrigin,
      context: {
        owner_id: input.ownerId,
        principal_id: input.principalId,
        source_instance_id: authority.environmentId,
        authority_namespace: authority.authorityNamespace,
        store_generation: authority.storeGeneration,
        enrollment_id: input.enrollmentId,
        protocol: WORKSTREAMS_T3_PROVIDER_PROTOCOL,
        build: trusted.build,
      },
      session: {
        session_id: AuthSessionId.make(NodeCrypto.randomUUID()),
        issued_at: DateTime.formatIso(issuedAt),
        expires_at: DateTime.formatIso(DateTime.add(issuedAt, { days: input.lifetimeDays })),
      },
    }).pipe(
      Effect.mapError(() => new WorkstreamProviderCliRequestError({ code: "invalid_request" })),
    );
    const bytes = canonicalNativeEnrollmentRequest(request);
    yield* Effect.try({
      try: () => {
        // `wx` never replaces an earlier request; a retry must reuse that file and its session ID.
        const fd = NodeFS.openSync(
          input.requestFile,
          NodeFS.constants.O_WRONLY |
            NodeFS.constants.O_CREAT |
            NodeFS.constants.O_EXCL |
            NodeFS.constants.O_NOFOLLOW,
          0o600,
        );
        try {
          NodeFS.writeSync(fd, bytes);
          NodeFS.fsyncSync(fd);
        } finally {
          NodeFS.closeSync(fd);
        }
      },
      catch: (cause) =>
        new WorkstreamProviderCliRequestError({
          code:
            typeof cause === "object" &&
            cause !== null &&
            "code" in cause &&
            cause.code === "EEXIST"
              ? "request_exists"
              : "request_unavailable",
        }),
    });
    yield* trusted.verifyUnchanged.pipe(
      Effect.mapError(
        () => new WorkstreamProviderCliRequestError({ code: "qualification_failed" }),
      ),
    );
    return {
      schema: "jones-code.workstreams-native-enrollment-prepared/v1",
      enrollment_id: request.enrollment_id,
      request_sha256: nativeEnrollmentRequestSha256(request),
      session_id: request.session.session_id,
      expires_at: request.session.expires_at,
      native_context: request.context,
    } satisfies NativeEnrollmentPrepared;
  },
);

export const runWorkstreamProviderCliOperation = Effect.fn("runWorkstreamProviderCliOperation")(
  function* (input: WorkstreamProviderCliInput) {
    const request = yield* readRequest(input.requestFile);
    if (
      (input.operation === "apply" &&
        input.expectedPriorSha256 !== null &&
        !/^[a-f0-9]{64}$/.test(input.expectedPriorSha256)) ||
      (input.operation === "readback" && !/^[a-f0-9]{64}$/.test(input.expectedCredentialSha256)) ||
      (input.operation !== "apply" &&
        input.sqliteSidecarEffects !== undefined &&
        input.sqliteSidecarEffects !== "deny" &&
        input.sqliteSidecarEffects !== "allow")
    )
      return yield* new WorkstreamProviderCliRequestError({ code: "invalid_request" });
    return yield* Effect.gen(function* () {
      const access =
        input.operation === "apply"
          ? "apply"
          : input.sqliteSidecarEffects === "allow"
            ? "operational_inspection"
            : "strict_inspection";
      const trusted = yield* makeTrustedNativeEnrollmentCliContext(input, access);
      const run = Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const authority =
          input.operation === "apply"
            ? yield* trusted.bindAuthorityToSqlClient(sql)
            : trusted.authority;
        const sessions = yield* AuthSessions.make;
        const enrollments = yield* makeNativeEnrollments({
          authority,
          build: Effect.succeed(Option.some(trusted.build)),
        }).pipe(Effect.provideService(AuthSessions.AuthSessionRepository, sessions));
        const credential = makeNativeCredentialWriter({
          baseDir: trusted.baseDir,
          credentialPath: input.credentialPath,
        });
        const operations = makeNativeEnrollmentOperations({
          enrollments,
          credential,
          // Plan/readback never construct the secret adapter or materializer and never read signing bytes.
          materialize: (expected) =>
            input.operation !== "apply"
              ? Effect.fail(new SessionCredentialIssueError({ cause: "read_only_operation" }))
              : makeReservedBearerSessionMaterializer.pipe(
                  Effect.provideService(AuthSessions.AuthSessionRepository, sessions),
                  Effect.provideService(
                    ServerSecretStore,
                    makeExistingEnrollmentSecretStore({
                      baseDir: trusted.baseDir,
                      secretsDir: trusted.secretsDir,
                    }),
                  ),
                  Effect.flatMap((materialize) => materialize(expected)),
                ),
        });
        yield* trusted.verifyUnchanged;
        const result = yield* input.operation === "plan"
          ? operations.plan(request)
          : input.operation === "readback"
            ? operations.readback(request, input.expectedCredentialSha256)
            : operations.apply(request, input.expectedPriorSha256);
        const qualified = yield* trusted.verifyUnchanged.pipe(Effect.result);
        return qualified._tag === "Success"
          ? result
          : { ...result, state: "unknown" as const, reason: "qualification_failed" as const };
      });
      // The fixed URI and actual runtime gate are qualified before constructing this raw client.
      // It neither migrates nor changes journal mode; its scope closes only its own connection.
      return yield* run.pipe(Effect.provide(NodeSqliteClient.layer(trusted.sqlite)), Effect.scoped);
    }).pipe(
      Effect.catch((error) =>
        Effect.succeed(
          unavailableReceipt(
            request,
            isContextError(error) && error.code === "database_wal_unqualified"
              ? "records_unavailable"
              : "qualification_failed",
          ),
        ),
      ),
    );
  },
);

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const showReceipt = (input: WorkstreamProviderCliInput) =>
  runWorkstreamProviderCliOperation(input).pipe(
    Effect.flatMap((receipt) => Console.log(encodeJson(receipt))),
  );
const prepareCommand = Command.make("prepare", {
  ...authorityLocationFlags,
  ownerId: Flag.String("owner-id").pipe(
    Flag.withDescription("Control-plane owner ID provisioned for this enrollment."),
  ),
  principalId: Flag.String("principal-id").pipe(
    Flag.withDescription("Control-plane principal ID provisioned for this enrollment."),
  ),
  enrollmentId: Flag.String("enrollment-id").pipe(
    Flag.withDescription("New stable native enrollment ID; a later rotation uses a new ID."),
  ),
  registryOrigin: Flag.String("registry-origin").pipe(
    Flag.withDescription(
      "Exact control-plane origin: HTTPS, or HTTP on literal 127.0.0.1 or [::1] for the same host.",
    ),
  ),
  lifetimeDays: Flag.Int("lifetime-days").pipe(
    Flag.withDefault(MAX_SESSION_LIFETIME_DAYS),
    Flag.withDescription("Native bearer session lifetime in days, 1 to 30."),
  ),
  requestFile: Argument.String("request-file").pipe(
    Argument.withDescription("New absolute request path; an existing file is never replaced."),
  ),
}).pipe(
  Command.withDescription(
    "Write a closed native enrollment request from persisted qualification; opens no database and reads no secret.",
  ),
  Command.withHandler((flags) =>
    prepareWorkstreamProviderRequest(flags).pipe(
      Effect.flatMap((prepared) => Console.log(encodeJson(prepared))),
    ),
  ),
);
const planCommand = Command.make("plan", { ...locationFlags, sqliteSidecarEffects }).pipe(
  Command.withDescription(
    "Inspect existing qualification, records and credential hash; operational inspection may write SQLite sidecars when explicitly selected.",
  ),
  Command.withHandler((flags) => showReceipt({ ...flags, operation: "plan" })),
);
const applyCommand = Command.make("apply", {
  ...locationFlags,
  expectedPriorSha256: Flag.String("expected-prior-sha256").pipe(
    Flag.withSchema(Schema.Union([Schema.Literal("absent"), Sha256])),
  ),
}).pipe(
  Command.withDescription(
    "Explicitly reserve native enrollment and publish its private credential; preserve conflicting existing bytes.",
  ),
  Command.withHandler((flags) =>
    showReceipt({
      ...flags,
      operation: "apply",
      expectedPriorSha256:
        flags.expectedPriorSha256 === "absent" ? null : flags.expectedPriorSha256,
    }),
  ),
);
const readbackCommand = Command.make("readback", {
  ...locationFlags,
  sqliteSidecarEffects,
  expectedCredentialSha256: Flag.String("expected-credential-sha256").pipe(Flag.withSchema(Sha256)),
}).pipe(
  Command.withDescription(
    "Read back exact reserved records and the credential hash; operational inspection may write SQLite sidecars when explicitly selected.",
  ),
  Command.withHandler((flags) => showReceipt({ ...flags, operation: "readback" })),
);
export const workstreamProviderCommand = Command.make("provider").pipe(
  Command.withDescription(
    "Prepare explicit native provider enrollment at existing locations. Commands and sidecar flags grant no operation authority.",
  ),
  Command.withSubcommands([prepareCommand, planCommand, applyCommand, readbackCommand]),
);
