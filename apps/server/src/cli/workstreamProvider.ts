// @effect-diagnostics nodeBuiltinImport:off -- The standalone CLI reads only the explicitly named enrollment request.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { Argument, Command, Flag } from "effect/unstable/cli";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as AuthSessions from "../persistence/AuthSessions.ts";
import {
  makeReservedBearerSessionMaterializer,
  SessionCredentialIssueError,
} from "../auth/SessionStore.ts";
import { ServerSecretStore } from "../auth/ServerSecretStore.ts";
import { deriveServerPaths } from "../config.ts";
import { makeNativeCredentialWriter } from "../workstreams/enrollment/credentialFile.ts";
import {
  makeNativeEnrollmentOperations,
  type NativeEnrollmentReceipt,
} from "../workstreams/enrollment/operations.ts";
import {
  NativeEnrollmentRequest,
  nativeEnrollmentRequestSha256,
} from "../workstreams/enrollment/request.ts";
import { makeExistingEnrollmentSecretStore } from "../workstreams/enrollment/secretStore.ts";
import { makeNativeEnrollments } from "../workstreams/enrollment/service.ts";
import {
  makeTrustedNativeEnrollmentCliContext,
  NativeEnrollmentCliContextError,
  type NativeEnrollmentCliLocation,
} from "../workstreams/enrollment/trustedCliContext.ts";

export class WorkstreamProviderCliRequestError extends Schema.TaggedError<WorkstreamProviderCliRequestError>()(
  "WorkstreamProviderCliRequestError",
  {
    code: Schema.Literals(["request_unavailable", "invalid_request"]),
  },
) {
  override get message(): string {
    return `Native provider enrollment request failed (${this.code}).`;
  }
}
const RequestFile = Argument.String("request-file").pipe(
  Argument.withDescription(
    "Existing closed native enrollment request JSON; retains its reserved session ID on retry.",
  ),
);
const Sha256 = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
const encodeReceiptJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const locationFlags = {
  baseDir: Flag.String("base-dir").pipe(
    Flag.withDescription("Explicit existing T3 home; never initialized by this command."),
  ),
  authorityDir: Flag.String("authority-dir").pipe(
    Flag.withDescription("Explicit existing native authority directory."),
  ),
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
          stat.size > 32768
        )
          throw new WorkstreamProviderCliRequestError({ code: "request_unavailable" });
        const bytes = Buffer.alloc(32769);
        const count = NodeFS.readSync(fd, bytes, 0, bytes.length, 0);
        if (count > 32768)
          throw new WorkstreamProviderCliRequestError({ code: "request_unavailable" });
        return new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, count));
      } finally {
        NodeFS.closeSync(fd);
      }
    },
    catch: () => new WorkstreamProviderCliRequestError({ code: "request_unavailable" }),
  }).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(Schema.fromJsonString(NativeEnrollmentRequest))),
    Effect.mapError((error) =>
      Schema.is(WorkstreamProviderCliRequestError)(error)
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
            Schema.is(NativeEnrollmentCliContextError)(error) &&
              error.code === "database_wal_unqualified"
              ? "records_unavailable"
              : "qualification_failed",
          ),
        ),
      ),
    );
  },
);

const showReceipt = Effect.fn("workstreamProvider.showReceipt")(function* (
  input:
    | Omit<Extract<WorkstreamProviderCliInput, { readonly operation: "plan" }>, "dbPath">
    | Omit<Extract<WorkstreamProviderCliInput, { readonly operation: "apply" }>, "dbPath">
    | Omit<Extract<WorkstreamProviderCliInput, { readonly operation: "readback" }>, "dbPath">,
) {
  const { dbPath } = yield* deriveServerPaths(input.baseDir, undefined, {
    baseDirIsExplicit: true,
  });
  const receipt = yield* runWorkstreamProviderCliOperation({ ...input, dbPath });
  yield* Console.log(yield* encodeReceiptJson(receipt).pipe(Effect.orDie));
});
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
  Command.withSubcommands([planCommand, applyCommand, readbackCommand]),
);
