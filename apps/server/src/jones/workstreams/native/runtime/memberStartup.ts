import { decodeWorkstreamsHomeBindBody } from "@t3tools/contracts/jones/workstreamsHomeBind";
import { canonicalWorkstreamsMemberEnrollment } from "@t3tools/contracts/jones/workstreamsMemberEnrollment";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import * as Keys from "../crypto/keyProvider.ts";
import { decodeNativeConfig, type NativeConfig } from "../mode/config.ts";
import * as Members from "./memberEnrollment.ts";
import { StartupError, type Selection } from "./startup.ts";

/** Read-only qualification of selected local resources, never current home permission or a mount. */
export const selectMember = Effect.fn("NativeMemberStartup.selectMember")(function* (input: {
  readonly text: string | null;
  readonly legacyActivationPresent: boolean;
  readonly home: string;
  readonly database: string;
  readonly environmentId: string;
}): Effect.fn.Return<Selection, never, Path.Path | SqlClient.SqlClient> {
  let config: NativeConfig | null = null;
  const blocked = (code: StartupError["code"]): Selection => ({
    mode: "transition-fenced",
    config,
    home: null,
    sourceSessionId: null,
    problem: new StartupError({ code }),
  });
  if (input.text === null) return blocked("explicit_selection_required");
  const decoded = yield* Effect.try({
    try: () => decodeNativeConfig(input.text!),
    catch: () => new StartupError({ code: "invalid_config" }),
  }).pipe(Effect.result);
  if (decoded._tag === "Failure") return blocked(decoded.failure.code);
  config = decoded.success;
  if (config.mode !== "native-member") return blocked("invalid_config");
  if (input.legacyActivationPresent) return blocked("authority_fenced");
  if (!config.registry || !config.bootstrap?.source_session_id)
    return blocked("explicit_selection_required");
  const registry = config.registry,
    bootstrap = config.bootstrap,
    sourceSessionId = config.bootstrap.source_session_id;
  const path = yield* Path.Path;
  if (
    !path.isAbsolute(input.home) ||
    !path.isAbsolute(input.database) ||
    !path.isAbsolute(bootstrap.home) ||
    !path.isAbsolute(bootstrap.database) ||
    path.resolve(bootstrap.home) !== path.resolve(input.home) ||
    path.resolve(bootstrap.database) !== path.resolve(input.database)
  )
    return blocked("database_mismatch");
  const sql = yield* SqlClient.SqlClient;
  const databaseCurrent = Effect.gen(function* () {
    const databases = yield* sql<{ name: string; file: string }>`PRAGMA database_list`.pipe(
      Effect.mapError(() => new StartupError({ code: "database_mismatch" })),
    );
    const main = databases.filter((row) => row.name === "main");
    if (
      main.length !== 1 ||
      !main[0]?.file ||
      !path.isAbsolute(main[0].file) ||
      path.resolve(main[0].file) !== path.resolve(bootstrap.database)
    )
      return yield* new StartupError({ code: "database_mismatch" });
  });
  const checked = yield* Effect.gen(function* () {
    yield* databaseCurrent;
    const members = yield* Effect.serviceOption(Members.MemberEnrollments);
    if (Option.isNone(members)) return yield* new StartupError({ code: "authority_fenced" });
    const keys = yield* Effect.serviceOption(Keys.WorkstreamsKeyProvider);
    if (Option.isNone(keys)) return yield* new StartupError({ code: "key_unavailable" });
    const read = Effect.gen(function* () {
      const bound = yield* members.value.getValidatedBySessionId(sourceSessionId).pipe(
        Effect.mapError(
          (error) =>
            new StartupError({
              code: error.code === "key_unavailable" ? "key_unavailable" : "authority_fenced",
            }),
        ),
      );
      if (Option.isNone(bound)) return yield* new StartupError({ code: "authority_fenced" });
      return yield* Effect.try({
        try: () => {
          const record = bound.value,
            body = decodeWorkstreamsHomeBindBody(record.intent.bind_request_body),
            home = body.expected_home,
            source = body.source;
          if (
            home.registry_instance_id !== registry.registry_instance_id ||
            home.owner_id !== registry.owner_id ||
            home.home_environment_id !== registry.home_environment_id ||
            home.server_generation !== registry.server_generation ||
            body.expected_canonical_origin !== registry.canonical_origin ||
            source.enrollment.registry_origin !== registry.canonical_origin ||
            source.authority.environment_id !== input.environmentId ||
            source.enrollment.context.source_instance_id !== input.environmentId ||
            source.enrollment.session.session_id !== sourceSessionId
          )
            throw new StartupError({ code: "binding_mismatch" });
          return canonicalWorkstreamsMemberEnrollment(record);
        },
        catch: () => new StartupError({ code: "binding_mismatch" }),
      });
    });
    // M owns source/build/session/pin/singleton/link currentness and local-envelope decryption.
    // Capture immutable bytes before key acquisition so in-place changes cannot hide drift.
    const before = yield* read;
    const localScope = { ...registry, key_scope: "local" as const };
    const keyId = yield* keys.value.activeKeyId(localScope);
    yield* keys.value.get(keyId);
    if ((yield* keys.value.activeKeyId(localScope)) !== keyId)
      return yield* new StartupError({ code: "key_unavailable" });
    if ((yield* read) !== before) return yield* new StartupError({ code: "binding_mismatch" });
    yield* databaseCurrent;
  }).pipe(
    Effect.mapError((error) =>
      error._tag === "KeyUnavailable" ? new StartupError({ code: "key_unavailable" }) : error,
    ),
    Effect.result,
  );
  if (checked._tag === "Failure") return blocked(checked.failure.code);
  return { mode: "native-member", config, home: null, sourceSessionId, problem: null };
});
