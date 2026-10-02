import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  WORKSTREAMS_REGISTRATION_CONTEXT_MAX_RESPONSE_BYTES,
  WorkstreamsRegistrationContextResponse,
  type WorkstreamsRegistrationContextT3Source,
} from "../../../../../packages/contracts/src/workstreamsRegistrationContext.ts";
import { NativeStoreAuthorityPersistenceError } from "../../environment/nativeStoreAuthorityPersistence.ts";
import {
  makeRegistrationFixture,
  build,
  response,
  t3Source,
  githubSource,
  encodeFixtureJson,
  fixtureTransportError,
} from "./testFixtures.ts";
import { RegistrationContextError, type T3RegistrationAvailability } from "./service.ts";

it.effect("qualifies closed current T3 and independent GitHub registration descriptors", () =>
  Effect.gen(function* () {
    const fixture = makeRegistrationFixture();
    const result = yield* fixture.service.read();
    assert.deepEqual(result.context, response);
    assert.deepEqual(result.t3Availability, { state: "ready" });
    yield* Schema.decodeEffect(WorkstreamsRegistrationContextResponse)(result.context);
    assert.deepEqual(fixture.reads, { registry: 1, authority: 1, build: 1 });
  }),
);

it.effect(
  "wrong owner, principal or authorization revision fails the whole context before native qualification",
  () =>
    Effect.gen(function* () {
      for (const changed of [
        { owner_id: "other-owner" },
        { principal_id: "other-principal" },
        { authorization_revision: response.authorization_revision + 1 },
      ]) {
        const fixture = makeRegistrationFixture({
          readRegistrationContext: Effect.succeed(encodeFixtureJson({ ...response, ...changed })),
        });
        const error = yield* fixture.service.read().pipe(Effect.flip);
        assert.instanceOf(error, RegistrationContextError);
        assert.strictEqual(error.reason, "binding_mismatch");
        assert.strictEqual(fixture.reads.authority, 0);
        assert.strictEqual(fixture.reads.build, 0);
      }
    }),
);

it.effect(
  "source, authority, generation and build mismatches omit only T3 and retain the qualification reason",
  () =>
    Effect.gen(function* () {
      const cases = [
        { source: { ...t3Source, source_instance_id: "other-source" }, reason: "source_mismatch" },
        {
          source: { ...t3Source, authority_namespace: "other-authority" },
          reason: "authority_mismatch",
        },
        {
          source: { ...t3Source, store_generation: t3Source.store_generation + 1 },
          reason: "generation_mismatch",
        },
        {
          source: { ...t3Source, build: { ...build, sha: "c".repeat(40) } },
          reason: "contract_mismatch",
        },
        {
          source: { ...t3Source, build: { ...build, tree: "c".repeat(40) } },
          reason: "contract_mismatch",
        },
      ] satisfies ReadonlyArray<{
        readonly source: WorkstreamsRegistrationContextT3Source;
        readonly reason: Extract<T3RegistrationAvailability, { state: "unavailable" }>["reason"];
      }>;
      for (const entry of cases) {
        const fixture = makeRegistrationFixture({
          readRegistrationContext: Effect.succeed(
            encodeFixtureJson({ ...response, sources: [entry.source, githubSource] }),
          ),
        });
        const result = yield* fixture.service.read();
        assert.deepEqual(result.context, { ...response, sources: [githubSource] });
        assert.deepEqual(result.t3Availability, { state: "unavailable", reason: entry.reason });
        yield* Schema.decodeEffect(WorkstreamsRegistrationContextResponse)(result.context);
      }
    }),
);

it.effect(
  "missing T3 descriptors keep empty or GitHub-only context independent of native availability",
  () =>
    Effect.gen(function* () {
      for (const sources of [[], [githubSource]]) {
        const fixture = makeRegistrationFixture({
          readRegistrationContext: Effect.succeed(encodeFixtureJson({ ...response, sources })),
        });
        const result = yield* fixture.service.read();
        assert.deepEqual(result.context, { ...response, sources });
        assert.deepEqual(result.t3Availability, {
          state: "unavailable",
          reason: "descriptor_absent",
        });
        assert.strictEqual(fixture.reads.authority, 0);
        assert.strictEqual(fixture.reads.build, 0);
      }
    }),
);

it.effect(
  "fenced, unavailable or unstamped native state filters T3 without leaking private causes",
  () =>
    Effect.gen(function* () {
      const privateCause =
        "Bearer synthetic-private-header https://private-origin.invalid session-private";
      for (const code of ["fenced", "missing"] as const) {
        const fixture = makeRegistrationFixture({
          authority: {
            readCurrent: Effect.fail(
              new NativeStoreAuthorityPersistenceError(code, privateCause, privateCause),
            ),
          },
        });
        const result = yield* fixture.service.read();
        assert.deepEqual(result.context.sources, [githubSource]);
        assert.deepEqual(result.t3Availability, {
          state: "unavailable",
          reason: code === "fenced" ? "authority_fenced" : "authority_unavailable",
        });
        assert.strictEqual(encodeFixtureJson(result).includes(privateCause), false);
      }
      const unstamped = makeRegistrationFixture({ build: Effect.succeedNone });
      const result = yield* unstamped.service.read();
      assert.deepEqual(result.context.sources, [githubSource]);
      assert.deepEqual(result.t3Availability, {
        state: "unavailable",
        reason: "build_unavailable",
      });
      assert.strictEqual(unstamped.reads.authority, 0);
    }),
);

it.effect(
  "closed decode rejects wrong family/native protocols and caller or private wire fields",
  () =>
    Effect.gen(function* () {
      const invalid = [
        { ...response, protocol: "workstreams-registration-context/2.0.0" },
        { ...response, owner_selector: response.owner_id },
        { ...response, registry_origin: "https://private-origin.invalid" },
        { ...response, session_id: "private-session" },
        { ...response, authorization_header: "Bearer synthetic-private-header" },
        {
          ...response,
          sources: [{ ...t3Source, native_protocol: "workstreams-t3-provider/2.0.0" }],
        },
        {
          ...response,
          sources: [{ ...t3Source, build: { ...build, repository: "other/repository" } }],
        },
        { ...response, sources: [{ ...t3Source, native_id: "caller-selected-thread" }] },
        { ...response, sources: [{ ...githubSource, credential_locator: "/private/credential" }] },
        { ...response, sources: [t3Source, t3Source] },
        { ...response, sources: [t3Source, githubSource, githubSource] },
      ];
      for (const value of invalid) {
        const fixture = makeRegistrationFixture({
          readRegistrationContext: Effect.succeed(encodeFixtureJson(value)),
        });
        const error = yield* fixture.service.read().pipe(Effect.flip);
        assert.strictEqual(error.reason, "invalid_response");
        assert.strictEqual(fixture.reads.authority, 0);
        assert.strictEqual(encodeFixtureJson(error).includes("private-origin"), false);
        assert.strictEqual(encodeFixtureJson(error).includes("synthetic-private-header"), false);
      }
    }),
);

it.effect("enforces the 8KiB UTF-8 and depth limits before closed response decoding", () =>
  Effect.gen(function* () {
    const text = encodeFixtureJson(response);
    const bounded = text.padEnd(WORKSTREAMS_REGISTRATION_CONTEXT_MAX_RESPONSE_BYTES, " ");
    assert.deepEqual(
      (yield* makeRegistrationFixture({
        readRegistrationContext: Effect.succeed(bounded),
      }).service.read()).context,
      response,
    );
    const cases = [
      { text: bounded + " ", reason: "response_too_large" },
      { text: '"' + "é".repeat(4_096) + '"', reason: "response_too_large" },
      { text: "[".repeat(11) + "null" + "]".repeat(11), reason: "response_too_deep" },
      { text: "not-json", reason: "invalid_response" },
    ];
    for (const entry of cases) {
      const fixture = makeRegistrationFixture({
        readRegistrationContext: Effect.succeed(entry.text),
      });
      const error = yield* fixture.service.read().pipe(Effect.flip);
      assert.strictEqual(error.reason, entry.reason);
      assert.strictEqual(fixture.reads.authority, 0);
    }
  }),
);

it.effect(
  "disabled activation and transport failure expose fixed reasons with no report cache",
  () =>
    Effect.gen(function* () {
      const disabled = makeRegistrationFixture({ configuredBinding: null });
      assert.strictEqual((yield* disabled.service.read().pipe(Effect.flip)).reason, "unconfigured");
      assert.deepEqual(disabled.reads, { registry: 0, authority: 0, build: 0 });
      const failed = makeRegistrationFixture({
        readRegistrationContext: Effect.fail(
          fixtureTransportError("private-origin Bearer private-header"),
        ),
      });
      const failure = yield* failed.service.read().pipe(Effect.flip);
      assert.strictEqual(failure.reason, "registry_unavailable");
      assert.strictEqual(encodeFixtureJson(failure).includes("private-origin"), false);
      const fixture = makeRegistrationFixture();
      yield* fixture.service.read();
      yield* fixture.service.read();
      assert.deepEqual(fixture.reads, { registry: 2, authority: 2, build: 2 });
    }),
);
