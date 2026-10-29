import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as TestClock from "effect/testing/TestClock";
import * as Origin from "./peerEffectOrigin.ts";
import * as Retained from "./retainedPeerSession.ts";
import * as Fixture from "./peerEffectRecovery.testkit.ts";

vi.mock("../../../../../package.json", () => ({
  default: {
    version: "0.0.0-preview.20261002.100",
    jonesSource: {
      repository: "Jones-Systems/Jones-Code",
      sha: "a".repeat(40),
      tree: "b".repeat(40),
    },
  },
}));
it.effect(
  "rechecks the exact original receiving native session without creating an invocation",
  () =>
    Fixture.withFixture((f) =>
      Effect.gen(function* () {
        const origins = yield* Origin.PeerEffectOrigins;
        const sessions = yield* Retained.OriginalReceivingSession;
        yield* origins.withOrigin(f.command.command_id, (origin) =>
          Effect.gen(function* () {
            let escaped: Retained.CurrentRetainedPeer | undefined;
            yield* sessions.withCurrent(origin, (proof) =>
              Effect.gen(function* () {
                escaped = proof;
                expect(proof.pin.session.session_id).toBe(f.pin.session.session_id);
                expect(proof.pin.pin_id).not.toBe(origin.parent.identity.member_pin_id);
                expect(proof.grant.grant_version).toBe(1);
                expect(proof.ceiling.permissions).toContain("workstreams:write");
                yield* f.sql.withTransaction(Retained.checkRetainedPeer(proof));
                expect(
                  (yield* Retained.checkRetainedPeer({ ...proof }).pipe(Effect.result))._tag,
                ).toBe("Failure");
              }),
            );
            if (escaped)
              expect((yield* Retained.checkRetainedPeer(escaped).pipe(Effect.result))._tag).toBe(
                "Failure",
              );
            expect((yield* f.native.current({ ...f.invocation }).pipe(Effect.result))._tag).toBe(
              "Failure",
            );
          }),
        );
      }),
    ),
);
it.effect("retained preparation rejects ambient writers before encrypted pin acquisition", () =>
  Fixture.withFixture((f) =>
    Effect.gen(function* () {
      const origins = yield* Origin.PeerEffectOrigins;
      const sessions = yield* Retained.OriginalReceivingSession;
      yield* origins.withOrigin(f.command.command_id, (origin) =>
        Effect.gen(function* () {
          let calls = 0;
          f.setSecretHook(() =>
            Effect.sync(() => {
              calls++;
            }),
          );
          const error = yield* f.sql
            .withTransaction(sessions.withCurrent(origin, () => Effect.void))
            .pipe(Effect.flip);
          expect(error).toMatchObject({ code: "preparation_in_transaction" });
          expect(calls).toBe(0);
        }),
      );
    }),
  ),
);
it.effect(
  "revocation of the original session is not repaired by issuing a replacement session",
  () =>
    Fixture.withFixture((f) =>
      Effect.gen(function* () {
        const origins = yield* Origin.PeerEffectOrigins;
        const sessions = yield* Retained.OriginalReceivingSession;
        yield* f.sessions.revoke({
          sessionId: f.sessionId,
          revokedAt: yield* DateTime.now,
        });
        const replacement = yield* f.auth.issueMcpClientSession({
          label: "replacement is not the original",
          access: "full-access",
          peerLink: true,
          client: { deviceType: "bot" },
        });
        expect(replacement.sessionId).not.toBe(f.pin.session.session_id);
        yield* origins.withOrigin(f.command.command_id, (origin) =>
          Effect.gen(function* () {
            expect(
              (yield* sessions.withCurrent(origin, () => Effect.void).pipe(Effect.result))._tag,
            ).toBe("Failure");
          }),
        );
      }),
    ),
);
it.effect("revocation after preparation fences the same writer-local proof", () =>
  Fixture.withFixture((f) =>
    Effect.gen(function* () {
      const origins = yield* Origin.PeerEffectOrigins;
      const sessions = yield* Retained.OriginalReceivingSession;
      yield* origins.withOrigin(f.command.command_id, (origin) =>
        sessions.withCurrent(origin, (proof) =>
          Effect.gen(function* () {
            yield* f.sessions.revoke({
              sessionId: f.sessionId,
              revokedAt: yield* DateTime.now,
            });
            expect(
              (yield* f.sql.withTransaction(Retained.checkRetainedPeer(proof)).pipe(Effect.result))
                ._tag,
            ).toBe("Failure");
          }),
        ),
      );
    }),
  ),
);
it.effect("a newly narrowed current home grant fences completion", () =>
  Fixture.withFixture((f) =>
    Effect.gen(function* () {
      const origins = yield* Origin.PeerEffectOrigins;
      const sessions = yield* Retained.OriginalReceivingSession;
      yield* origins.withOrigin(f.command.command_id, (origin) =>
        sessions.withCurrent(origin, (proof) =>
          Effect.gen(function* () {
            yield* f.registry.appendPlain(Fixture.scope, "grants", {
              grant_id: "grant:narrowed",
              principal_id: "principal:test",
              grant_version: 2,
              permissions: JSON.stringify(["workstreams:read"]),
              state: "active",
            });
            expect((yield* Retained.checkRetainedPeer(proof).pipe(Effect.result))._tag).toBe(
              "Failure",
            );
          }),
        ),
      );
    }),
  ),
);
it.effect("a revoked source binding does not inherit authority from its historical receipt", () =>
  Fixture.withFixture((f) =>
    Effect.gen(function* () {
      const origins = yield* Origin.PeerEffectOrigins;
      const sessions = yield* Retained.OriginalReceivingSession;
      yield* f.registry.appendPlain(Fixture.scope, "source_bindings", {
        provider: "t3",
        source_instance_id: "source:test",
        binding_version: 2,
        authority_namespace: "source-store",
        store_generation: 3,
        state: "revoked",
      });
      yield* origins.withOrigin(f.command.command_id, (origin) =>
        sessions
          .withCurrent(origin, () => Effect.void)
          .pipe(
            Effect.result,
            Effect.map((r) => expect(r._tag).toBe("Failure")),
          ),
      );
    }),
  ),
);
it.effect("pin revocation changes its retained revision and fences the original preparation", () =>
  Fixture.withFixture((f) =>
    Effect.gen(function* () {
      const origins = yield* Origin.PeerEffectOrigins;
      const sessions = yield* Retained.OriginalReceivingSession;
      yield* origins.withOrigin(f.command.command_id, (origin) =>
        sessions.withCurrent(origin, (proof) =>
          Effect.gen(function* () {
            yield* f.records.transition({ ...f.pin, state: "revoked", revision: 3 }, 2);
            expect((yield* Retained.checkRetainedPeer(proof).pipe(Effect.result))._tag).toBe(
              "Failure",
            );
          }),
        ),
      );
    }),
  ),
);
it.effect("expiry of the original pin is checked again after preparation", () =>
  Fixture.withFixture((f) =>
    Effect.gen(function* () {
      const origins = yield* Origin.PeerEffectOrigins;
      const sessions = yield* Retained.OriginalReceivingSession;
      yield* origins.withOrigin(f.command.command_id, (origin) =>
        sessions.withCurrent(origin, (proof) =>
          Effect.gen(function* () {
            yield* TestClock.setTime(Date.parse(f.pin.expires_at));
            expect((yield* Retained.checkRetainedPeer(proof).pipe(Effect.result))._tag).toBe(
              "Failure",
            );
          }),
        ),
      );
    }),
  ),
);
it.effect("auth-row scope narrowing removes write permission without inventing a new caller", () =>
  Fixture.withFixture((f) =>
    Effect.gen(function* () {
      const origins = yield* Origin.PeerEffectOrigins;
      const sessions = yield* Retained.OriginalReceivingSession;
      yield* f.sql`UPDATE auth_sessions SET scopes=${JSON.stringify(["orchestration:read"])} WHERE session_id=${f.pin.session.session_id}`;
      yield* origins.withOrigin(f.command.command_id, (origin) =>
        sessions
          .withCurrent(origin, () => Effect.void)
          .pipe(
            Effect.result,
            Effect.map((r) => expect(r._tag).toBe("Failure")),
          ),
      );
    }),
  ),
);
it.effect(
  "changed original-session facts refuse even while the session row is otherwise live",
  () =>
    Fixture.withFixture((f) =>
      Effect.gen(function* () {
        const origins = yield* Origin.PeerEffectOrigins;
        const sessions = yield* Retained.OriginalReceivingSession;
        yield* f.sql`UPDATE auth_sessions SET subject='replacement-subject' WHERE session_id=${f.pin.session.session_id}`;
        yield* origins.withOrigin(f.command.command_id, (origin) =>
          sessions
            .withCurrent(origin, () => Effect.void)
            .pipe(
              Effect.result,
              Effect.map((r) => expect(r._tag).toBe("Failure")),
            ),
        );
        const row = yield* f.sessions.getById({ sessionId: f.sessionId });
        expect(Option.isSome(row)).toBe(true);
      }),
    ),
);

it.effect("preserves the callback failure through both genuine preparation scopes", () =>
  Fixture.withFixture((f) =>
    Effect.gen(function* () {
      const origins = yield* Origin.PeerEffectOrigins;
      const sessions = yield* Retained.OriginalReceivingSession;
      const marker = { code: "provider_refused_after_observation" };
      const error = yield* origins
        .withOrigin(f.command.command_id, (origin) =>
          sessions.withCurrent(origin, () => Effect.fail(marker)),
        )
        .pipe(Effect.flip);
      expect(error).toBe(marker);
    }),
  ),
);
