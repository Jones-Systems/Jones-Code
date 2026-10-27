import * as NodeServices from "@effect/platform-node/NodeServices";
import { EnvironmentId } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";

import * as ServerSecretStore from "../../auth/ServerSecretStore.ts";
import * as ServerConfig from "../../config.ts";
import * as ServerEnvironment from "../../environment/ServerEnvironment.ts";
import * as PeerLinks from "../../peer/PeerLinks.ts";
import { descriptorOf } from "../../peer/PeerLinks.testkit.ts";
import * as Sqlite from "../../persistence/Sqlite.ts";

const target = descriptorOf("coalesced-0", "Peer");
const fixture = (http: HttpClient.HttpClient) =>
  PeerLinks.layer.pipe(
    Layer.provideMerge(Sqlite.layerMemory),
    Layer.provide(
      Layer.mock(ServerSecretStore.ServerSecretStore)({
        get: () => Effect.succeedSome(new TextEncoder().encode("synthetic-token")),
      }),
    ),
    Layer.provide(
      Layer.succeed(ServerEnvironment.ServerEnvironment, {
        getEnvironmentId: Effect.succeed(EnvironmentId.make("source")),
        getDescriptor: Effect.succeed(descriptorOf("source", "Source")),
      }),
    ),
    Layer.provide(Layer.succeed(HttpClient.HttpClient, http)),
    Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-peer-lifecycle-" })),
    Layer.provide(NodeServices.layer),
    Layer.fresh,
  );

const insert = (sql: SqlClient.SqlClient, environmentId: string, url: string) =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    yield* sql`
      INSERT INTO jones_peer_environment_links (
        environment_id, secret_name, label, urls_json, access, linked_at, expires_at,
        last_reached_at, last_error, fenced
      ) VALUES (
        ${environmentId}, ${`generation-${environmentId}`}, ${environmentId}, ${JSON.stringify([url])},
        'auto', ${DateTime.formatIso(now)}, ${DateTime.formatIso(DateTime.add(now, { days: 1 }))},
        NULL, NULL, 1
      )
    `;
  });

it.effect(
  "descriptor sharing is in-flight only, normalizes origins, and keeps each identity check",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        let gets = 0;
        const origins: Array<string> = [];
        const http = HttpClient.make((request, url) =>
          Effect.gen(function* () {
            gets++;
            origins.push(url.origin);
            yield* Deferred.succeed(started, undefined);
            yield* Deferred.await(release);
            return HttpClientResponse.fromWeb(request, Response.json(target));
          }),
        );
        const context = yield* fixture(http).pipe(Layer.build);
        const links = Context.get(context, PeerLinks.PeerLinks);
        const sql = Context.get(context, SqlClient.SqlClient);
        for (let index = 0; index < 8; index++) {
          yield* insert(sql, `coalesced-${index}`, `https://peer.invalid/path-${index}`);
        }
        const listing = yield* links.list.pipe(Effect.forkChild);
        yield* Deferred.await(started);
        yield* Deferred.succeed(release, undefined);
        const results = yield* Fiber.join(listing);
        expect(gets).toBe(1);
        expect(results.map((link) => link.status)).toEqual([
          "reachable",
          ...Array(7).fill("unreachable"),
        ]);
        expect(origins).toEqual(["https://peer.invalid"]);
        // A completed descriptor is never an identity cache, including after a wrong identity.
        yield* links.resolve(EnvironmentId.make("coalesced-0"));
        expect(gets).toBe(2);
        expect(
          (yield* links.resolve(EnvironmentId.make("coalesced-1")).pipe(Effect.flip)).reason,
        ).toBe("unreachable");
        expect(gets).toBe(3);
      }),
    ),
);

it.effect("both old-generation outcome writes are ignored after replacement or unlink", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const http = HttpClient.make((request) =>
        Effect.succeed(HttpClientResponse.fromWeb(request, Response.json(target))),
      );
      const context = yield* fixture(http).pipe(Layer.build);
      const links = Context.get(context, PeerLinks.PeerLinks);
      const sql = Context.get(context, SqlClient.SqlClient);
      const environmentId = target.environmentId;
      yield* insert(sql, environmentId, "https://peer.invalid");
      const resolved = yield* links.resolve(environmentId);
      expect(yield* links.currentGeneration(environmentId)).toEqual(
        Option.some(resolved.generation),
      );
      yield* sql`UPDATE jones_peer_environment_links SET secret_name = 'replacement', last_error = 'current failure', last_reached_at = NULL`;
      yield* links.recordOutcome(environmentId, resolved.generation, { error: null });
      yield* links.recordOutcome(environmentId, resolved.generation, { error: "obsolete failure" });
      const [row] = yield* sql<{
        last_error: string | null;
        last_reached_at: string | null;
      }>`SELECT last_error, last_reached_at FROM jones_peer_environment_links`;
      expect(row).toEqual({ last_error: "current failure", last_reached_at: null });
      yield* links.recordOutcome(environmentId, "replacement", { error: null });
      expect(Option.getOrThrow(yield* links.get(environmentId)).lastError).toBeNull();
      yield* sql`DELETE FROM jones_peer_environment_links WHERE environment_id = ${environmentId}`;
      yield* links.recordOutcome(environmentId, "replacement", { error: "after unlink" });
      expect(yield* links.currentGeneration(environmentId)).toEqual(Option.none());
    }),
  ),
);
