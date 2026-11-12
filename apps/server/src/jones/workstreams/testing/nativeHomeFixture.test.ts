// @effect-diagnostics nodeBuiltinImport:off -- Test-owned disk fixtures are removed in finally after all scoped consumers close.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeSqlite from "node:sqlite";
import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as TestClock from "effect/testing/TestClock";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Wire from "@t3tools/contracts";
import * as Net from "@t3tools/shared/Net";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpPlatform from "effect/http/HttpPlatform";
import * as Etag from "effect/http/Etag";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import { Command } from "effect/cli";
import * as Secrets from "../../../auth/ServerSecretStore.ts";
import * as ServerConfig from "../../../config.ts";
import * as Gateway from "../../../workstreams/WorkstreamGateway.ts";
import { workstreamHttpApiLayer } from "../../../workstreams/http.ts";
import * as Registration from "../registrationContext/service.ts";
import { nativePersistenceLayer } from "../native/integration.ts";
import { NativeHome } from "../native/mode/authorityMode.ts";
import * as Startup from "../native/runtime/startup.ts";
import { registryCommand } from "../native/runtime/cli.ts";
import * as Registry from "../native/registry/service.ts";
import * as Session from "../native/registry/session.ts";
import * as Local from "../native/registry/localTransport.ts";
import * as Projection from "../native/read/projection.ts";
import * as Reads from "../native/read/routes.ts";
import {
  WorkstreamsHomeReadResult,
  WORKSTREAMS_HOME_READ_RESULT_CONTRACT,
} from "@t3tools/contracts/jones/workstreamsHomeReadResult";
import { NativeHomeFixtureError, prepareNativeHomeFixture } from "./nativeHomeFixture.ts";

vi.mock("../../../../package.json", () => ({
  default: {
    version: "0.0.0-preview.20261002.100",
    jonesSource: {
      repository: "Jones-Systems/Jones-Code",
      sha: "a".repeat(40),
      tree: "b".repeat(40),
    },
  },
}));

const platform = Layer.mergeAll(NodeServices.layer, Net.layer);
const run = <A, E>(effect: Effect.Effect<A, E, Layer.Success<typeof platform> | Scope.Scope>) =>
  effect.pipe(Effect.scoped, Effect.provide(platform));
const fresh = () => {
  const parent = NodePath.join(
    NodeOS.homedir(),
    ".cache",
    "jones-code-tests",
    "native-home-fixture",
  );
  NodeFS.mkdirSync(parent, { recursive: true });
  return NodeFS.mkdtempSync(NodePath.join(parent, "home-"));
};
const decodeRead = Schema.decodeUnknownSync(WorkstreamsHomeReadResult);
const isFixtureError = Schema.is(NativeHomeFixtureError);
const withHome = <A, E, R>(use: (home: string) => Effect.Effect<A, E, R>) =>
  Effect.acquireUseRelease(Effect.sync(fresh), use, (home) =>
    Effect.sync(() => NodeFS.rmSync(home, { recursive: true, force: true })),
  );
class ReadApi extends HttpApi.make("environment").add(Wire.EnvironmentHttpApi.groups.workstreams) {}

it.effect(
  "starts an empty native home through real setup, selects native and reads the empty HTTP projection",
  () =>
    withHome((home) =>
      Effect.gen(function* () {
        yield* TestClock.setTime(0);
        const f = yield* run(prepareNativeHomeFixture(home));
        const db = new NodeSqlite.DatabaseSync(f.database, { readOnly: true });
        try {
          for (const table of [
            "auth_sessions",
            "jones_workstreams_source_enrollments",
            "jones_workstreams_source_bindings",
          ])
            expect(db.prepare("SELECT count(*) AS n FROM " + table).get()).toMatchObject({ n: 1 });
          expect(
            db.prepare("SELECT registry_version FROM jones_workstreams_registries").get(),
          ).toMatchObject({ registry_version: 2 });
          expect(
            db.prepare("SELECT count(*) AS n FROM workstreams_native_enrollments").get(),
          ).toMatchObject({ n: 0 });
          expect(db.prepare("SELECT session_id FROM auth_sessions").get()).toMatchObject({
            session_id: f.config.bootstrap?.source_session_id,
          });
        } finally {
          db.close();
        }
        const args = [
          "--home",
          home,
          "--key-provider",
          "server-secret-store",
          "--request",
          JSON.stringify(f.request),
        ];
        yield* run(
          Command.runWith(registryCommand, { version: "test" })([
            "source-setup",
            "readback",
            ...args,
          ]),
        );
        const authorityFile = NodePath.join(
          home,
          "native-store-authority",
          "native-store-authority-v1.json",
        );
        NodeFS.renameSync(authorityFile, authorityFile + ".saved");
        try {
          expect(
            yield* run(
              Command.runWith(registryCommand, { version: "test" })([
                "source-setup",
                "plan",
                ...args,
              ]),
            ).pipe(Effect.result),
          ).toMatchObject({
            _tag: "Failure",
            failure: { _tag: "NativeEnrollmentCliContextError", code: "runtime_unqualified" },
          });
        } finally {
          NodeFS.renameSync(authorityFile + ".saved", authorityFile);
        }
        const config = yield* run(
          ServerConfig.ServerConfig.pipe(Effect.provide(ServerConfig.layerTest(home, home))),
        );
        const storage = nativePersistenceLayer.pipe(
          Layer.provideMerge(
            Secrets.layer.pipe(
              Layer.provide(
                ServerConfig.layer({
                  ...config,
                  baseDir: home,
                  dbPath: f.database,
                  secretsDir: NodePath.join(home, "userdata", "secrets"),
                }),
              ),
            ),
          ),
          Layer.provideMerge(NodeSqliteClient.layer({ filename: f.database })),
        );
        yield* run(
          Effect.gen(function* () {
            const selection = yield* Startup.select({
              text: NodeFS.readFileSync(NodePath.join(home, "workstreams-native.json"), "utf8"),
              legacyActivationPresent: false,
              home,
              database: f.database,
              environmentId: f.environmentId,
            });
            expect(selection).toMatchObject({
              mode: "native-home",
              problem: null,
              sourceSessionId: f.request.enrollment.session.session_id,
            });
            if (!selection.home) return yield* Effect.die("Missing selected native home");
            class Choice extends Context.Service<Choice, string>()(
              "t3/jones/workstreams/testing/nativeHomeFixture.test/Choice",
            ) {}
            const chosen = Startup.chooseLayer({
              legacy: () => Layer.succeed(Choice, "legacy"),
              home: () => Layer.succeed(Choice, "native"),
              unavailable: () => Layer.succeed(Choice, "unavailable"),
            }).pipe(Layer.provide(Layer.succeed(Startup.Startup, selection)));
            expect(yield* Choice.pipe(Effect.provide(chosen))).toBe("native");
            const registry = Registry.layer.pipe(
              Layer.provideMerge(Session.layer),
              Layer.provideMerge(storage),
            );
            const reads = Reads.layer.pipe(
              Layer.provideMerge(Projection.layer),
              Layer.provideMerge(registry),
              Layer.provide(
                Layer.mock(Local.LocalTransport)({
                  forCaller: () => Effect.die("Projection read must not need a transport"),
                }),
              ),
              Layer.provide(Layer.succeed(NativeHome, selection.home)),
            );
            const homeReads = yield* Reads.HomeReads.pipe(Effect.provide(reads));
            // The HTTP authentication edge supplies a synthetic browser principal. Registry/session/key reads are real.
            const routes = HttpApiBuilder.layer(ReadApi).pipe(
              Layer.provide(workstreamHttpApiLayer),
              Layer.provide(
                Layer.succeed(Reads.AuthorizedReads, {
                  read: (request, contract) =>
                    homeReads.read(
                      {
                        principal_id: f.request.enrollment.context.principal_id,
                        source_environment_id: f.environmentId,
                        binding_version: 1,
                        origin: { kind: "app" },
                      },
                      request,
                      contract,
                    ),
                }),
              ),
              Layer.provide(
                Layer.mock(Gateway.WorkstreamGateway)({ purgeAuthorization: () => {} }),
              ),
              Layer.provide(Layer.mock(Registration.WorkstreamsRegistrationContext)({})),
              Layer.provide(
                Layer.succeed(Wire.EnvironmentAuthenticatedAuth, (effect) =>
                  effect.pipe(
                    Effect.provideService(Wire.EnvironmentAuthenticatedPrincipal, {
                      sessionId: Wire.AuthSessionId.make("fixture-browser"),
                      subject: "synthetic-browser",
                      method: "browser-session-cookie",
                      scopes: new Set([Wire.AuthOrchestrationReadScope]),
                    }),
                  ),
                ),
              ),
              Layer.provide(HttpPlatform.layer.pipe(Layer.provide(NodeServices.layer))),
              Layer.provide(Etag.layerWeak),
              Layer.provide(NodeServices.layer),
            );
            const web = yield* Effect.acquireRelease(
              Effect.sync(() => HttpRouter.toWebHandler(routes, { disableLogger: true })),
              (value) => Effect.promise(() => value.dispose()),
            );
            const response = yield* Effect.promise(() =>
              web.handler(
                new Request("http://fixture/api/workstreams/native/v1/read", {
                  method: "POST",
                  headers: { "content-type": "application/json" },
                  body: JSON.stringify({
                    contract: WORKSTREAMS_HOME_READ_RESULT_CONTRACT,
                    request: { route: "read-projection", params: {} },
                  }),
                }),
              ),
            );
            expect(response.status).toBe(200);
            const result = decodeRead(yield* Effect.promise(() => response.json()));
            expect(result).toMatchObject({
              route: "read-projection",
              value: { items: [], next_cursor: null },
            });
            expect(result.envelope).toMatchObject({
              provenance: { authority: "native-home" },
              registry: { registry_instance_id: f.config.registry?.registry_instance_id },
            });
          }).pipe(Effect.provide(storage)),
        );
      }),
    ),
);

it.effect(
  "refuses nonempty, shared, symlinked and relative targets without adopting or changing them",
  () =>
    withHome((home) =>
      Effect.gen(function* () {
        const link = NodePath.join(home, "link");
        NodeFS.writeFileSync(NodePath.join(home, "sentinel"), "preserve");
        const before = NodeFS.readdirSync(home);
        expect(yield* run(prepareNativeHomeFixture(home)).pipe(Effect.flip)).toSatisfy(
          isFixtureError,
        );
        expect(NodeFS.readdirSync(home)).toEqual(before);
        expect(NodeFS.readFileSync(NodePath.join(home, "sentinel"), "utf8")).toBe("preserve");
        NodeFS.symlinkSync(home, link);
        for (const target of [link, ".", NodePath.join(home, "missing")])
          expect(yield* run(prepareNativeHomeFixture(target)).pipe(Effect.flip)).toSatisfy(
            isFixtureError,
          );
        const shared = NodePath.join(home, "shared");
        NodeFS.mkdirSync(shared, { mode: 0o755 });
        expect(yield* run(prepareNativeHomeFixture(shared)).pipe(Effect.flip)).toSatisfy(
          isFixtureError,
        );
        expect(NodeFS.readdirSync(shared)).toEqual([]);
      }),
    ),
);
