// @effect-diagnostics nodeBuiltinImport:off -- Reopen proofs use synthetic SQLite files in a scoped temporary directory.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { expect, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import { WorkstreamOperationReadResponse } from "@t3tools/contracts/jones/workstreamOperations";
import migration from "../../../persistence/Migrations/125_JonesWorkstreamsNative.ts";
import { canonicalJson } from "../../operations/encoding.ts";
import { Ports } from "../../operations/ports.ts";
import * as Operations from "../../operations/service.ts";
import {
  fakeControlPlane,
  request,
  createRequest,
  childId,
  hash,
  offline,
} from "../../operations/testFixtures.ts";
import * as BindingStore from "../store/bindingStore.ts";
import * as OperationStore from "../store/operationStore.ts";
import { testLayer, secrets } from "../store/sql.testkit.ts";
import * as DurableEntries from "./durableEntries.ts";
import * as HomeExecutor from "./homeExecutor.ts";

const isReadResponse = Schema.is(WorkstreamOperationReadResponse);

type CP = ReturnType<typeof fakeControlPlane>;
const destination = {
  registry_instance_id: "registry-test",
  owner_id: "owner-synthetic",
  home_environment_id: "home-synthetic",
  home_generation: 5,
  principal_id: "principal-synthetic",
  binding_version: 1,
};
const initialize = Effect.gen(function* () {
  yield* migration;
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO jones_workstreams_keys(key_id,registry_instance_id,scope,state,created_at) VALUES ('test-local','registry-test','local','active','1970-01-01T00:00:00.000Z')`;
  const bindings = yield* BindingStore.BindingStore;
  yield* bindings.bind({ ...destination, state: "active" }, null);
});
const persistence = (filename: string, keys = secrets(), cp?: CP) =>
  Layer.mergeAll(
    DurableEntries.layer,
    HomeExecutor.layer.pipe(
      Layer.provide(
        Layer.succeed(HomeExecutor.HomeTransport, {
          lookup: (target, id) =>
            cp
              ? cp.ports.gateway.readReceipt(id).pipe(
                  Effect.map((result) => {
                    expect(target).toEqual(destination);
                    return result.kind === "receipt"
                      ? {
                          kind: "present" as const,
                          source: "native" as const,
                          receipt: result.receipt,
                        }
                      : {
                          kind: "proven_absent" as const,
                          witness: {
                            server_generation: cp.binding().serverGeneration,
                            registry_version: result.witness_version,
                            archive: "complete" as const,
                          },
                        };
                  }),
                )
              : Effect.fail(offline()),
          resubmit: (target, id, body) => {
            expect(target).toEqual(destination);
            return cp ? cp.ports.gateway.resubmit(id, body) : Effect.fail(offline());
          },
        }),
      ),
    ),
  ).pipe(Layer.provideMerge(testLayer({ filename, keys })), Layer.provideMerge(NodeCrypto.layer));
const serviceLayer = (cp: CP, filename: string) =>
  Operations.layer.pipe(
    Layer.provide(
      Layer.effect(
        Ports,
        Effect.gen(function* () {
          return Ports.of({
            ...cp.ports,
            native: {
              mode: "native",
              entries: yield* DurableEntries.DurableEntries,
              executor: yield* HomeExecutor.HomeExecutor,
            },
          });
        }),
      ),
    ),
    Layer.provideMerge(persistence(filename, secrets(), cp)),
  );
const withFile = <A, E, R>(run: (filename: string) => Effect.Effect<A, E, R>) =>
  Effect.scoped(
    Effect.gen(function* () {
      const dir = yield* Effect.acquireRelease(
        Effect.tryPromise(() => NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "nw-j2d-"))),
        (path) => Effect.promise(() => NodeFSP.rm(path, { recursive: true, force: true })),
      );
      return yield* run(NodePath.join(dir, "operations.sqlite"));
    }),
  );

it.effect(
  "records encrypted intent before effects and replays the terminal result across restart beyond 30 days",
  () =>
    withFile((filename) =>
      Effect.gen(function* () {
        yield* initialize.pipe(Effect.provide(persistence(filename)));
        const cp = fakeControlPlane();
        const queued = {
          ...request,
          origin: { kind: "queue" as const, request_id: "queue-retained-sentinel" },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const service = yield* Operations.WorkstreamOperations;
            const store = yield* OperationStore.OperationStore;
            cp.state.beforeSend = (command, body) =>
              Effect.gen(function* () {
                const saved = yield* store.read(queued.operation_id);
                expect(saved?.state).toMatch(/dispatching|in_progress/);
                expect(saved?.request_body).toBe(canonicalJson(queued));
                expect(saved?.request_sha256).toBe(hash(canonicalJson(queued)));
                expect(saved?.destination).toEqual(destination);
                expect(saved?.child_command_ids).toContain(command.command_id);
                expect(JSON.parse(saved!.result_body!).unknown.body).toBe(body);
              }).pipe(Effect.orDie);
            const result = yield* service.submit({ request: queued, wait_ms: 3000 });
            expect(result.state).toBe("committed");
            const sql = yield* SqlClient.SqlClient;
            const [row] = yield* sql<{
              request_envelope: string;
              result_envelope: string;
            }>`SELECT request_envelope,result_envelope FROM jones_workstreams_operations`;
            expect(row!.request_envelope).not.toContain("queue-retained-sentinel");
            expect(row!.result_envelope).not.toContain('"workstream_id":"target"');
            return result;
          }).pipe(Effect.provide(serviceLayer(cp, filename))),
        );
        const writes = cp.state.writes.length;
        cp.state.beforeSend = undefined;
        yield* TestClock.adjust(40 * 86_400_000);
        yield* Effect.scoped(
          Effect.gen(function* () {
            const service = yield* Operations.WorkstreamOperations;
            expect(yield* service.read({ operation_id: queued.operation_id })).toEqual({
              operation: result,
            });
            expect(yield* service.submit({ request: queued, wait_ms: 0 })).toEqual(result);
            expect(cp.state.writes).toHaveLength(writes);
            const store = yield* OperationStore.OperationStore;
            expect((yield* store.read(queued.operation_id))?.child_command_ids).toEqual(
              ["reg", "ver", "mem", "cre"].flatMap((kind) =>
                Array.from({ length: 9 }, (_, i) => childId(queued, kind, i + 1)),
              ),
            );
            expect(yield* store.open()).toEqual([]);
          }).pipe(Effect.provide(serviceLayer(cp, filename))),
        );
      }),
    ),
);

it.effect(
  "retains an unresolved exact child and the thread lock across restart past 24 hours without redispatch",
  () =>
    withFile((filename) =>
      Effect.gen(function* () {
        yield* initialize.pipe(Effect.provide(persistence(filename)));
        const cp = fakeControlPlane();
        const sent = yield* Deferred.make<void>();
        cp.state.beforeSend = () =>
          Deferred.succeed(sent, undefined).pipe(Effect.andThen(Effect.never));
        yield* Effect.scoped(
          Effect.gen(function* () {
            const service = yield* Operations.WorkstreamOperations;
            yield* service.submit({ request, wait_ms: 0 });
            yield* Deferred.await(sent);
          }).pipe(Effect.provide(serviceLayer(cp, filename))),
        );
        const original = yield* Effect.gen(function* () {
          const store = yield* OperationStore.OperationStore;
          return yield* store.read(request.operation_id);
        }).pipe(Effect.provide(persistence(filename)));
        expect(original?.state).toBe("dispatching");
        expect(JSON.parse(original!.result_body!).unknown.command.command_id).toBe(
          childId(request, "reg"),
        );
        const writes = cp.state.writes.length;
        cp.state.beforeSend = undefined;
        yield* TestClock.adjust(25 * 3_600_000);
        yield* Effect.scoped(
          Effect.gen(function* () {
            const service = yield* Operations.WorkstreamOperations;
            yield* service.submit({ request, wait_ms: 0 });
            const response = yield* service.read({ open: true });
            expect(isReadResponse(response)).toBe(true);
            expect("operations" in response && response.operations).toHaveLength(1);
            const conflict = yield* Effect.exit(
              service.submit({
                request: { ...request, operation_id: "another-operation-001" },
                wait_ms: 0,
              }),
            );
            expect(Exit.isFailure(conflict)).toBe(true);
            expect(cp.state.writes).toHaveLength(writes);
            const store = yield* OperationStore.OperationStore;
            expect((yield* store.open())[0]?.destination).toEqual(original?.destination);
            const executor = yield* HomeExecutor.HomeExecutor;
            expect(
              Exit.isFailure(
                yield* Effect.exit(
                  executor.assertSession(original!, { ...cp.binding(), serverGeneration: 6 }),
                ),
              ),
            ).toBe(true);
          }).pipe(Effect.provide(serviceLayer(cp, filename))),
        );
      }),
    ),
);

it.effect("recovers a committed lost reply on restart without executing the child again", () =>
  withFile((filename) =>
    Effect.gen(function* () {
      yield* initialize.pipe(Effect.provide(persistence(filename)));
      const cp = fakeControlPlane();
      const committed = yield* Deferred.make<void>();
      cp.state.afterSend = () =>
        Deferred.succeed(committed, undefined).pipe(Effect.andThen(Effect.never));
      yield* Effect.scoped(
        Effect.gen(function* () {
          const service = yield* Operations.WorkstreamOperations;
          yield* service.submit({ request: createRequest, wait_ms: 0 });
          yield* Deferred.await(committed);
        }).pipe(Effect.provide(serviceLayer(cp, filename))),
      );
      const writes = cp.state.writes.length;
      cp.state.afterSend = undefined;
      yield* Effect.scoped(
        Effect.gen(function* () {
          const service = yield* Operations.WorkstreamOperations;
          const result = yield* service.submit({ request: createRequest, wait_ms: 3000 });
          expect(result.state).toBe("committed");
          expect(cp.state.creates).toBe(1);
          expect(cp.state.writes).toHaveLength(writes);
          expect(yield* service.read({ operation_id: createRequest.operation_id })).toEqual({
            operation: result,
          });
        }).pipe(Effect.provide(serviceLayer(cp, filename))),
      );
    }),
  ),
);

it.effect(
  "absence uses the version witnessed before lookup, even when the next observation advances",
  () =>
    withFile((filename) =>
      Effect.gen(function* () {
        yield* initialize.pipe(Effect.provide(persistence(filename)));
        const cp = fakeControlPlane();
        const waiting = yield* Deferred.make<void>();
        cp.state.beforeSend = () =>
          Effect.sync(() => {
            cp.state.beforeRead = () => {
              cp.state.version++;
              cp.state.beforeRead = undefined;
            };
          }).pipe(Effect.andThen(Effect.fail(offline())));
        cp.state.onSleep = () => Deferred.succeed(waiting, undefined).pipe(Effect.asVoid);
        yield* Effect.scoped(
          Effect.gen(function* () {
            const service = yield* Operations.WorkstreamOperations;
            yield* service.submit({ request: createRequest, wait_ms: 0 });
            yield* Deferred.await(waiting);
            expect(cp.state.writes).toHaveLength(1);
            expect(cp.state.creates).toBe(0);
            const store = yield* OperationStore.OperationStore;
            const row = yield* store.read(createRequest.operation_id);
            expect(row?.state).toBe("reconciling");
            expect(JSON.parse(row!.result_body!).unknown.command.expected_registry_version).toBe(1);
            expect(cp.state.version).toBe(2);
          }).pipe(Effect.provide(serviceLayer(cp, filename))),
        );
      }),
    ),
);

it.effect(
  "a proven-absence witness newer than the old command permits a fenced fresh attempt",
  () =>
    withFile((filename) =>
      Effect.gen(function* () {
        yield* initialize.pipe(Effect.provide(persistence(filename)));
        const cp = fakeControlPlane();
        cp.state.beforeSend = () =>
          Effect.sync(() => {
            cp.state.version++;
            cp.state.beforeSend = undefined;
          }).pipe(Effect.andThen(Effect.fail(offline())));
        const retrying = yield* Deferred.make<void>();
        cp.state.onSleep = (milliseconds) =>
          milliseconds === 250
            ? Deferred.succeed(retrying, undefined).pipe(Effect.asVoid)
            : Effect.void;
        yield* Effect.scoped(
          Effect.gen(function* () {
            const service = yield* Operations.WorkstreamOperations;
            const submitted = yield* service
              .submit({ request: createRequest, wait_ms: 3000 })
              .pipe(Effect.forkChild);
            yield* Deferred.await(retrying);
            yield* TestClock.adjust(250);
            const result = yield* Fiber.join(submitted);
            expect(result.state).toBe("committed");
            expect(cp.state.creates).toBe(1);
            expect(cp.state.writes).toHaveLength(2);
            expect(
              cp.state.writes.map((body) => JSON.parse(body).expected_registry_version),
            ).toEqual([1, 2]);
          }).pipe(Effect.provide(serviceLayer(cp, filename))),
        );
      }),
    ),
);

it.effect("generation-mismatched absence remains unknown and cannot retarget or dispatch", () =>
  withFile((filename) =>
    Effect.gen(function* () {
      yield* initialize.pipe(Effect.provide(persistence(filename)));
      const cp = fakeControlPlane();
      const waiting = yield* Deferred.make<void>();
      cp.state.beforeSend = () =>
        Effect.sync(() => {
          const binding = cp.binding;
          cp.binding = () => ({ ...binding(), serverGeneration: 6 });
          cp.state.version++;
        }).pipe(Effect.andThen(Effect.fail(offline())));
      cp.state.onSleep = () => Deferred.succeed(waiting, undefined).pipe(Effect.asVoid);
      yield* Effect.scoped(
        Effect.gen(function* () {
          const service = yield* Operations.WorkstreamOperations;
          yield* service.submit({ request: createRequest, wait_ms: 0 });
          yield* Deferred.await(waiting);
          const store = yield* OperationStore.OperationStore;
          const row = yield* store.read(createRequest.operation_id);
          expect(row?.state).toBe("reconciling");
          expect(row?.destination.home_generation).toBe(5);
          expect(JSON.parse(row!.result_body!).unknown.command.expected_server_generation).toBe(5);
          expect(cp.state.writes).toHaveLength(1);
          expect(cp.state.creates).toBe(0);
        }).pipe(Effect.provide(serviceLayer(cp, filename))),
      );
    }),
  ),
);

it.effect("missing local keys fail closed and never dispatch or create replacement keys", () =>
  withFile((filename) =>
    Effect.gen(function* () {
      yield* initialize.pipe(Effect.provide(persistence(filename)));
      const keys = secrets();
      keys.delete("workstreams-content-key-test-local");
      yield* Effect.gen(function* () {
        const entries = yield* DurableEntries.DurableEntries;
        expect(
          Exit.isFailure(
            yield* Effect.exit(
              entries.accept(request, [childId(request, "reg")], "1970-01-01T00:00:00.000Z"),
            ),
          ),
        ).toBe(true);
        const store = yield* OperationStore.OperationStore;
        expect(yield* store.read(request.operation_id)).toBeNull();
      }).pipe(Effect.provide(persistence(filename, keys)));
    }),
  ),
);
