// @effect-diagnostics nodeBuiltinImport:off
// Each native protocol fixture owns its exact root and drains its startup before cleanup.
import * as NodeServices from "@effect/platform-node/NodeServices";
import { EnvironmentId } from "@t3tools/contracts";
import { HostProcessEnvironment } from "@t3tools/shared/hostProcess";
import { expect, it } from "@effect/vitest";
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import type * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import type * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as NetAddress from "effect/net/NetAddress";
import * as Ref from "effect/Ref";
import { vi } from "vite-plus/test";
import * as ServerConfig from "../../config.ts";
import * as ServerEnvironment from "../../environment/ServerEnvironment.ts";
import {
  awaitJonesTrialStartup,
  awaitSelectedJonesTrialStartup,
  hasJonesTrialAuthority,
  hasJonesTrialDescriptor,
  readJonesStartupGateProtocol,
} from "./trialStartup.ts";
import * as ServiceLauncherClient from "../../cloud/serviceLauncherClient.ts";
import type { QualifiedTrialRuntimeWitness } from "../cloud/qualifiedStartup.ts";
import { runOrderedV2StartupPhases, runRuntimeShutdown } from "../../serverRuntimeStartup.ts";

vi.mock("../../../package.json", () => ({
  default: {
    version: "0.0.45-preview.20261007.1",
    jonesSource: {
      repository: "Jones-Systems/Jones-Code",
      sha: "a".repeat(40),
      tree: "b".repeat(40),
    },
  },
}));

class FixtureIoError extends Data.TaggedError("FixtureIoError")<{
  readonly cause: unknown;
}> {}

const io = <A>(run: (signal: AbortSignal) => Promise<A>) =>
  Effect.tryPromise({
    try: run,
    catch: (cause) => new FixtureIoError({ cause }),
  });
const fileIo = <A>(run: () => Promise<A>) => io(run).pipe(Effect.uninterruptible);

function latch() {
  let resolve!: () => void;
  const promise = new Promise<void>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
}

function fixture<A, E, R>(body: (root: string) => Effect.Effect<A, E, R>) {
  return Effect.acquireUseRelease(
    fileIo(() => NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-trial-startup-test-"))),
    (root) =>
      Effect.scoped(
        Effect.gen(function* () {
          yield* fileIo(async () => {
            await NodeFSP.mkdir(NodePath.join(root, "userdata"));
            await NodeFSP.mkdir(NodePath.join(root, "profile"));
            await NodeFSP.writeFile(
              NodePath.join(root, "userdata/statev2.sqlite"),
              "synthetic state",
            );
          });
          const canonicalRoot = yield* fileIo(() => NodeFSP.realpath(root));
          return yield* body(canonicalRoot);
        }),
      ),
    (root) =>
      fileIo(async () => {
        await NodeFSP.rm(root, { recursive: true, force: true });
        await expect(NodeFSP.lstat(root)).rejects.toMatchObject({ code: "ENOENT" });
      }),
  );
}

async function descriptor(root: string) {
  const value = {
    protocol: 1,
    startupGateProtocol: 1,
    transactionId: "synthetic-startup",
    stagedHandle: "f".repeat(64),
    home: await NodeFSP.realpath(root),
    databasePath: await NodeFSP.realpath(NodePath.join(root, "userdata/statev2.sqlite")),
    profile: await NodeFSP.realpath(NodePath.join(root, "profile")),
    environmentId: "fixture",
    version: "0.0.45-preview.20261007.1",
    sourceSha: "a".repeat(40),
    sourceTree: "b".repeat(40),
    listener: "http://127.0.0.1:4888",
    trialReceiptPath: NodePath.join(root, "trial.json"),
    commitGrantPath: NodePath.join(root, "grant.json"),
  };
  const path = NodePath.join(root, "descriptor.json");
  await NodeFSP.writeFile(path, JSON.stringify(value));
  return { value, path };
}

function withRuntime<A, E>(
  root: string,
  environment: NodeJS.ProcessEnv,
  effect: Effect.Effect<
    A,
    E,
    | ServerConfig.ServerConfig
    | ServerEnvironment.ServerEnvironment
    | Context.Service.Identifier<typeof HostProcessEnvironment>
    | FileSystem.FileSystem
  >,
) {
  return Effect.scoped(
    effect.pipe(
      Effect.provideService(HostProcessEnvironment, environment),
      Effect.provideService(ServerEnvironment.ServerEnvironment, {
        getEnvironmentId: Effect.succeed(EnvironmentId.make("fixture")),
        getDescriptor: Effect.die("Startup adapter must bind the environment ID directly."),
      }),
      Effect.provide(
        Layer.mergeAll(
          ServerConfig.layerTest(root, root).pipe(Layer.provide(NodeServices.layer)),
          NodeServices.layer,
        ),
      ),
    ),
  );
}

async function publishGrant(path: string, value: unknown) {
  const pending = `${path}.unpublished`;
  await NodeFSP.writeFile(pending, JSON.stringify(value));
  await NodeFSP.rename(pending, path);
}

async function observe(file: string, signal: AbortSignal) {
  const watcher = NodeFS.watch(NodePath.dirname(file));
  let rejectWait: ((cause: unknown) => void) | undefined;
  const abort = () => rejectWait?.(new Error("Synthetic observation cancelled"));
  try {
    await new Promise<void>((resolve, reject) => {
      rejectWait = reject;
      const inspect = () => {
        void NodeFSP.lstat(file).then(
          () => resolve(),
          (cause: NodeJS.ErrnoException) => {
            if (cause.code !== "ENOENT") reject(cause);
          },
        );
      };
      watcher.on("change", inspect);
      watcher.on("error", reject);
      signal.addEventListener("abort", abort, { once: true });
      if (signal.aborted) abort();
      else inspect();
    });
  } finally {
    watcher.close();
    signal.removeEventListener("abort", abort);
  }
}

const milestone = <A, E>(effect: Effect.Effect<void, FixtureIoError>, startup: Fiber.Fiber<A, E>) =>
  Effect.raceFirst(effect, Fiber.join(startup));
const absent = (path: string) =>
  fileIo(() => expect(NodeFSP.lstat(path)).rejects.toMatchObject({ code: "ENOENT" }));

it.live("ignores only an absent descriptor without waiting for trial barriers", () =>
  fixture((root) =>
    Effect.gen(function* () {
      let waited = false;
      const adapter = awaitJonesTrialStartup({
        waitUntilParked: Effect.sync(() => {
          waited = true;
        }),
        observedListener: Effect.die("An ordinary startup has no trial listener inspection."),
      });
      expect(yield* withRuntime(root, {}, hasJonesTrialDescriptor)).toBe(false);
      yield* withRuntime(root, {}, adapter);
      expect(waited).toBe(false);
    }),
  ),
);

it.live.each(["", "malformed", "unsupported"])("holds a present %s descriptor", (kind) =>
  fixture((root) =>
    Effect.gen(function* () {
      const input = yield* fileIo(() => descriptor(root));
      if (kind === "malformed") yield* fileIo(() => NodeFSP.writeFile(input.path, "{broken"));
      if (kind === "unsupported")
        yield* fileIo(() =>
          NodeFSP.writeFile(input.path, JSON.stringify({ ...input.value, startupGateProtocol: 2 })),
        );
      const environment = {
        T3CODE_JONES_TRIAL_DESCRIPTOR: kind === "" ? "" : input.path,
        T3CODE_DESKTOP_USER_DATA_DIR: input.value.profile,
      };
      expect(yield* withRuntime(root, environment, hasJonesTrialDescriptor)).toBe(true);
      const exit = yield* withRuntime(
        root,
        environment,
        awaitJonesTrialStartup({
          waitUntilParked: Effect.void,
          observedListener: Effect.succeed(
            NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 4888),
          ),
        }).pipe(Effect.exit),
      );
      expect(exit._tag).toBe("Failure");
      yield* absent(input.value.trialReceiptPath);
    }),
  ),
);

it.live("requires the inherited native desktop profile", () =>
  fixture((root) =>
    Effect.gen(function* () {
      const input = yield* fileIo(() => descriptor(root));
      const exit = yield* withRuntime(
        root,
        { T3CODE_JONES_TRIAL_DESCRIPTOR: input.path },
        awaitJonesTrialStartup({
          waitUntilParked: Effect.void,
          observedListener: Effect.succeed(
            NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 4888),
          ),
        }).pipe(Effect.exit),
      );
      expect(exit._tag).toBe("Failure");
      yield* absent(input.value.trialReceiptPath);
    }),
  ),
);

it.live(
  "holds the unchanged receiving recovery sequence until parked readiness, exact grant and reservation",
  () =>
    fixture((root) =>
      Effect.gen(function* () {
        const input = yield* fileIo(() => descriptor(root));
        const entered = latch();
        const parked = latch();
        const calls: string[] = [];
        const record = (label: string) =>
          Effect.sync(() => {
            calls.push(label);
          });
        const client = launcher({
          requiresQualifiedTrialGate: false,
          prepareQualifiedTrial: () => Effect.die("Darwin must not prepare a qualified IPC trial."),
          prepareTrial: Effect.succeed(undefined),
        });
        const startup = yield* withRuntime(
          root,
          {
            T3CODE_JONES_TRIAL_DESCRIPTOR: input.path,
            T3CODE_DESKTOP_USER_DATA_DIR: input.value.profile,
          },
          runOrderedV2StartupPhases({
            awaitTrialCommit: awaitSelectedJonesTrialStartup({
              waitUntilParked: Effect.sync(entered.resolve).pipe(
                Effect.andThen(io(() => parked.promise)),
              ),
              observedListener: Effect.succeed(
                NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 4888),
              ),
            }),
            importLegacyShells: fileIo(async () => {
              const reservation = JSON.parse(
                await NodeFSP.readFile(NodePath.join(root, "resume-dispatched.json"), "utf8"),
              );
              const {
                trialReceiptPath: _receipt,
                commitGrantPath: _grant,
                ...identity
              } = input.value;
              expect(reservation).toMatchObject(identity);
              calls.push("legacy");
            }),
            recover: record("provider"),
            recoverDelegatedTasks: record("delegated"),
            startEffectWorker: record("worker"),
            autoBootstrap: record("bootstrap"),
          }).pipe(Effect.provideService(ServiceLauncherClient.ServiceLauncherClient, client)),
        ).pipe(Effect.forkScoped);
        yield* Effect.gen(function* () {
          yield* milestone(
            io(() => entered.promise),
            startup,
          );
          expect(calls).toEqual([]);
          yield* absent(input.value.trialReceiptPath);
          parked.resolve();
          yield* milestone(
            io((signal) => observe(input.value.trialReceiptPath, signal)),
            startup,
          );
          expect(calls).toEqual([]);
          const heldReceipt = yield* fileIo(async () =>
            JSON.parse(await NodeFSP.readFile(input.value.trialReceiptPath, "utf8")),
          );
          expect(heldReceipt.startupGateProtocol).toBe(readJonesStartupGateProtocol());
          expect(heldReceipt.resumeHeld).toBe(true);
          yield* fileIo(() =>
            publishGrant(input.value.commitGrantPath, {
              ...input.value,
              generation: input.value.transactionId,
            }),
          );
          yield* Fiber.join(startup);
          expect(calls).toEqual(["legacy", "provider", "delegated", "worker", "bootstrap"]);
          const duplicate = yield* withRuntime(
            root,
            {
              T3CODE_JONES_TRIAL_DESCRIPTOR: input.path,
              T3CODE_DESKTOP_USER_DATA_DIR: input.value.profile,
            },
            awaitJonesTrialStartup({
              waitUntilParked: Effect.void,
              observedListener: Effect.succeed(
                NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 4888),
              ),
            }).pipe(Effect.exit),
          );
          expect(duplicate._tag).toBe("Failure");
        }).pipe(
          Effect.ensuring(
            Effect.sync(parked.resolve).pipe(Effect.andThen(Fiber.interrupt(startup))),
          ),
        );
      }),
    ),
);

it.live("drains scoped cancellation before synthetic cleanup and leaves recovery held", () =>
  fixture((root) =>
    Effect.gen(function* () {
      const input = yield* fileIo(() => descriptor(root));
      const calls: string[] = [];
      const record = Effect.sync(() => {
        calls.push("must remain held");
      });
      const startup = yield* withRuntime(
        root,
        {
          T3CODE_JONES_TRIAL_DESCRIPTOR: input.path,
          T3CODE_DESKTOP_USER_DATA_DIR: input.value.profile,
        },
        runOrderedV2StartupPhases({
          awaitTrialCommit: awaitJonesTrialStartup({
            waitUntilParked: Effect.void,
            observedListener: Effect.succeed(
              NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 4888),
            ),
          }),
          importLegacyShells: record,
          recover: record,
          recoverDelegatedTasks: record,
          startEffectWorker: record,
          autoBootstrap: record,
        }),
      ).pipe(Effect.forkScoped);
      yield* milestone(
        io((signal) => observe(input.value.trialReceiptPath, signal)),
        startup,
      );
      yield* Fiber.interrupt(startup);
      const stopped = yield* Fiber.await(startup);
      expect(stopped._tag).toBe("Failure");
      yield* fileIo(() =>
        publishGrant(input.value.commitGrantPath, {
          ...input.value,
          generation: input.value.transactionId,
        }),
      );
      expect(calls).toEqual([]);
      yield* absent(NodePath.join(root, "resume-dispatched.json"));
    }),
  ),
);

function launcher(
  input: Pick<
    ServiceLauncherClient.ServiceLauncherClient["Service"],
    "requiresQualifiedTrialGate" | "prepareQualifiedTrial" | "prepareTrial"
  >,
) {
  return ServiceLauncherClient.ServiceLauncherClient.of({
    managed: true,
    requestUpdate: () => Effect.die("Startup must not request another update."),
    ...input,
  });
}

it.live(
  "rejects conflicting descriptor and qualified IPC authorities before local readiness or IPC",
  () =>
    fixture((root) =>
      Effect.gen(function* () {
        const input = yield* fileIo(() => descriptor(root));
        const calls: string[] = [];
        const client = launcher({
          requiresQualifiedTrialGate: true,
          prepareQualifiedTrial: () => Effect.die("Conflicting authority must not prepare IPC."),
          prepareTrial: Effect.die("Conflicting authority must not prepare legacy IPC."),
        });
        const exit = yield* withRuntime(
          root,
          {
            T3CODE_JONES_TRIAL_DESCRIPTOR: input.path,
            T3CODE_DESKTOP_USER_DATA_DIR: input.value.profile,
          },
          awaitSelectedJonesTrialStartup({
            waitUntilParked: Effect.sync(() => {
              calls.push("parked");
            }),
            observedListener: Effect.sync(() => {
              calls.push("listener");
              return NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 4888);
            }),
          }).pipe(
            Effect.provideService(ServiceLauncherClient.ServiceLauncherClient, client),
            Effect.tapError((error) =>
              Effect.sync(() => {
                expect(error).toMatchObject({ step: "identity", uncertain: false });
              }),
            ),
            Effect.exit,
          ),
        );
        expect(exit._tag).toBe("Failure");
        expect(calls).toEqual([]);
        yield* absent(input.value.trialReceiptPath);
      }),
    ),
);

it.live("ordinary and legacy launcher startup keep preparation in the receiving late phase", () =>
  fixture((root) =>
    Effect.gen(function* () {
      const calls: string[] = [];
      const record = (label: string) =>
        Effect.sync(() => {
          calls.push(label);
        });
      const client = launcher({
        requiresQualifiedTrialGate: false,
        prepareQualifiedTrial: () =>
          Effect.die("A legacy launcher has no qualified trial authority."),
        prepareTrial: record("legacy-prepare").pipe(Effect.as(undefined)),
      });
      yield* withRuntime(
        root,
        {},
        runOrderedV2StartupPhases({
          awaitTrialCommit: awaitSelectedJonesTrialStartup({
            waitUntilParked: Effect.die("Ordinary startup must not wait on early trial barriers."),
            observedListener: Effect.die("Ordinary startup must not collect a trial witness."),
          }),
          importLegacyShells: record("legacy"),
          recover: record("provider"),
          recoverDelegatedTasks: record("delegated"),
          startEffectWorker: record("worker"),
          autoBootstrap: record("bootstrap"),
        }).pipe(
          Effect.andThen(client.prepareTrial),
          Effect.provideService(ServiceLauncherClient.ServiceLauncherClient, client),
        ),
      );
      expect(calls).toEqual([
        "legacy",
        "provider",
        "delegated",
        "worker",
        "bootstrap",
        "legacy-prepare",
      ]);
    }),
  ),
);

it.live(
  "binds qualified IPC once to configured canonical paths and actual socket before receiving recovery",
  () =>
    fixture((root) =>
      Effect.gen(function* () {
        const entered = latch();
        const parked = latch();
        const preparing = latch();
        const granted = latch();
        const calls: string[] = [];
        let captured: QualifiedTrialRuntimeWitness | undefined;
        let listenerCaptures = 0;
        let preparations = 0;
        let established = false;
        const socket = NetAddress.inetAddressFromIpStringUnsafe("0.0.0.0", 4991);
        const outcome = {
          id: "qualified-startup",
          fromVersion: "0.0.44",
          targetVersion: "0.0.45-preview.20261007.1",
          status: "committed" as const,
        };
        const client = launcher({
          requiresQualifiedTrialGate: true,
          prepareQualifiedTrial: (witness) =>
            Effect.sync(() => {
              preparations++;
              captured = witness;
              preparing.resolve();
            }).pipe(
              Effect.andThen(Effect.promise(() => granted.promise)),
              Effect.andThen(
                Effect.sync(() => {
                  calls.push("reservation");
                  established = true;
                  return outcome;
                }),
              ),
            ),
          prepareTrial: Effect.sync(() => {
            expect(established).toBe(true);
            calls.push("outcome-read");
            return outcome;
          }),
        });
        const record = (label: string) =>
          Effect.sync(() => {
            calls.push(label);
          });
        const startup = yield* withRuntime(
          root,
          {},
          runOrderedV2StartupPhases({
            awaitTrialCommit: awaitSelectedJonesTrialStartup({
              waitUntilParked: Effect.sync(entered.resolve).pipe(
                Effect.andThen(io(() => parked.promise)),
              ),
              observedListener: Effect.sync(() => {
                listenerCaptures++;
                return socket;
              }),
            }),
            importLegacyShells: record("legacy"),
            recover: record("provider"),
            recoverDelegatedTasks: record("delegated"),
            startEffectWorker: record("worker"),
            autoBootstrap: record("bootstrap"),
          }).pipe(
            Effect.andThen(client.prepareTrial),
            Effect.provideService(ServiceLauncherClient.ServiceLauncherClient, client),
          ),
        ).pipe(Effect.forkScoped);
        yield* Effect.gen(function* () {
          yield* milestone(
            io(() => entered.promise),
            startup,
          );
          expect(preparations).toBe(0);
          expect(listenerCaptures).toBe(0);
          parked.resolve();
          yield* milestone(
            io(() => preparing.promise),
            startup,
          );
          expect(calls).toEqual([]);
          const [home, databasePath, serviceUserdata] = yield* fileIo(() =>
            Promise.all([
              NodeFSP.realpath(root),
              NodeFSP.realpath(NodePath.join(root, "userdata/statev2.sqlite")),
              NodeFSP.realpath(NodePath.join(root, "userdata")),
            ]),
          );
          expect(captured).toEqual({
            home,
            databasePath,
            serviceUserdata,
            environmentId: "fixture",
            version: outcome.targetVersion,
            buildMetadata: {
              version: outcome.targetVersion,
              jonesSource: {
                repository: "Jones-Systems/Jones-Code",
                sha: "a".repeat(40),
                tree: "b".repeat(40),
              },
            },
            listener: socket,
            processId: process.pid,
          });
          granted.resolve();
          yield* Fiber.join(startup);
          expect(preparations).toBe(1);
          expect(listenerCaptures).toBe(1);
          expect(calls).toEqual([
            "reservation",
            "legacy",
            "provider",
            "delegated",
            "worker",
            "bootstrap",
            "outcome-read",
          ]);
        }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              parked.resolve();
              granted.resolve();
            }).pipe(Effect.andThen(Fiber.interrupt(startup))),
          ),
        );
      }),
    ),
);

it.live("a qualified witness or grant failure holds every receiving recovery phase", () =>
  fixture((root) =>
    Effect.gen(function* () {
      const calls: string[] = [];
      const record = Effect.sync(() => {
        calls.push("must remain held");
      });
      const client = launcher({
        requiresQualifiedTrialGate: true,
        prepareQualifiedTrial: () =>
          Effect.fail(
            new ServiceLauncherClient.ServiceLauncherClientError({ operation: "qualified-proof" }),
          ),
        prepareTrial: Effect.die("A failed qualified trial has no established late outcome."),
      });
      const exit = yield* withRuntime(
        root,
        {},
        runOrderedV2StartupPhases({
          awaitTrialCommit: awaitSelectedJonesTrialStartup({
            waitUntilParked: Effect.void,
            observedListener: Effect.succeed(
              NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 4888),
            ),
          }),
          importLegacyShells: record,
          recover: record,
          recoverDelegatedTasks: record,
          startEffectWorker: record,
          autoBootstrap: record,
        }).pipe(
          Effect.provideService(ServiceLauncherClient.ServiceLauncherClient, client),
          Effect.tapError((error) =>
            Effect.sync(() => {
              expect(error).toMatchObject({ operation: "qualified-proof" });
            }),
          ),
          Effect.exit,
        ),
      );
      expect(exit._tag).toBe("Failure");
      expect(calls).toEqual([]);
    }),
  ),
);

it.live.each([
  ["descriptor", { T3CODE_JONES_TRIAL_DESCRIPTOR: "" }, false],
  ["qualified IPC", {}, true],
  ["conflicting authorities", { T3CODE_JONES_TRIAL_DESCRIPTOR: "" }, true],
] as const)(
  "pre-activation %s shutdown preserves continuations and performs local cleanup",
  ([_label, environment, qualified]) =>
    fixture((root) =>
      Effect.gen(function* () {
        const client = launcher({
          requiresQualifiedTrialGate: qualified,
          prepareQualifiedTrial: () => Effect.die("Shutdown does not grant trial activation."),
          prepareTrial: Effect.die("Shutdown does not prepare another trial."),
        });
        const result = yield* withRuntime(
          root,
          environment,
          Effect.gen(function* () {
            const rows = yield* Ref.make(["paired-continuation"]);
            const cleaned = yield* Ref.make(false);
            const trialBound = yield* hasJonesTrialAuthority;
            yield* runRuntimeShutdown({
              continuationWritesAllowed: !trialBound,
              prepareForShutdown: Ref.set(rows, ["rewritten"]),
              shutdownSessions: Ref.set(cleaned, true),
              reconcile: Ref.set(rows, []),
            });
            return { rows: yield* Ref.get(rows), cleaned: yield* Ref.get(cleaned) };
          }).pipe(Effect.provideService(ServiceLauncherClient.ServiceLauncherClient, client)),
        );
        expect(result).toEqual({ rows: ["paired-continuation"], cleaned: true });
      }),
    ),
);
