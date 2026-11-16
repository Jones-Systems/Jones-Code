// @effect-diagnostics nodeBuiltinImport:off -- Configured fixtures run actual CLI handlers with private files and a fake native child.
// @effect-diagnostics unsafeEffectTypeAssertion:off -- Command.make interception captures only the two explicitly typed build handlers.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import type * as Scope from "effect/Scope";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { afterAll, expect, it, vi } from "vite-plus/test";
import { DEVELOPMENT_ICON_OVERRIDES } from "../../lib/brand-assets.ts";
import { nativePlatform } from "../../../apps/server/src/jones/workspaceRetirement/Filesystem.ts";
import { beginProduction } from "../../../apps/server/src/jones/workspaceRetirement/ProducerFinalizer.ts";
import {
  finiteFixture,
  finiteProductionCases,
  finiteScratchReceipts,
} from "./finite-production.testkit.ts";
import { withFiniteProduction } from "./finite-production.ts";

type BuildHandler = (config: {
  verbose: boolean;
  target: Option.Option<string>;
}) => Effect.Effect<
  void,
  Error,
  Path.Path | FileSystem.FileSystem | ChildProcessSpawner.ChildProcessSpawner | Scope.Scope
>;
const handlers = vi.hoisted(() => new Map<string, BuildHandler>());
vi.mock("@effect/platform-node/NodeRuntime", () => ({ runMain: () => undefined }));
vi.mock("effect/cli", async (importOriginal) => {
  const actual = await importOriginal<typeof import("effect/cli")>();
  return {
    ...actual,
    Command: {
      ...actual.Command,
      make: (...args: Parameters<typeof actual.Command.make>) => {
        if (args[2]) handlers.set(args[0], args[2] as BuildHandler);
        return Reflect.apply(actual.Command.make, actual.Command, args);
      },
    },
  };
});
vi.mock("@t3tools/shared/shell", async () => {
  const effect = await import("effect/Effect");
  return {
    resolveSpawnCommand: () =>
      effect.succeed({ command: "fixture-vp", args: ["pack"], shell: false }),
  };
});
vi.mock("./completion.ts", () => ({
  completeProducer: () => {
    throw new Error("optional capture unavailable");
  },
}));
await import("../../../apps/server/scripts/cli.ts");

it.each(Object.entries(finiteProductionCases))("%s", (_name, run) => run());

it.each([
  "build",
  "build-exe",
  "partial-copy",
  "icon-failure",
  "validation-failure",
  "leader-failure",
])(
  "actual server CLI %s encloses writes and native scope without changing its result",
  (scenario) =>
    finiteFixture(async ({ worktree, root: dist, records }) => {
      const executable = scenario === "build-exe" || scenario === "validation-failure";
      const root = executable ? NodePath.join(worktree, "apps/server/dist-exe") : dist;
      const handler = handlers.get(executable ? "build-exe" : "build")!;
      const copyFailure = new Error("partial-copy fixture");
      let released = 0;
      let calls = 0;
      // build-exe now runs two packs, the embedded CLI bundle then the executable, over one asset directory.
      let cliAssetDir = "";
      for (const override of DEVELOPMENT_ICON_OVERRIDES) {
        const source = NodePath.join(worktree, override.sourceRelativePath);
        const web = NodePath.join(
          worktree,
          "apps/web/dist",
          NodePath.basename(override.targetRelativePath),
        );
        await NodeFSP.mkdir(NodePath.dirname(source), { recursive: true });
        await NodeFSP.mkdir(NodePath.dirname(web), { recursive: true });
        if (scenario !== "icon-failure") await NodeFSP.writeFile(source, "development icon");
        await NodeFSP.writeFile(web, "web icon");
      }
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.gen(function* () {
          expect(command._tag).toBe("StandardCommand");
          if (command._tag !== "StandardCommand") return yield* Effect.die("unexpected pipeline");
          const call = ++calls;
          expect(command.command).toBe(executable ? "fixture-vp" : process.execPath);
          expect(command.args).toEqual(executable ? ["pack"] : ["--run", "build:bundle:internal"]);
          expect(command.options.cwd).toBe(NodePath.join(worktree, "apps/server"));
          expect(command.options.stdout).toBe("ignore");
          expect(command.options.stderr).toBe("inherit");
          expect(command.options.shell).toBe(false);
          expect(command.options.detached).toBe(nativePlatform !== "win32");
          if (executable) {
            expect(command.options.env?.T3CODE_PACK_EXE).toBe(call === 1 ? "bundle" : "1");
            expect(command.options.env?.T3CODE_PACK_EXE_TARGET).toBe("darwin-arm64");
            const assetDir = command.options.env?.T3CODE_CLI_ASSET_DIR;
            if (typeof assetDir !== "string" || assetDir === "")
              throw new Error("expected a plain non-empty CLI asset directory");
            if (call === 1) cliAssetDir = assetDir;
            else expect(assetDir).toBe(cliAssetDir);
          }
          yield* Effect.acquireRelease(
            Effect.promise(async () => {
              const [active] = await records();
              expect(active?.status).toBe("active");
              expect(active?.roots).toEqual([root]);
              // Each pack records its own intent; earlier packs are already bound to their session.
              expect(active?.holders?.spawns).toHaveLength(call);
              for (const earlier of active?.holders?.spawns.slice(0, call - 1) ?? [])
                expect(earlier).toMatchObject({ state: "session", sid: 123 });
              expect(active?.holders?.spawns[call - 1]?.state).toBe("pending");
              await NodeFSP.mkdir(root, { recursive: true });
              if (executable && call === 1) {
                await NodeFSP.writeFile(NodePath.join(cliAssetDir, "binMain.mjs"), "export {};\n");
              } else {
                await NodeFSP.writeFile(
                  NodePath.join(root, "bin.mjs"),
                  scenario === "validation-failure"
                    ? 'import "fixture-external";\n'
                    : "export {};\n",
                );
              }
            }),
            () =>
              Effect.promise(async () => {
                expect((await records())[0]?.status).toBe("active");
                released++;
              }),
          );
          return ChildProcessSpawner.makeHandle({
            pid: ChildProcessSpawner.ProcessId(123),
            exitCode: Effect.succeed(
              ChildProcessSpawner.ExitCode(scenario === "leader-failure" ? 7 : 0),
            ),
            isRunning: Effect.succeed(false),
            kill: () => Effect.void,
            stdin: Sink.drain,
            stdout: Stream.empty,
            stderr: Stream.empty,
            all: Stream.empty,
            getInputFd: () => Sink.drain,
            getOutputFd: () => Stream.empty,
            unref: Effect.succeed(Effect.void),
          });
        }),
      );
      // oxlint-disable-next-line t3code/no-manual-effect-runtime-in-tests -- The actual CLI handler must run inside the promise-owned private scratch fixture (shared with the node:test runner), whose ownership-proved cleanup it.effect cannot own.
      const result = await Effect.runPromise(
        Effect.gen(function* () {
          const path = yield* Path.Path;
          const fs = yield* FileSystem.FileSystem;
          const fixtureFs: FileSystem.FileSystem =
            scenario === "partial-copy"
              ? {
                  ...fs,
                  copy: (...args) => fs.copy(...args).pipe(Effect.andThen(Effect.die(copyFailure))),
                }
              : fs;
          return yield* Effect.exit(
            handler({ verbose: false, target: Option.some("darwin-arm64") }).pipe(
              Effect.provideService(Path.Path, {
                ...path,
                fromFileUrl: () => Effect.succeed(worktree),
              }),
              Effect.provideService(FileSystem.FileSystem, fixtureFs),
              Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
              Effect.scoped,
            ),
          );
        }).pipe(Effect.provide(NodeServices.layer)),
      );
      expect(calls).toBe(executable ? 2 : 1);
      expect(released).toBe(calls);
      const stored = await records();
      expect(stored).toHaveLength(1);
      const succeeded = scenario === "build" || scenario === "build-exe";
      expect(stored[0]?.status).toBe(succeeded ? "succeeded" : "failed");
      expect(stored[0]?.outcome).toBe("unresolved");
      expect(stored[0]?.reasons).toContain("writer_completion_not_observed");
      if (succeeded) {
        expect(Exit.isSuccess(result)).toBe(true);
        for (const override of executable ? [] : DEVELOPMENT_ICON_OVERRIDES) {
          expect(
            await NodeFSP.readFile(
              NodePath.join(worktree, "apps/server", override.targetRelativePath),
              "utf8",
            ),
          ).toBe("development icon");
        }
      } else {
        expect(Exit.isFailure(result)).toBe(true);
        if (!Exit.isFailure(result)) throw new Error("expected original failure");
        const failure = Cause.squash(result.cause);
        if (scenario === "partial-copy") expect(failure).toBe(copyFailure);
        else
          expect(failure).toMatchObject({
            _tag:
              scenario === "validation-failure"
                ? "ServerCliExecutableImportError"
                : scenario === "leader-failure"
                  ? "ServerCliCommandExitError"
                  : "ServerCliDevelopmentIconSourceMissingError",
          });
        if (scenario === "leader-failure") expect(failure).toMatchObject({ exitCode: 7 });
        if (scenario === "validation-failure")
          expect(failure).toMatchObject({ specifiers: ["fixture-external"] });
      }
    }),
);

it("configured Effect interruption during admission cannot expose allocation before release is attached", () =>
  finiteFixture(async ({ worktree, root, records }) => {
    let admitted!: () => void;
    let allowAdmission!: () => void;
    const started = new Promise<void>((resolve) => {
      admitted = resolve;
    });
    const gate = new Promise<void>((resolve) => {
      allowAdmission = resolve;
    });
    let allocations = 0;
    // oxlint-disable-next-line t3code/no-manual-effect-runtime-in-tests -- Same promise-owned private scratch fixture; the interruption race runs inside its lifetime.
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const fiber = yield* withFiniteProduction(
            { producer: "jones-server-build", worktree, root },
            () =>
              Effect.sync(() => {
                allocations++;
              }),
            {
              begin: async (...args) => {
                const session = await beginProduction(...args);
                admitted();
                await gate;
                return session;
              },
              report: () => {},
            },
          ).pipe(Effect.forkChild);
          yield* Effect.promise(() => started);
          const cancellation = yield* Fiber.interrupt(fiber).pipe(Effect.forkChild);
          yield* Effect.yieldNow;
          allowAdmission();
          yield* Fiber.join(cancellation);
        }),
      ),
    );
    expect(allocations).toBe(0);
    expect((await records())[0]?.status).toBe("active");
    expect((await records())[0]?.outcome).toBe("unresolved");
  }));

afterAll(() => {
  expect(finiteScratchReceipts.every((receipt) => receipt.cleaned)).toBe(true);
});
