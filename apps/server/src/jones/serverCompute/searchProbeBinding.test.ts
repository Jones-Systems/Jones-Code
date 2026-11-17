// @effect-diagnostics nodeBuiltinImport:off -- Bounded synthetic private-pipe fixture; no child/process evidence.
import { expect, it } from "@effect/vitest";
import * as NodeStream from "node:stream";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { ChildProcessSpawner } from "effect/process";
import * as VcsProcess from "../../vcs/VcsProcess.ts";
import * as SearchGitProbe from "../workspace/SearchGitProbe.ts";
import { SearchProbeBinding, searchProbePort } from "./searchProbeBinding.ts";
import { CurrentProbeOwner, SearchProbeClient } from "./searchProbeClient.ts";
import type { Bootstrap, Request } from "./protocol.ts";

const boot: Bootstrap = {
  version: 1,
  build: "b".repeat(64),
  authorityId: "authority",
  slotId: 0,
  slotGeneration: 1,
  nonce: "nonce",
};
const token = {
  authorityId: boot.authorityId,
  slotId: 0,
  slotGeneration: 1,
  keyId: "key",
  incarnation: 1,
};
const open: Request = {
  ...boot,
  token,
  operation: "open",
  requestId: 1,
  input: { cwd: "/workspace", variant: "paths" },
};

it.effect(
  "round-trips chunked uploads/results and retains independent resident owners after foreground completion",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const toParent = new NodeStream.PassThrough(),
          toWorker = new NodeStream.PassThrough();
        const parentPipe = NodeStream.Duplex.from({ readable: toParent, writable: toWorker });
        const workerPipe = NodeStream.Duplex.from({ readable: toWorker, writable: toParent });
        const faults: unknown[] = [],
          commands: VcsProcess.VcsProcessInput[] = [];
        const fs = yield* FileSystem.FileSystem;
        const output = "λ-needle\n".repeat(10_000);
        const probe = yield* SearchGitProbe.make.pipe(
          // This case exercises pipe ownership; probe deadline behavior has its own virtual-clock cases.
          Effect.provideService(SearchGitProbe.SearchProbeClock, {
            now: () => 0,
            schedule: () => () => {},
          }),
          Effect.provideService(FileSystem.FileSystem, {
            ...fs,
            realPath: (p) => Effect.succeed(p),
          }),
          Effect.provideService(VcsProcess.VcsProcess, {
            run: (input) =>
              Effect.sync(() => {
                commands.push(input);
                input.onSpawn?.();
                return {
                  exitCode: ChildProcessSpawner.ExitCode(0),
                  stdout: output,
                  stderr: "",
                  stdoutTruncated: false,
                  stderrTruncated: false,
                };
              }),
          }),
        );
        const binding = new SearchProbeBinding(boot, parentPipe, searchProbePort(probe), (cause) =>
          faults.push(cause),
        );
        const client = new SearchProbeClient(boot, workerPipe, (cause) => faults.push(cause));
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            binding.revoke();
            workerPipe.destroy();
          }),
        );
        const capability = client.forToken(token);
        yield* Effect.promise(() => binding.foregroundBegin(open, 30_000));
        const ignored = yield* capability
          .run({ kind: "ignoredPaths" })
          .pipe(Effect.provideService(CurrentProbeOwner, { kind: "foreground", requestId: 1 }));
        expect(ignored.stdout).toBe(output);
        yield* Effect.promise(() => binding.foregroundEnd(open, true));
        const list: Request = { ...open, requestId: 2, operation: "list", input: null };
        yield* Effect.promise(() => binding.foregroundBegin(list, 30_000));
        const scan = yield* capability.beginScan.pipe(
          Effect.provideService(CurrentProbeOwner, { kind: "foreground", requestId: 2 }),
        );
        yield* Effect.promise(() => binding.foregroundEnd(list, true));
        const paths = Array.from({ length: 4000 }, (_, i) => `nested/${i}-λ\n\\file.ts`);
        const result = yield* capability
          .run({ kind: "ignoredCandidates", repository: "nested", paths })
          .pipe(Effect.provideService(CurrentProbeOwner, scan.owner));
        expect(result.stdout).toBe(output);
        expect(commands.at(-1)?.cwd).toBe("/workspace/nested");
        expect(commands.at(-1)?.stdin).toBe(`${paths.join("\0")}\0`);
        yield* scan.end;
        expect(faults).toEqual([]);
        const search: Request = {
          ...open,
          requestId: 3,
          operation: "searchContents",
          input: {
            query: "captured",
            caseSensitive: true,
            useRegex: false,
            wholeWord: false,
            limit: 10,
          },
        };
        yield* Effect.promise(() => binding.foregroundBegin(search, 30_000));
        const refused = yield* capability
          .run({
            kind: "grep",
            pattern: "substituted",
            regexMode: false,
            ignoreCase: false,
            onlyMatching: false,
          })
          .pipe(
            Effect.provideService(CurrentProbeOwner, { kind: "foreground", requestId: 3 }),
            Effect.flip,
          );
        expect(refused.reason).toBe("protocol");
        expect(commands).toHaveLength(2);
        expect(faults).toHaveLength(1);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
