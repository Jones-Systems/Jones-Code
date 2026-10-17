// @effect-diagnostics nodeBuiltinImport:off
import { expect, it } from "@effect/vitest";
import { ServerSelfUpdateError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { withRunningThreadContinuation } from "../../cloud/selfUpdate.ts";
import { ServiceLauncherClientError } from "../../cloud/serviceLauncherClient.ts";
import {
  reconcileUpdateOperation,
  reserveUpdateOperation,
  type NativeOperationBinding,
} from "./launcherOperation.ts";

const operationId = "12345678-1234-4234-8234-123456789abc";
const otherId = "22345678-1234-4234-8234-123456789abc";

// Drain fixture I/O before the release removes its exact scratch root.
const fixtureIo = <A>(run: () => Promise<A>) => Effect.promise(run).pipe(Effect.uninterruptible);

it.live.each(["send", "disconnect", "timeout"] as const)(
  "retries only the exact uncertain %s operation after native receipts prove it absent",
  (operation) =>
    Effect.acquireUseRelease(
      fixtureIo(() => NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-operation-handoff-"))),
      (allocated) =>
        Effect.gen(function* () {
          const baseDir = yield* fixtureIo(() => NodeFSP.realpath(allocated));
          const binding: NativeOperationBinding = {
            baseDir,
            dbPath: NodePath.join(baseDir, "userdata", "statev2.sqlite"),
            environmentId: "synthetic-environment",
            currentVersion: "0.0.0-preview.20261010.1.1",
            expectedInstalledSource: "a".repeat(40),
            targetSource: "b".repeat(40),
            targetVersion: "0.0.0-preview.20261010.2.1",
            stagedHandle: "fixed-candidate",
          };
          let attempts = 0;
          let clears = 0;
          const wrapped = yield* withRunningThreadContinuation({
            mode: "web",
            selfUpdate: {
              update: () => Effect.die("unexpected legacy update"),
              commitDesktopUpdate: () => Effect.never,
              installQualified: () =>
                Effect.sync(() => {
                  attempts += 1;
                }).pipe(
                  Effect.andThen(
                    Effect.fail(
                      new ServerSelfUpdateError({
                        reason: "uncertain handoff",
                        cause: new ServiceLauncherClientError({ operation }),
                      }),
                    ),
                  ),
                ),
            },
            prepare: Effect.succeed([]),
            clear: () =>
              Effect.sync(() => {
                clears += 1;
              }),
            reconcileQualifiedOperation: (id) =>
              fixtureIo(() => reconcileUpdateOperation(baseDir, id, undefined)),
          });
          const install = wrapped.installQualified!;
          const request = {
            operationId,
            stagedHandle: binding.stagedHandle,
            continueRunningThreads: true,
          };
          yield* install(request).pipe(Effect.flip);
          expect(attempts).toBe(1);
          expect(
            (yield* install({ ...request, operationId: otherId }).pipe(Effect.flip)).reason,
          ).toContain("needs reconciliation");
          expect(attempts).toBe(1);
          yield* install(request).pipe(Effect.flip);
          expect(attempts).toBe(2);
          yield* fixtureIo(() => reserveUpdateOperation(baseDir, operationId, binding));
          expect((yield* install(request).pipe(Effect.flip)).reason).toContain(
            "needs reconciliation",
          );
          expect(attempts).toBe(2);
          expect(clears).toBe(0);
        }),
      (allocated) =>
        fixtureIo(async () => {
          await NodeFSP.rm(allocated, { recursive: true, force: true });
          await expect(NodeFSP.lstat(allocated)).rejects.toMatchObject({ code: "ENOENT" });
        }),
    ),
);
