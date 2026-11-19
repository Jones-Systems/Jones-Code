// @effect-diagnostics nodeBuiltinImport:off -- Real loopback and UNIX socket fixtures exercise the CLI transports.
import * as NodeHttp from "node:http";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";

import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  AuthSessionId,
  EnvironmentId,
  type AuthEnvironmentScope,
} from "@t3tools/contracts";
import type {
  JonesInstallPlan,
  JonesInstallPlanCliRequest,
} from "@t3tools/contracts/jones/installPlan";
import type { JonesOperationReceipt } from "@t3tools/contracts/jones/jonesOperations";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import { resolveDesktopAppControlAddress } from "@t3tools/shared/desktopAppControl";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NetService from "@t3tools/shared/Net";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import { Command } from "effect/cli";
import * as TestConsole from "effect/testing/TestConsole";
import { expect, vi } from "vite-plus/test";

import {
  makePersistedServerRuntimeState,
  persistServerRuntimeState,
} from "../../serverRuntimeState.ts";
import { installRecoveryCommand } from "./installRecoveryCli.ts";
import { JONES_OPERATION_RECEIPTS_HEADER } from "./serverCapability.ts";

const auth = vi.hoisted(() => ({
  scopes: [] as ReadonlyArray<AuthEnvironmentScope>[],
  revoked: [] as string[],
}));

vi.mock("../../auth/EnvironmentAuth.ts", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../auth/EnvironmentAuth.ts")>();
  return {
    ...actual,
    layerRuntime: Layer.mock(actual.EnvironmentAuth)({
      issueSession: (input) =>
        Effect.map(DateTime.now, (expiresAt) => {
          auth.scopes.push(input?.scopes ?? []);
          return {
            method: "bearer-access-token" as const,
            scopes: input?.scopes ?? [],
            subject: "fixture",
            client: { deviceType: "unknown" },
            sessionId: AuthSessionId.make("fixture-session"),
            token: "fixture-bearer",
            expiresAt: DateTime.toUtc(expiresAt),
          };
        }),
      revokeSession: (id) => Effect.sync(() => (auth.revoked.push(id), true)),
    }),
  };
});

const environmentId = EnvironmentId.make("fixture-environment");
const operationId = "061263ab-ba15-4c78-9c77-9b016a260e2b";
const jobId = "6a34778d-048a-43f2-86f2-47372e5e85dd";
const stopId = "e7add241-0f00-4e2e-92c5-b31aee653713";
const recoveryToken = "323b2536-28a3-4803-9afb-362b2cba6417";
const observedAt = "2026-11-19T00:00:00Z";
const receipt: JonesOperationReceipt = {
  protocol: 1,
  environmentId,
  operationId,
  jobId,
  phase: "held",
  code: "cleanup-unconfirmed",
  updatedAt: observedAt,
  expectedInstalledSource: "a".repeat(40),
  action: { kind: "download-exact", targetSource: "b".repeat(40) },
};
const state: JonesUpdateState = {
  source: "jones-actions",
  channel: "jones-main",
  phase: "blocked",
  capability: { check: true, download: true, install: true, operations: 1 },
  environmentId,
  operation: receipt,
};

function listen(server: NodeNet.Server, address?: string) {
  return new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    if (address === undefined) server.listen(0, "127.0.0.1", resolve);
    else server.listen(address, resolve);
  });
}

const serve = (server: NodeNet.Server, address?: string) =>
  Effect.acquireRelease(
    Effect.promise(() => listen(server, address)),
    () => Effect.promise(() => new Promise<void>((resolve) => server.close(() => resolve()))),
  );

const run = (baseDir: string, ...args: string[]) =>
  Command.runWith(installRecoveryCommand, { version: "0.0.0" })([
    ...args,
    "--base-dir",
    baseDir,
    "--json",
  ]);

it.layer(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer, NetService.layer, TestConsole.layer))(
  "install recovery CLI",
  (it) => {
    it.effect(
      "uses the live HTTP endpoints, binds exact IDs, and keeps cleanup tokens private",
      () =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-update-recovery-cli-" });
          const calls: Array<{
            url?: string | undefined;
            auth?: string | undefined;
            body: unknown;
          }> = [];
          const server = NodeHttp.createServer(async (request, response) => {
            const chunks: Buffer[] = [];
            for await (const chunk of request) chunks.push(chunk);
            const text = Buffer.concat(chunks).toString("utf8");
            const body = text === "" ? null : JSON.parse(text);
            calls.push({ url: request.url, auth: request.headers.authorization, body });
            let result: unknown = state;
            if (request.url?.endsWith("/operations/read")) {
              result = {
                receipts: [{ operationId, receipt }],
                ...(body.inspectCleanup
                  ? { cleanupInspection: { operationId, status: "eligible", recoveryToken } }
                  : {}),
              };
            } else if (request.url?.endsWith("/recover-cleanup")) {
              result = { recoveredCleanup: { ...receipt, phase: "interrupted" } };
            } else if (request.url?.endsWith("/stop")) {
              result = { ...receipt, stop: { stopId: body.stopId, requestedAt: observedAt } };
            }
            response.setHeader("content-type", "application/json");
            response.end(JSON.stringify(result));
          });
          yield* serve(server);
          const address = server.address();
          if (address === null || typeof address === "string")
            throw new Error("Missing fixture port.");
          yield* persistServerRuntimeState({
            path: path.join(baseDir, "userdata", "server-runtime.json"),
            state: yield* makePersistedServerRuntimeState({
              config: { host: "127.0.0.1", devUrl: undefined },
              port: address.port,
            }),
          });

          const target = [operationId, "--environment-id", environmentId];
          const issuedBefore = auth.scopes.length;
          const revokedBefore = auth.revoked.length;
          yield* run(baseDir, "status");
          yield* run(baseDir, "operation", ...target);
          yield* run(baseDir, "refresh-outcome", ...target);
          yield* run(baseDir, "inspect-cleanup", ...target);
          const before = calls.length;
          expect(
            (yield* Effect.exit(run(baseDir, "recover-cleanup", ...target, "--job-id", jobId)))
              ._tag,
          ).toBe("Failure");
          expect(calls).toHaveLength(before);
          expect(
            (yield* Effect.exit(
              run(baseDir, "operation", operationId, "--environment-id", "elsewhere"),
            ))._tag,
          ).toBe("Failure");
          expect(calls).toHaveLength(before + 1);
          expect(
            (yield* Effect.exit(
              run(baseDir, "recover-cleanup", ...target, "--job-id", stopId, "--confirm"),
            ))._tag,
          ).toBe("Failure");
          expect(calls.some((call) => call.url?.endsWith("/recover-cleanup"))).toBe(false);
          yield* run(baseDir, "recover-cleanup", ...target, "--job-id", jobId, "--confirm");
          yield* run(baseDir, "stop", ...target, "--stop-id", stopId, "--confirm");
          expect(calls.every((call) => call.auth === "Bearer fixture-bearer")).toBe(true);
          expect(
            calls.filter((call) => call.url?.endsWith("/operations/read")).map((call) => call.body),
          ).toEqual([
            { environmentId, operationIds: [operationId] },
            { environmentId, operationIds: [operationId], refreshInstallOutcome: operationId },
            ...Array.from({ length: 3 }, () => ({
              environmentId,
              operationIds: [operationId],
              inspectCleanup: operationId,
            })),
          ]);
          expect(calls.find((call) => call.url?.endsWith("/recover-cleanup"))?.body).toEqual({
            environmentId,
            operationId,
            jobId,
            recoveryToken,
            confirmed: true,
          });
          expect(calls.find((call) => call.url?.endsWith("/stop"))?.body).toEqual({
            environmentId,
            operationId,
            stopId,
          });
          const scopes = auth.scopes.slice(issuedBefore);
          expect(scopes.slice(0, 4)).toEqual([
            [AuthOrchestrationReadScope],
            [AuthOrchestrationReadScope],
            [AuthOrchestrationReadScope, AuthOrchestrationOperateScope],
            [AuthOrchestrationReadScope, AuthOrchestrationOperateScope],
          ]);
          expect(auth.revoked.slice(revokedBefore)).toHaveLength(scopes.length);
          const output = (yield* TestConsole.logLines).join("\n");
          expect(output).toContain("cleanup-unconfirmed");
          expect(output).not.toContain(recoveryToken);
          expect(output).not.toContain("fixture-bearer");
        }),
    );

    it.effect(
      "reads exact receipts with admission disabled and distinguishes unproved absence",
      () =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-restore-recovery-cli-" });
          const installed: JonesOperationReceipt = {
            protocol: 1,
            environmentId,
            operationId,
            updatedAt: observedAt,
            expectedInstalledSource: "a".repeat(40),
            action: {
              kind: "install",
              stagedHandle: "stage",
              currentVersion: "old",
              targetSource: "b".repeat(40),
            },
            phase: "handed-off",
            updateId: operationId,
          };
          const terminal: JonesOperationReceipt = {
            ...installed,
            phase: "blocked",
            outcome: {
              status: "blocked",
              fromVersion: "old",
              targetVersion: "new",
            },
          };
          let readback:
            | "retained"
            | "unsupported"
            | "unreadable"
            | "absent"
            | "header-stripped"
            | "legacy"
            | "missing-entry" = "retained";
          const server = NodeHttp.createServer(async (request, response) => {
            response.setHeader("content-type", "application/json");
            if (!request.url?.endsWith("/operations/read")) {
              response.end(
                JSON.stringify({
                  ...state,
                  operation: undefined,
                  capability: {
                    check: false,
                    download: false,
                    install: false,
                    reason: "blocked",
                    ...(readback === "legacy" || readback === "unsupported"
                      ? {}
                      : { operationReceipts: 1 }),
                  },
                }),
              );
              return;
            }
            const chunks: Buffer[] = [];
            for await (const chunk of request) chunks.push(chunk);
            const body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
            if (readback !== "legacy" && readback !== "header-stripped") {
              response.setHeader(
                JONES_OPERATION_RECEIPTS_HEADER,
                readback === "unsupported" ? "unsupported" : "supported",
              );
            }
            if (readback === "unsupported" || readback === "unreadable") {
              response.statusCode = 500;
              response.end(
                JSON.stringify({
                  code: "internal_error",
                  reason: "internal_error",
                  traceId: "synthetic",
                }),
              );
            } else {
              const retained = body.refreshInstallOutcome === operationId ? terminal : installed;
              response.end(
                JSON.stringify({
                  receipts:
                    readback === "missing-entry"
                      ? []
                      : [{ operationId, receipt: readback === "retained" ? retained : null }],
                }),
              );
            }
          });
          yield* serve(server);
          const address = server.address();
          if (address === null || typeof address === "string")
            throw new Error("Missing fixture port.");
          yield* persistServerRuntimeState({
            path: path.join(baseDir, "userdata", "server-runtime.json"),
            state: yield* makePersistedServerRuntimeState({
              config: { host: "127.0.0.1", devUrl: undefined },
              port: address.port,
            }),
          });
          const target = [operationId, "--environment-id", environmentId];
          const output = TestConsole.logLines.pipe(
            Effect.map((lines) => JSON.parse(String(lines.at(-1))) as unknown),
          );
          yield* run(baseDir, "operation", ...target);
          expect(yield* output).toEqual({ receipt: installed });
          yield* run(baseDir, "refresh-outcome", ...target);
          expect(yield* output).toEqual({ receipt: terminal });
          readback = "absent";
          yield* run(baseDir, "operation", ...target);
          expect(yield* output).toEqual({ receipt: null });
          readback = "header-stripped";
          yield* run(baseDir, "operation", ...target);
          expect(yield* output).toEqual({ receipt: null });
          for (const [mode, expected] of [
            ["unsupported", "unsupported"],
            ["unreadable", "could not be read"],
            ["legacy", "did not establish"],
            ["missing-entry", "did not return a result"],
          ] as const) {
            readback = mode;
            expect((yield* Effect.flip(run(baseDir, "operation", ...target))).message).toContain(
              expected,
            );
          }
        }),
    );

    it.effect("reads the native desktop status from the selected private socket", () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const userId = yield* HostProcess.UserId;
        if (userId === undefined) return;
        const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-install-plan-cli-" });
        const target = resolveDesktopAppControlAddress({
          stateDir: path.join(baseDir, "userdata"),
          platform: "darwin",
          tempDir: NodeOS.tmpdir(),
          userId,
          joinPath: path.join,
        });
        yield* fs.makeDirectory(target.directory!, { recursive: true, mode: 0o700 });
        const requests: JonesInstallPlanCliRequest[] = [];
        let receiptMode: "empty" | "archived" | "wrong-environment" = "empty";
        const archived: JonesInstallPlan = {
          schema: 1,
          planId: operationId,
          revision: 8,
          createdAt: observedAt,
          origin: "settings",
          target: {
            repository: "Jones-Systems/Jones-Code",
            sourceSha: "a".repeat(40),
            sourceTree: "b".repeat(40),
            version: "next",
          },
          from: { version: "previous" },
          laptop: { environmentId, operations: [] },
          devices: [],
          cancelled: true,
          cancelledFromRevision: 4,
          dismissedAt: observedAt,
        };
        yield* serve(
          NodeNet.createServer((socket) => {
            socket.setEncoding("utf8");
            let text = "";
            socket.on("data", (chunk) => {
              text += chunk;
              if (!text.includes("\n")) return;
              const request = JSON.parse(text) as JonesInstallPlanCliRequest;
              requests.push(request);
              socket.end(
                JSON.stringify({
                  version: 1,
                  requestId: request.requestId,
                  ok: true,
                  result: {
                    state: {
                      schema: 1,
                      current: null,
                      last: receiptMode === "empty" ? null : { ...archived, planId: jobId },
                      laptop: { observedAt, environmentId, activationBlocked: false },
                      view: { complete: false },
                    },
                    ...(receiptMode === "empty"
                      ? {}
                      : {
                          historyPlan:
                            receiptMode === "wrong-environment"
                              ? {
                                  ...archived,
                                  laptop: {
                                    ...archived.laptop,
                                    environmentId: EnvironmentId.make("another-laptop"),
                                  },
                                }
                              : archived,
                        }),
                  },
                }) + "\n",
              );
            });
          }),
          target.address,
        );
        const onLaptop = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
          Effect.provideService(effect, HostProcess.Platform, "darwin");
        yield* onLaptop(run(baseDir, "plan", "status"));
        expect(requests[0]?.action).toEqual({ type: "read" });
        const cancel = [
          "plan",
          "cancel",
          operationId,
          "--revision",
          "4",
          "--environment-id",
          environmentId,
        ];
        expect((yield* Effect.exit(onLaptop(run(baseDir, ...cancel))))._tag).toBe("Failure");
        expect(requests).toHaveLength(1);
        expect((yield* Effect.exit(onLaptop(run(baseDir, ...cancel, "--confirm"))))._tag).toBe(
          "Failure",
        );
        expect(requests[1]?.action).toEqual({
          type: "cancel-plan",
          confirmed: true,
          planId: operationId,
          expectedRevision: 4,
          environmentId,
        });
        receiptMode = "archived";
        yield* onLaptop(run(baseDir, "plan", "status", "--plan-id", operationId));
        expect(requests[2]?.action).toEqual({ type: "read", planId: operationId });
        expect(
          yield* Effect.flip(onLaptop(run(baseDir, "plan", "status", "--plan-id", stopId))),
        ).toMatchObject({
          message: "The desktop did not return the requested plan ID.",
        });
        yield* onLaptop(run(baseDir, ...cancel, "--confirm"));
        receiptMode = "wrong-environment";
        expect(yield* Effect.flip(onLaptop(run(baseDir, ...cancel, "--confirm")))).toMatchObject({
          message: "Cancellation is unconfirmed. Read plan status again.",
        });
      }),
    );
  },
);
