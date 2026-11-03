// This acceptance fixture owns a Node child and its bounded shutdown outside the Effect service under test.
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeReadline from "node:readline";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { McpSchema, McpServer } from "effect/ai";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import { FleetCheckpointInput } from "@t3tools/contracts/jones/fleetCheckpoint";
import * as Schema from "effect/Schema";
import * as ProcessRunner from "../../processRunner.ts";
import * as McpHttpServer from "../../mcp/McpHttpServer.ts";
import { McpInvocationContext } from "../../mcp/McpInvocationContext.ts";
import { liveThreadsLayer } from "../../mcp/McpToolAccess.testkit.ts";
import { FleetCheckpointToolkit } from "../mcp/fleetCheckpoint/tools.ts";
import { FleetCheckpointHandlersLive } from "../mcp/fleetCheckpoint/handlers.ts";
import * as Checkpoint from "./Checkpoint.ts";
import { canonical, decode, digest, document } from "./Json.ts";
import { observe } from "./Evidence.ts";

const hostSource = process.env.JONES_FLEET_TEST_HOST_SOURCE;
const python = process.env.JONES_FLEET_TEST_PYTHON;
const decodeInput = Schema.decodeUnknownSync(FleetCheckpointInput);
const fixture = NodeURL.fileURLToPath(new URL("./controller.fixture.py", import.meta.url));
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "checkpoint-test", version: "1" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "checkpoint-test", version: "1" },
  },
  getClient: Effect.die("unused"),
});

// The external source/dependency qualification is unavailable in ordinary Jones-only CI.
it.live.skipIf(!hostSource || !python)(
  "registered native MCP places, replays and reads an attributed receipt through the actual CLI/controller/worker/native owners",
  () =>
    Effect.gen(function* () {
      const root = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(
          NodePath.join(process.env.JONES_FLEET_TEST_SCRATCH ?? NodeOS.tmpdir(), "jfc-"),
        ),
      );
      yield* Effect.addFinalizer(() =>
        Effect.promise(() => NodeFSP.rm(root, { recursive: true, force: true })),
      );
      const env: NodeJS.ProcessEnv = {
        ...process.env,
        PYTHONPATH: `${hostSource}/src:${hostSource}/tests`,
        PYTHONDONTWRITEBYTECODE: "1",
        TMPDIR: root,
        FLEET_BC_TEST_SCRATCH: root,
        FLEET_BC_HOST_TEST_ROOT: root,
        FLEET_CONTROLLER_TEST_ROOT: root,
        FLEET_BC_INSTALL_TEST_ROOT: root,
        FLEET_JOBS_TEST_SCRATCH: root,
      };
      delete env.PYTHONPYCACHEPREFIX;
      const child = NodeChildProcess.spawn(python!, ["-B", fixture, "serve"], {
        env,
        stdio: ["pipe", "pipe", "pipe"],
        detached: true,
      });
      const exited = new Promise<void>((resolve, reject) => {
        child.once("exit", () => resolve());
        child.once("error", reject);
      });
      let errors = "";
      child.stderr.on("data", (bytes) => {
        if (errors.length < 32768) errors += bytes.toString();
      });
      const lines = NodeReadline.createInterface({ input: child.stdout })[Symbol.asyncIterator]();
      const next = async () => {
        const line = await lines.next();
        if (line.done) throw new Error(`Fleet fixture stopped: ${errors}`);
        return document(JSON.parse(line.value));
      };
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          if (child.exitCode === null && !child.stdin.destroyed)
            child.stdin.end('{"action":"stop"}\n');
          const stop = (signal: NodeJS.Signals) => {
            if (child.pid && child.exitCode === null) {
              try {
                process.kill(-child.pid, signal);
              } catch (error) {
                if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
              }
            }
          };
          const drain = Effect.promise(() => exited);
          const finished = () =>
            drain.pipe(
              Effect.as(true),
              Effect.timeoutOrElse({ duration: 5000, orElse: () => Effect.succeed(false) }),
            );
          if (!(yield* finished())) {
            stop("SIGTERM");
            if (!(yield* finished())) {
              stop("SIGKILL");
              yield* drain;
            }
          }
        }),
      );
      const ready = yield* Effect.promise(next);
      const original = document(ready.request),
        originalKey = document(original.work_key);
      const { work_id: _, ...key } = originalKey;
      const input = decodeInput({
        originalRequestId: "native-original",
        request: { ...original, work_key: key },
      });
      const configuration = {
        executable: python!,
        arguments: ["-B", fixture, "client"],
        binding: String(ready.binding),
        cwd: hostSource!,
        controllerId: String(key.controller_id),
      };
      // Environment inheritance belongs to the test process. Production callers cannot supply it.
      const realRunner = ProcessRunner.layer;
      let lastOutput: ProcessRunner.ProcessRunOutput | undefined;
      const runnerWithFixtureEnvironment = Layer.effect(
        ProcessRunner.ProcessRunner,
        Effect.gen(function* () {
          const runner = yield* ProcessRunner.ProcessRunner;
          return ProcessRunner.ProcessRunner.of({
            run: (command) =>
              runner.run({ ...command, env }).pipe(
                Effect.tap((o) =>
                  Effect.sync(() => {
                    lastOutput = o;
                  }),
                ),
              ),
          });
        }),
      ).pipe(Layer.provide(realRunner));
      const registered = McpHttpServer.toolkitRegistration(
        FleetCheckpointToolkit,
        FleetCheckpointHandlersLive,
      ).pipe(
        Layer.provide(Checkpoint.layer(configuration)),
        Layer.provide(runnerWithFixtureEnvironment),
        Layer.provideMerge(liveThreadsLayer),
        Layer.provideMerge(McpServer.McpServer.layer),
        Layer.provideMerge(NodeServices.layer),
      );
      const invocation = {
        environmentId: EnvironmentId.make("native-environment"),
        thread: {
          threadId: ThreadId.make("native-thread"),
          providerSessionId: "provider-one",
          providerInstanceId: ProviderInstanceId.make("codex"),
        },
        client: undefined,
        requestNamespace: "provider-one",
        capabilities: new Set(["orchestration"] as const),
        issuedAt: 1,
      };
      yield* Effect.scoped(
        Effect.gen(function* () {
          const server = yield* McpServer.McpServer;
          const call = (name: string, request = input, provider = "provider-one") =>
            server.callTool({ name, arguments: request }).pipe(
              Effect.provideService(McpInvocationContext, {
                ...invocation,
                requestNamespace: provider,
                thread: { ...invocation.thread, providerSessionId: provider },
              }),
              Effect.provideService(McpSchema.McpServerClient, client),
              Effect.map((response) => {
                if (response.isError) {
                  if (lastOutput?.code === 0) {
                    const workKey = {
                      ...input.request.work_key,
                      work_id:
                        "jones-" +
                        digest([
                          invocation.environmentId,
                          invocation.thread.threadId,
                          input.originalRequestId,
                        ]),
                    };
                    observe(decode(lastOutput.stdout), {
                      input,
                      workKey,
                      requestDigest: digest({ ...input.request, work_key: workKey }),
                    });
                  }
                  throw new Error(JSON.stringify(response));
                }
                return document(response.structuredContent);
              }),
            );
          const invalid = yield* server
            .callTool({
              name: "fleet_checkpoint_place",
              arguments: { ...input, request: { ...input.request, target_id: "injected" } },
            })
            .pipe(
              Effect.provideService(McpInvocationContext, invocation),
              Effect.provideService(McpSchema.McpServerClient, client),
              Effect.result,
            );
          expect(invalid._tag).toBe("Failure");
          if (invalid._tag === "Failure")
            expect(String(invalid.failure).replace(/\s+/g, " ")).toContain(
              'Expected no excess property at ["request"]["target_id"]',
            );
          expect(lastOutput).toBeUndefined();
          const placed = yield* call("fleet_checkpoint_place");
          expect(placed.bindingValidation).toBe("bound");
          expect(placed.verificationOutcome).toBe("pending");
          const pending = { ...input, originalRequestId: "cancel-before-offer" };
          yield* call("fleet_checkpoint_place", pending);
          const cancelled = yield* call("fleet_checkpoint_cancel_pending", pending);
          expect(document(cancelled.cancellation).state).toBe("cancelled-before-dispatch");
          const replayed = yield* call("fleet_checkpoint_place", input, "provider-after-restart");
          expect(document(replayed.fleet).status).toBe("replayed");
          expect(replayed.workKey).toEqual(placed.workKey);
          const selected = document(document(placed.fleet).selection);
          yield* Effect.promise(async () => {
            child.stdin.write(
              JSON.stringify({ action: "execute", binding: selected.submission }) + "\n",
            );
            expect((await next()).state).toBe("settled");
          });
          const completed = yield* call("fleet_checkpoint_get", input, "provider-after-restart");
          expect(completed.verificationOutcome).toBe("passed");
          expect(completed.resourceClosure).toBe("unknown");
          expect(document(completed.cost).queue_ms).toBeNull();
          expect(document(completed.cost).return_ms).toBeNull();
          expect(completed.artifacts).toHaveLength(3);
          expect(JSON.parse(JSON.stringify(completed))).toEqual(completed);
          const expected = {
            input,
            workKey: input.request.work_key,
            requestDigest: String(completed.expectedRequestDigest),
          };
          const bound = {
            ...expected,
            workKey: {
              ...expected.workKey,
              work_id: String(document(completed.workKey).work_id),
            },
          };
          for (const field of [
            "capsule_id",
            "reservation_request_digest",
            "native_request_digest",
            "verification_receipt_sha256",
          ]) {
            const substituted = structuredClone(document(completed.fleet));
            const result = document(
              document(document(substituted.original_result).result).observed_postcondition,
            );
            const attribution = document(document(result.values).attribution);
            const changed = {
              ...attribution,
              [field]: field === "reservation_request_digest" ? "invalid" : "0".repeat(64),
            };
            const mutated = {
              ...substituted,
              original_result: {
                ...document(substituted.original_result),
                result: {
                  ...document(document(substituted.original_result).result),
                  observed_postcondition: {
                    ...result,
                    values: { ...document(result.values), attribution: changed },
                  },
                },
              },
            };
            expect(() => observe(mutated, bound)).toThrow();
          }
          const originalEnvelope = document(completed.fleet);
          const receipt = document(originalEnvelope.original_result);
          const operation = document(receipt.result),
            post = document(operation.observed_postcondition),
            report = document(post.values);
          const failedReceipt = {
            ...document(report.receipt),
            checks: [{ id: "catalog", outcome: "failed", exit_code: 2 }],
            outcome: "failed",
          };
          const failedReport = {
            ...report,
            receipt: failedReceipt,
            validation: {
              ...document(report.validation),
              outcome: "failed",
              receipt_sha256: digest(failedReceipt),
            },
            attribution: {
              ...document(report.attribution),
              verification_receipt_sha256: digest(failedReceipt),
            },
          };
          const { report_artifact: _omitted, ...stored } = document(failedReport);
          const failed = {
            ...originalEnvelope,
            original_result: {
              ...receipt,
              result: {
                ...operation,
                observed_postcondition: {
                  ...post,
                  values: {
                    ...failedReport,
                    report_artifact: {
                      sha256: digest(stored),
                      bytes: Buffer.byteLength(canonical(stored)),
                    },
                  },
                },
              },
            },
          };
          expect(observe(failed, bound).outcome).toBe("failed");
          const summary = {
            schema: "fleet-ops.retained-terminal-summary/v1",
            binding: selected.submission,
            terminal_state: "terminal",
            last_verified_effect: "completed",
            native_references: [],
            proof_digest: digest(receipt),
            detail_available: false,
          };
          expect(observe({ ...originalEnvelope, original_result: summary }, bound)).toEqual({
            outcome: "unknown",
            cost: null,
            artifacts: [],
          });
          const originalResult = document(document(completed.fleet).original_result);
          expect(originalResult.dispatch_knowledge).not.toBe("proven-not-dispatched");
          const cancel = yield* server
            .callTool({ name: "fleet_checkpoint_cancel_pending", arguments: input })
            .pipe(
              Effect.provideService(McpInvocationContext, invocation),
              Effect.provideService(McpSchema.McpServerClient, client),
            );
          expect(cancel.isError).toBe(true);
        }).pipe(Effect.provide(registered)),
      );
    }),
);
