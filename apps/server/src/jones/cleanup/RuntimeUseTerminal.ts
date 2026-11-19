import type { NativeSetupControl } from "../../terminal/NativeSetupControl.ts";
import { readRuntimeUsePreparedTerminal } from "./RuntimeUsePreparedTerminal.ts";
import { OrchestrationV2AppThreadJson, TerminalCwdStatError } from "@t3tools/contracts";
import { SHELL_RUN_THREAD_ID } from "@t3tools/contracts/jones/shellDestinationFence";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { ServerConfig } from "../../config.ts";
import { withWorkspaceLease } from "../../workspace/workspaceLease.ts";
import { overlaps } from "../ordinaryCheckout/CheckoutTarget.ts";
import {
  HeldRuntimeOwner,
  assertCheckedDatabaseOwner,
  bindHeldDatabaseOwner,
} from "./CheckedRuntimeOwner.ts";
import { currentRuntimeUseClaim } from "./RuntimeUseClaim.ts";
import { prepareRuntimeUseInvocation } from "./RuntimeUseInvocation.ts";
import { canonical, digest, RuntimeUseError, runtimeUseError } from "./RuntimeUseProtocol.ts";
import { captureRuntimeUseWorkspace } from "./RuntimeUseWorkspace.ts";

const decodeThread = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2AppThreadJson),
);

const destinationShellWorkspace = (join: Path.Path["Service"]["join"], stateDir: string) =>
  join(stateDir, "destination-shell");

export const makeDestinationShellLaunch = Effect.gen(function* () {
  const config = yield* ServerConfig;
  const path = yield* Path.Path;
  const fs = yield* FileSystem.FileSystem;
  const cwd = destinationShellWorkspace(path.join, config.stateDir);
  return Effect.fn("RuntimeUseTerminal.prepareDestinationShell")(function* <
    Input extends {
      readonly threadId: string;
      readonly cwd?: string | undefined;
      readonly worktreePath?: string | null | undefined;
    },
  >(input: Input) {
    if (input.threadId !== SHELL_RUN_THREAD_ID || input.cwd === undefined) return input;
    if (input.worktreePath != null || input.cwd !== config.cwd)
      return yield* new TerminalCwdStatError({
        cwd: input.cwd,
        cause: "runtime_use_terminal_scope_changed",
      });
    // Keep destination-shell runtime receipts outside the host's project tree.
    yield* fs
      .makeDirectory(cwd, { recursive: true, mode: 0o700 })
      .pipe(Effect.mapError((cause) => new TerminalCwdStatError({ cwd, cause })));
    return { ...input, cwd };
  });
});

/** One logical terminal start, including its existing shell fallbacks. Neither
 * PTY exit nor terminal close proves retirement of detached terminal writers. */
export const make = Effect.gen(function* () {
  const held = yield* Effect.serviceOption(HeldRuntimeOwner);
  const storage = yield* Effect.serviceOption(SqlClient.SqlClient);
  const config = yield* Effect.serviceOption(ServerConfig);
  const path = yield* Path.Path;
  const guard = Effect.fn("RuntimeUseTerminal.guard")(function* <A, E, R>(
    input: {
      readonly logicalStartId: string;
      readonly threadId: string;
      readonly terminalId: string;
      readonly cwd: string;
      readonly worktreePath: string | null;
      readonly nativeSetupControl?: NativeSetupControl;
    },
    start: (beforeSpawn: Effect.Effect<void, RuntimeUseError>) => Effect.Effect<A, E, R>,
  ): Effect.fn.Return<A, E | RuntimeUseError, R> {
    if (Option.isNone(held)) return yield* start(Effect.void);
    if (Option.isNone(storage))
      return yield* new RuntimeUseError({ reason: "runtime_use_owner_without_actual_database" });
    const destinationShell = input.threadId === SHELL_RUN_THREAD_ID;
    if (
      destinationShell &&
      (Option.isNone(config) ||
        input.cwd !== destinationShellWorkspace(path.join, config.value.stateDir) ||
        input.worktreePath !== null ||
        input.nativeSetupControl !== undefined)
    )
      return yield* new RuntimeUseError({ reason: "runtime_use_terminal_scope_changed" });
    const sql = storage.value;
    const owner = yield* bindHeldDatabaseOwner(held.value.token).pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
      Effect.mapError(runtimeUseError),
    );
    const workspace = yield* captureRuntimeUseWorkspace(input.worktreePath ?? input.cwd);
    const cwd = yield* captureRuntimeUseWorkspace(input.cwd);
    if (
      cwd.canonicalPath !== workspace.canonicalPath &&
      !cwd.canonicalPath.startsWith(workspace.canonicalPath.replace(/\/$/, "") + "/")
    )
      return yield* new RuntimeUseError({ reason: "runtime_use_terminal_cwd_outside_workspace" });
    const read = Effect.gen(function* () {
      if (destinationShell) return { destinationShell: workspace.canonicalPath };
      const rows = yield* sql<{
        readonly thread_id: string;
        readonly project_id: string;
        readonly payload_json: string;
        readonly workspace_root: string;
        readonly project_deleted_at: string | null;
      }>`SELECT t.thread_id,t.project_id,t.payload_json,p.workspace_root,p.deleted_at AS project_deleted_at
        FROM orchestration_v2_projection_threads t JOIN projection_projects p ON p.project_id=t.project_id
        WHERE t.thread_id=${input.threadId}`;
      const row = rows[0];
      if (row === undefined)
        return yield* readRuntimeUsePreparedTerminal({
          threadId: input.threadId,
          terminalId: input.terminalId,
          workspacePath: workspace.canonicalPath,
          cwd: cwd.canonicalPath,
          control: input.nativeSetupControl,
        }).pipe(Effect.provideService(SqlClient.SqlClient, sql));
      if (row.project_deleted_at !== null)
        return yield* new RuntimeUseError({ reason: "runtime_use_terminal_thread_missing" });
      const thread = yield* decodeThread(row.payload_json);
      if (
        thread.id !== input.threadId ||
        thread.id !== row.thread_id ||
        thread.projectId !== row.project_id ||
        thread.deletedAt !== null ||
        (thread.worktreePath ?? row.workspace_root) !== workspace.canonicalPath
      )
        return yield* new RuntimeUseError({ reason: "runtime_use_terminal_scope_changed" });
      return { thread, projectRoot: row.workspace_root };
    }).pipe(Effect.mapError(runtimeUseError));
    const snapshot = yield* read;
    const claim = Option.getOrNull(yield* currentRuntimeUseClaim);
    const validate = Effect.gen(function* () {
      if (canonical(yield* read) !== canonical(snapshot))
        return yield* new RuntimeUseError({ reason: "runtime_use_terminal_scope_changed" });
      const deletions = yield* sql<{ readonly worktree_path: string }>`SELECT worktree_path
        FROM jones_deletion_worktree_admissions WHERE state!='completed' ORDER BY worktree_path`;
      if (deletions.some((row) => overlaps(workspace.canonicalPath, row.worktree_path, "/")))
        return yield* new RuntimeUseError({ reason: "runtime_use_conflicting_deletion" });
    }).pipe(Effect.mapError(runtimeUseError));
    const revalidateWorkspace = Effect.gen(function* () {
      yield* workspace.revalidate;
      yield* cwd.revalidate;
    });
    return yield* withWorkspaceLease(
      workspace.canonicalPath,
      Effect.gen(function* () {
        yield* Effect.try({ try: () => assertCheckedDatabaseOwner(owner), catch: runtimeUseError });
        const use = yield* prepareRuntimeUseInvocation({
          owner,
          validate,
          revalidateWorkspace,
          binding: {
            schema: "jones.workspace-runtime-use/v1",
            useId: `terminal-start:v1:${digest({ logicalStartId: input.logicalStartId })}`,
            canonicalPath: workspace.canonicalPath,
            directoryIdentity: workspace.directoryIdentity,
            threadId: input.threadId,
            actor: { kind: "terminal_spawn", terminalSessionId: input.terminalId },
            original: {
              commandId: claim?.commandId ?? null,
              runId: claim?.runId ?? null,
              attemptId: claim?.attemptId ?? null,
              outboxReference: claim?.effectId ?? null,
              nativeReference:
                claim?.nativeReference?.claimId ??
                ("nativeReference" in snapshot ? snapshot.nativeReference : null),
              claim,
            },
          },
        }).pipe(Effect.provideService(SqlClient.SqlClient, sql));
        return yield* use.invoke(start(use.revalidateProducer));
      }),
    );
  });
  return { guard };
});
