import type { CommandId, OrchestrationV2AppThread, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { ServerConfig } from "../config.ts";
import { GitWorkflowService } from "../git/GitWorkflowService.ts";
import {
  DelegatedCheckoutPlanError,
  planDelegatedCheckout,
  type DelegatedCheckoutPlanV1,
} from "./DelegatedCheckoutPolicy.ts";

export class DelegatedCheckoutPlanner extends Context.Service<
  DelegatedCheckoutPlanner,
  {
    readonly capture: (input: {
      readonly commandId: CommandId;
      readonly parent: OrchestrationV2AppThread;
      readonly projectWorkspaceRoot: string;
      readonly childThreadId: ThreadId;
    }) => Effect.Effect<DelegatedCheckoutPlanV1, DelegatedCheckoutPlanError>;
  }
>()("t3/orchestration-v2/DelegatedCheckoutPlanner") {}

export const layer = Layer.effect(
  DelegatedCheckoutPlanner,
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const git = yield* GitWorkflowService;
    const config = yield* ServerConfig;
    return DelegatedCheckoutPlanner.of({
      capture: (input) =>
        Effect.gen(function* () {
          const canonicalProjectRoot = yield* fs.realPath(input.projectWorkspaceRoot);
          const parentCheckoutPath = yield* fs.realPath(
            input.parent.worktreePath ?? input.projectWorkspaceRoot,
          );
          const canonicalWorktreesDir = yield* fs.realPath(config.worktreesDir);
          const { commitSha: parentCommit } = yield* git.resolveCommit({
            cwd: parentCheckoutPath,
            revision: "HEAD",
          });
          return planDelegatedCheckout({
            ...input,
            parentThreadId: input.parent.id,
            canonicalProjectRoot,
            parentCheckoutPath,
            canonicalWorktreesDir,
            parentCommit,
          });
        }).pipe(
          Effect.mapError((cause) =>
            Schema.is(DelegatedCheckoutPlanError)(cause)
              ? cause
              : new DelegatedCheckoutPlanError({
                  message: `The parent's local committed checkout could not be captured: ${String(cause)}`,
                }),
          ),
        ),
    });
  }),
);
