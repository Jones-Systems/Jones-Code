import {
  isLinkedRequestSentAt,
  type LinkedRequestContext,
  LinkedRequestParent,
  LinkedRequestProject,
  LinkedRequestProjectIcon,
  LinkedRequestThread,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import type { Caller } from "../../mcp/threadAccess.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import { screensRemoteOperation } from "../remoteAdmissionIntegration/operations.ts";
import { LinkedRequestSentAtOverride, normalizeAdvisoryTitle } from "./LinkedRequestContext.ts";

const decodeThread = Schema.decodeUnknownOption(LinkedRequestThread);
const decodeParent = Schema.decodeUnknownOption(LinkedRequestParent);
const decodeProject = Schema.decodeUnknownOption(LinkedRequestProject);
const decodeIcon = Schema.decodeUnknownOption(LinkedRequestProjectIcon);
const titleFields = (value: string | null | undefined) => {
  const title = normalizeAdvisoryTitle(value);
  return title === undefined ? {} : { title };
};

// Enrichment shares contextFor's span. Suspend also catches synchronous dependency defects.
const advisoryLookup = Effect.fnUntraced(function* <A, E, R>(lookup: () => Effect.Effect<A, E, R>) {
  return yield* Effect.suspend(lookup).pipe(
    Effect.catchCause((cause) =>
      Cause.hasInterrupts(cause)
        ? Effect.failCause(Cause.fromReasons<never>(cause.reasons.filter(Cause.isInterruptReason)))
        : Effect.succeed(undefined),
    ),
  );
});

export const contextFor = Effect.fn("RequestContextSender.contextFor")(function* (input: {
  readonly scope: McpInvocationScope;
  readonly caller: Caller;
  readonly toolName: string;
  readonly args: Record<string, unknown>;
  readonly readOnly: boolean;
}): Effect.fn.Return<LinkedRequestContext | undefined, never, ProjectService.ProjectService> {
  if (!screensRemoteOperation(input.toolName, input.args, input.readOnly)) return undefined;
  const override = yield* LinkedRequestSentAtOverride;
  const sentAt = isLinkedRequestSentAt(override)
    ? override
    : DateTime.formatIso(yield* DateTime.now);
  const base: LinkedRequestContext = { schemaVersion: "t3.linked-request-context/v1", sentAt };
  const source = input.caller.caller;
  if (source === undefined) return base;

  const thread = decodeThread({
    threadId: source.id,
    ...titleFields(source.title),
  });
  const parent = yield* advisoryLookup(() =>
    Effect.gen(function* () {
      if (source.lineage.relationshipToParent === "subagent" && source.lineage.parentThreadId) {
        const parent = yield* input.caller.threads.getThreadShell(source.lineage.parentThreadId);
        return parent === null
          ? Option.none()
          : decodeParent({
              threadId: source.lineage.parentThreadId,
              ...titleFields(parent.title),
            });
      }
      return source.delegatedFrom === undefined
        ? Option.none()
        : decodeParent({
            threadId: source.delegatedFrom.threadId,
            ...titleFields(source.delegatedFrom.title),
            environmentId: source.delegatedFrom.environmentId,
          });
    }),
  );
  const projects = yield* ProjectService.ProjectService;
  const project = yield* advisoryLookup(() =>
    projects.getById(source.projectId).pipe(
      Effect.map((found) => {
        if (Option.isNone(found)) return Option.none();
        const value = found.value;
        const icon = decodeIcon(value.projectIcon);
        return decodeProject({
          projectId: value.id,
          ...titleFields(value.title),
          ...(Option.isSome(icon) && icon.value !== null ? { icon: icon.value } : {}),
        });
      }),
    ),
  );
  return {
    ...base,
    ...(Option.isSome(thread) ? { thread: thread.value } : {}),
    ...(parent !== undefined && Option.isSome(parent) ? { parent: parent.value } : {}),
    ...(project !== undefined && Option.isSome(project) ? { project: project.value } : {}),
  };
});
