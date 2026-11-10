// @effect-diagnostics nodeBuiltinImport:off -- Build callbacks use a bounded, dependency-free metadata sink outside the application runtime.
import * as NodePath from "node:path";
import {
  availableLineage,
  completionObservation,
  recordCompletion,
  type CaptureResult,
  type CompletionObservation,
} from "../../../apps/server/src/jones/workspaceRetirement/CompletionObservation.ts";

export type ProducerCompletion = {
  producer: string;
  worktree: string;
  status: CompletionObservation["status"];
  roots: readonly string[];
  omittedEnrichment?: readonly string[];
};

export function completeProducer(
  input: ProducerCompletion,
  dependencies: {
    record?: typeof recordCompletion;
    report?: (message: string) => void;
    env?: NodeJS.ProcessEnv;
  } = {},
): CaptureResult {
  const report = dependencies.report ?? ((message) => process.stderr.write(`${message}\n`));
  let result: CaptureResult;
  try {
    const lineage = availableLineage(dependencies.env ?? process.env);
    result = (dependencies.record ?? recordCompletion)(
      completionObservation({
        ...input,
        owner: lineage.threadId ?? input.producer,
        roots: input.roots.map((root) => NodePath.resolve(input.worktree, root)),
        lineage,
        // An observation is never a replacement for the producer's allocation owner.
        omittedEnrichment: ["allocation_disposition_not_observed", ...(input.omittedEnrichment ?? [])],
      }),
    );
  } catch {
    result = { status: "unavailable", reason: "metadata_sink_unavailable" };
  }
  if (result.status !== "recorded") {
    try {
      report(
        `[${input.producer}] completion metadata unavailable: ${result.reason}; outcome=${input.status}.`,
      );
    } catch {
      // Optional diagnostics must preserve the producer's native result too.
    }
  }
  return result;
}
