import { useAtomValue } from "@effect/atom-react";
import {
  createQueuedSteerBatchSubmission,
  deriveQueuedSteerBatch,
} from "@t3tools/client-runtime/jones/queue";
import {
  CommandId,
  type EnvironmentId,
  type OrchestrationV2ThreadProjection,
  type ThreadId,
} from "@t3tools/contracts";
import { randomUUID } from "../../lib/utils";
import { useState } from "react";
import { serverEnvironment } from "../../state/server";
import { threadEnvironment } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";

export function useQueuedSteerBatch(input: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly projection: OrchestrationV2ThreadProjection | undefined;
  readonly editing: boolean;
  readonly hasUnsavedMessages: boolean;
  readonly busy: boolean;
  readonly mutationLock: { readonly acquire: () => boolean; readonly release: () => void };
}) {
  const config = useAtomValue(serverEnvironment.configValueAtom(input.environmentId));
  const promote = useAtomCommand(threadEnvironment.promoteQueuedBatch);
  const identity = `${input.environmentId}:${input.threadId}`;
  const [submission, setSubmission] = useState(() => ({
    identity,
    controller: createQueuedSteerBatchSubmission(() => CommandId.make(randomUUID())),
  }));
  if (submission.identity !== identity) {
    setSubmission({
      identity,
      controller: createQueuedSteerBatchSubmission(() => CommandId.make(randomUUID())),
    });
  }
  const controller = submission.controller;
  const [status, setStatus] = useState({ controller, pending: false, failed: false });
  const pending = status.controller === controller && status.pending;
  const failed = status.controller === controller && status.failed;
  const view = deriveQueuedSteerBatch({
    ...input,
    capability: config?.environment.capabilities.queuedSteerBatch,
  });
  const awaitingProjection = view !== null && controller.awaitingProjection(view.items);
  const reason = pending
    ? "Sending queued messages together."
    : awaitingProjection
      ? "Waiting for the queue to update."
      : (view?.reason ?? null);
  return {
    view,
    pending,
    failed,
    reason,
    label: pending
      ? "Steering…"
      : controller.retry
        ? `Retry steer (${controller.retry.items.length})`
        : (view?.label ?? ""),
    async send() {
      if (!view || reason || !view.targetRunId || !input.mutationLock.acquire()) return;
      const lock = input.mutationLock;
      setStatus({ controller, pending: true, failed: false });
      try {
        const result = await controller.submit(
          {
            threadId: input.threadId,
            targetRunId: view.targetRunId,
            items: view.items,
          },
          (command) => promote({ environmentId: input.environmentId, input: command }),
        );
        setStatus((current) =>
          current.controller === controller
            ? { controller, pending: false, failed: result === "failure" }
            : current,
        );
      } finally {
        lock.release();
      }
    },
  };
}
