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
import { uuidv4 } from "../../lib/uuid";
import { useState } from "react";
import { View } from "react-native";
import { AppText } from "../../components/ui/AppText";
import { Button } from "../../components/ui/Button";
import { serverEnvironment } from "../../state/server";
import { threadEnvironment } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";
import { useThreadOutboxMessages } from "../../state/use-thread-outbox";
import { scopedThreadKey } from "../../lib/scopedEntities";

export function useMobileQueuedSteerBatch(input: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly projection: OrchestrationV2ThreadProjection | null;
  readonly editing: boolean;
  readonly busy: boolean;
  readonly mutationLock: { readonly acquire: () => boolean; readonly release: () => void };
}) {
  const config = useAtomValue(serverEnvironment.configValueAtom(input.environmentId));
  const outbox = useThreadOutboxMessages();
  const promote = useAtomCommand(
    threadEnvironment.promoteQueuedBatch,
    "steer queued messages together",
  );
  const identity = scopedThreadKey(input.environmentId, input.threadId);
  const [submission, setSubmission] = useState(() => ({
    identity,
    controller: createQueuedSteerBatchSubmission(() => CommandId.make(uuidv4())),
  }));
  if (submission.identity !== identity) {
    setSubmission({
      identity,
      controller: createQueuedSteerBatchSubmission(() => CommandId.make(uuidv4())),
    });
  }
  const controller = submission.controller;
  const [status, setStatus] = useState({ controller, pending: false, failed: false });
  const pending = status.controller === controller && status.pending;
  const failed = status.controller === controller && status.failed;
  const unsaved = outbox[scopedThreadKey(input.environmentId, input.threadId)] ?? [];
  const view = deriveQueuedSteerBatch({
    ...input,
    capability: config?.environment.capabilities.queuedSteerBatch,
    hasUnsavedMessages: unsaved.some(
      (message) => !input.projection?.messages.some((saved) => saved.id === message.messageId),
    ),
  });
  const reason = pending
    ? "Sending queued messages together."
    : view && controller.awaitingProjection(view.items)
      ? "Waiting for the queue to update."
      : (view?.reason ?? null);
  return {
    view,
    pending,
    failed,
    reason,
    label: controller.retry
      ? `Retry steer (${controller.retry.items.length})`
      : (view?.label ?? ""),
    async send() {
      if (!view || reason || !view.targetRunId || !input.mutationLock.acquire()) return;
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
        input.mutationLock.release();
      }
    },
  };
}

export function MobileQueuedSteerBatchAction({
  batch,
}: {
  readonly batch: ReturnType<typeof useMobileQueuedSteerBatch>;
}) {
  if (!batch.view) return null;
  return (
    <View className="gap-2 py-3">
      <Button
        label={batch.label}
        icon="arrow.turn.left.up"
        tone="text"
        loading={batch.pending}
        disabled={batch.reason !== null}
        accessibilityHint={batch.reason ?? batch.view.detail}
        onPress={() => void batch.send()}
      />
      {batch.reason || batch.view.remainingCount > 0 ? (
        <AppText className="text-sm text-foreground-muted">
          {batch.reason ?? batch.view.detail}
        </AppText>
      ) : null}
      {batch.failed ? (
        <AppText accessibilityRole="alert" className="text-sm text-danger">
          Batch steering was not confirmed. Retry the same batch.
        </AppText>
      ) : null}
    </View>
  );
}
