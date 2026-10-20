import { CornerUpRightIcon } from "lucide-react";
import { Button } from "../../components/ui/button";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import type { useQueuedSteerBatch } from "./useQueuedSteerBatch";

export function QueuedSteerBatchAction({
  batch,
}: {
  readonly batch: ReturnType<typeof useQueuedSteerBatch>;
}) {
  if (!batch.view) return null;
  return (
    <>
      {batch.view.count > 0 && batch.view.remainingCount > 0 ? (
        <span className="text-muted-foreground">{batch.view.detail}</span>
      ) : null}
      {batch.failed ? (
        <span role="alert" className="text-destructive">
          Batch steering was not confirmed. Retry the same batch.
        </span>
      ) : null}
      <Tooltip>
        <TooltipTrigger render={<span className="flex shrink-0" />}>
          <Button
            size="xs"
            variant="ghost-muted"
            disabled={batch.reason !== null}
            aria-busy={batch.pending}
            onClick={() => void batch.send()}
          >
            <CornerUpRightIcon />
            {batch.label}
          </Button>
        </TooltipTrigger>
        <TooltipPopup>{batch.reason ?? batch.view.detail}</TooltipPopup>
      </Tooltip>
    </>
  );
}
