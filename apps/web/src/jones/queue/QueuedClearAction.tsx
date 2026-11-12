import { Button } from "../../components/ui/button";

export function QueuedClearAction({
  disabled,
  onClear,
  failedCount,
}: {
  readonly disabled: boolean;
  readonly onClear: () => void;
  readonly failedCount: number;
}) {
  return (
    <>
      <Button size="xs" variant="ghost-muted" disabled={disabled} onClick={onClear}>
        Clear All
      </Button>
      {failedCount > 0 ? (
        <span role="alert" className="text-destructive">
          {failedCount} queued {failedCount === 1 ? "removal was" : "removals were"} not confirmed.
          Retry remaining items.
        </span>
      ) : null}
    </>
  );
}
