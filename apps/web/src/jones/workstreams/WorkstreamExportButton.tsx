import { useLayoutEffect, useRef, useState } from "react";
import { workstreamBindingKey } from "@t3tools/client-runtime/state/workstreams";
import type { WorkstreamListView } from "../../state/workstreams";
import { Button } from "../../components/ui/button";

/** Only the controller can release a complete, current export; never rebuild its JSON lines. */
export function WorkstreamExportButton({
  controller,
}: {
  readonly controller: Pick<WorkstreamListView, "data" | "stale" | "exportNative">;
}) {
  const data = controller.data;
  const scope =
    data &&
    controller.exportNative &&
    !controller.stale &&
    !data.stale &&
    data.source === "live" &&
    data.binding.permissions.includes("workstreams:read")
      ? workstreamBindingKey(data.binding)
      : null;
  return <ExportAction key={scope} controller={controller} scope={scope} />;
}

function ExportAction({
  controller,
  scope,
}: {
  readonly controller: Pick<WorkstreamListView, "data" | "stale" | "exportNative">;
  readonly scope: string | null;
}) {
  const job = useRef<AbortController | null>(null);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  // oxlint-disable-next-line t3code/react-effect-budget -- aborts an in-flight export when the component unmounts; no render or handler owns unmount.
  useLayoutEffect(
    () => () => {
      job.current?.abort();
    },
    [],
  );
  if (!controller.exportNative) return null;
  return (
    <div className="px-1 py-1">
      <Button
        size="xs"
        variant="ghost"
        disabled={scope === null || busy}
        onClick={() => {
          if (scope === null || job.current || !controller.exportNative) return;
          const request = new AbortController();
          job.current = request;
          setBusy(true);
          setMessage(null);
          void controller
            .exportNative({ signal: request.signal })
            .then((result) => {
              if (request.signal.aborted || job.current !== request) return;
              const bytes = new Uint8Array(result.bytes);
              const url = URL.createObjectURL(new Blob([bytes], { type: "application/x-ndjson" }));
              // Schedule before clicking so even a failed download releases its URL.
              setTimeout(() => URL.revokeObjectURL(url), 30_000);
              const anchor = document.createElement("a");
              anchor.href = url;
              anchor.download = "workstreams.ndjson";
              anchor.click();
              setMessage("Workstreams export downloaded.");
            })
            .catch(() => {
              if (!request.signal.aborted && job.current === request)
                setMessage("Export could not be completed. Refresh Workstreams and try again.");
            })
            .finally(() => {
              if (job.current === request) {
                job.current = null;
                setBusy(false);
              }
            });
        }}
      >
        {busy ? "Exporting Workstreams…" : "Export Workstreams"}
      </Button>
      {message ? (
        <p role="status" className="text-xs text-muted-foreground">
          {message}
        </p>
      ) : null}
    </div>
  );
}
