import { useLayoutEffect, useRef, useState } from "react";
import { Pressable, Text, View } from "react-native";
import {
  workstreamBindingKey,
  isNativeWorkstreams,
} from "@t3tools/client-runtime/state/workstreams";
import type { MobileWorkstreams } from "../../features/workstreams/types";
import type { MobileWorkstreamSnapshot } from "../../features/workstreams/projection";

export function MobileWorkstreamExport({
  api,
  snapshot,
}: {
  readonly api: Pick<MobileWorkstreams, "exportNative" | "bindingRevision">;
  readonly snapshot: MobileWorkstreamSnapshot | undefined;
}) {
  const scope =
    snapshot &&
    isNativeWorkstreams(snapshot.nativeCapability) &&
    api.exportNative &&
    !snapshot.stale &&
    !snapshot.data.stale &&
    snapshot.data.source === "live" &&
    snapshot.data.binding.permissions.includes("workstreams:read")
      ? api.bindingRevision + ":" + workstreamBindingKey(snapshot.data.binding)
      : null;
  if (!api.exportNative || !isNativeWorkstreams(snapshot?.nativeCapability)) return null;
  return <ExportAction key={scope} api={api} snapshot={snapshot} scope={scope} />;
}

function ExportAction({
  api,
  snapshot,
  scope,
}: {
  readonly api: Pick<MobileWorkstreams, "exportNative" | "bindingRevision">;
  readonly snapshot: MobileWorkstreamSnapshot | undefined;
  readonly scope: string | null;
}) {
  const job = useRef<AbortController | null>(null);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  useLayoutEffect(
    () => () => {
      job.current?.abort();
    },
    [],
  );
  if (!api.exportNative || !isNativeWorkstreams(snapshot?.nativeCapability)) return null;
  return (
    <View>
      <Pressable
        nativeID="workstreams-export"
        accessibilityRole="button"
        accessibilityLabel="Export Workstreams"
        disabled={scope === null || busy}
        onPress={() => {
          if (!snapshot || scope === null || job.current || !api.exportNative) return;
          const request = new AbortController();
          job.current = request;
          setBusy(true);
          setMessage(null);
          void api
            .exportNative(snapshot, { signal: request.signal })
            .then(async (result) => {
              if (request.signal.aborted || job.current !== request) return;
              const { shareGeneratedAttachment } = await import("../../lib/attachmentDownload");
              if (request.signal.aborted || job.current !== request) return;
              const shared = await shareGeneratedAttachment({
                bytes: result.bytes,
                attachment: { name: "workstreams.ndjson", mimeType: "application/x-ndjson" },
                signal: request.signal,
                sourceIdentifier: "workstreams-export",
              });
              if (!shared && !request.signal.aborted)
                throw new Error("Saving and sharing files is unavailable on this device.");
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
        style={{ paddingVertical: 12, opacity: scope === null || busy ? 0.45 : 1 }}
      >
        <Text style={{ color: "#2563eb" }}>
          {busy ? "Exporting Workstreams…" : "Export Workstreams"}
        </Text>
      </Pressable>
      {message ? <Text accessibilityRole="alert">{message}</Text> : null}
    </View>
  );
}
