import type { ScopedThreadRef } from "@t3tools/contracts";
import { ThreadVisitWatermark as DwellQualifiedThreadVisitWatermark } from "../../jones/threadReadDwell/ThreadVisitWatermark";

// Keep the live shell subscription in the dwell-qualified leaf, away from ChatView's quiet shell.
// Its controller owns visit throttling and explicit mark-unread suppression until navigation.
export function ThreadVisitWatermark(props: { readonly threadRef: ScopedThreadRef }) {
  return <DwellQualifiedThreadVisitWatermark threadRef={props.threadRef} />;
}
