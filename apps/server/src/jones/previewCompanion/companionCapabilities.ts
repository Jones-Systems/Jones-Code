import * as Schema from "effect/Schema";
import type { CompanionOrigin } from "./RenderPlacement.ts";

export type RenderOrigin = "local" | CompanionOrigin | null;
export type CompanionRestrictedOperation = "recording" | "upload" | "clearProfile";
export class CompanionOperationUnsupported extends Schema.TaggedError<CompanionOperationUnsupported>()(
  "CompanionOperationUnsupported",
  {
    hostId: Schema.String,
    label: Schema.String,
    operation: Schema.Literals(["recording", "upload", "clearProfile"]),
  },
) {
  override get message() {
    return `${this.operation} is unsupported on preview browser host ${this.label}.`;
  }
}

export function assertCompanionCapability(
  origin: RenderOrigin,
  operation: CompanionRestrictedOperation,
): void {
  if (origin !== null && origin !== "local")
    throw new CompanionOperationUnsupported({
      hostId: origin.hostId,
      label: origin.label,
      operation,
    });
}

export async function cancelCompanionDownload(
  origin: RenderOrigin,
  download: { cancel: () => Promise<void> },
): Promise<boolean> {
  if (origin === null || origin === "local") return false;
  await download.cancel().catch(() => {});
  return true;
}

export async function cancelCompanionChooser(
  origin: RenderOrigin,
  chooser: { setFiles: (files: []) => Promise<void> },
  closed: () => void,
): Promise<boolean> {
  if (origin === null || origin === "local") return false;
  try {
    await chooser.setFiles([]);
  } catch {
    /* A lost guest still closes the viewer's unsupported chooser. */
  }
  closed();
  return true;
}
