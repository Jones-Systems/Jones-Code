import { HistoryIcon } from "lucide-react";
import { useLayoutEffect, useRef, useState } from "react";
import {
  CommandId,
  type EnvironmentId,
  type ThreadId,
  type ImportedHistoryDelivery,
} from "@t3tools/contracts";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { createImportedHistoryCommands } from "@t3tools/client-runtime/jones/imported-history/commands";
import {
  createImportedHistoryChoiceController,
  importedHistoryReasonText,
  type ImportedHistoryChoiceState,
} from "@t3tools/client-runtime/jones/imported-history/continuation";
import { connectionAtomRuntime } from "../../connection/runtime";
import { importedHistoryCorrelationStorage } from "../../composerDraftStore";
import { useAtomCommand } from "../../state/use-atom-command";
import { ComposerBanner } from "../../components/chat/ComposerBanner";
import { Button } from "../../components/ui/button";
import { randomUUID } from "../../lib/utils";

export const importedHistoryCommands = createImportedHistoryCommands(connectionAtomRuntime);
export type PreparedImportedHistoryChoice = {
  readonly delivery: ImportedHistoryDelivery;
  readonly reviewedBasis: string;
  readonly draftIdentity: string;
  readonly unchanged: () => boolean;
};
export function ContinuationChoiceBanner(props: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly prepared: PreparedImportedHistoryChoice | null;
  readonly reason?: string | null;
  readonly onDismiss?: () => void;
}) {
  const start = useAtomCommand(importedHistoryCommands.start, { reportFailure: false });
  const observe = useAtomCommand(importedHistoryCommands.observe, { reportFailure: false });
  const identity = useAtomCommand(importedHistoryCommands.identity, { reportFailure: false });
  const storage = importedHistoryCorrelationStorage(
    scopeThreadRef(props.environmentId, props.threadId),
  );
  const controller = createImportedHistoryChoiceController(storage, {
    start: async (input) => {
      const result = await start({ environmentId: props.environmentId, input });
      return result._tag === "Success" ? result.value : null;
    },
    observe: async (input) => {
      const result = await observe({ environmentId: props.environmentId, input });
      return result._tag === "Success" ? result.value : null;
    },
  });
  const readSavedChoice = () => {
    try {
      return { saved: storage.read() !== null, state: null as ImportedHistoryChoiceState | null };
    } catch {
      return {
        saved: true,
        state: {
          status: "unknown" as const,
          intentAccepted: false,
          reason: "Saved choice is unavailable. Resolve storage before another start.",
        },
      };
    }
  };
  const [initialChoice] = useState(readSavedChoice);
  const [state, setState] = useState<ImportedHistoryChoiceState | null>(initialChoice.state);
  const [saved, setSaved] = useState(initialChoice.saved);
  const [busy, setBusy] = useState(false);
  const busyRef = useRef(false);
  const scope = `${props.environmentId}:${props.threadId}`;
  const scopeRef = useRef<string | null>(scope);
  useLayoutEffect(() => {
    scopeRef.current = scope;
    return () => {
      scopeRef.current = null;
    };
  }, [scope]);
  const [basis, setBasis] = useState({ scope, prepared: props.prepared, reason: props.reason });
  if (basis.scope !== scope || basis.prepared !== props.prepared || basis.reason !== props.reason) {
    setBasis({ scope, prepared: props.prepared, reason: props.reason });
    const next = readSavedChoice();
    setSaved(next.saved);
    setState(next.state);
  }
  const act = async () => {
    if (busyRef.current) return;
    busyRef.current = true;
    setBusy(true);
    return await (async () => {
      try {
        let next: ImportedHistoryChoiceState;
        if (saved) next = await controller.observe();
        else {
          const prepared = props.prepared;
          if (prepared === null || !prepared.unchanged()) return;
          const commandId =
            prepared.delivery.type === "message"
              ? prepared.delivery.command.commandId
              : CommandId.make(randomUUID());
          const command = {
            type: "thread.imported-history.start" as const,
            commandId,
            threadId: props.threadId,
            reviewedBasis: prepared.reviewedBasis,
            delivery: prepared.delivery,
          };
          const digests = await identity({ environmentId: props.environmentId, input: command });
          if (digests._tag !== "Success") {
            next = {
              status: "unknown",
              intentAccepted: false,
              reason: "Could not bind the reviewed command. No start was submitted.",
            };
          } else
            next = await controller.start(
              {
                environmentId: props.environmentId,
                command,
                draftIdentity: prepared.draftIdentity,
                ...digests.value,
              },
              () => scopeRef.current === scope && prepared.unchanged(),
            );
        }
        if (scopeRef.current === scope) {
          setState(next);
          setSaved(storage.read() !== null);
          if (next.status === "rejected") props.onDismiss?.();
        }
      } catch {
        if (scopeRef.current === scope)
          setState({
            status: "unknown",
            intentAccepted: false,
            reason: "Check continuation status before continuing.",
          });
      }
    })().finally(() => {
      busyRef.current = false;
      setBusy(false);
    });
  };
  if (!saved && props.prepared === null && !props.reason && state === null) return null;
  return (
    <ComposerBanner.Attachment>
      <ComposerBanner.Root variant="warning" aria-label="Imported history continuation">
        <ComposerBanner.Row layout="wrap-actions">
          <ComposerBanner.Icon>
            <HistoryIcon />
          </ComposerBanner.Icon>
          <ComposerBanner.Content>
            <span role="status">
              {importedHistoryReasonText(state?.reason ?? props.reason) ??
                (saved
                  ? "Your continuation request is saved. Check its status; execution is not confirmed."
                  : "Start a fresh provider conversation using the old history and your message?")}
            </span>
          </ComposerBanner.Content>
          <ComposerBanner.Actions>
            {saved || props.prepared !== null ? (
              <Button
                size="xs"
                variant="ghost"
                disabled={
                  busy || (!saved && (props.prepared === null || !props.prepared.unchanged()))
                }
                onClick={() => {
                  void act();
                }}
              >
                {saved ? "Check continuation status" : "Start fresh conversation"}
              </Button>
            ) : null}
            {!saved && props.onDismiss ? (
              <Button size="xs" variant="ghost" disabled={busy} onClick={props.onDismiss}>
                Dismiss
              </Button>
            ) : null}
          </ComposerBanner.Actions>
        </ComposerBanner.Row>
      </ComposerBanner.Root>
    </ComposerBanner.Attachment>
  );
}
