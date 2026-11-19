/* oxlint-disable shadcn/no-raw-colors -- The approved route arrow specifies these exact two colors. */
import { Tooltip, TooltipTrigger, TooltipPopup } from "../../components/ui/tooltip";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { useEnvironment } from "../../state/environments";
import { useQuietThreadShell } from "../../state/entities";
import { MachineIcon } from "./MachineIcon";
export { MachineIcon } from "./MachineIcon";

function useOriginMachine(environmentId: EnvironmentId, threadId: ThreadId) {
  const thread = useQuietThreadShell(scopeThreadRef(environmentId, threadId));
  const source = useEnvironment(thread?.delegatedFrom?.environmentId ?? null);
  // A linked launch can have only the saved client label.
  const savedLabel = thread?.linkOrigin?.label?.replace(/^(?:T3|Jones) Code ·\s*/i, "").trim();
  return {
    label: source?.label ?? savedLabel,
    kind: source?.serverConfig?.environment.platform.machine,
  };
}

export function MachineRouteBadge({
  environmentId,
  threadId,
}: {
  environmentId: EnvironmentId;
  threadId: ThreadId;
}) {
  const source = useOriginMachine(environmentId, threadId);
  const destination = useEnvironment(environmentId);
  if (!source.label) return null;
  const sourceLabel = source.label;
  const destinationLabel = destination?.label ?? "Unknown environment";
  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <span
            className="relative inline-flex h-4 w-8 shrink-0 items-center"
            role="img"
            aria-label={`From ${sourceLabel} to ${destinationLabel}`}
          />
        }
      >
        <MachineIcon label={sourceLabel} kind={source.kind} className="size-4 shrink-0" />
        <MachineIcon
          label={destinationLabel}
          kind={destination?.serverConfig?.environment.platform.machine}
          className="size-4 shrink-0"
        />
        <svg
          aria-hidden="true"
          viewBox="0 0 12 10"
          width="12.96"
          height="10.8"
          className="absolute left-1/2 top-1/2"
          style={{ transform: "translate(-50%, -50%) translateY(3.2px)" }}
          fill="none"
          strokeLinecap="round"
          strokeLinejoin="round"
        >
          <path d="M1.8 5h8.4M7 1.8 10.2 5 7 8.2" stroke="#202020" strokeWidth="3.1" />
          <path d="M1.8 5h8.4M7 1.8 10.2 5 7 8.2" stroke="#A7A7A7" strokeWidth="1" />
        </svg>
      </TooltipTrigger>
      <TooltipPopup>{`From ${sourceLabel} to ${destinationLabel}`}</TooltipPopup>
    </Tooltip>
  );
}

export function MachineOrigin({
  environmentId,
  threadId,
}: {
  environmentId: EnvironmentId;
  threadId: ThreadId;
}) {
  const { label, kind } = useOriginMachine(environmentId, threadId);
  return label ? (
    <Tooltip>
      <TooltipTrigger
        render={
          <span
            className="ml-auto inline-flex shrink-0 items-center gap-1 text-xs text-muted-foreground/60"
            aria-label={`From ${label}`}
          />
        }
      >
        from <MachineIcon label={label} kind={kind} className="size-4" />
      </TooltipTrigger>
      <TooltipPopup>{`From ${label}`}</TooltipPopup>
    </Tooltip>
  ) : null;
}
