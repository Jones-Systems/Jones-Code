import { ChevronRightIcon } from "lucide-react";
import {
  isNativeWorkstreams,
  nativeWorkstreamsLabel,
  type NativeWorkstreamsCapability,
} from "@t3tools/client-runtime/state/workstreams";
import type { WorkstreamSetupStatus } from "@t3tools/contracts/jones/workstreamOperations";
import { Badge } from "../../components/ui/badge";
import { useRelativeTimeState } from "../../hooks/useRelativeTimeState";

export const sourceReasonLabels = {
  descriptor_absent: "Source descriptor unavailable. Check native source enrollment.",
  build_unavailable: "Build identity unavailable. Check the running server build.",
  contract_mismatch: "Running build differs from enrollment. Refresh enrollment for this build.",
  source_mismatch: "This environment is not enrolled. Enroll its native source.",
  authority_mismatch: "Source authority differs. Check the enrolled authority.",
  generation_mismatch: "Source generation differs. Check native source enrollment.",
  authority_fenced: "Source authority is fenced. Resolve the fence before retrying.",
  authority_unavailable: "Source authority is unavailable. Restore the source connection.",
} as const;

export type SetupStatusTone = "success" | "warning" | "error";

export function SetupStatusLine({
  tone,
  badge,
  children,
}: {
  tone: SetupStatusTone;
  badge: string;
  children: string;
}) {
  return (
    <p role="status" className="flex flex-wrap items-center gap-1.5">
      <Badge size="sm" variant={tone}>
        {badge}
      </Badge>
      <span>{children}</span>
    </p>
  );
}

function sourceIssue(status: WorkstreamSetupStatus, native: boolean): string | null {
  const reason = status.t3_source.reason;
  if (status.t3_source.state === "ready" || reason === null) return null;
  if (!native) return sourceReasonLabels[reason];
  if (reason === "authority_fenced") return sourceReasonLabels.authority_fenced;
  if (reason === "generation_mismatch")
    return "Source generation changed. Refresh its native authority binding.";
  return "Source authority is unavailable. Restore the source connection and refresh native setup.";
}

function summarize(
  status: WorkstreamSetupStatus,
  native: boolean,
  issue: string | null,
): { tone: SetupStatusTone; badge: string; sentence: string } {
  if (status.registry.state === "not_configured")
    return {
      tone: "warning",
      badge: "Not configured",
      sentence: native
        ? "Configure native Workstreams on this server."
        : "Configure Workstreams activation on this server.",
    };
  if (status.registry.state === "unavailable")
    return {
      tone: "error",
      badge: "Offline",
      sentence: "Registry unreachable. Restore its connection, then refresh.",
    };
  if (issue) return { tone: "warning", badge: "Needs attention", sentence: issue };
  if (status.activation.missing_keys.length > 0)
    return {
      tone: "warning",
      badge: "Needs attention",
      sentence: "Some activation settings are missing.",
    };
  return {
    tone: "success",
    badge: "Ready",
    sentence: status.registry.can_write
      ? "Registry connected with write access."
      : "Registry connected, read-only.",
  };
}

const guideLabel = (code: string, native: boolean) =>
  code === "configure_activation"
    ? native
      ? "Native setup guide"
      : "Activation guide"
    : code === "enroll_native_source"
      ? "Native enrollment guide"
      : "Verification guide";

/** Read-only setup diagnosis: badge and top issue first; codes, guides and timestamps in Details. */
export function SetupStatusContent({
  status,
  capability,
}: {
  status: WorkstreamSetupStatus;
  capability?: NativeWorkstreamsCapability | null;
}) {
  const native = isNativeWorkstreams(capability);
  const label = nativeWorkstreamsLabel(capability);
  const issue = sourceIssue(status, native);
  const summary = summarize(status, native, issue);
  const checked = useRelativeTimeState(status.observed_at);
  const checkedLabel =
    checked.status === "relative"
      ? `Checked ${checked.suffix ? `${checked.value} ${checked.suffix}` : checked.value}`
      : null;
  const guides = status.next_steps.filter(
    (step) => !native || step.code !== "enroll_native_source",
  );
  return (
    <div className="space-y-1">
      <SetupStatusLine tone={summary.tone} badge={summary.badge}>
        {summary.sentence}
      </SetupStatusLine>
      <p>
        Placements {status.placements.readiness} · {status.operations.open} open{" "}
        {status.operations.open === 1 ? "change" : "changes"}
        {checkedLabel ? (
          <>
            {" · "}
            <time dateTime={status.observed_at}>{checkedLabel}</time>
          </>
        ) : null}
      </p>
      <details className="group">
        <summary className="inline-flex cursor-pointer list-none items-center gap-1 rounded-sm outline-none hover:text-foreground focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring [&::-webkit-details-marker]:hidden">
          Details
          <ChevronRightIcon aria-hidden className="size-3 group-open:rotate-90" />
        </summary>
        <dl className="mt-1.5 grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 gap-y-1">
          {label ? (
            <>
              <dt>Mode</dt>
              <dd>{label}</dd>
            </>
          ) : null}
          <dt>Registry</dt>
          <dd>
            {status.registry.state.replace("_", " ")}
            {status.registry.state === "ready"
              ? ` · ${status.registry.can_write ? "write access" : "read-only"}`
              : null}
          </dd>
          <dt>Source</dt>
          <dd>
            {issue ? (
              <>
                {issue} <code>{status.t3_source.reason}</code>
              </>
            ) : (
              "Native source ready"
            )}
          </dd>
          {status.activation.missing_keys.length > 0 ? (
            <>
              <dt>Missing</dt>
              <dd>{status.activation.missing_keys.join(", ")}</dd>
            </>
          ) : null}
          {native ? (
            <>
              <dt>Authority</dt>
              <dd>This environment’s own; no source enrollment needed.</dd>
            </>
          ) : null}
          {guides.length > 0 ? (
            <>
              <dt>Guides</dt>
              <dd className="flex flex-wrap gap-x-3">
                {guides.map((step) => (
                  <a
                    key={step.code}
                    className="underline underline-offset-2 hover:text-foreground"
                    href={`https://github.com/Jones-Systems/Jones-Code/blob/main/${step.doc}`}
                    target="_blank"
                    rel="noreferrer"
                  >
                    {guideLabel(step.code, native)}
                  </a>
                ))}
              </dd>
            </>
          ) : null}
          <dt>Checked</dt>
          <dd>{status.observed_at} · read-only diagnosis</dd>
        </dl>
      </details>
    </div>
  );
}
