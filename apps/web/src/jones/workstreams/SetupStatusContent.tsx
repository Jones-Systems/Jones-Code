import {
  isNativeWorkstreams,
  nativeWorkstreamsLabel,
  type NativeWorkstreamsCapability,
} from "@t3tools/client-runtime/state/workstreams";
import type { WorkstreamSetupStatus } from "@t3tools/contracts/jones/workstreamOperations";

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

export function SetupStatusContent({
  status,
  capability,
}: {
  status: WorkstreamSetupStatus;
  capability?: NativeWorkstreamsCapability | null;
}) {
  const native = isNativeWorkstreams(capability);
  const label = nativeWorkstreamsLabel(capability);
  return (
    <div className="space-y-2 text-sm">
      {label ? <p role="status">{label}</p> : null}
      <p>
        {status.registry.state === "not_configured"
          ? native
            ? "Not configured — configure native Workstreams on this server."
            : "Not configured — configure Workstreams activation on this server."
          : status.registry.state === "unavailable"
            ? "Offline — restore the registry connection, then refresh."
            : `Registry ready · ${status.registry.can_write ? "Write access" : "Read-only"}`}
      </p>
      {status.activation.missing_keys.length > 0 ? (
        <p>Missing configuration: {status.activation.missing_keys.join(", ")}</p>
      ) : null}
      <p>
        {status.t3_source.state === "ready"
          ? "Native source ready"
          : `${status.t3_source.reason}: ${
              native
                ? status.t3_source.reason === "authority_fenced"
                  ? "Source authority is fenced. Resolve the fence before retrying."
                  : status.t3_source.reason === "generation_mismatch"
                    ? "Source generation changed. Refresh its native authority binding."
                    : "Source authority is unavailable. Restore the source connection and refresh native setup."
                : sourceReasonLabels[status.t3_source.reason]
            }`}
      </p>
      <p>
        Placements: {status.placements.readiness} · Open changes: {status.operations.open}
      </p>
      <ul className="space-y-1">
        {status.next_steps
          .filter((step) => !native || step.code !== "enroll_native_source")
          .map((step) => (
            <li key={step.code}>
              <a
                className="underline"
                href={`https://github.com/Jones-Systems/Jones-Code/blob/main/${step.doc}`}
                target="_blank"
                rel="noreferrer"
              >
                {step.code === "configure_activation"
                  ? native
                    ? "Native setup guide"
                    : "Activation guide"
                  : step.code === "enroll_native_source"
                    ? "Native enrollment guide"
                    : "Verification guide"}
              </a>
            </li>
          ))}
      </ul>
      {native ? (
        <p>
          Native setup uses this environment’s authority. No control-plane source enrollment is
          required.
        </p>
      ) : null}
      <p className="text-xs text-muted-foreground">
        Read-only diagnosis. Last checked {status.observed_at}.
      </p>
    </div>
  );
}
