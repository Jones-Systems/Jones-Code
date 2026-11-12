import { Link } from "@tanstack/react-router";
import { useAdmissionOverview } from "./useAdmission";

export function RemoteAdmissionCount() {
  return <RemoteAdmissionAttention compact />;
}

function RemoteAdmissionAttention({ compact = false }: { readonly compact?: boolean }) {
  const { held, unknown, capped } = useAdmissionOverview();
  if (!compact)
    return (
      <Link to="/requests" className="mx-2 rounded-md border px-2 py-1.5 text-xs" role="status">
        Requests{held ? ` · ${capped ? "At least " : ""}${held} need review` : ""}
        {unknown ? " · Some environments unavailable or loading" : ""}
      </Link>
    );
  if (!held && !unknown) return null;
  return (
    <span
      className="text-3xs tabular-nums"
      aria-label={`${capped ? "At least " : ""}${held} requests need review${unknown ? "; some environments unavailable or loading" : ""}`}
    >
      {held || ""}
      {capped && held ? "+" : ""}
      {unknown ? "?" : ""}
    </span>
  );
}
