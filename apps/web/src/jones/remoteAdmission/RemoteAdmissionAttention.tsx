import { Link } from "@tanstack/react-router";
import { useAdmissionOverview } from "./useAdmission";

export function RemoteAdmissionCount() {
  const { held, unknown, capped } = useAdmissionOverview();
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

export function RemoteAdmissionAttention() {
  const { held, unknown, capped } = useAdmissionOverview();
  return (
    <Link to="/requests" className="mx-2 rounded-md border px-2 py-1.5 text-xs" role="status">
      Requests{held ? ` · ${capped ? "At least " : ""}${held} need review` : ""}
      {unknown ? " · Some environments unavailable or loading" : ""}
    </Link>
  );
}
