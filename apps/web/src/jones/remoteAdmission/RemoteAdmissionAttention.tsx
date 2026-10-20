import { useEffect, useState } from "react";
import { Link } from "@tanstack/react-router";
import type { EnvironmentId } from "@t3tools/contracts";
import { usePrimaryEnvironmentId } from "../../state/environments";
import { remoteAdmissionRequest } from "./runtime";

function Attention({ environmentId }: { readonly environmentId: EnvironmentId }) {
  const [count, setCount] = useState(0);
  useEffect(() => {
    let active = true;
    let inFlight = false;
    const refresh = async () => {
      if (inFlight || document.visibilityState === "hidden") return;
      inFlight = true;
      try {
        const result = await remoteAdmissionRequest(environmentId, { action: "attention" });
        if (active && "count" in result) setCount(result.count);
      } catch {
        if (active) setCount(0);
      }
      inFlight = false;
    };
    void refresh();
    const interval = window.setInterval(() => {
      void refresh();
    }, 10_000);
    document.addEventListener("visibilitychange", refresh);
    return () => {
      active = false;
      window.clearInterval(interval);
      document.removeEventListener("visibilitychange", refresh);
    };
  }, [environmentId]);
  if (count === 0) return null;
  return (
    <Link to="/settings/connections" className="mx-2 rounded-md border p-2 text-sm" role="status">
      Needs input · {count} remote {count === 1 ? "request" : "requests"}
    </Link>
  );
}
export function RemoteAdmissionAttention() {
  const environmentId = usePrimaryEnvironmentId();
  return environmentId ? <Attention key={environmentId} environmentId={environmentId} /> : null;
}
