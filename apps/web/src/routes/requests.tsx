import { createFileRoute, useSearch } from "@tanstack/react-router";
import { RequestsPage } from "../jones/remoteAdmission/RequestsPage";
import type { RequestsSearch } from "../jones/remoteAdmission/navigation";

function validateRequestsSearch(raw: Record<string, unknown>): RequestsSearch {
  const bounded = (value: unknown) =>
    typeof value === "string" && value.length > 0 && value.length <= 1024 ? value : undefined;
  return {
    receiver: bounded(raw.receiver),
    request: bounded(raw.request),
    // An invalid supplied binding must never become an ID-only lookup.
    binding: raw.binding === undefined ? undefined : (bounded(raw.binding) ?? null),
    access: bounded(raw.access),
  };
}

function RequestsRoute() {
  const search = useSearch({ from: "/requests" });
  return <RequestsPage search={search} />;
}

export const Route = createFileRoute("/requests")({
  component: RequestsRoute,
  validateSearch: validateRequestsSearch,
});
