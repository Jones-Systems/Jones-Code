import { createFileRoute } from "@tanstack/react-router";
import { RequestsPage } from "../jones/remoteAdmission/RequestsPage";

export function validateRequestsSearch(raw: Record<string, unknown>) {
  const bounded = (value: unknown) => typeof value === "string" && value.length > 0 && value.length <= 1024 ? value : undefined;
  return { receiver: bounded(raw.receiver), request: bounded(raw.request), binding: bounded(raw.binding), access: bounded(raw.access) };
}

export const Route = createFileRoute("/requests")({ component: RequestsPage, validateSearch: validateRequestsSearch });
