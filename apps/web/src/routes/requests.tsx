import { createFileRoute } from "@tanstack/react-router";
import { RequestsPage } from "../jones/remoteAdmission/RequestsPage";

export const Route = createFileRoute("/requests")({ component: RequestsPage });
