import { createFileRoute } from "@tanstack/react-router";
import { DesignRequestsPage } from "../jones/designRequests/DesignRequestsPage";

export const Route = createFileRoute("/design-requests")({ component: DesignRequestsPage });
