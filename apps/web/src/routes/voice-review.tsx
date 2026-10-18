import { createFileRoute } from "@tanstack/react-router";
import { WorkQueuePage } from "../components/workQueue/WorkQueuePage";

export const Route = createFileRoute("/voice-review")({ component: WorkQueuePage });
