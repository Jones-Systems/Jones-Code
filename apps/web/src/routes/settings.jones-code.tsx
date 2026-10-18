import { createFileRoute } from "@tanstack/react-router";
import { PromptsSettings } from "../jones/workQueue/PromptsSettings";

export const Route = createFileRoute("/settings/jones-code")({ component: PromptsSettings });
