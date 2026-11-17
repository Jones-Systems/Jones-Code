import { createFileRoute } from "@tanstack/react-router";
import { WorkQueuePage, validatePromptSearch } from "../components/workQueue/WorkQueuePage";

export const Route = createFileRoute("/work-queue")({
  component: PromptsRoute,
  validateSearch: validatePromptSearch,
});

function PromptsRoute() {
  const search = Route.useSearch();
  const navigate = Route.useNavigate();
  return (
    <WorkQueuePage
      navigation={{
        search,
        replace: (patch) => {
          void navigate({
            search: (previous) => ({ ...previous, ...patch }),
            replace: true,
            ignoreBlocker: true,
          });
        },
      }}
    />
  );
}
