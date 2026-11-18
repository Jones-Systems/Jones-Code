import { useId } from "react";
import { ProjectFavicon, type ProjectFaviconProject } from "../../components/ProjectFavicon";
import {
  Select,
  SelectItem,
  SelectPopup,
  SelectTrigger,
  SelectValue,
} from "../../components/ui/select";

export type WorkstreamNameIconProject = ProjectFaviconProject & {
  readonly projectKey: string;
  readonly displayName: string;
};

function ProjectLabel({ project }: { project: WorkstreamNameIconProject }) {
  return (
    <span className="flex min-w-0 items-center gap-1.5">
      <ProjectFavicon project={project} className="size-3.5 shrink-0" />
      <span className="min-w-0 wrap-anywhere">{project.displayName}</span>
    </span>
  );
}

export function WorkstreamNameIconPicker({
  projects,
  value,
  onChange,
  disabled,
}: {
  projects: readonly WorkstreamNameIconProject[];
  value: string | null;
  onChange: (value: string | null) => void;
  disabled?: boolean;
}) {
  const labelId = useId();
  const selection = projects.find(({ projectKey }) => projectKey === value);
  return (
    <div className="@container/name-icon flex min-w-0 items-center justify-between gap-2">
      <span
        id={labelId}
        className="shrink-0 text-xs text-muted-foreground @max-[8rem]/name-icon:sr-only"
      >
        Name icon
      </span>
      <Select
        value={value ?? ""}
        onValueChange={(key) => onChange(key || null)}
        disabled={disabled}
      >
        <SelectTrigger
          aria-labelledby={labelId}
          size="compact"
          valueSizing
          className="ms-auto min-w-0"
        >
          <SelectValue className="min-w-0 !flex-auto">
            {selection ? (
              <ProjectLabel project={selection} />
            ) : value ? (
              "Unavailable project"
            ) : (
              <>
                <span className="@max-[8rem]/name-icon:hidden">None</span>
                <span className="hidden @max-[8rem]/name-icon:inline">Name icon</span>
              </>
            )}
          </SelectValue>
        </SelectTrigger>
        <SelectPopup align="end" alignItemWithTrigger={false} className="max-w-[min(20rem,90vw)]">
          <SelectItem value="" label="None">
            None
          </SelectItem>
          {projects.map((project) => (
            <SelectItem
              key={project.projectKey}
              value={project.projectKey}
              label={project.displayName}
            >
              <ProjectLabel project={project} />
            </SelectItem>
          ))}
        </SelectPopup>
      </Select>
    </div>
  );
}
