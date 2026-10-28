import type { ProjectIconOverride } from "@t3tools/contracts";
import { FolderCodeIcon } from "lucide-react";
import { createElement, lazy, Suspense } from "react";
import { ProjectMonogram } from "../../components/ProjectMonogram";
import { deriveProjectIdentity } from "../../projectIdentity";
import { projectIconColorClassName } from "../../projectIconColors";

const LucideGlyph = lazy(() =>
  import("../../lib/lucideIconByName").then(({ lucideIconByName }) => ({
    default: function LucideGlyph({ name }: { name: string }) {
      return createElement(lucideIconByName(name) ?? FolderCodeIcon, { className: "size-3.5" });
    },
  })),
);

export function SenderProjectIcon({
  icon,
  title,
}: {
  readonly icon: ProjectIconOverride | null | undefined;
  readonly title: string | null | undefined;
}) {
  if (icon?.kind === "monogram") {
    return <ProjectMonogram text={icon.text} color={icon.color} className="size-3.5" />;
  }
  if (icon?.kind === "emoji") {
    return (
      <span
        aria-hidden="true"
        className="inline-flex size-3.5 shrink-0 items-center justify-center leading-none"
      >
        {icon.emoji}
      </span>
    );
  }
  if (icon?.kind === "lucide") {
    return (
      <span
        aria-hidden="true"
        className={`inline-flex size-3.5 shrink-0 ${projectIconColorClassName(icon.color)}`}
      >
        <Suspense fallback={<FolderCodeIcon className="size-3.5" />}>
          <LucideGlyph name={icon.name} />
        </Suspense>
      </span>
    );
  }
  if (title) {
    const identity = deriveProjectIdentity(title);
    return <ProjectMonogram text={identity.monogram} color={identity.color} className="size-3.5" />;
  }
  return <FolderCodeIcon aria-hidden="true" className="size-3.5 shrink-0 text-icon-muted" />;
}
