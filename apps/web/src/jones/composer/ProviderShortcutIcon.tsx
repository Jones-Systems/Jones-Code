import { useId } from "react";
import { providerInstanceInitials } from "@t3tools/client-runtime/state/provider-instance-display";
import type { ProviderInstanceEntry } from "../../providerInstances";
import { ProviderInstanceIcon } from "../../components/chat/ProviderInstanceIcon";

export function ProviderShortcutIcon({
  entry,
}: {
  entry: Pick<ProviderInstanceEntry, "driverKind" | "displayName" | "accentColor">;
}) {
  const labelId = useId();
  const initials = Array.from(providerInstanceInitials(entry.displayName));
  const letters =
    initials.length === 1 ? (
      <text x={0} y={0} textAnchor="middle">
        {initials[0]}
      </text>
    ) : (
      <>
        <text x={-0.25} y={0} textAnchor="end">
          {initials[0]}
        </text>
        <text x={0.25} y={0} textAnchor="start">
          {initials.slice(1).join("")}
        </text>
      </>
    );

  return (
    <span className="relative inline-flex size-4 shrink-0 items-center justify-center">
      <ProviderInstanceIcon
        driverKind={entry.driverKind}
        displayName={entry.displayName}
        accentColor={entry.accentColor}
        className="size-4"
        iconClassName={
          entry.driverKind === "codex" || entry.driverKind === "claudeAgent" ? "size-5" : "size-4"
        }
      />
      <svg
        data-composer-control-icon
        data-provider-shortcut-label
        aria-hidden
        viewBox="-18 -13 36 26"
        className="pointer-events-none absolute left-1/2 top-1/2 z-40 h-6.5 w-9 -translate-x-1/2 -translate-y-1/2 overflow-visible"
        style={{
          width: 36,
          height: 26,
          fontFamily: "system-ui, sans-serif",
          fontSize: 9,
          fontWeight: 600,
        }}
        dominantBaseline="central"
      >
        <defs>
          <g id={labelId}>{letters}</g>
        </defs>
        <use
          href={`#${labelId}`}
          className="stroke-black fill-black"
          strokeWidth={6.25}
          strokeLinejoin="round"
        />
        <use href={`#${labelId}`} className="fill-white" />
      </svg>
    </span>
  );
}
