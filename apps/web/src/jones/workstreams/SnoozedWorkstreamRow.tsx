import type {
  PersonalWorkstreamsController,
  PersonalWorkstreamView,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { AlarmClockOffIcon } from "lucide-react";
import { useState } from "react";
import { snoozeWakeLabel } from "../../components/Sidebar.snooze";
import { showPersonalFailure } from "./personalNotifications";

export function SnoozedWorkstreamRow({
  item,
  controller,
  canWake,
  now,
}: {
  item: PersonalWorkstreamView;
  controller: PersonalWorkstreamsController;
  canWake: boolean;
  now: string;
}) {
  const [pending, setPending] = useState(false);
  const wake = async () => {
    if (!controller.setSnoozedUntil) return;
    setPending(true);
    try {
      const result = await controller.setSnoozedUntil(item.id, null);
      if (!result.ok) showPersonalFailure(result.failure);
    } finally {
      setPending(false);
    }
  };
  return (
    <li className="min-w-0 list-none">
      <button
        type="button"
        aria-label={`Unsnooze Workstream ${item.name}`}
        disabled={!canWake || pending}
        onClick={() => void wake()}
        className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left text-xs text-sidebar-muted-foreground hover:bg-sidebar-row-hover hover:text-sidebar-foreground disabled:opacity-50"
      >
        <AlarmClockOffIcon className="size-3.5 shrink-0" aria-hidden />
        <span className="min-w-0 flex-1 truncate font-medium">{item.name}</span>
        {item.snoozedUntil ? (
          <span className="shrink-0 tabular-nums">
            {snoozeWakeLabel(item.snoozedUntil, { now })}
          </span>
        ) : null}
      </button>
    </li>
  );
}
