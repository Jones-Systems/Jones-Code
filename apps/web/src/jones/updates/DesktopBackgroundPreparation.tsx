import type { DesktopUpdateState } from "@t3tools/contracts";
import { useState } from "react";

import { SettingsRow } from "~/components/settings/settingsLayout";
import { Switch } from "~/components/ui/switch";
import { stackedThreadToast, toastManager } from "~/components/ui/toast";
import { parseTimestampDate } from "~/timestampFormat";

type DesktopPreparation = NonNullable<DesktopUpdateState["jonesPreparation"]>;

const timeFormatter = new Intl.DateTimeFormat(undefined, { hour: "numeric", minute: "2-digit" });

function at(iso: string | undefined): string {
  const date = iso === undefined ? null : parseTimestampDate(iso);
  return date === null ? "later" : `at ${timeFormatter.format(date)}`;
}

/** One sentence for each phase; installing always stays a manual action. */
export function describeDesktopBackgroundPreparation(preparation: DesktopPreparation): string {
  const target = preparation.targetVersion ?? "The newest build";
  switch (preparation.phase) {
    case "off":
      return "When on, newer builds download and verify while you work. Installing stays your choice.";
    case "unavailable":
      return preparation.reason ?? "This app cannot install Jones builds, so nothing is prepared.";
    case "blocked":
      return preparation.reason ?? "Paused until retained update work is reconciled.";
    case "pinned":
      return preparation.targetVersion === undefined
        ? "An install is in progress. Its build stays selected until it commits or rolls back."
        : `An install is in progress. ${preparation.targetVersion} waits until it commits or rolls back.`;
    case "preparing":
      return preparation.replacingVersion === undefined
        ? `Downloading and verifying ${target}.`
        : `Replacing the unused ${preparation.replacingVersion} download with ${target}.`;
    case "watching":
      return preparation.reason ?? "Watching for a newer build.";
    case "settling":
      return `${target} is the newest build. It will be prepared ${at(preparation.nextAt)} if nothing newer arrives.`;
    case "backoff":
      return `Preparing ${target} failed. It will be retried ${at(preparation.nextAt)}.`;
    case "ready":
      return `${target} is downloaded and verified. Install it when you are ready.`;
  }
}

/** The desktop's opt-in to prepare newer builds in the background. Off by default. */
export function DesktopBackgroundPreparationRow({
  preparation,
}: {
  readonly preparation: DesktopPreparation | undefined;
}) {
  const [pending, setPending] = useState(false);
  const setEnabled = window.desktopBridge?.setJonesBackgroundPreparation;
  if (preparation === undefined || setEnabled === undefined) return null;

  return (
    <SettingsRow
      title="Prepare updates in the background"
      description={describeDesktopBackgroundPreparation(preparation)}
      control={
        <Switch
          aria-label="Prepare updates in the background"
          checked={preparation.enabled}
          disabled={pending}
          onCheckedChange={(checked) => {
            setPending(true);
            void setEnabled(Boolean(checked))
              .catch((error: unknown) => {
                toastManager.add(
                  stackedThreadToast({
                    type: "error",
                    title: "Could not change background preparation",
                    description:
                      error instanceof Error ? error.message : "The setting was not changed.",
                  }),
                );
              })
              .finally(() => setPending(false));
          }}
        />
      }
    />
  );
}
