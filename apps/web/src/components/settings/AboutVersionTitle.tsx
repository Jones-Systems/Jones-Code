import { BuildVersion } from "../../jones/updates/updateDisplay";
import { APP_SOURCE_SHA, APP_BUILD_STARTED_AT } from "../../branding";

import { APP_VERSION } from "../../branding";
import { useCopyToClipboard } from "../../hooks/useCopyToClipboard";
import { InlineButton } from "../ui/button";
import { stackedThreadToast, toastManager } from "../ui/toast";
import { SharedTooltipTrigger } from "../ui/shared-tooltip";
import { useGT, msg, T } from "gt-react";

export function AboutVersionTitle() {
  const gt = useGT();
  const { copyToClipboard } = useCopyToClipboard<void>({
    target: {
      target: "version",
      unavailableMessage: msg("Clipboard API is unavailable while copying version."),
      failureMessage: msg("Failed to copy version to the clipboard."),
    },
    onError: (error) => {
      toastManager.add(
        stackedThreadToast({
          type: "error",
          title: gt("Could not copy version"),
          description: error.message,
        }),
      );
    },
  });

  return (
    <span className="inline-flex items-baseline gap-2">
      <span>Version</span>
      <BuildVersion
        label="app version"
        version={APP_VERSION}
        sourceSha={APP_SOURCE_SHA}
        startedAt={APP_BUILD_STARTED_AT}
      />
      <code className="text-2xs font-medium">
        <SharedTooltipTrigger
          render={
            <InlineButton
              tone="muted"
              aria-label={gt("Copy version {version}", { version: APP_VERSION })}
              onClick={() => copyToClipboard(APP_VERSION, undefined)}
            >
              {APP_VERSION}
            </InlineButton>
          }
          label={
            <>
              <T>Copy version</T>
            </>
          }
        />
      </code>
    </span>
  );
}
