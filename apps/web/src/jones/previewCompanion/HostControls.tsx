import { useAtomValue } from "@effect/atom-react";
import {
  AuthPreviewOperateScope,
  type EnvironmentId,
  type PreviewRenderHostSelection,
  type ScopedThreadRef,
} from "@t3tools/contracts";
import { runAtomCommand } from "@t3tools/client-runtime/state/runtime";
import { AsyncResult } from "effect/reactivity";
import { useState } from "react";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import { useEnvironmentScope } from "../../state/session";
import { toastManager } from "../../components/ui/toast";
import {
  Select,
  SelectTrigger,
  SelectValue,
  SelectPopup,
  SelectItem,
} from "../../components/ui/select";
import {
  hostsQuery,
  bindingsQuery,
  setDefaultCommand,
  setThreadCommand,
  useCompanionBindings,
} from "./state.ts";

const selectionValue = (selection: PreviewRenderHostSelection | null) =>
  selection?._tag === "companion"
    ? selection.hostId
    : selection?._tag === "server"
      ? "server"
      : "inherit";
export function BrowserHostSelect({
  environmentId,
  threadRef,
}: {
  readonly environmentId: EnvironmentId;
  readonly threadRef?: ScopedThreadRef;
}) {
  const result = useAtomValue(hostsQuery({ environmentId, input: {} }));
  const [pending, setPending] = useState(false);
  const canOperate = useEnvironmentScope(environmentId, AuthPreviewOperateScope);
  if (!AsyncResult.isSuccess(result) || result.value.status !== "ready")
    return (
      <span className="text-xs text-muted-foreground">
        {AsyncResult.isSuccess(result) && result.value.status === "unsupported"
          ? "Browser hosts aren't supported by this server."
          : "Browser hosts unavailable."}
      </span>
    );
  return (
    <ReadyHostSelect
      environmentId={environmentId}
      {...(threadRef ? { threadRef } : {})}
      hosts={result.value.value}
      pending={pending}
      disabled={!canOperate || pending}
      onSelect={async (value) => {
        const selection: PreviewRenderHostSelection | null =
          value === "inherit"
            ? null
            : value === "server"
              ? { _tag: "server" }
              : { _tag: "companion", hostId: value };
        setPending(true);
        try {
          const outcome = threadRef
            ? await runAtomCommand(appAtomRegistry, setThreadCommand, {
                environmentId,
                input: { threadId: threadRef.threadId, selection },
              })
            : await runAtomCommand(appAtomRegistry, setDefaultCommand, {
                environmentId,
                input: selection ?? { _tag: "server" },
              });
          if (outcome._tag !== "Success")
            toastManager.add({ type: "error", title: "Could not change browser host" });
          appAtomRegistry.refresh(hostsQuery({ environmentId, input: {} }));
          if (threadRef)
            appAtomRegistry.refresh(
              bindingsQuery({ environmentId, input: { threadId: threadRef.threadId } }),
            );
        } finally {
          setPending(false);
        }
      }}
    />
  );
}

function ReadyHostSelect(props: {
  readonly environmentId: EnvironmentId;
  readonly threadRef?: ScopedThreadRef;
  readonly hosts: import("@t3tools/contracts").PreviewCompanionHostsResponse;
  readonly pending: boolean;
  readonly disabled: boolean;
  readonly onSelect: (value: string) => void;
}) {
  return props.threadRef ? (
    <ThreadHostSelect {...props} threadRef={props.threadRef} />
  ) : (
    <HostOptions {...props} value={selectionValue(props.hosts.environmentDefault)} />
  );
}
function ThreadHostSelect(
  props: Parameters<typeof ReadyHostSelect>[0] & { readonly threadRef: ScopedThreadRef },
) {
  const binding = useCompanionBindings(props.threadRef);
  return (
    <HostOptions
      {...props}
      disabled={props.disabled || binding?.status !== "ready"}
      value={binding?.status === "ready" ? selectionValue(binding.value.selection) : "inherit"}
    />
  );
}
function HostOptions(props: Parameters<typeof ReadyHostSelect>[0] & { readonly value: string }) {
  const selectedHost = props.hosts.hosts.find((host) => host.hostId === props.value);
  const title =
    props.value === "inherit"
      ? "Environment default"
      : props.value === "server"
        ? "Server"
        : selectedHost
          ? `${selectedHost.label}${selectedHost.online ? "" : " (offline)"}`
          : "Unavailable host";
  return (
    <Select
      disabled={props.disabled}
      value={props.value}
      onValueChange={(value) => {
        if (value) props.onSelect(value);
      }}
    >
      <SelectTrigger
        size="sm"
        className="w-44"
        aria-label={
          props.threadRef ? "Browser host for new tabs in this thread" : "Default browser host"
        }
      >
        <SelectValue>{title}</SelectValue>
      </SelectTrigger>
      <SelectPopup align="end">
        {props.threadRef ? <SelectItem value="inherit">Environment default</SelectItem> : null}
        <SelectItem value="server">Server</SelectItem>
        {props.hosts.hosts.map((host) => (
          <SelectItem key={host.hostId} value={host.hostId}>
            {host.label}
            {host.online ? "" : " (offline)"}
          </SelectItem>
        ))}
      </SelectPopup>
    </Select>
  );
}

export function CompanionToolbar({
  threadRef,
  tabId,
}: {
  readonly threadRef: ScopedThreadRef;
  readonly tabId: string | null;
}) {
  const binding = useCompanionBindings(threadRef);
  const hosts = useAtomValue(hostsQuery({ environmentId: threadRef.environmentId, input: {} }));
  if (binding?.status === "unsupported") return null;
  const tab =
    binding?.status === "ready" ? binding.value.tabs.find((tab) => tab.tabId === tabId) : undefined;
  const host =
    tab?.hostId && AsyncResult.isSuccess(hosts) && hosts.value.status === "ready"
      ? hosts.value.value.hosts.find((host) => host.hostId === tab.hostId)
      : null;
  return (
    <div className="flex flex-wrap items-center gap-2 border-b px-3 py-1 text-xs text-muted-foreground">
      <span>
        {tab
          ? tab.hostId === null
            ? "Host: Server"
            : `Host: ${host?.label ?? "Companion"}${host?.online ? "" : " (unavailable)"}`
          : "Resolving browser host…"}
      </span>
      <BrowserHostSelect environmentId={threadRef.environmentId} threadRef={threadRef} />
      <span>New tabs only; existing tabs stay bound.</span>
      {tab?.hostId ? (
        <span>
          Screenshots use server artifacts. Recording, uploads, downloads, profile clearing and
          pop-up windows are unsupported.
        </span>
      ) : null}
    </div>
  );
}
