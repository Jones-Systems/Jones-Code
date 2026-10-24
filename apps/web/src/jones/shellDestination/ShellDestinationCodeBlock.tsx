import { useEffect, useState, useSyncExternalStore, type ReactNode } from "react";
import { PlayIcon } from "lucide-react";
import {
  AuthTerminalOperateScope,
  EnvironmentId,
  ThreadId,
  type ScopedThreadRef,
} from "@t3tools/contracts";
import { parseShellDestinationFence } from "@t3tools/contracts/jones/shellDestinationFence";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { Button } from "../../components/ui/button";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import { TerminalViewport } from "../../components/ThreadTerminalDrawer";
import { useEnvironment } from "../../state/environments";
import { readEnvironmentScope } from "../../state/session";
import { useEnvironmentQuery } from "../../state/query";
import { terminalEnvironment } from "../../state/terminal";
import { useAtomCommand } from "../../state/use-atom-command";
import { randomUUID } from "../../lib/utils";
import { ShellRunController, SHELL_RUN_THREAD_ID, type ShellTerminalPort } from "./controller";
import { shellCommandEligibility } from "./model";
import { legacyShellRunKey, shellRunKey, shellRunStore } from "./runStore";
import { readShellDestination, useShellDestination } from "./useShellDestination";

let controller: ShellRunController | undefined;
function runController() {
  return (controller ??= new ShellRunController(shellRunStore(), randomUUID));
}

export function SourceShellRunLabel({ environmentId }: { environmentId: EnvironmentId | null }) {
  const environment = useEnvironment(environmentId);
  return <>Run in terminal{environment ? ` on ${environment.label}` : ""}</>;
}

export function ShellDestinationCodeBlock({
  code,
  meta,
  complete,
  blockPosition,
  messageId,
  threadRef,
  onRunShellCommand,
  children,
}: {
  readonly code: string;
  readonly meta: string | null | undefined;
  readonly complete: boolean;
  readonly blockPosition: number | undefined;
  readonly messageId: string | undefined;
  readonly threadRef: ScopedThreadRef | null;
  readonly onRunShellCommand: ((command: string) => void) | undefined;
  readonly children: (controls: {
    headerAccessory: ReactNode;
    runControl: ReactNode;
    fenceTitle: string | null;
  }) => ReactNode;
}) {
  const fence = parseShellDestinationFence(meta);
  const { destination, target } = useShellDestination(fence, threadRef?.environmentId ?? "");
  const eligibility = shellCommandEligibility(code, complete);
  const key = shellRunKey(
    threadRef?.environmentId ?? "",
    threadRef?.threadId ?? "",
    blockPosition ?? -1,
    destination.environmentId ?? "",
    code,
    messageId ?? "",
  );
  const controller = runController();
  const run = useSyncExternalStore(
    controller.store.subscribe,
    () => controller.store.get(key),
    () => null,
  );
  const legacyKey = legacyShellRunKey(key);
  const legacyRun = useSyncExternalStore(
    controller.store.subscribe,
    () => (legacyKey ? controller.store.get(legacyKey) : null),
    () => null,
  );
  const legacyBlocked = !run && legacyRun !== null;
  const [focusRequestId, setFocusRequestId] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const [closing, setClosing] = useState(false);
  const open = useAtomCommand(terminalEnvironment.open, { reportFailure: false });
  const write = useAtomCommand(terminalEnvironment.write, { reportFailure: false });
  const close = useAtomCommand(terminalEnvironment.close, { reportFailure: false });
  const terminal: ShellTerminalPort = {
    open: async ({ environmentId, input }) =>
      (await open({ environmentId: EnvironmentId.make(environmentId), input }))._tag === "Success",
    write: async ({ environmentId, input }) =>
      (await write({ environmentId: EnvironmentId.make(environmentId), input }))._tag === "Success",
    close: async ({ environmentId, input }) =>
      (await close({ environmentId: EnvironmentId.make(environmentId), input }))._tag === "Success",
  };
  const enabled = destination.status === "ready" || destination.status === "self";
  const reason =
    !threadRef || !onRunShellCommand || blockPosition === undefined
      ? "Run is available only in a project-backed chat."
      : !messageId?.trim()
        ? "Run is unavailable because this message has no stable identity."
        : (eligibility.reason ?? destination.reason);
  const sessions = useEnvironmentQuery(
    target && run && enabled && run.phase !== "closed"
      ? terminalEnvironment.metadata({ environmentId: target.environmentId, input: null })
      : null,
  );
  const matchingSession = sessions.data?.find(
    (session) => session.threadId === SHELL_RUN_THREAD_ID && session.terminalId === run?.terminalId,
  );
  useEffect(() => {
    // Streaming queries keep waiting=true while subscribed, even after a valid snapshot.
    if (!run || !sessions.isSuccess || !sessions.data) return;
    if (!matchingSession && sessions.dataUpdatedAt < run.updatedAt) return;
    void controller
      .reconcile(
        key,
        sessions.data.filter((session) => session.threadId === SHELL_RUN_THREAD_ID),
      )
      .catch(() => setError("Run coordination is unavailable. No command will be sent."));
  }, [
    controller,
    key,
    run,
    matchingSession,
    sessions.data,
    sessions.dataUpdatedAt,
    sessions.isSuccess,
  ]);

  const start = (again: boolean) => {
    if (reason || !enabled || !threadRef || !onRunShellCommand || (legacyBlocked && !again)) return;
    setError(null);
    const current = () => readShellDestination(fence, threadRef.environmentId);
    if (current().status === "self") {
      onRunShellCommand(eligibility.command);
      return;
    }
    setFocusRequestId((value) => value + 1);
    if (run && !again) return;
    void controller
      .run({
        key,
        code,
        complete,
        destination: current,
        terminal,
        again,
        ...(run ? { expectedTerminalId: run.terminalId } : {}),
      })
      .catch((error: unknown) => {
        setError(
          error instanceof Error && error.message.includes("storage is full")
            ? "Run tracking is full. Close an earlier terminal before starting another. No command was sent."
            : "Run tracking or cross-tab coordination is unavailable. No command was sent.",
        );
      });
  };
  const isPending = run?.phase === "opening" || run?.phase === "writing";
  const sessionConfirmed = sessions.isSuccess && matchingSession !== undefined;
  const runStatus = !run
    ? null
    : run.phase === "closed" || isPending
      ? run.phase
      : !enabled
        ? "disconnected"
        : run.phase === "unavailable"
          ? "unavailable"
          : !sessionConfirmed
            ? "checking previous terminal"
            : run.phase === "writeUnknown"
              ? "delivery unknown"
              : matchingSession?.status;
  const runLabel =
    run && run.phase !== "closed"
      ? `Focus terminal on ${destination.label}`
      : `Run on ${destination.label}`;
  const headerAccessory = (
    <Tooltip>
      <TooltipTrigger
        render={
          <span
            className="min-w-0 break-words text-2xs text-muted-foreground"
            aria-label={`Destination computer: ${destination.environmentId ?? "invalid"}`}
            tabIndex={0}
          />
        }
      >
        {destination.label} · {destination.status}
        {destination.claimedLabel &&
        (destination.status === "unknown" || destination.status === "label-mismatch")
          ? ` · Agent claimed: ${destination.claimedLabel}`
          : null}
      </TooltipTrigger>
      <TooltipPopup side="top">{destination.environmentId ?? destination.reason}</TooltipPopup>
    </Tooltip>
  );
  const runControl = (
    <Tooltip>
      <TooltipTrigger
        render={
          <Button
            type="button"
            variant="ghost-muted"
            size="icon-xs"
            aria-label={runLabel}
            disabled={!!reason || !enabled || closing || legacyBlocked || run?.phase === "closed"}
            onClick={() => start(false)}
          />
        }
      >
        <PlayIcon className="size-3" />
      </TooltipTrigger>
      <TooltipPopup side="top">{reason ?? runLabel}</TooltipPopup>
    </Tooltip>
  );
  return (
    <div data-shell-destination={destination.status}>
      {children({
        headerAccessory,
        runControl,
        fenceTitle: fence._tag === "Valid" ? (fence.title ?? null) : null,
      })}
      {reason ? <p className="px-3 text-xs text-muted-foreground">{reason}</p> : null}
      {error ? (
        <p role="alert" className="px-3 text-xs text-destructive">
          {error}
        </p>
      ) : null}
      {legacyBlocked ? (
        <div className="px-3 py-2 text-xs">
          <p role="alert">
            An earlier run of this block has no message identity. It may already have run, and its
            terminal cannot be safely assigned to this message. The command will not be replayed.
            Run again deliberately starts a new terminal.
          </p>
          <Button
            size="xs"
            variant="ghost"
            disabled={!!reason || !enabled}
            onClick={() => start(true)}
          >
            Run again
          </Button>
        </div>
      ) : null}
      {run ? (
        <div
          data-thread-terminal-drawer
          className="my-2 overflow-hidden rounded-lg border border-border/70 bg-background text-foreground"
        >
          <div className="flex flex-wrap items-center justify-between gap-2 border-b border-border/60 px-3 py-1.5">
            <span className="min-w-0 break-words text-2xs text-muted-foreground">
              {destination.label} · {run.cwd} · {runStatus}
            </span>
            <div className="flex shrink-0 items-center gap-1">
              <Button
                size="xs"
                variant="ghost"
                disabled={!!reason || !enabled || isPending || closing}
                onClick={() => start(true)}
              >
                Run again
              </Button>
              <Button
                size="xs"
                variant="ghost-muted"
                disabled={!enabled || isPending || closing || run.phase === "closed"}
                onClick={() => {
                  setClosing(true);
                  void controller
                    .close(
                      key,
                      terminal,
                      () =>
                        readEnvironmentScope(
                          EnvironmentId.make(run.environmentId),
                          AuthTerminalOperateScope,
                        ),
                      run.terminalId,
                    )
                    .then((closed) => {
                      if (!closed)
                        setError("Could not close this terminal. It may still be running.");
                    })
                    .catch(() =>
                      setError("Could not confirm terminal closure. It may still be running."),
                    )
                    .finally(() => setClosing(false));
                }}
              >
                Close
              </Button>
            </div>
          </div>
          {run.phase === "writeUnknown" ? (
            <p role="alert" className="px-3 py-2 text-xs">
              Command delivery is unknown. It may already have run. Reattach to inspect it; the
              command will not be resent. Run again deliberately starts a new terminal.
            </p>
          ) : null}
          {run.phase === "unavailable" ? (
            <p role="status" className="px-3 py-2 text-xs">
              The previous terminal is unavailable. This computer may have restarted or removed it.
              The command will not be replayed.
            </p>
          ) : null}
          {!enabled && run.phase !== "closed" ? (
            <p className="px-3 py-2 text-xs">
              Reconnect with terminal permission to reattach. No command will be resent.
            </p>
          ) : null}
          {enabled && sessions.error ? (
            <p role="status" className="px-3 py-2 text-xs">
              Unable to verify the previous terminal. Reattach when the destination is available; no
              command will be resent.
            </p>
          ) : null}
          {enabled && run.phase !== "closed" && !isPending && !matchingSession ? (
            <div className="px-3 py-2">
              <Button size="xs" variant="ghost" onClick={sessions.refresh}>
                Reattach
              </Button>
            </div>
          ) : null}
          {enabled &&
          sessionConfirmed &&
          target?.serverConfig &&
          matchingSession &&
          !isPending &&
          run.phase !== "closed" &&
          run.phase !== "unavailable" ? (
            <div className="h-64">
              <TerminalViewport
                threadRef={scopeThreadRef(target.environmentId, ThreadId.make(SHELL_RUN_THREAD_ID))}
                threadId={ThreadId.make(SHELL_RUN_THREAD_ID)}
                terminalId={run.terminalId}
                terminalLabel={`Run on ${destination.label}`}
                cwd={run.cwd}
                attachExistingOnly
                advancedTypography={false}
                onSessionExited={() => {
                  if (run.phase !== "writeUnknown")
                    void controller.store.mark(key, "exited", run.terminalId);
                }}
                focusRequestId={focusRequestId}
                autoFocus
                visible
                resizeEpoch={0}
                drawerHeight={256}
                keybindings={target.serverConfig.keybindings}
              />
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
