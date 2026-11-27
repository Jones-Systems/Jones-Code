import { requestConfirmDialog } from "~/confirmDialog";
import { CleanupResults } from "../../jones/storageCleanupResults/CleanupResults";
import {
  retireAllConfirmation,
  retireConfirmation,
} from "../../jones/storageCleanupResults/retirementCopy";
import {
  cleanupFleetTimestamp,
  cleanupReportSummary,
  cleanupRequestSummary,
  cleanupRequestIsOutstanding,
  type CleanupOperation,
  type CleanupRequest,
} from "../../jones/storageCleanupResults/runStatus";
import { useClientSettings } from "../../hooks/useSettings";
import type {
  StorageCleanupReport,
  StorageCleanupSettings,
  WorktreeCleanupRules,
} from "@t3tools/contracts";
import { resolvePastedPath } from "@t3tools/shared/path";
import { resolveWorktreeCleanup } from "@t3tools/shared/projectSettings";
import { useId, useRef, useState } from "react";
import { useAtomValue } from "@effect/atom-react";
import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import { environmentAtomList, environmentIdsKey } from "../../state/environmentAtomList";
import { serverEnvironment } from "../../state/server";
import { useAtomCommand } from "../../state/use-atom-command";
import { useNowMinute } from "../../hooks/useNowMinute";
import { useDebouncedValue } from "../../state/queries";
import { AnimatedHeight } from "../AnimatedHeight";
import { Button } from "../ui/button";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";

import { Input } from "../ui/input";
import { Select, SelectItem, SelectPopup, SelectTrigger, SelectValue } from "../ui/select";
import { Switch } from "../ui/switch";
import {
  NumberField,
  NumberFieldDecrement,
  NumberFieldGroup,
  NumberFieldIncrement,
  NumberFieldInput,
} from "../ui/number-field";
import { SettingResetButton, SettingsRow, SettingsSection } from "./settingsLayout";
import { DatabaseStorageNotice } from "./DatabaseStorageNotice";
import { AdvancedSettings } from "./AdvancedSettings";
import { FoldedSettingsSection } from "./FoldedSettingsSection";
import { SettingsScopeNotice } from "./SettingsScopeNotice";
import { SettingsGroup } from "./SettingsGroup";
import type { ScopedSettingsTarget } from "./scopedSettings";
import { useSettingsScope } from "./SettingsScopeContext";
import { useSearchableSetting } from "./settingsSearch";
import {
  useClearScopedSettings,
  useScopedSettings,
  useScopedSettingsMixed,
  useUpdateScopedSettings,
} from "./useScopedSettings";
import { settingsScopeEnvironmentLabel } from "./settingsScopeAxis";
import { useGT, T, msg, useMessages } from "gt-react";
import { type MessageResolver, m } from "../../i18n/messages";

export function WorktreesDirectoryRow() {
  const setting = useSearchableSetting();
  const gt = useGT();
  const { connectedEnvironments, targets } = useSettingsScope();
  const settings = useScopedSettings();
  const updateSettings = useUpdateScopedSettings();
  const mixed = useScopedSettingsMixed(["worktreesDirectory"]);
  const edited = useRef(false);
  const draft = useRef("");
  if (
    connectedEnvironments.some(
      (environment) =>
        environment.serverConfig?.environment.capabilities.worktreesDirectory !== true,
    )
  )
    return null;
  const scopeKey = targets.map((target) => target.environmentId).join(",");

  return (
    <SettingsRow
      {...setting("storage-worktrees-location")}
      description={gt(
        "Folder where new worktrees are created, on any drive, such as D:\\worktrees or ~/worktrees. Existing worktrees stay where they are. Leave empty to use the T3 home folder.",
      )}
      serverScoped
      settingKeys={["worktreesDirectory"]}
      resetAction={
        mixed || settings.worktreesDirectory !== "" ? (
          <SettingResetButton
            ariaLabel={gt("Reset worktree location to default")}
            onClick={() => updateSettings({ worktreesDirectory: "" })}
          />
        ) : null
      }
      control={
        <Input
          key={`${scopeKey}:${mixed}:${settings.worktreesDirectory}`}
          aria-label={gt("Worktree location")}
          autoCapitalize="none"
          spellCheck={false}
          placeholder={mixed ? gt("Mixed") : gt("Default")}
          defaultValue={mixed ? "" : settings.worktreesDirectory}
          onFocus={(event) => {
            draft.current = event.target.value;
          }}
          onChange={(event) => {
            edited.current = true;
            draft.current = resolvePastedPath(draft.current, event.target.value);
            event.target.value = draft.current;
          }}
          onBlur={(event) => {
            const value = event.target.value.trim();
            if (edited.current && (mixed || value !== settings.worktreesDirectory))
              updateSettings({ worktreesDirectory: value });
            edited.current = false;
          }}
        />
      }
    />
  );
}

type SaveOptions = Parameters<ReturnType<typeof useUpdateScopedSettings>>[1];

function RetentionControl({
  label,
  value,
  onChange,
  minimum = 1,
}: {
  label: string;
  value: number | null;
  onChange: (value: number | null, onFailed: () => void) => void;
  minimum?: number;
}) {
  const gt = useGT();
  const [draft, setDraft] = useState(value);
  const [savedValue, setSavedValue] = useState(value);
  // Saving an age can start a cleanup sweep, so turning a rule on only opens a draft.
  // While its first save is in flight this holds that submission.
  const [enabling, setEnabling] = useState<"draft" | "edited" | { days: number } | null>(null);
  // Holds an age until Enter or focus leaves the row: one edited before focus moved to the
  // switch, so switching off never saves it first, or one stepped toward a rule's first age,
  // so raising it from 8 never saves a shorter age on the way.
  const [held, setHeld] = useState(false);
  // Hides a saved rule's age once it is switched off, so no blur can save it after null.
  const [switchedOff, setSwitchedOff] = useState(false);
  if (savedValue !== value) {
    setSavedValue(value);
    setDraft(value);
    setEnabling(null);
    setHeld(false);
    setSwitchedOff(false);
  }
  const pending = typeof enabling === "object" ? enabling : null;
  const on = (value !== null && !switchedOff) || enabling !== null;
  const row = useRef<HTMLDivElement>(null);

  const commit = (next: number) => {
    const days = Math.min(3650, Math.max(minimum, Math.round(next)));
    setDraft(days);
    setHeld(false);
    if (days === pending?.days) return;
    if (value !== null) {
      onChange(days, () => {});
      return;
    }
    const submission = { days };
    setEnabling(submission);
    onChange(days, () => setEnabling((current) => (current === submission ? null : current)));
  };

  const switchOff = () => {
    setDraft(value);
    setHeld(false);
    setEnabling(null);
    if (value === null && pending === null) return;
    setSwitchedOff(true);
    onChange(null, () => setSwitchedOff(false));
  };

  return (
    <div
      ref={row}
      className="flex items-center gap-3"
      onBlur={(event) => {
        if (event.currentTarget.contains(event.relatedTarget)) return;
        if (held && draft !== null) commit(draft);
        else setEnabling((current) => (typeof current === "string" ? null : current));
      }}
    >
      {on ? (
        <NumberField
          value={draft}
          min={minimum}
          max={3650}
          step={1}
          size="sm"
          className="w-auto"
          onValueChange={(next) => {
            setDraft(next);
            setEnabling((current) => (current === "draft" ? "edited" : current));
          }}
          onValueCommitted={(next, { reason, event }) => {
            if (next === null) setDraft(value);
            else if (
              reason === "input-blur"
                ? event instanceof FocusEvent &&
                  event.relatedTarget instanceof Node &&
                  row.current?.contains(event.relatedTarget)
                : value === null
            ) {
              setDraft(next);
              setHeld(true);
            } else commit(next);
          }}
        >
          <NumberFieldGroup>
            <NumberFieldDecrement aria-label={gt("Decrease {label}", { label })} />
            <NumberFieldInput
              aria-label={gt("{label} in days", { label })}
              autoFocus={value === null}
              size={5}
              className="w-auto grow-0 text-right"
              // Any typed or pasted edit replaces a held age: NumberField commits it on blur itself,
              // and text it cannot parse yet must not leave the held age to be saved.
              onChange={() => setHeld(false)}
              onPaste={() => setHeld(false)}
              onKeyDown={(event) => {
                if (event.nativeEvent.isComposing || event.keyCode === 229) return;
                if (event.key === "Enter") {
                  // Blurring lets NumberField parse the visible text and commit it once. It
                  // skips an unedited value, so an untouched draft is committed here.
                  const input = event.currentTarget;
                  const shown =
                    draft !== null && input.value === new Intl.NumberFormat().format(draft);
                  if (enabling === "draft" && shown) commit(draft);
                  input.blur();
                } else if (event.key === "Escape" && (held || typeof enabling === "string")) {
                  // An unhandled Escape closes the settings page.
                  event.preventDefault();
                  event.stopPropagation();
                  setDraft(pending?.days ?? value);
                  setHeld(false);
                  setEnabling((current) => (typeof current === "string" ? null : current));
                }
              }}
            />
            <span aria-hidden="true" className="self-center pr-2 text-xs">
              days
            </span>
            <NumberFieldIncrement aria-label={gt("Increase {label}", { label })} />
          </NumberFieldGroup>
        </NumberField>
      ) : (
        <span className="text-xs text-muted-foreground">
          <T>Off</T>
        </span>
      )}
      <Switch
        aria-label={label}
        checked={on}
        // Keeps focus in the age field, so switching off never commits an edited age first.
        onMouseDown={(event) => event.preventDefault()}
        onCheckedChange={(enabled) => {
          if (enabled) {
            setDraft(8);
            setEnabling("draft");
          } else switchOff();
        }}
      />
    </div>
  );
}

const KEEP_WHEN_LABELS = {
  "any-local-files": msg("Any local files"),
  "uncommitted-changes": msg("Uncommitted changes"),
  "tracked-changes": msg("Edited tracked files"),
} as const;

function lastRunSummary(
  entries: readonly { readonly label: string; readonly counts: string }[],
  machineCount: number,
  resolve: MessageResolver = m,
) {
  const first = entries[0];
  if (!first) return "";
  if (machineCount <= 1) return first.counts;
  if (entries.length === machineCount && entries.every((entry) => entry.counts === first.counts)) {
    return resolve(msg("{counts} on every machine", { counts: first.counts }));
  }
  return entries.map((entry) => `${entry.label}: ${entry.counts}`).join(" · ");
}

const COVERAGE_LABEL = {
  declared_cache: "Discard declared caches",
  dependency_reproduction: "Discard reproducible dependencies",
  format_recognized: "Discard recognized format files",
} as const;

const storageCleanupReportsAtom = environmentAtomList(
  "web-storage-cleanup-reports",
  (environmentId) => serverEnvironment.storageCleanupReport({ environmentId, input: {} }),
);
const storageCleanupPermissionsAtom = environmentAtomList(
  "web-storage-cleanup-permissions",
  serverEnvironment.runStorageCleanup.permissionAtom,
);

function CleanupSection() {
  const gt = useGT();
  const setting = useSearchableSetting();
  const timestampId = useId();
  const nowMinute = useNowMinute();
  const nowMs = Date.parse(`${nowMinute}:00.000Z`);
  const { scope, connectedEnvironments } = useSettingsScope();
  const [pending, setPending] = useState(false);
  const [requests, setRequests] = useState<Record<string, CleanupRequest & { label: string }>>({});
  // Set when a proposal or retirement report arrives in this view; it opens Last run so the owner sees the results.
  // Kept apart from `requests`, which are dropped once the retained report catches up.
  const [arrivedReportKey, setArrivedReportKey] = useState<string | null>(null);
  const timestampFormat = useClientSettings((settings) => settings.timestampFormat);
  const run = useAtomCommand(serverEnvironment.runStorageCleanup);
  const supported = connectedEnvironments.filter(
    (environment) => environment.serverConfig?.environment.capabilities.storageCleanupRun === true,
  );
  const supportedIds = supported.map(({ environmentId }) => environmentId);
  const reports = useAtomValue(storageCleanupReportsAtom(environmentIdsKey(supportedIds)));
  const permissions = useAtomValue(storageCleanupPermissionsAtom(environmentIdsKey(supportedIds)));
  const state = supportedIds.map((environmentId, index) => {
    const query = reports[index]!;
    return {
      environmentId,
      allowed: permissions[index]!,
      report: query._tag === "Success" ? query.value : null,
      loading: query.waiting,
      failed: query._tag === "Failure",
    };
  });
  const acknowledgedIds = state.flatMap(({ environmentId, report }) => {
    const request = requests[environmentId];
    return request && !cleanupRequestIsOutstanding(request, report) ? [environmentId] : [];
  });
  if (acknowledgedIds.length > 0) {
    const remaining = { ...requests };
    for (const environmentId of acknowledgedIds) delete remaining[environmentId];
    setRequests(remaining);
  }
  const hasRules = supported.some((environment) => {
    const settings = environment.serverConfig?.settings;
    if (!settings) return false;
    const enabled = (rules: WorktreeCleanupRules) =>
      rules.worktreeAfterDays !== null ||
      rules.worktreeSettledAfterDays !== null ||
      rules.worktreeOnMerge ||
      rules.worktreeOnDelete ||
      rules.worktreeUnchanged;
    return (
      settings.storageCleanup.browserArtifactsAfterDays !== null ||
      settings.storageCleanup.logsAfterDays !== null ||
      enabled(resolveWorktreeCleanup(settings, null)) ||
      Object.values(settings.projectSettingsOverrides).some(
        (override) =>
          override.worktreeCleanup?.mode === "custom" && enabled(override.worktreeCleanup.rules),
      )
    );
  });
  const displayTargets = [
    ...supported.map((environment) => ({
      environmentId: environment.environmentId,
      label: settingsScopeEnvironmentLabel(environment, connectedEnvironments),
    })),
    ...Object.entries(requests).flatMap(([environmentId, request]) =>
      scope.environmentIds.some((id) => id === environmentId) &&
      !supportedIds.some((id) => id === environmentId)
        ? [{ environmentId, label: request.label }]
        : [],
    ),
  ];
  if (displayTargets.length === 0) return null;
  const summaries = displayTargets.map(({ environmentId, label }) => {
    const query = state.find((entry) => entry.environmentId === environmentId);
    const request = requests[environmentId];
    const report = query
      ? query.report
      : request?.status === "completed"
        ? (request.report ?? null)
        : null;
    return {
      environmentId,
      report,
      label,
      counts:
        request && request.status !== "completed"
          ? cleanupRequestSummary(request)
          : report
            ? cleanupReportSummary(report)
            : query?.failed
              ? "Report unavailable"
              : query?.loading
                ? "Loading report…"
                : "No completed report available",
      description: report
        ? cleanupReportSummary(report)
        : query?.failed
          ? "Could not load the last report."
          : query?.loading
            ? "Loading last report…"
            : "No completed report available for this server session.",
    };
  });
  const timestamp = cleanupFleetTimestamp(summaries, timestampFormat, nowMs);
  const execute = async (
    environment: (typeof supported)[number],
    operation: CleanupOperation,
    input: Parameters<typeof run>[0]["input"],
    batch?: { position: number; total: number },
  ) => {
    const { environmentId } = environment;
    const label = settingsScopeEnvironmentLabel(environment, connectedEnvironments);
    const update = (request: CleanupRequest) =>
      setRequests((current) => ({ ...current, [environmentId]: { ...request, label } }));
    const unconfirmed = (detail: string) => {
      update({
        operation,
        status: "unconfirmed",
        detail: batch
          ? gt("Batch stopped at worktree {position} of {total}. {detail}", { ...batch, detail })
          : detail,
      });
      return false;
    };
    update({
      operation,
      status: "running",
      ...(batch ? { detail: gt("Retiring worktree {position} of {total}…", batch) } : {}),
    });
    try {
      const result = await run({ environmentId, input });
      if (result._tag !== "Success")
        return unconfirmed(
          isAtomCommandInterrupted(result)
            ? "The request was interrupted; check the retained report before trying again."
            : String(squashAtomCommandFailure(result)),
        );
      if (operation === "propose" || operation === "retire")
        setArrivedReportKey(
          JSON.stringify([environmentId, result.value.startedAt, result.value.finishedAt]),
        );
      if (batch && input.retirement) {
        const target = input.retirement;
        const targetState = result.value.entries.find(
          (entry) => entry.threadId === target.threadId && entry.path === target.worktreePath,
        )?.retirement?.state;
        if (targetState !== "retired" && targetState !== "held" && targetState !== "not_retired")
          return unconfirmed(gt("Check the retained report before trying again."));
      }
      update({ operation, status: "completed", report: result.value });
      return true;
    } catch (error) {
      return unconfirmed(error instanceof Error ? error.message : String(error));
    }
  };
  const propose = async () => {
    if (pending) return;
    const targets = supported.filter(
      (environment) =>
        environment.serverConfig?.environment.capabilities.settledWorktreeRetirement === true &&
        state.some((entry) => entry.environmentId === environment.environmentId && entry.allowed),
    );
    setPending(true);
    setRequests((current) => ({
      ...current,
      ...Object.fromEntries(
        targets.map((environment) => [
          environment.environmentId,
          {
            operation: "propose",
            status: "queued",
            label: settingsScopeEnvironmentLabel(environment, connectedEnvironments),
          } as const,
        ]),
      ),
    }));
    await targets
      .reduce(
        (previous, environment) =>
          previous.then(async () => {
            await execute(environment, "propose", { retirement: { action: "propose" } });
          }),
        Promise.resolve(),
      )
      .finally(() => setPending(false));
  };
  const retireTargets = async (
    environmentId: (typeof supportedIds)[number],
    entries: StorageCleanupReport["entries"],
  ) => {
    if (pending || entries.length === 0) return;
    const environment = supported.find((candidate) => candidate.environmentId === environmentId);
    if (
      !environment ||
      !state.some((entry) => entry.environmentId === environmentId && entry.allowed)
    )
      return;
    const targets = [];
    for (const entry of entries) {
      if (!entry.threadId || !entry.path || !entry.retirement?.fingerprint) return;
      targets.push({
        path: entry.path,
        coveredPaths: entry.retirement.paths,
        recoverability: entry.retirement.recoverability,
        threadId: entry.threadId,
        expectedFingerprint: entry.retirement.fingerprint,
      });
    }
    const total = targets.length;
    const message =
      total === 1
        ? retireConfirmation(targets[0]!)
        : retireAllConfirmation(targets, {
            title: gt("Retire {count} worktrees on {machine}?", {
              count: total,
              machine: settingsScopeEnvironmentLabel(environment, connectedEnvironments),
            }),
            description: gt(
              "Jones Code rechecks each checkout before removing it. Branches and recovery references are kept.\n\nThis authorizes disposal of the listed checkouts and their exact selections, including additions within the listed roots.",
            ),
            noSelection: gt("No local content selected."),
          });
    setPending(true);
    const retireConfirmedTargets = async (confirmed: boolean) => {
      if (!confirmed) return;
      for (const [index, target] of targets.entries()) {
        const resultKnown = await execute(
          environment,
          "retire",
          {
            retirement: {
              action: "retire",
              threadId: target.threadId,
              worktreePath: target.path,
              expectedFingerprint: target.expectedFingerprint,
              coveredPaths: target.coveredPaths,
            },
          },
          total > 1 ? { position: index + 1, total } : undefined,
        );
        if (!resultKnown) break;
      }
    };
    await (requestConfirmDialog(message, { variant: "destructive" }) ?? Promise.resolve(false))
      .then(retireConfirmedTargets)
      .finally(() => setPending(false));
  };
  const deleteNow = async () => {
    if (pending) return;
    setPending(true);
    await Promise.all(
      supported
        .filter((environment) =>
          state.some((entry) => entry.environmentId === environment.environmentId && entry.allowed),
        )
        .map((environment) => execute(environment, "cleanup", {})),
    ).finally(() => setPending(false));
  };
  return (
    <SettingsSection id="storage-cleanup" title={gt("Cleanup")} variant="plain">
      <SettingsGroup>
        <SettingsRow
          {...setting("storage-delete-now")}
          description={
            <span>
              {summaries.map(({ environmentId, label, description }) => (
                <span key={environmentId} className="block">
                  {displayTargets.length > 1 ? `${label}: ` : ""}
                  {description}
                </span>
              ))}
              {!hasRules && (
                <span className="block">Turn on a cleanup rule to use Delete now.</span>
              )}
            </span>
          }
          control={
            <>
              {supported.some(
                (e) => e.serverConfig?.environment.capabilities.settledWorktreeRetirement === true,
              ) && (
                <Button
                  variant="outline"
                  size="sm"
                  disabled={
                    pending ||
                    !supported.some(
                      (environment) =>
                        environment.serverConfig?.environment.capabilities
                          .settledWorktreeRetirement === true &&
                        state.some(
                          (entry) =>
                            entry.environmentId === environment.environmentId && entry.allowed,
                        ),
                    )
                  }
                  onClick={() => void propose()}
                >
                  Propose retirements
                </Button>
              )}
              <Button
                variant="outline"
                size="sm"
                disabled={pending || !hasRules || !state.some((entry) => entry.allowed)}
                onClick={() => void deleteNow()}
              >
                Delete now
              </Button>
            </>
          }
        />
      </SettingsGroup>
      <FoldedSettingsSection
        id="storage-cleanup-last-run"
        title="Last run"
        headerPlacement="inside"
        wrapHeader={false}
        openKey={arrivedReportKey}
        summary={lastRunSummary(summaries, scope.environmentIds.length)}
        control={
          <Tooltip>
            <TooltipTrigger
              render={<span tabIndex={0} />}
              className="truncate whitespace-nowrap text-right text-xs text-muted-foreground"
              aria-label={`Last completed run: ${timestamp.exact}`}
              aria-describedby={timestampId}
            >
              {timestamp.label}
            </TooltipTrigger>
            <TooltipPopup role="tooltip" id={timestampId}>
              {timestamp.exact}
            </TooltipPopup>
          </Tooltip>
        }
      >
        <div className="px-3 py-2.5 text-xs text-muted-foreground sm:px-4">
          Reports are retained until each server restarts. Manual reports stay visible ahead of
          automatic cleanup. Progress is shown for requests started in this view.
        </div>
        {summaries.map(({ environmentId, label, report }) => {
          const request = requests[environmentId];
          const query = state.find((entry) => entry.environmentId === environmentId);
          if (request && cleanupRequestIsOutstanding(request, report))
            return (
              <p key={environmentId} role="status" className="px-3 py-2.5 text-xs sm:px-4">
                {label}: {cleanupRequestSummary(request)}{" "}
                {request.status === "completed"
                  ? query?.failed
                    ? "Could not load the retained report."
                    : "Waiting for the retained report to update."
                  : ""}{" "}
                {report
                  ? "Previous completed report shown below."
                  : "No completed report available."}
              </p>
            );
          if (!report)
            return (
              <p
                key={environmentId}
                role="status"
                className="px-3 py-2.5 text-xs text-muted-foreground sm:px-4"
              >
                {label}:{" "}
                {query?.failed
                  ? "Could not load the last report."
                  : query?.loading
                    ? "Loading last report…"
                    : "No completed report available for this server session."}
              </p>
            );
          return null;
        })}
        <CleanupResults
          selectedEnvironmentIds={displayTargets.map(({ environmentId }) => environmentId)}
          reports={summaries.flatMap(({ environmentId, label, report }) => {
            const environment = supported.find(
              (candidate) => candidate.environmentId === environmentId,
            );
            return report
              ? [
                  {
                    environmentId,
                    environmentLabel: label,
                    report,
                    timestampFormat,
                    nowMs,
                    ...(environment?.serverConfig?.environment.capabilities
                      .settledWorktreeRetirement === true &&
                    state.some((e) => e.environmentId === environment.environmentId && e.allowed)
                      ? {
                          retirementPending: pending,
                          onRetire: (entry: StorageCleanupReport["entries"][number]) =>
                            void retireTargets(environment.environmentId, [entry]),
                          onRetireAll: (entries: StorageCleanupReport["entries"]) =>
                            void retireTargets(environment.environmentId, entries),
                        }
                      : {}),
                  },
                ]
              : [];
          })}
        />
      </FoldedSettingsSection>
    </SettingsSection>
  );
}

function supportsStorageCleanup(
  environments: ReadonlyArray<{
    readonly serverConfig: {
      readonly environment: { readonly capabilities: { readonly storageCleanup?: boolean } };
    } | null;
  }>,
) {
  return environments.every(
    (environment) => environment.serverConfig?.environment.capabilities.storageCleanup === true,
  );
}

/** Narrows a mixed selection to the machines that support cleanup. */
function StorageCleanupScopeNotice() {
  const gt = useGT();
  const { connectedEnvironments } = useSettingsScope();
  return (
    <SettingsScopeNotice
      inline
      target="environment"
      eligibleEnvironmentIds={connectedEnvironments
        .filter(
          (environment) =>
            environment.serverConfig?.environment.capabilities.storageCleanup === true,
        )
        .map((environment) => environment.environmentId)}
    >
      {gt(
        "Update the selected environments to use storage cleanup, or choose a machine that supports it.",
      )}
    </SettingsScopeNotice>
  );
}

export function WorktreeCleanupSection() {
  const messages = useMessages();
  const setting = useSearchableSetting();
  const gt = useGT();
  const { scope, environments, connectedEnvironments, targets, target } = useSettingsScope();
  const scopedSettings = useScopedSettings();
  const isProjectScope = scope.kind === "project" || scope.kind === "checkout";
  const settings = {
    ...scopedSettings.storageCleanup,
    ...resolveWorktreeCleanup(scopedSettings, null),
  };
  const projectMode = (entry: ScopedSettingsTarget | null) =>
    entry?.sources.worktreeCleanup === "project"
      ? (entry.settings.worktreeCleanup?.mode ?? "inherit")
      : "inherit";
  const mode = projectMode(target);
  const mixedModes = targets.some((entry) => projectMode(entry) !== mode);
  const updateSettings = useUpdateScopedSettings();
  const clearSettings = useClearScopedSettings();
  const ruleStatus = (key: keyof StorageCleanupSettings) =>
    targets.some(
      (target) =>
        JSON.stringify(
          { ...target.settings.storageCleanup, ...resolveWorktreeCleanup(target.settings, null) }[
            key
          ],
        ) !== JSON.stringify(settings[key]),
    )
      ? gt("Mixed across selected machines")
      : undefined;
  // A policy locks these through worktreeCleanup, which also reaches each project's own rules.
  const lockablePath = (key: keyof WorktreeCleanupRules) =>
    isProjectScope ? ["worktreeCleanup"] : ["worktreeCleanup", `storageCleanup.${key}`];
  const update = (patch: Partial<StorageCleanupSettings>, options?: SaveOptions) =>
    updateSettings({ storageCleanup: patch }, options);
  const updateWorktree = (patch: Partial<WorktreeCleanupRules>, options?: SaveOptions) =>
    isProjectScope
      ? updateSettings({ worktreeCleanup: { mode: "custom", rules: patch } }, options)
      : update(patch, options);

  if (
    isProjectScope &&
    connectedEnvironments.some(
      (environment) =>
        environment.serverConfig?.environment.capabilities.projectWorktreeCleanup !== true,
    )
  ) {
    return (
      <SettingsSection id="storage-worktrees" title={gt("Cleanup")} variant="plain">
        <SettingsScopeNotice inline target="all">
          {gt("Update the selected machines to configure project worktree cleanup.")}
        </SettingsScopeNotice>
      </SettingsSection>
    );
  }
  if (!supportsStorageCleanup(connectedEnvironments)) {
    return (
      <SettingsSection id="storage-worktrees" title={gt("Cleanup")} variant="plain">
        <StorageCleanupScopeNotice />
      </SettingsSection>
    );
  }

  return (
    <SettingsSection id="storage-worktrees" title={gt("Cleanup")}>
      <>
        {isProjectScope && (
          <SettingsRow
            title={gt("Automatic worktree cleanup")}
            description={
              mode === "off"
                ? gt("Keep this project's worktrees until you delete them manually.")
                : mode === "custom"
                  ? gt("Use these rules for this project.")
                  : gt("Use each machine's worktree cleanup settings.")
            }
            serverScoped
            settingKeys={["worktreeCleanup"]}
            mixed={mixedModes}
            control={
              <Select
                value={mixedModes ? null : mode}
                onValueChange={(next) => {
                  if (next === "inherit") clearSettings(["worktreeCleanup"]);
                  else if (next === "off") updateSettings({ worktreeCleanup: { mode: "off" } });
                  else if (next === "custom")
                    updateSettings({ worktreeCleanup: { mode: "custom", rules: {} } });
                }}
              >
                <SelectTrigger size="sm" aria-label={gt("Automatic worktree cleanup")}>
                  <SelectValue>
                    {mixedModes
                      ? gt("Mixed")
                      : mode === "inherit"
                        ? gt("Inherit")
                        : mode === "off"
                          ? gt("Off")
                          : gt("Custom")}
                  </SelectValue>
                </SelectTrigger>
                <SelectPopup align="end" alignItemWithTrigger={false}>
                  <SelectItem value="inherit">
                    <T>Inherit</T>
                  </SelectItem>
                  <SelectItem value="off">
                    <T>Off</T>
                  </SelectItem>
                  <SelectItem value="custom">
                    <T>Custom</T>
                  </SelectItem>
                </SelectPopup>
              </Select>
            }
          />
        )}
        {(!isProjectScope || (!mixedModes && mode === "custom")) && (
          <>
            <SettingsRow
              title={gt("Delete worktrees with deleted threads")}
              status={ruleStatus("worktreeOnDelete")}
              managedPaths={lockablePath("worktreeOnDelete")}
              description={gt(
                "Remove unused worktrees when active or archived threads are deleted. Worktrees with local changes are kept.",
              )}
              serverScoped={!isProjectScope}
              control={
                <Switch
                  aria-label={gt("Delete worktrees with deleted threads")}
                  checked={settings.worktreeOnDelete}
                  onCheckedChange={(worktreeOnDelete) => updateWorktree({ worktreeOnDelete })}
                />
              }
            />
            <SettingsRow
              title={gt("Delete merged worktrees")}
              status={ruleStatus("worktreeOnMerge")}
              managedPaths={lockablePath("worktreeOnMerge")}
              description={gt(
                "Remove worktrees whose pull request is merged and whose commits are included in the default branch.",
              )}
              serverScoped={!isProjectScope}
              control={
                <Switch
                  aria-label={gt("Delete merged worktrees")}
                  checked={settings.worktreeOnMerge}
                  onCheckedChange={(worktreeOnMerge) => updateWorktree({ worktreeOnMerge })}
                />
              }
            />
            <SettingsRow
              title={gt("Delete inactive worktrees")}
              status={ruleStatus("worktreeAfterDays")}
              managedPaths={lockablePath("worktreeAfterDays")}
              description={gt(
                "Remove worktrees after their threads have been inactive for this many days. Branches and thread history are kept.",
              )}
              serverScoped={!isProjectScope}
              control={
                <RetentionControl
                  label={gt("Delete inactive worktrees")}
                  value={settings.worktreeAfterDays}
                  onChange={(worktreeAfterDays, onFailed) =>
                    updateWorktree({ worktreeAfterDays }, { onFailed })
                  }
                />
              }
            />
            <AdvancedSettings
              // The keep-when search result lands on this section, not its row.
              targetIds={["storage-worktrees", setting("storage-worktree-keep-when").id]}
            >
              {connectedEnvironments.length > 0 &&
                connectedEnvironments.every(
                  (environment) =>
                    environment.serverConfig?.environment.capabilities.settledWorktreeCleanup ===
                    true,
                ) && (
                  <SettingsRow
                    title={gt("Delete settled worktrees")}
                    managedPaths={lockablePath("worktreeSettledAfterDays")}
                    status={[
                      ruleStatus("worktreeSettledAfterDays"),
                      environments.length > connectedEnvironments.length
                        ? gt("Offline machines will not get this setting")
                        : undefined,
                    ]
                      .filter(Boolean)
                      .join(". ")}
                    description="Remove worktrees this many days after their thread settles and its final checkpoint completes. Branches and history are kept."
                    serverScoped={!isProjectScope}
                    control={
                      <RetentionControl
                        label={gt("Delete settled worktrees")}
                        value={settings.worktreeSettledAfterDays}
                        minimum={0}
                        onChange={(worktreeSettledAfterDays, onFailed) =>
                          updateWorktree({ worktreeSettledAfterDays }, { onFailed })
                        }
                      />
                    }
                  />
                )}
              {connectedEnvironments.length > 0 &&
                connectedEnvironments.every(
                  (e) =>
                    e.serverConfig?.environment.capabilities.settledWorktreeRetirement === true,
                ) && (
                  <>
                    <SettingsRow
                      title="Settled checkout scope"
                      managedPaths={lockablePath("worktreeSettledScope")}
                      description="Contained requires every commit to be reachable elsewhere. Branch retained also accepts the saved branch."
                      serverScoped={!isProjectScope}
                      control={
                        <Select
                          value={settings.worktreeSettledScope}
                          onValueChange={(value) => {
                            if (value === "contained" || value === "branch-retained")
                              updateWorktree({ worktreeSettledScope: value });
                          }}
                        >
                          <SelectTrigger size="sm" aria-label="Settled checkout scope">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectPopup>
                            <SelectItem value="contained">Contained</SelectItem>
                            <SelectItem value="branch-retained">Branch retained</SelectItem>
                          </SelectPopup>
                        </Select>
                      }
                    />
                    <SettingsRow
                      managedPaths={lockablePath("worktreeRegenerableDiscard")}
                      title="Discard regenerable build output"
                      description="Dependency installs, build output and caches such as node_modules, dist and build. A nested repository, or a .env or key file in build output, stays protected."
                      serverScoped={!isProjectScope}
                      control={
                        <Switch
                          aria-label="Discard regenerable build output"
                          checked={settings.worktreeRegenerableDiscard}
                          onCheckedChange={(worktreeRegenerableDiscard) =>
                            updateWorktree({ worktreeRegenerableDiscard })
                          }
                        />
                      }
                    />
                    {(
                      ["declared_cache", "dependency_reproduction", "format_recognized"] as const
                    ).map((coverage) => (
                      <SettingsRow
                        key={coverage}
                        managedPaths={lockablePath("worktreeCoveredDiscard")}
                        title={COVERAGE_LABEL[coverage]}
                        description="Ignored content only. Sensitive files and nested repositories stay protected."
                        serverScoped={!isProjectScope}
                        control={
                          <Switch
                            aria-label={COVERAGE_LABEL[coverage]}
                            checked={settings.worktreeCoveredDiscard.includes(coverage)}
                            onCheckedChange={(checked) =>
                              updateWorktree({
                                worktreeCoveredDiscard: checked
                                  ? [...settings.worktreeCoveredDiscard, coverage]
                                  : settings.worktreeCoveredDiscard.filter((c) => c !== coverage),
                              })
                            }
                          />
                        }
                      />
                    ))}
                  </>
                )}
              <SettingsRow
                title={gt("Delete unchanged worktrees")}
                status={ruleStatus("worktreeUnchanged")}
                managedPaths={lockablePath("worktreeUnchanged")}
                description={gt("Remove worktrees with no commits beyond the default branch.")}
                serverScoped={!isProjectScope}
                control={
                  <Switch
                    aria-label={gt("Delete unchanged worktrees")}
                    checked={settings.worktreeUnchanged}
                    onCheckedChange={(worktreeUnchanged) => updateWorktree({ worktreeUnchanged })}
                  />
                }
              />
              {connectedEnvironments.every(
                (environment) =>
                  environment.serverConfig?.environment.capabilities.storageCleanupRun === true,
              ) && (
                <SettingsRow
                  {...setting("storage-worktree-keep-when")}
                  status={ruleStatus("worktreeKeepWhen")}
                  managedPaths={lockablePath("worktreeKeepWhen")}
                  description={
                    settings.worktreeKeepWhen === "any-local-files"
                      ? "Keeps any untracked or ignored content."
                      : settings.worktreeKeepWhen === "tracked-changes"
                        ? "Keeps tracked edits. Removes untracked and ignored files; sensitive paths need confirmation."
                        : "Keeps edits and untracked files. Removes only ignored files that are safe to discard."
                  }
                  serverScoped={!isProjectScope}
                  control={
                    <Select
                      value={ruleStatus("worktreeKeepWhen") ? null : settings.worktreeKeepWhen}
                      onValueChange={(next) => {
                        if (
                          next === "any-local-files" ||
                          next === "uncommitted-changes" ||
                          next === "tracked-changes"
                        )
                          updateWorktree({ worktreeKeepWhen: next });
                      }}
                    >
                      <SelectTrigger size="sm" aria-label={gt("Keep worktrees with local changes")}>
                        <SelectValue>
                          {ruleStatus("worktreeKeepWhen")
                            ? gt("Mixed")
                            : messages(KEEP_WHEN_LABELS[settings.worktreeKeepWhen])}
                        </SelectValue>
                      </SelectTrigger>
                      <SelectPopup align="end" alignItemWithTrigger={false}>
                        <SelectItem value="any-local-files">
                          <T>Any local files</T>
                        </SelectItem>
                        <SelectItem value="uncommitted-changes">
                          <T>Uncommitted changes</T>
                        </SelectItem>
                        <SelectItem value="tracked-changes">
                          <T>Edited tracked files</T>
                        </SelectItem>
                      </SelectPopup>
                    </Select>
                  }
                />
              )}
            </AdvancedSettings>
          </>
        )}
      </>
    </SettingsSection>
  );
}

/** Retention for logs and captures, and the manual cleanup run, which covers worktrees too. */
export function StorageCleanupSections() {
  const gt = useGT();
  const { scope, connectedEnvironments, targets } = useSettingsScope();
  const scopedSettings = useScopedSettings();
  const updateSettings = useUpdateScopedSettings();
  const settings = scopedSettings.storageCleanup;
  const installRetentionMixed = targets.some(
    (target) => target.settings.storageCleanup.installRetention !== settings.installRetention,
  );
  const targetIdsKey = environmentIdsKey(targets.map((target) => target.environmentId).sort());
  const artifactsMixed =
    targets.length > 1 &&
    targets.some(
      (target) =>
        target.settings.storageCleanup.browserArtifactsAfterDays !==
        settings.browserArtifactsAfterDays,
    );
  const logsMixed =
    targets.length > 1 &&
    targets.some(
      (target) => target.settings.storageCleanup.logsAfterDays !== settings.logsAfterDays,
    );
  const settledArtifactsStatus = useDebouncedValue(`${targetIdsKey}:${artifactsMixed}`, 300);
  const settledLogsStatus = useDebouncedValue(`${targetIdsKey}:${logsMixed}`, 300);
  const artifactsStatus =
    targets.length > 1 && settledArtifactsStatus === `${targetIdsKey}:true`
      ? gt("Mixed across selected machines")
      : undefined;
  const logsStatus =
    targets.length > 1 && settledLogsStatus === `${targetIdsKey}:true`
      ? gt("Mixed across selected machines")
      : undefined;
  if (scope.kind === "project" || scope.kind === "checkout") return null;
  const update = (patch: Partial<StorageCleanupSettings>, options?: SaveOptions) =>
    updateSettings({ storageCleanup: patch }, options);
  const databaseStorageNotice = (
    <DatabaseStorageNotice
      environments={connectedEnvironments.filter(
        (environment) =>
          environment.serverConfig?.environment.capabilities.databaseStorage === true,
      )}
    />
  );
  if (!supportsStorageCleanup(connectedEnvironments)) {
    return (
      <>
        <SettingsSection id="storage-artifacts" title={gt("Artifacts and logs")} variant="plain">
          <StorageCleanupScopeNotice />
        </SettingsSection>
        {databaseStorageNotice}
      </>
    );
  }

  // Machines report their databases after the page renders, so the notice goes last to push nothing down.
  return (
    <>
      <SettingsSection id="storage-artifacts" title={gt("Artifacts and logs")}>
        <AnimatedHeight>
          {connectedEnvironments.every(
            (environment) =>
              environment.serverConfig?.environment.capabilities.installRetention === true,
          ) && (
            <SettingsRow
              title="Install retention"
              status={installRetentionMixed}
              description="Propose keeps old installs. Retire allows approved cleanup; recovery installs stay protected."
              serverScoped
              control={
                <Select
                  value={installRetentionMixed ? null : settings.installRetention}
                  onValueChange={(next) => {
                    if (next === "propose" || next === "retire") update({ installRetention: next });
                  }}
                >
                  <SelectTrigger size="sm" aria-label="Install retention">
                    <SelectValue>
                      {installRetentionMixed
                        ? "Mixed"
                        : settings.installRetention === "retire"
                          ? "Retire"
                          : "Propose"}
                    </SelectValue>
                  </SelectTrigger>
                  <SelectPopup align="end" alignItemWithTrigger={false}>
                    <SelectItem value="propose">Propose</SelectItem>
                    <SelectItem value="retire">Retire</SelectItem>
                  </SelectPopup>
                </Select>
              }
            />
          )}
          <SettingsRow
            title={gt("Delete old browser artifacts")}
            status={artifactsStatus}
            managedPaths={["storageCleanup.browserArtifactsAfterDays"]}
            description={gt(
              "Delete saved browser captures after this many days. Older capture links will no longer open.",
            )}
            serverScoped
            control={
              <RetentionControl
                label={gt("Delete old browser artifacts")}
                value={settings.browserArtifactsAfterDays}
                onChange={(browserArtifactsAfterDays, onFailed) =>
                  update({ browserArtifactsAfterDays }, { onFailed })
                }
              />
            }
          />
        </AnimatedHeight>
        <AnimatedHeight>
          <SettingsRow
            title={gt("Delete old rotated logs")}
            status={logsStatus}
            managedPaths={["storageCleanup.logsAfterDays"]}
            description={gt(
              "Delete inactive rotated log files after this many days. Current logs are kept.",
            )}
            serverScoped
            control={
              <RetentionControl
                label={gt("Delete old rotated logs")}
                value={settings.logsAfterDays}
                onChange={(logsAfterDays, onFailed) => update({ logsAfterDays }, { onFailed })}
              />
            }
          />
        </AnimatedHeight>
      </SettingsSection>
      <CleanupSection />
      {databaseStorageNotice}
    </>
  );
}
