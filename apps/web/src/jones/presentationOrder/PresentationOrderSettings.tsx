import { connectionStatusText } from "@t3tools/client-runtime/connection";
import { DEFAULT_CLIENT_SETTINGS, resolveEnvironmentMachineKind } from "@t3tools/contracts";
import { SidebarProjectSortOrder } from "@t3tools/contracts/settings";
import { EnvironmentMachineIcon } from "../../components/EnvironmentMachineIcon";
import {
  SettingsRow,
  SettingsSection,
  SettingResetButton,
} from "../../components/settings/settingsLayout";
import { searchableSetting } from "../../components/settings/settingsSearch";
import {
  Select,
  SelectItem,
  SelectPopup,
  SelectTrigger,
  SelectValue,
} from "../../components/ui/select";
import { useSidebarProjectGroups } from "../../hooks/useSidebarProjectGroups";
import { persistClientSettingsPatch, useClientSettings } from "../../hooks/useSettings";
import { projectGroupsSpanEnvironments } from "../../sidebarProjectGrouping";
import { useAllEnvironmentProjectSnapshotsReady } from "../../state/entities";
import { useEnvironments, usePrimaryEnvironmentId } from "../../state/environments";
import { useUiStateStore } from "../../uiStateStore";
import { sortEnvironmentOptions } from "./environmentOrder";
import { groupPreferenceIds, mergeVisibleOrder } from "./projectOrder";
import { SortableOrderList } from "./SortableOrderList";

const labels: Record<SidebarProjectSortOrder, string> = {
  updated_at: "Last user message",
  created_at: "Created at",
  manual: "Manual",
};

export function PresentationOrderSettings() {
  const sortOrder = useClientSettings((settings) => settings.sidebarProjectSortOrder);
  const projectOrder = useUiStateStore((state) => state.projectOrder);
  const environmentOrder = useUiStateStore((state) => state.environmentOrder);
  const groups = useSidebarProjectGroups().filter((group) => !group.isScratch);
  const ready = useAllEnvironmentProjectSnapshotsReady();
  const { environments } = useEnvironments();
  const primaryId = usePrimaryEnvironmentId();
  const environmentOptions = sortEnvironmentOptions(
    environments.map((environment) => ({
      ...environment,
      isPrimary: environment.environmentId === primaryId,
    })),
    environmentOrder,
  );
  const showEnvironments = projectGroupsSpanEnvironments(groups);
  return (
    <SettingsSection title="Project and environment order">
      <p className="px-3 py-2 text-xs text-muted-foreground sm:px-4">
        Saved on this device across all connected environments. Other devices keep their own order.
      </p>
      <SettingsRow
        {...searchableSetting("project-order")}
        description="Order of projects in the sidebar, new-thread palette and draft picker. Choose Manual to reorder."
        resetAction={
          sortOrder !== DEFAULT_CLIENT_SETTINGS.sidebarProjectSortOrder ||
          projectOrder.length > 0 ? (
            <SettingResetButton
              label="project order"
              onClick={() => {
                useUiStateStore.getState().setProjectOrder([]);
                void persistClientSettingsPatch({
                  sidebarProjectSortOrder: DEFAULT_CLIENT_SETTINGS.sidebarProjectSortOrder,
                });
              }}
            />
          ) : null
        }
        control={
          <Select
            value={sortOrder}
            onValueChange={(value) => {
              if (value === "manual" || value === "created_at" || value === "updated_at")
                void persistClientSettingsPatch({ sidebarProjectSortOrder: value });
            }}
          >
            <SelectTrigger size="sm" className="w-full sm:w-44" aria-label="Project order">
              <SelectValue>{labels[sortOrder]}</SelectValue>
            </SelectTrigger>
            <SelectPopup align="end" alignItemWithTrigger={false}>
              {SidebarProjectSortOrder.literals.map((value) => (
                <SelectItem hideIndicator key={value} value={value}>
                  {labels[value]}
                </SelectItem>
              ))}
            </SelectPopup>
          </Select>
        }
      >
        {sortOrder === "manual" && (
          <>
            {groups.length > 0 ? (
              <SortableOrderList
                label="Project order"
                rows={groups.map((group) => ({
                  key: group.projectKey,
                  label: group.displayName,
                  ...(showEnvironments
                    ? {
                        detail: [
                          ...new Set(
                            group.memberProjects.map(
                              (member) => member.environmentLabel ?? member.environmentId,
                            ),
                          ),
                        ].join(" · "),
                      }
                    : {}),
                }))}
                onReorder={(keys) =>
                  useUiStateStore
                    .getState()
                    .setProjectOrder(
                      mergeVisibleOrder(projectOrder, keys, (key) =>
                        groupPreferenceIds(groups.find((group) => group.projectKey === key)!),
                      ),
                    )
                }
              />
            ) : (
              <p className="py-3 text-xs text-muted-foreground">Add a project to set its order.</p>
            )}
            {!ready && (
              <p className="pb-3 text-xs text-muted-foreground">
                Projects from loading environments keep their saved positions.
              </p>
            )}
          </>
        )}
      </SettingsRow>
      <SettingsRow
        {...searchableSetting("environment-order")}
        description="Order of environments in Run on and project host pickers. This does not change the default environment."
        resetAction={
          environmentOrder.length > 0 ? (
            <SettingResetButton
              label="environment order"
              onClick={() => useUiStateStore.getState().setEnvironmentOrder([])}
            />
          ) : null
        }
      >
        {environmentOptions.length > 0 ? (
          <SortableOrderList
            label="Environment order"
            rows={environmentOptions.map((environment) => ({
              key: environment.environmentId,
              label: environment.label,
              detail: [
                environment.isPrimary ? "Primary" : null,
                connectionStatusText(environment.connection),
              ]
                .filter(Boolean)
                .join(" · "),
              icon: (
                <EnvironmentMachineIcon
                  kind={resolveEnvironmentMachineKind(environment.serverConfig)}
                  className="size-4 shrink-0 text-muted-foreground"
                />
              ),
            }))}
            onReorder={(keys) =>
              useUiStateStore
                .getState()
                .setEnvironmentOrder(mergeVisibleOrder(environmentOrder, keys))
            }
          />
        ) : (
          <p className="py-3 text-xs text-muted-foreground">
            Connect an environment to set its order.
          </p>
        )}
      </SettingsRow>
    </SettingsSection>
  );
}
