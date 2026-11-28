import { type NavigationTab } from "./navigation-tab-order";
import { NativeStackScreenOptions } from "../../../native/StackHeader";
import { UsageRouteScreen } from "../../usage/UsageRouteScreen";
import { WorkflowsScreen } from "../../workflows/WorkflowsScreen";
import { MobileOrchestratorScreen } from "../../../jones/mobileOrchestrator/MobileOrchestratorScreen";

export function WorkspaceExtraTab(props: {
  tab: Extract<NavigationTab, "usage" | "workflows" | "orchestrator">;
}) {
  return (
    <>
      <NativeStackScreenOptions options={{ headerSearchBarOptions: undefined }} />
      {props.tab === "orchestrator" ? (
        <MobileOrchestratorScreen />
      ) : props.tab === "workflows" ? (
        <WorkflowsScreen />
      ) : (
        <UsageRouteScreen workspace />
      )}
    </>
  );
}
