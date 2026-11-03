import type { MenuAction } from "@react-native-menu/menu";
import { supportsSettlementRestore } from "@t3tools/client-runtime/jones/settlement-restore";
import type { ExecutionEnvironmentCapabilities } from "@t3tools/contracts";

export function settlementRestoreMenuItems(input: {
  readonly settled: boolean;
  readonly canOperate: boolean;
  readonly capabilities: Pick<ExecutionEnvironmentCapabilities, "settlementRestore"> | undefined;
}): MenuAction[] {
  return input.settled && input.canOperate && supportsSettlementRestore(input.capabilities)
    ? [{ id: "undo-settle", title: "Undo settle", image: "arrow.uturn.backward" }]
    : [];
}
