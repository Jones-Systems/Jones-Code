import type { ContextMenuItem } from "@t3tools/contracts";

export const SETTLEMENT_RESTORE_MENU_ID = "jones:undo-settle" as const;
type SettlementRestoreMenuId = typeof SETTLEMENT_RESTORE_MENU_ID;

/**
 * Adds Undo settle beside Un-settle. It needs no record lookup to open the
 * menu: choosing it asks the server which settle to undo.
 */
export function withSettlementRestoreMenuItem<T extends string>(
  items: ReadonlyArray<ContextMenuItem<T>>,
  options: { readonly available: boolean; readonly canOperate: boolean },
): ReadonlyArray<ContextMenuItem<T | SettlementRestoreMenuId>> {
  const unsettle = items.findIndex((item) => item.id === "unsettle");
  if (!options.available || unsettle === -1) return items;
  return [
    ...items.slice(0, unsettle + 1),
    {
      id: SETTLEMENT_RESTORE_MENU_ID,
      label: "Undo settle",
      icon: "circle-check",
      disabled: !options.canOperate,
    },
    ...items.slice(unsettle + 1),
  ];
}
