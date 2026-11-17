import { createContext, useContext, type ReactNode } from "react";
import type { SidebarAppearance } from "./model";

const SidebarAppearanceContext = createContext<SidebarAppearance | null>(null);

export function useSidebarAppearancePresentation(): SidebarAppearance | null {
  return useContext(SidebarAppearanceContext);
}

export function SidebarAppearanceOverride(props: {
  value: SidebarAppearance;
  children: ReactNode;
}) {
  return <SidebarAppearanceContext value={props.value}>{props.children}</SidebarAppearanceContext>;
}
