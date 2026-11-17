import { createContext, useContext, useState, type ReactNode } from "react";
import { SidebarAppearanceOverride } from "./SidebarAppearanceContext";
import {
  SIDEBAR_APPEARANCE_PRESETS,
  SIDEBAR_APPEARANCE_SCOPE_ATTRIBUTE_NAMES,
  SIDEBAR_APPEARANCE_SCOPE_VARIABLE_NAMES,
  sidebarAppearanceScope,
  type SidebarAppearance,
  type SidebarAppearancePresetName,
} from "./model";
import {
  useSidebarAppearanceStore,
  type SidebarAppearanceStore,
} from "./useSidebarAppearanceStore";
import "./sidebarAppearance.cards.css";
import "./sidebarAppearance.workstreams.css";

export interface SidebarAppearanceHost extends SidebarAppearanceStore {
  readonly presented: SidebarAppearance;
  readonly comparingPreset: SidebarAppearancePresetName | null;
  readonly setComparingPreset: (name: SidebarAppearancePresetName | null) => void;
  readonly scopeProps: Readonly<Record<string, string>> & {
    readonly style?: Readonly<Record<string, string>>;
  };
}

export function useSidebarAppearanceHost(): SidebarAppearanceHost {
  const store = useSidebarAppearanceStore();
  // Comparing Original (or Selected) changes presentation only, preserving the stored boost.
  const storedSignature = JSON.stringify(store.stored);
  const [comparison, setComparison] = useState<{
    storedSignature: string;
    preset: SidebarAppearancePresetName | null;
  }>({ storedSignature, preset: null });
  const comparingPreset = comparison.storedSignature === storedSignature ? comparison.preset : null;
  // Discard the old comparison on an observed storage change, even if storage later returns to it.
  if (comparison.storedSignature !== storedSignature) {
    setComparison({ storedSignature, preset: null });
  }
  const setComparingPreset = (preset: SidebarAppearancePresetName | null) => {
    setComparison({ storedSignature, preset });
  };
  const presented =
    comparingPreset === null
      ? store.stored
      : { ...store.stored, ...SIDEBAR_APPEARANCE_PRESETS[comparingPreset] };
  const scope = sidebarAppearanceScope(presented);
  return {
    ...store,
    presented,
    comparingPreset,
    setComparingPreset,
    scopeProps: Object.assign({}, scope.attributes, { style: scope.variables }),
    update: (patch) => {
      setComparingPreset(null);
      store.update(patch);
    },
    applyPreset: (name) => {
      setComparingPreset(null);
      store.applyPreset(name);
    },
    reset: () => {
      setComparingPreset(null);
      store.reset();
    },
  };
}

const SidebarAppearanceHostContext = createContext<SidebarAppearanceHost | null>(null);

export function SidebarAppearanceProvider(props: {
  host: SidebarAppearanceHost;
  children: ReactNode;
}) {
  return (
    <SidebarAppearanceHostContext value={props.host}>
      <SidebarAppearanceOverride value={props.host.presented}>
        {props.children}
      </SidebarAppearanceOverride>
    </SidebarAppearanceHostContext>
  );
}

export function useSidebarAppearanceHostContext(): SidebarAppearanceHost {
  const host = useContext(SidebarAppearanceHostContext);
  if (!host) throw new Error("Sidebar appearance host is missing.");
  return host;
}

export function copySidebarAppearanceScope(source: Element, target: HTMLElement): void {
  const scope = source.closest("[data-jones-sidebar-appearance]");
  for (const name of SIDEBAR_APPEARANCE_SCOPE_ATTRIBUTE_NAMES) {
    const value = scope?.getAttribute(name);
    if (value == null) target.removeAttribute(name);
    else target.setAttribute(name, value);
  }
  for (const name of SIDEBAR_APPEARANCE_SCOPE_VARIABLE_NAMES) {
    // Read only the scoped, inline boost; never copy computed theme or scale tokens.
    const value = (scope as HTMLElement | null)?.style?.getPropertyValue(name).trim() ?? "";
    const boost = /^(?:[1-9]\d?|100)%$/.test(value) ? value : null;
    if (boost && scope?.hasAttribute("data-jones-thread-title-boost")) {
      target.style.setProperty(name, boost);
    } else {
      target.style.removeProperty(name);
    }
  }
}
