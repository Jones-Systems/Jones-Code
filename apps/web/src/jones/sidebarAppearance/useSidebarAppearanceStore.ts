import { useState } from "react";
import {
  getLocalStorageItem,
  useLocalStorage,
  writeLocalStorageValue,
} from "../../hooks/useLocalStorage";
import {
  SIDEBAR_APPEARANCE_PRESETS,
  SIDEBAR_APPEARANCE_STORAGE_KEY,
  StoredSidebarAppearance,
  patchSidebarAppearance,
  resolveSidebarAppearance,
  type SidebarAppearance,
  type SidebarAppearancePresetName,
} from "./model";

export interface SidebarAppearanceStore {
  readonly stored: SidebarAppearance;
  readonly update: (patch: Partial<SidebarAppearance>) => void;
  readonly applyPreset: (name: SidebarAppearancePresetName) => void;
  readonly reset: () => void;
  readonly saveFailed: boolean;
}

export function useSidebarAppearanceStore(): SidebarAppearanceStore {
  // The shared hook catches denied reads, and subscribes to both same-tab and cross-tab writes.
  const [record] = useLocalStorage(SIDEBAR_APPEARANCE_STORAGE_KEY, null, StoredSidebarAppearance);
  const [saveFailed, setSaveFailed] = useState(false);
  const write = (patch: Partial<SidebarAppearance> | null) => {
    let expected: SidebarAppearance | null | undefined;
    writeLocalStorageValue(
      SIDEBAR_APPEARANCE_STORAGE_KEY,
      (current: SidebarAppearance | null) => {
        expected = patch === null ? null : patchSidebarAppearance(current, patch);
        return expected;
      },
      null,
      StoredSidebarAppearance,
    );
    // The shared writer swallows errors. Only a successful readback confirms persistence.
    try {
      const actual = getLocalStorageItem(SIDEBAR_APPEARANCE_STORAGE_KEY, StoredSidebarAppearance);
      setSaveFailed(expected === undefined || JSON.stringify(actual) !== JSON.stringify(expected));
    } catch {
      setSaveFailed(true);
    }
  };
  return {
    stored: resolveSidebarAppearance(record),
    update: (patch) => write(patch),
    applyPreset: (name) => write(SIDEBAR_APPEARANCE_PRESETS[name]),
    reset: () => write(null),
    saveFailed,
  };
}
