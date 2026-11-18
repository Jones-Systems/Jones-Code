import { useState } from "react";
import {
  getLocalStorageItem,
  useLocalStorage,
  writeLocalStorageValue,
} from "../../hooks/useLocalStorage";
import {
  THREAD_BOARD_APPEARANCE_KEY,
  StoredThreadBoardAppearance,
  setThreadBoardAppearanceField,
  type ThreadBoardAppearance,
} from "./threadBoardAppearance";

const EMPTY_APPEARANCE: ThreadBoardAppearance = {};

export function useStoredThreadBoardAppearance() {
  const [stored] = useLocalStorage(THREAD_BOARD_APPEARANCE_KEY, null, StoredThreadBoardAppearance);
  return stored ?? EMPTY_APPEARANCE;
}

export function useThreadBoardAppearanceControls() {
  const appearance = useStoredThreadBoardAppearance();
  const [saveFailed, setSaveFailed] = useState(false);

  const write = (
    update: (current: ThreadBoardAppearance | null) => ThreadBoardAppearance | null,
  ) => {
    let expected: ThreadBoardAppearance | null | undefined;
    writeLocalStorageValue(
      THREAD_BOARD_APPEARANCE_KEY,
      (current) => (expected = update(current)),
      null,
      StoredThreadBoardAppearance,
    );
    try {
      const actual = getLocalStorageItem(THREAD_BOARD_APPEARANCE_KEY, StoredThreadBoardAppearance);
      setSaveFailed(expected === undefined || JSON.stringify(actual) !== JSON.stringify(expected));
    } catch {
      setSaveFailed(true);
    }
  };

  return {
    appearance,
    saveFailed,
    set: <Key extends keyof ThreadBoardAppearance>(key: Key, value: ThreadBoardAppearance[Key]) =>
      write((current) => setThreadBoardAppearanceField(current, key, value)),
    reset: () => write(() => null),
  };
}
