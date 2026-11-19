import type { EnvironmentId } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { getLocalStorageItem, useLocalStorage } from "../../hooks/useLocalStorage";

const EnvironmentBadgeAppearance = Schema.Struct({
  display: Schema.optionalKey(Schema.Literals(["icon-and-name", "icon-only", "name-only"])),
  symbolFill: Schema.optionalKey(Schema.Literals(["filled", "outline"])),
});
const StoredEnvironmentBadgeAppearance = Schema.NullOr(EnvironmentBadgeAppearance);

type EnvironmentBadgeAppearance = typeof EnvironmentBadgeAppearance.Type;

export function useEnvironmentBadgeAppearance(environmentId: EnvironmentId) {
  const key = `jones:thread-board-environment-badge:v1:${environmentId}`;
  const [appearance, setAppearance] = useLocalStorage(key, null, StoredEnvironmentBadgeAppearance);
  return {
    appearance,
    update: <Field extends keyof EnvironmentBadgeAppearance>(
      field: Field,
      value: EnvironmentBadgeAppearance[Field],
    ) => {
      let expected: EnvironmentBadgeAppearance | null | undefined;
      setAppearance((current) => {
        const next = { ...current };
        if (value === undefined) delete next[field];
        else next[field] = value;
        expected = Object.keys(next).length > 0 ? next : null;
        return expected;
      });

      try {
        const actual = getLocalStorageItem(key, StoredEnvironmentBadgeAppearance);
        return (
          expected !== undefined &&
          actual?.display === expected?.display &&
          actual?.symbolFill === expected?.symbolFill
        );
      } catch {
        return false;
      }
    },
  };
}
