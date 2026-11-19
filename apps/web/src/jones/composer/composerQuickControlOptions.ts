import type { ProviderOptionDescriptor } from "@t3tools/contracts";

type SelectDescriptor = Extract<ProviderOptionDescriptor, { type: "select" }>;

export const QUICK_EFFORTS = [
  {
    value: "medium",
    label: "M",
    name: "Medium",
    alternate: { value: "low", label: "L", name: "Low" },
  },
  { value: "high", label: "H", name: "High", alternate: null },
  {
    value: "xhigh",
    label: "E",
    name: "Extra high",
    alternate: { value: "max", label: "Max", name: "Max" },
  },
] as const;

export function getQuickServiceTier(selects: ReadonlyArray<SelectDescriptor>) {
  return selects.find(
    ({ id, options }) =>
      id === "serviceTier" &&
      options.some(({ id }) => id === "priority") &&
      options.some(({ id }) => id === "default"),
  );
}

export function getQuickUltrafast(tier: SelectDescriptor | undefined) {
  return tier?.options.find(({ id }) => id === "ultrafast");
}

export function hasQuickAlternateEffort(descriptor: SelectDescriptor) {
  return QUICK_EFFORTS.some(
    ({ value, alternate }) =>
      alternate &&
      descriptor.options.some(({ id }) => id === value) &&
      descriptor.options.some(({ id }) => id === alternate.value),
  );
}
