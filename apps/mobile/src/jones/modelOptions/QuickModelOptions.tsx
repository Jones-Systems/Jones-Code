import type { ModelSelection, ProviderOptionDescriptor } from "@t3tools/contracts";
import { getProviderOptionCurrentValue } from "@t3tools/shared/model";
import { useGT } from "gt-react-native";
import { Pressable, View } from "react-native";

import { SymbolView } from "../../components/ui/AppSymbol";
import { AppText as Text } from "../../components/ui/AppText";

const EFFORT_ORDER = ["medium", "high", "xhigh"];
const SPEED_ORDER = ["default", "standard", "fast", "priority", "ultrafast", "ultra-fast"];

export function isQuickModelOption(descriptor: ProviderOptionDescriptor): boolean {
  return descriptor.type === "select"
    ? descriptor.id === "reasoningEffort" ||
        descriptor.id === "effort" ||
        descriptor.id === "serviceTier"
    : descriptor.id === "fastMode";
}

export function QuickModelOption(props: {
  readonly descriptor: ProviderOptionDescriptor;
  readonly selection: ModelSelection | null;
  readonly reportedSelection?: ModelSelection | null;
  readonly onChange: (value: string | boolean) => void;
  readonly onMore?: () => void;
}) {
  const gt = useGT();
  const { descriptor } = props;
  const current = getProviderOptionCurrentValue(
    descriptor,
    props.selection,
    props.reportedSelection,
  );
  const choices =
    descriptor.type === "select"
      ? descriptor.options
          .filter((option) => !descriptor.promptInjectedValues?.includes(option.id))
          .map((option) => ({ value: option.id, label: option.label }))
      : [
          { value: false, label: gt("Standard") },
          { value: true, label: gt("Fast") },
        ];
  const preferred = descriptor.id === "serviceTier" ? SPEED_ORDER : EFFORT_ORDER;
  const primary = [
    ...preferred.flatMap((id) => choices.filter((choice) => choice.value === id)),
    ...choices.filter(
      (choice) => typeof choice.value !== "string" || !preferred.includes(choice.value),
    ),
  ].slice(0, 3);
  const hiddenSelection =
    !primary.some((choice) => choice.value === current) &&
    choices.find((choice) => choice.value === current);

  return (
    <View className="gap-1.5 px-3 py-2">
      <Text className="text-xs font-t3-medium text-foreground-muted">
        {hiddenSelection ? `${descriptor.label} · ${hiddenSelection.label}` : descriptor.label}
      </Text>
      <View className="flex-row flex-wrap items-center gap-1.5">
        {primary.map(({ label, value }) => {
          const selected =
            (current ?? (descriptor.type === "boolean" ? false : undefined)) === value;
          return (
            <Pressable
              key={String(value)}
              accessibilityRole="button"
              accessibilityLabel={`${descriptor.label}, ${label}`}
              accessibilityState={{ selected }}
              onPress={() => props.onChange(value)}
              className={
                selected
                  ? "min-h-[44px] justify-center rounded-full bg-primary px-3"
                  : "min-h-[44px] justify-center rounded-full bg-subtle-strong px-3"
              }
            >
              <Text
                className={selected ? "text-sm text-primary-foreground" : "text-sm text-foreground"}
              >
                {label}
              </Text>
            </Pressable>
          );
        })}
        {props.onMore && choices.length > primary.length ? (
          <Pressable
            accessibilityRole="button"
            accessibilityLabel={gt("More {option} options", { option: descriptor.label })}
            onPress={props.onMore}
            className="size-11 items-center justify-center rounded-full bg-subtle-strong"
          >
            <SymbolView name="ellipsis" size={18} tintColorClassName="accent-icon-muted" />
          </Pressable>
        ) : null}
      </View>
    </View>
  );
}
