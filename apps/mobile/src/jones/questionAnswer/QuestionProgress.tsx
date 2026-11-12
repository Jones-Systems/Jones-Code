import { Pressable, View } from "react-native";
import { AppText as Text } from "../../components/ui/AppText";
import { cn } from "../../lib/cn";

export function QuestionProgress({
  index,
  answered,
  questionIds,
  disabled,
  onSelect,
}: {
  index: number;
  answered: ReadonlyArray<boolean>;
  questionIds: ReadonlyArray<string>;
  disabled: boolean;
  onSelect: (index: number) => void;
}) {
  if (answered.length < 2) return null;
  return (
    <View className="flex-row items-center">
      {answered.map((complete, i) => (
        <Pressable
          key={questionIds[i]}
          accessibilityRole="button"
          accessibilityLabel={`Question ${i + 1} of ${answered.length}, ${complete ? "answered" : "unanswered"}`}
          accessibilityState={{ selected: index === i, disabled }}
          disabled={disabled}
          className="h-10 w-7 items-center justify-center"
          onPress={() => onSelect(i)}
        >
          <View
            className={cn(
              "h-2 w-2 rounded-full",
              index === i
                ? "bg-primary"
                : complete
                  ? "bg-foreground-secondary"
                  : "bg-foreground-muted/30",
            )}
          />
        </Pressable>
      ))}
      <Text className="ml-1 font-sans text-xs text-foreground-muted">
        {index + 1}/{answered.length}
      </Text>
    </View>
  );
}
