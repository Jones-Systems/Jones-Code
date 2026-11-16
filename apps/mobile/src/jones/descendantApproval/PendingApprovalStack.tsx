import type { ReactNode } from "react";
import Animated, { useAnimatedStyle, type DerivedValue } from "react-native-reanimated";

export function descendantApprovalStackHeight(input: {
  readonly windowHeight: number;
  readonly keyboardHeight: number;
  readonly navigationHeaderHeight: number;
  readonly composerOverlapHeight: number;
}): number {
  "worklet";
  return Math.max(
    0,
    Math.min(
      560,
      input.windowHeight -
        Math.max(0, input.keyboardHeight) -
        Math.max(0, input.navigationHeaderHeight) -
        Math.max(0, input.composerOverlapHeight) -
        12,
    ),
  );
}

/** One bounded scroll area keeps parent and descendant controls reachable together. */
export function PendingApprovalStack({
  enabled,
  maxHeight,
  children,
}: {
  readonly enabled: boolean;
  readonly maxHeight: DerivedValue<number>;
  readonly children: ReactNode;
}) {
  const style = useAnimatedStyle(() => ({ maxHeight: maxHeight.value }));
  if (!enabled) return children;
  return (
    <Animated.ScrollView
      accessibilityLabel="Pending requests"
      style={style}
      contentContainerStyle={{ gap: 12 }}
      keyboardShouldPersistTaps="handled"
      keyboardDismissMode="on-drag"
      nestedScrollEnabled
      showsVerticalScrollIndicator
    >
      {children}
    </Animated.ScrollView>
  );
}
