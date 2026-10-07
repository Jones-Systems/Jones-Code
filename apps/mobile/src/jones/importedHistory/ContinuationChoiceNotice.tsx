import { useEffect, useMemo, useSyncExternalStore } from "react";
import { Pressable, View } from "react-native";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { AppText as Text } from "../../components/AppText";
import { getMobileImportedHistoryChoice } from "./runtime";
export function useMobileImportedHistoryChoice(environmentId: EnvironmentId, threadId: ThreadId) {
  const choice = useMemo(
    () => getMobileImportedHistoryChoice(environmentId, threadId),
    [environmentId, threadId],
  );
  const state = useSyncExternalStore(choice.subscribe, choice.snapshot);
  useEffect(() => {
    void choice.hydrate();
  }, [choice]);
  return { choice, state };
}
export function ContinuationChoiceNotice(props: {
  environmentId: EnvironmentId;
  threadId: ThreadId;
}) {
  const { choice, state } = useMobileImportedHistoryChoice(props.environmentId, props.threadId);
  if (state.notice === null) return null;
  return (
    <View className="px-3 py-2 gap-2">
      <Text accessibilityLiveRegion="polite">{state.notice}</Text>
      {state.pending || state.canStart ? (
        <Pressable
          accessibilityRole="button"
          disabled={state.busy}
          onPress={() => {
            void (state.pending ? choice.observe() : choice.start());
          }}
        >
          <Text>{state.pending ? "Observe choice" : "Use imported history"}</Text>
        </Pressable>
      ) : null}
    </View>
  );
}
