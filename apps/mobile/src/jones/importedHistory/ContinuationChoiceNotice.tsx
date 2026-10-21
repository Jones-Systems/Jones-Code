import {
  importedHistoryContinuationEligible,
  importedHistoryReasonText,
} from "@t3tools/client-runtime/jones/imported-history/continuation";
import * as Option from "effect/Option";
import { useEnvironmentThread } from "../../state/threads";
import { useEffect, useMemo, useSyncExternalStore } from "react";
import { Pressable, View } from "react-native";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { AppText as Text } from "../../components/ui/AppText";
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
  const projection = Option.getOrNull(
    useEnvironmentThread(props.environmentId, props.threadId).data,
  );
  if (!importedHistoryContinuationEligible(projection) || state.notice === null) return null;
  return (
    <View className="px-3 py-2 gap-2">
      <Text accessibilityLiveRegion="polite">{importedHistoryReasonText(state.notice)}</Text>
      {state.pending || state.canStart ? (
        <Pressable
          accessibilityRole="button"
          disabled={state.busy}
          onPress={() => {
            void (state.pending ? choice.observe() : choice.start());
          }}
        >
          <Text>{state.pending ? "Check continuation status" : "Start fresh conversation"}</Text>
        </Pressable>
      ) : null}
    </View>
  );
}
