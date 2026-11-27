import { useAtomSet } from "@effect/atom-react";
import { StackActions, useNavigation, type StaticScreenProps } from "@react-navigation/native";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { useGT } from "gt-react-native";
import { useEffectEvent, useState } from "react";
import { View } from "react-native";
import { Gesture, GestureDetector } from "react-native-gesture-handler";

import { ThreadRouteScreen } from "../../features/threads/ThreadRouteScreen";
import { InspectorContentScope } from "../../features/threads/thread-inspector-preview";
import { startOpeningEnvironmentThread } from "../../state/threads";
import {
  adjacentOrchestratorThread,
  orchestratorThreadRef,
  type OrchestratorQueue,
  type OrchestratorThreadRef,
} from "./queues";
import { orchestratorViewAtom, useOrchestratorQueues } from "./useOrchestratorQueues";

type OrchestratorThreadParams = OrchestratorThreadRef & {
  readonly queue: OrchestratorQueue;
  readonly entryOrder: readonly OrchestratorThreadRef[];
};

export function OrchestratorThreadScreen(props: StaticScreenProps<OrchestratorThreadParams>) {
  const gt = useGT();
  const navigation = useNavigation();
  const setView = useAtomSet(orchestratorViewAtom);
  const { queues } = useOrchestratorQueues();
  const params = props.route.params;
  const live = queues[params.queue].map(orchestratorThreadRef);
  const previous = adjacentOrchestratorThread(params, live, params.entryOrder, -1);
  const next = adjacentOrchestratorThread(params, live, params.entryOrder, 1);

  const open = (ref: OrchestratorThreadRef | undefined) => {
    if (!ref) return;
    startOpeningEnvironmentThread(
      EnvironmentId.make(ref.environmentId),
      ThreadId.make(ref.threadId),
    );
    navigation.dispatch(
      StackActions.replace("OrchestratorThread", {
        ...ref,
        queue: params.queue,
        entryOrder: live,
      } satisfies OrchestratorThreadParams),
    );
  };

  const onSwipe = useEffectEvent((translationX: number, translationY: number) => {
    if (Math.abs(translationX) < 72 || Math.abs(translationX) < Math.abs(translationY) * 1.6)
      return;
    open(translationX > 0 ? next : previous);
  });
  const [swipe] = useState(() =>
    Gesture.Pan()
      .runOnJS(true)
      .activeOffsetX([-28, 28])
      .failOffsetY([-16, 16])
      .maxPointers(1)
      .onEnd(({ translationX, translationY }, success) => {
        if (success) onSwipe(translationX, translationY);
      }),
  );
  return (
    <GestureDetector gesture={swipe}>
      <View className="flex-1 bg-screen" collapsable={false}>
        <InspectorContentScope>
          <ThreadRouteScreen
            route={props.route}
            inspectorPresentation={{
              onClose: () => navigation.goBack(),
              actions: [
                {
                  accessibilityLabel: gt("Previous thread"),
                  icon: "arrow.left",
                  onPress: () => open(previous),
                  disabled: !previous,
                },
                {
                  accessibilityLabel: gt("Next thread"),
                  icon: "arrow.right",
                  onPress: () => open(next),
                  disabled: !next,
                },
                {
                  accessibilityLabel: gt("Return to Orchestrator composer"),
                  icon: "square.and.pencil",
                  onPress: () => {
                    setView("compose");
                    navigation.goBack();
                  },
                },
              ],
            }}
          />
        </InspectorContentScope>
      </View>
    </GestureDetector>
  );
}
