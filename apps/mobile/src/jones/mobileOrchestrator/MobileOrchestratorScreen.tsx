import { useAtom, useAtomValue } from "@effect/atom-react";
import { LegendList } from "@legendapp/list/react-native";
import { StackActions, useNavigation } from "@react-navigation/native";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import { AsyncResult } from "effect/reactivity";
import { useGT, useLocale } from "gt-react-native";
import { useState } from "react";
import { Keyboard, Modal, Platform, Pressable, TextInput, View } from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";

import { AppBar } from "../../components/ui/AppBar";
import { SymbolView } from "../../components/ui/AppSymbol";
import { AppText as Text } from "../../components/ui/AppText";
import { Button } from "../../components/ui/Button";
import { ScreenScrollView } from "../../components/ui/ScreenScrollView";
import { useControlTheme } from "../../components/ui/useControlTheme";
import { useWorkspaceTabs } from "../../features/layout/workspace/WorkspaceTabsContext";
import { scopedThreadKey } from "../../lib/scopedEntities";
import { NativeScrollView } from "../../native/NativeScrollViewMarker";
import { mobilePreferencesAtom, saveMobilePreferences } from "../../state/preferences";
import { startOpeningEnvironmentThread } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";
import { useSavedRemoteConnections } from "../../state/use-remote-environment-registry";
import { OrchestratorComposer } from "./OrchestratorComposer";
import {
  finishedAt,
  isOrchestratorConversation,
  orchestratorNeedsApproval,
  orchestratorThreadRef,
  sameOrchestratorThread,
} from "./queues";
import { orchestratorViewAtom, useOrchestratorQueues } from "./useOrchestratorQueues";

const threadKey = (thread: EnvironmentThreadShell) =>
  scopedThreadKey(thread.environmentId, thread.id);

export function MobileOrchestratorScreen() {
  const gt = useGT();
  const locale = useLocale();
  const navigation = useNavigation();
  const [view, setView] = useAtom(orchestratorViewAtom);
  const { threads, queues } = useOrchestratorQueues();
  const { savedConnectionsById } = useSavedRemoteConnections();
  const preferences = useAtomValue(mobilePreferencesAtom);
  const savePreferences = useAtomCommand(saveMobilePreferences);
  const [choosingThread, setChoosingThread] = useState(false);
  const [targetError, setTargetError] = useState<string | null>(null);
  const bottomClearance = useWorkspaceTabs()?.bottomClearance;
  const target = AsyncResult.isSuccess(preferences)
    ? preferences.value.mobileOrchestratorTarget
    : undefined;
  const routingThread = target
    ? threads.find(
        (thread) =>
          isOrchestratorConversation(thread) &&
          sameOrchestratorThread(orchestratorThreadRef(thread), target),
      )
    : undefined;
  const format = new Intl.DateTimeFormat(locale, { hour: "numeric", minute: "2-digit" });
  const environmentLabel = (thread: EnvironmentThreadShell) =>
    savedConnectionsById[thread.environmentId]?.environmentLabel;
  const selectThread = (thread: EnvironmentThreadShell) => {
    if (view === "compose") return;
    startOpeningEnvironmentThread(thread.environmentId, thread.id);
    navigation.dispatch(
      StackActions.push("OrchestratorThread", {
        ...orchestratorThreadRef(thread),
        queue: view,
        entryOrder: queues[view].map(orchestratorThreadRef),
      }),
    );
  };

  return (
    <View className="flex-1 bg-screen">
      {Platform.OS === "android" ? <AppBar embedded title={gt("Orchestrator")} /> : null}
      <ScreenScrollView
        className="flex-1 bg-screen"
        contentInsetAdjustmentBehavior="automatic"
        automaticallyAdjustKeyboardInsets
        keyboardDismissMode="interactive"
        keyboardShouldPersistTaps="handled"
        contentContainerStyle={{ flexGrow: 1 }}
      >
        <View className="w-full max-w-[720px] flex-1 self-center px-4 py-4">
          {view === "compose" ? (
            <View className="flex-1 justify-center py-3">
              <OrchestratorComposer
                thread={routingThread}
                hasSavedTarget={target !== undefined}
                environmentLabel={routingThread ? environmentLabel(routingThread) : undefined}
                onChooseThread={() => {
                  setTargetError(null);
                  setChoosingThread(true);
                }}
              />
              {targetError ? (
                <Text
                  accessibilityLiveRegion="polite"
                  className="mt-2 text-sm text-foreground-muted"
                >
                  {targetError}
                </Text>
              ) : null}
            </View>
          ) : (
            <View className="flex-1 gap-3">
              <View className="flex-row flex-wrap items-center justify-between gap-2">
                <Text
                  accessibilityRole="header"
                  className="text-2xl font-t3-semibold text-foreground"
                >
                  {view === "done" ? gt("Done") : gt("Needs input")}
                </Text>
                <Button
                  label={gt("Composer")}
                  icon="square.and.pencil"
                  size="compact"
                  onPress={() => setView("compose")}
                />
              </View>
              {queues[view].length === 0 ? (
                <Text className="py-8 text-center text-foreground-muted">
                  {view === "done"
                    ? gt("No threads finished in the last hour.")
                    : gt("No threads are waiting for your input.")}
                </Text>
              ) : (
                <View className="overflow-hidden rounded-2xl bg-grouped-card">
                  {queues[view].map((thread) => {
                    const completed = view === "done" ? finishedAt(thread) : null;
                    const detail =
                      view === "input"
                        ? orchestratorNeedsApproval(thread)
                          ? gt("Approval")
                          : gt("Question")
                        : completed === null
                          ? undefined
                          : format.format(completed);
                    return (
                      <OrchestratorThreadRow
                        key={threadKey(thread)}
                        title={thread.title}
                        detail={[environmentLabel(thread), detail].filter(Boolean).join(" · ")}
                        onPress={() => selectThread(thread)}
                      />
                    );
                  })}
                </View>
              )}
            </View>
          )}
        </View>
      </ScreenScrollView>
      <View
        className="w-full max-w-[720px] self-center px-4 pt-2"
        style={{ paddingBottom: bottomClearance ?? 16 }}
      >
        <View className="flex-row rounded-full bg-grouped-card p-1">
          {(["done", "input"] as const).map((queue) => (
            <Pressable
              key={queue}
              accessibilityRole="button"
              accessibilityState={{ selected: view === queue }}
              onPress={() => {
                Keyboard.dismiss();
                setView(view === queue ? "compose" : queue);
              }}
              className={
                view === queue
                  ? "min-h-[44px] flex-1 flex-row items-center justify-center gap-2 rounded-full bg-subtle-strong px-3"
                  : "min-h-[44px] flex-1 flex-row items-center justify-center gap-2 rounded-full px-3"
              }
            >
              <SymbolView
                name={queue === "done" ? "checkmark.circle" : "tray.and.arrow.down"}
                size={17}
                tintColorClassName="accent-icon-muted"
              />
              <Text className="font-t3-medium text-foreground">
                {queue === "done" ? gt("Done") : gt("Input")}
              </Text>
              <Text className="text-xs text-foreground-muted">{queues[queue].length}</Text>
            </Pressable>
          ))}
        </View>
      </View>
      {choosingThread ? (
        <RoutingThreadPicker
          threads={threads}
          environmentLabel={environmentLabel}
          onClose={() => setChoosingThread(false)}
          onSelect={(thread) => {
            const mobileOrchestratorTarget = orchestratorThreadRef(thread);
            setChoosingThread(false);
            void savePreferences({ mobileOrchestratorTarget }).then((result) => {
              if (result._tag !== "Success")
                setTargetError(gt("Could not save the routing thread."));
            });
          }}
        />
      ) : null}
    </View>
  );
}

function OrchestratorThreadRow(props: {
  readonly title: string;
  readonly detail?: string;
  readonly onPress: () => void;
}) {
  return (
    <Pressable
      accessibilityRole="button"
      onPress={props.onPress}
      className="min-h-[58px] flex-row items-center gap-3 border-b border-subtle-strong px-4 py-2 active:bg-subtle-strong"
    >
      <SymbolView name="text.bubble" size={17} tintColorClassName="accent-icon-muted" />
      <View className="flex-1">
        <Text className="font-t3-medium text-foreground" numberOfLines={2}>
          {props.title}
        </Text>
        {props.detail ? (
          <Text className="text-sm text-foreground-muted">{props.detail}</Text>
        ) : null}
      </View>
      <SymbolView name="chevron.right" size={12} tintColorClassName="accent-icon-muted" />
    </Pressable>
  );
}

function RoutingThreadPicker(props: {
  readonly threads: readonly EnvironmentThreadShell[];
  readonly environmentLabel: (thread: EnvironmentThreadShell) => string | undefined;
  readonly onClose: () => void;
  readonly onSelect: (thread: EnvironmentThreadShell) => void;
}) {
  const gt = useGT();
  const colors = useControlTheme().colors;
  const [query, setQuery] = useState("");
  const search = query.trim().toLocaleLowerCase();
  const candidates = props.threads
    .filter(
      (thread) =>
        isOrchestratorConversation(thread) &&
        thread.source.threadMessagesBlocked !== true &&
        (search === "" ||
          [thread.title, props.environmentLabel(thread)].some((value) =>
            value?.toLocaleLowerCase().includes(search),
          )),
    )
    .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt) || a.title.localeCompare(b.title));
  return (
    <Modal
      visible
      animationType="slide"
      presentationStyle="pageSheet"
      onRequestClose={props.onClose}
    >
      <SafeAreaView className="flex-1 bg-screen">
        <View className="gap-3 px-4 py-3">
          <View className="flex-row flex-wrap items-center justify-between gap-2">
            <Text accessibilityRole="header" className="text-xl font-t3-semibold text-foreground">
              {gt("Choose routing thread")}
            </Text>
            <Button label={gt("Close")} size="compact" onPress={props.onClose} />
          </View>
          <TextInput
            accessibilityLabel={gt("Search threads")}
            placeholder={gt("Search threads")}
            placeholderTextColor={colors["--color-foreground-muted"]}
            value={query}
            onChangeText={setQuery}
            className="min-h-[44px] rounded-xl bg-grouped-card px-3 text-foreground"
          />
        </View>
        <LegendList
          data={candidates}
          estimatedItemSize={58}
          extraData={props.environmentLabel}
          keyExtractor={threadKey}
          keyboardShouldPersistTaps="handled"
          renderScrollComponent={NativeScrollView}
          renderItem={({ item }) => (
            <OrchestratorThreadRow
              title={item.title}
              detail={props.environmentLabel(item)}
              onPress={() => props.onSelect(item)}
            />
          )}
          ListEmptyComponent={
            <Text className="px-6 py-4 text-foreground-muted">
              {search
                ? gt("No matching threads.")
                : gt("Open or create a routing thread in Chats first.")}
            </Text>
          }
        />
      </SafeAreaView>
    </Modal>
  );
}
