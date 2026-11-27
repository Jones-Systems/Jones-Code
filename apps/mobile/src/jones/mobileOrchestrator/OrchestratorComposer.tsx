import { StackActions, useNavigation } from "@react-navigation/native";
import {
  AuthOrchestrationOperateScope,
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  MessageId,
  type ModelSelection,
} from "@t3tools/contracts";
import {
  getPromptControlledTraits,
  getTraitSelectChange,
} from "@t3tools/client-runtime/model-traits";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import { useGT } from "gt-react-native";
import { useRef, useState } from "react";
import { Keyboard, TextInput, View } from "react-native";

import { AppText as Text } from "../../components/ui/AppText";
import { Button } from "../../components/ui/Button";
import { useControlTheme } from "../../components/ui/useControlTheme";
import type { ExistingThreadSettingsRouteParams } from "../../features/threads/ThreadSettingsSheet";
import { makeQueuedMessageMetadata } from "../../lib/commandMetadata";
import {
  describeUnavailableModelSelection,
  isModelSelectionUnavailable,
  serverConfigForCwd,
} from "../../lib/modelOptions";
import {
  applyProviderOptionSelection,
  resolveProviderOptionDescriptors,
} from "../../lib/providerOptions";
import { useProject } from "../../state/entities";
import { readEnvironmentScope, useEnvironmentScope } from "../../state/session";
import { enqueueThreadOutboxMessage } from "../../state/thread-outbox";
import {
  getComposerDraftSnapshot,
  setComposerDraftText,
  updateComposerDraftSettings,
  useComposerDraft,
  useComposerDraftSettings,
} from "../../state/use-composer-drafts";
import { useRemoteEnvironmentRuntime } from "../../state/use-remote-environment-registry";
import { isQuickModelOption, QuickModelOption } from "../modelOptions/QuickModelOptions";
import { ORCHESTRATOR_TEXT_DRAFT_KEY, orchestratorOptionsKey, orchestratorRequest } from "./draft";
import { isOrchestratorConversation, orchestratorThreadRef } from "./queues";

export function OrchestratorComposer(props: {
  readonly thread: EnvironmentThreadShell | undefined;
  readonly onChooseThread: () => void;
  readonly environmentLabel: string | undefined;
  readonly hasSavedTarget: boolean;
}) {
  const gt = useGT();
  const navigation = useNavigation();
  const colors = useControlTheme().colors;
  const target = props.thread;
  const environmentId = target?.environmentId ?? null;
  const draftKey = ORCHESTRATOR_TEXT_DRAFT_KEY;
  const { text } = useComposerDraft(draftKey);
  const optionsKey = target ? orchestratorOptionsKey(orchestratorThreadRef(target)) : null;
  const options = useComposerDraftSettings(optionsKey);
  const runtime = useRemoteEnvironmentRuntime(environmentId);
  const project = useProject(
    target ? { environmentId: target.environmentId, projectId: target.projectId } : null,
  );
  const canOperate = useEnvironmentScope(environmentId, AuthOrchestrationOperateScope);
  const selection = options.modelSelection ?? target?.modelSelection ?? null;
  const config = serverConfigForCwd(
    runtime?.serverConfig,
    target?.worktreePath ?? project?.workspaceRoot ?? null,
  );
  const model = config?.providers
    .find((provider) => provider.instanceId === selection?.instanceId)
    ?.models.find((candidate) => candidate.slug === selection?.model);
  const descriptors = resolveProviderOptionDescriptors({
    capabilities: model?.capabilities,
    selections: selection?.options,
  });
  const promptTraits = getPromptControlledTraits(descriptors, text);
  const inFlight = useRef(false);
  const [saving, setSaving] = useState(false);
  const [feedback, setFeedback] = useState<string | null>(null);
  const canQueue =
    target !== undefined &&
    canOperate &&
    runtime?.connectionState === "connected" &&
    target.source.threadMessagesBlocked !== true;
  const openModelPicker = () => {
    if (!target || !optionsKey) return;
    Keyboard.dismiss();
    navigation.dispatch(
      StackActions.push("ThreadSettingsSheet", {
        environmentId: target.environmentId,
        threadId: target.id,
        draftKey: optionsKey,
        orchestrator: true,
      } satisfies ExistingThreadSettingsRouteParams),
    );
  };
  const chooseThread = () => {
    Keyboard.dismiss();
    setFeedback(null);
    props.onChooseThread();
  };

  const send = async () => {
    const draft = getComposerDraftSnapshot(draftKey).text;
    if (
      !draft.trim() ||
      inFlight.current ||
      !canQueue ||
      !isOrchestratorConversation(target) ||
      !readEnvironmentScope(target.environmentId, AuthOrchestrationOperateScope)
    )
      return;
    if (selection && isModelSelectionUnavailable(config, selection, target.projectId)) {
      setFeedback(describeUnavailableModelSelection(config, selection, target.projectId).detail);
      return;
    }
    inFlight.current = true;
    setSaving(true);
    setFeedback(null);
    const metadata = makeQueuedMessageMetadata();
    try {
      await enqueueThreadOutboxMessage({
        environmentId: target.environmentId,
        threadId: target.id,
        messageId: MessageId.make(metadata.messageId),
        commandId: CommandId.make(metadata.commandId),
        text: orchestratorRequest(draft.trim()),
        attachments: [],
        ...(options.modelSelection
          ? { modelSelection: options.modelSelection, modelSelectionExplicit: true }
          : {}),
        runtimeMode: options.runtimeMode ?? target.runtimeMode,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        dispatchMode: "queue",
        createdAt: metadata.createdAt,
      });
      if (getComposerDraftSnapshot(draftKey).text === draft) setComposerDraftText(draftKey, "");
      setFeedback(gt("Queued in {thread}.", { thread: target.title }));
    } catch {
      setFeedback(gt("Could not save the message. Your draft is still here."));
    } finally {
      inFlight.current = false;
      setSaving(false);
    }
  };

  return (
    <View className="gap-3">
      <View className="flex-row flex-wrap items-center gap-2">
        <Button
          label={target ? target.title : gt("Choose routing thread")}
          accessibilityLabel={
            target
              ? gt("Change routing thread, currently {thread}", { thread: target.title })
              : undefined
          }
          icon="point.3.connected.trianglepath.dotted"
          size="compact"
          onPress={chooseThread}
          disabled={saving}
        />
        {target ? (
          <Button
            label={gt("Open thread")}
            tone="text"
            size="compact"
            onPress={() =>
              navigation.dispatch(
                StackActions.push("ThreadInspectorFullscreen", orchestratorThreadRef(target)),
              )
            }
          />
        ) : null}
      </View>
      {props.environmentLabel && target ? (
        <Text className="text-sm text-foreground-muted">{props.environmentLabel}</Text>
      ) : null}
      {!target ? (
        <Text className="text-sm text-foreground-muted">
          {props.hasSavedTarget
            ? gt("The saved routing thread is unavailable. Reconnect or choose a thread.")
            : gt("Choose the thread that will route your requests.")}
        </Text>
      ) : !canQueue ? (
        <Text className="text-sm text-foreground-muted">
          {gt("This routing thread is unavailable for sending.")}
        </Text>
      ) : null}
      <View className="gap-2 rounded-[24px] bg-composer-panel p-3">
        <TextInput
          accessibilityLabel={gt("Orchestrator message")}
          multiline
          value={text}
          onChangeText={(value) => setComposerDraftText(draftKey, value)}
          placeholder={gt("What should I route?")}
          placeholderTextColor={colors["--color-foreground-muted"]}
          selectionColor={colors["--color-primary"]}
          submitBehavior="newline"
          textAlignVertical="top"
          className="min-h-40 px-2 pt-2 text-base text-foreground"
        />
        <View className="flex-row flex-wrap items-center justify-between gap-2">
          {target ? (
            <Button
              label={model?.name ?? selection?.model ?? gt("Model")}
              accessibilityLabel={gt("Choose model")}
              tone="text"
              size="compact"
              onPress={openModelPicker}
            />
          ) : null}
          <Button
            label={gt("Send")}
            icon="arrow.up"
            tone="primary"
            loading={saving}
            disabled={!canQueue || text.trim() === ""}
            onPress={() => void send()}
          />
        </View>
      </View>
      {selection && optionsKey ? (
        <View className="rounded-2xl bg-grouped-card">
          {descriptors.filter(isQuickModelOption).map((descriptor) => (
            <QuickModelOption
              key={descriptor.id}
              descriptor={descriptor}
              selection={selection}
              onMore={openModelPicker}
              onChange={(value) => {
                const change =
                  descriptor.type === "select" && typeof value === "string"
                    ? getTraitSelectChange(
                        { ...promptTraits, descriptors },
                        descriptor,
                        text,
                        value,
                      )
                    : null;
                if (descriptor.type === "select" && change === null) return;
                const selections = change
                  ? (change.modelOptions ?? [])
                  : applyProviderOptionSelection(
                      descriptors,
                      { id: descriptor.id, value },
                      selection.options,
                    );
                if (selections === null) return;
                if (change && change.prompt !== text) setComposerDraftText(draftKey, change.prompt);
                const modelSelection: ModelSelection = { ...selection, options: selections };
                updateComposerDraftSettings(optionsKey, { modelSelection });
              }}
            />
          ))}
        </View>
      ) : null}
      {feedback ? (
        <Text accessibilityLiveRegion="polite" className="text-sm text-foreground-muted">
          {feedback}
        </Text>
      ) : null}
    </View>
  );
}
