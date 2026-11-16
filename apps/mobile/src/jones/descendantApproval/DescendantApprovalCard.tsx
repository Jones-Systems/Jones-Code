import type { DescendantApprovalState } from "@t3tools/client-runtime/jones/descendant-approval";
import {
  FULL_ACCESS_OVERRIDE_QUESTION,
  fullAccessOverrideExplanation,
} from "@t3tools/client-runtime/state/thread-requests";
import type {
  ProviderApprovalDecision,
  ProviderApprovalOption,
  RuntimeRequestId,
  ScopedThreadRef,
} from "@t3tools/contracts";
import { ScrollView, View } from "react-native";

import { AppText as Text } from "../../components/ui/AppText";
import { Button } from "../../components/ui/Button";
import { PendingApprovalCard } from "../../features/threads/PendingApprovalCard";

interface DescendantApprovalCardProps {
  readonly state: DescendantApprovalState;
  readonly onRespond: (
    requestId: RuntimeRequestId,
    decision: ProviderApprovalDecision,
  ) => Promise<unknown>;
  readonly onNext: () => void;
  readonly onOpen: (ref: ScopedThreadRef) => void;
}

const DEFAULT_OPTIONS: ReadonlyArray<ProviderApprovalOption> = [
  { decision: "accept", label: "Allow once" },
  { decision: "acceptForSession", label: "Allow session" },
  { decision: "decline", label: "Decline" },
];

export function DescendantApprovalCard({
  state,
  onRespond,
  onNext,
  onOpen,
}: DescendantApprovalCardProps) {
  const { view, selected } = state;
  const approval = view.approval;
  const options = approval?.options ?? DEFAULT_OPTIONS;
  const warning = options.find((option) => option.warning)?.warning;
  return (
    <View className="gap-2 rounded-[20px] border border-border bg-card-alt p-3">
      <Text numberOfLines={2} className="font-t3-bold text-sm text-foreground">
        {selected.title} needs approval
      </Text>
      {state.candidates.length > 1 ? (
        <Text className="font-sans text-xs text-foreground-secondary">
          {state.position} of {state.candidates.length} subagents waiting
        </Text>
      ) : null}
      {view.kind === "unavailable" ? (
        <Text className="font-sans text-sm text-foreground-muted">{view.message}</Text>
      ) : null}
      <ScrollView
        style={{ maxHeight: 220 }}
        nestedScrollEnabled
        keyboardShouldPersistTaps="handled"
      >
        {view.kind === "actionable" ? (
          <PendingApprovalCard
            canOperateThread
            approval={view.approval}
            respondingApprovalId={state.responding ? view.approval.requestId : null}
            onRespond={(requestId, decision) => {
              if (state.responding) return Promise.resolve();
              return onRespond(requestId, decision);
            }}
          />
        ) : approval ? (
          <View className="gap-2">
            <Text className="font-t3-bold text-sm text-foreground">
              {approval.appName ?? approval.requestKind}
            </Text>
            {approval.detail ? (
              <Text className="font-sans text-sm text-foreground-secondary">{approval.detail}</Text>
            ) : null}
            {approval.reason ? (
              <Text className="font-sans text-xs text-warning-foreground">{approval.reason}</Text>
            ) : null}
            {approval.fullAccessOverride ? (
              <View className="gap-1">
                <Text className="font-t3-bold text-xs text-foreground-secondary">
                  {FULL_ACCESS_OVERRIDE_QUESTION}
                </Text>
                <Text className="font-sans text-xs text-foreground-muted">
                  {fullAccessOverrideExplanation(approval.fullAccessOverride)}
                </Text>
              </View>
            ) : null}
            {warning ? (
              <Text className="font-sans text-xs text-warning-foreground">{warning}</Text>
            ) : null}
            <View className="flex-row flex-wrap gap-2">
              {options.map((option) => (
                <Button
                  key={option.decision}
                  label={option.label}
                  disabled
                  accessibilityHint={view.message}
                  onPress={() => {}}
                />
              ))}
            </View>
          </View>
        ) : null}
      </ScrollView>
      {state.responding ? (
        <Text className="font-sans text-xs text-foreground-muted">Sending approval response…</Text>
      ) : null}
      <View className="flex-row flex-wrap gap-2">
        {state.candidates.length > 1 ? (
          <Button label="Next" accessibilityLabel="Next subagent approval" onPress={onNext} />
        ) : null}
        <Button
          label="Open"
          accessibilityLabel={`Open ${selected.title}`}
          onPress={() => onOpen(selected.ref)}
        />
      </View>
    </View>
  );
}
