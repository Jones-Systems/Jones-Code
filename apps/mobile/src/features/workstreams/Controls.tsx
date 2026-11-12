import { useState } from "react";
import { Alert, Modal, Pressable, ScrollView, Text, TextInput, View } from "react-native";
import type {
  PersonalThreadRef,
  PersonalWorkstreamsController,
  PersonalWorkstreamsState,
  PersonalResult,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { WorkstreamColorPicker } from "../../jones/workstreamAppearance/WorkstreamColorPicker";
import { personalEnvironmentStatusText } from "./personalPresentation";

type Selection = { thread?: PersonalThreadRef; groupKey?: string };
export function MobileWorkstreamControls(props: {
  controller: PersonalWorkstreamsController;
  state: PersonalWorkstreamsState;
  selection: Selection | null;
  onSelect: (selection: Selection) => void;
  onClose: () => void;
  perform: <A>(action: () => Promise<PersonalResult<A>>) => Promise<boolean>;
  environmentNames: ReadonlyMap<string, string>;
}) {
  const [name, setName] = useState("");
  const [busy, setBusy] = useState(false);
  const [colorOpen, setColorOpen] = useState(false);
  const group = props.state.workstreams.find((entry) => entry.id === props.selection?.groupKey);
  const thread = props.selection?.thread;
  const writable = thread ? props.controller.canAssign(thread) : props.state.canCreate;
  const run = async (action: () => Promise<PersonalResult<unknown>>) => {
    if (busy) return;
    setBusy(true);
    try {
      if (await props.perform(action)) setName("");
    } finally {
      setBusy(false);
    }
  };
  const button = (label: string, action: () => void, disabled = false) => (
    <Pressable
      key={label}
      accessibilityRole="button"
      accessibilityLabel={label}
      accessibilityState={{ disabled: disabled || busy }}
      disabled={disabled || busy}
      onPress={action}
      style={{ paddingVertical: 12, opacity: disabled || busy ? 0.45 : 1 }}
    >
      <Text>{label}</Text>
    </Pressable>
  );
  const move = (direction: -1 | 1) => {
    if (!group) return;
    const ordered = props.state.workstreams.filter((entry) => entry.id !== group.id);
    const oldIndex = props.state.workstreams.findIndex((entry) => entry.id === group.id);
    const index = Math.max(0, Math.min(ordered.length, oldIndex + direction));
    void run(() =>
      props.controller.reorder(group.id, {
        beforeId: ordered[index - 1]?.id ?? null,
        afterId: ordered[index]?.id ?? null,
      }),
    );
  };
  return (
    <Modal
      visible={props.selection !== null}
      animationType="slide"
      presentationStyle="pageSheet"
      onRequestClose={props.onClose}
    >
      <ScrollView
        contentContainerStyle={{ padding: 24, gap: 12 }}
        keyboardShouldPersistTaps="handled"
      >
        <Text accessibilityRole="header" style={{ fontSize: 24, fontWeight: "600" }}>
          {group?.name ?? "Workstreams"}
        </Text>
        <TextInput
          accessibilityLabel={group ? "Workstream name" : "New Workstream name"}
          value={name}
          onChangeText={setName}
          placeholder={group?.name ?? "Workstream name"}
          maxLength={160}
          editable={!busy && writable}
          style={{ padding: 12, borderWidth: 1, borderColor: "#9CA3AF" }}
        />
        {group ? (
          <>
            {button(
              "Rename Workstream",
              () => void run(() => props.controller.rename(group.id, name.trim())),
              !writable || !name.trim(),
            )}
            {button("Change color", () => setColorOpen((value) => !value), !writable)}
            {colorOpen && (
              <WorkstreamColorPicker
                key={group.id}
                name={group.name}
                color={group.color}
                save={async (color) => {
                  const ok = await props.perform(() => props.controller.setColor(group.id, color));
                  if (ok) setColorOpen(false);
                  return ok;
                }}
                onClose={() => setColorOpen(false)}
              />
            )}
            <Text accessibilityRole="header">Status</Text>
            {(["planned", "active", "paused", "completed", "deferred", "abandoned"] as const).map(
              (lifecycle) =>
                button(
                  `${group.lifecycle === lifecycle ? "✓ " : ""}${lifecycle[0]!.toUpperCase()}${lifecycle.slice(1)}`,
                  () => void run(() => props.controller.setLifecycle(group.id, lifecycle)),
                  !writable,
                ),
            )}
            {button(
              "Move Workstream up",
              () => move(-1),
              !writable || props.state.workstreams[0]?.id === group.id,
            )}
            {button(
              "Move Workstream down",
              () => move(1),
              !writable || props.state.workstreams.at(-1)?.id === group.id,
            )}
            {button(
              "Delete Workstream",
              () =>
                Alert.alert("Delete Workstream?", "Threads stay in your thread list.", [
                  { text: "Cancel", style: "cancel" },
                  {
                    text: "Delete",
                    style: "destructive",
                    onPress: () =>
                      void run(async () => {
                        const result = await props.controller.remove(group.id);
                        if (result.ok) props.onSelect({});
                        return result;
                      }),
                  },
                ]),
              !writable,
            )}
            {button("All Workstreams", () => {
              setColorOpen(false);
              setName("");
              props.onSelect({});
            })}
          </>
        ) : (
          <>
            {button(
              thread ? "Create and move thread" : "Create Workstream",
              () =>
                void run(() =>
                  thread
                    ? props.controller.createAndAssign(thread, name.trim())
                    : props.controller.create({ name: name.trim() }),
                ),
              !writable || !name.trim(),
            )}
            {thread &&
              button(
                "Remove from Workstream",
                () => void run(() => props.controller.assign(thread, null)),
                !writable,
              )}
            {props.state.workstreams.map((entry) =>
              button(
                entry.name,
                () => {
                  if (thread) void run(() => props.controller.assign(thread, entry.id));
                  else {
                    setName("");
                    props.onSelect({ groupKey: entry.id });
                  }
                },
                !!thread && !writable,
              ),
            )}
            <View accessibilityLabel="Workstreams connection status" style={{ gap: 12 }}>
              {props.state.environments.map((entry) => (
                <View
                  key={entry.environmentId}
                  style={{ flexDirection: "row", justifyContent: "space-between" }}
                >
                  <Text>{props.environmentNames.get(entry.environmentId) ?? "Environment"}</Text>
                  <Text>{personalEnvironmentStatusText[entry.status]}</Text>
                </View>
              ))}
            </View>
          </>
        )}
        {button("Done", props.onClose)}
      </ScrollView>
    </Modal>
  );
}
