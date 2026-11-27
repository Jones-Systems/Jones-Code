import { useEffect, useState } from "react";
import { Alert, Modal, Pressable, ScrollView, Text, TextInput, View } from "react-native";
import { isClosedPersonalWorkstream } from "@t3tools/contracts/jones/workstreamsPersonal";
import {
  openPersonalWorkstreams,
  personalThreadKey,
  type PersonalWorkstreamHistory,
  type PersonalThreadRef,
  type PersonalWorkstreamsController,
  type PersonalWorkstreamsState,
  type PersonalResult,
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
  onOpenThread?: ((thread: PersonalThreadRef) => void) | undefined;
  perform: <A>(action: () => Promise<PersonalResult<A>>) => Promise<boolean>;
  environmentNames: ReadonlyMap<string, string>;
}) {
  const [name, setName] = useState("");
  const [busy, setBusy] = useState(false);
  const [colorOpen, setColorOpen] = useState(false);
  const group = props.state.workstreams.find((entry) => entry.id === props.selection?.groupKey);
  const closeRequest = props.state.fleet?.operations.find(
    (operation) =>
      operation.workstreamId === group?.id &&
      operation.kind === "close" &&
      (operation.status === "pending" || operation.status === "stale"),
  );
  const waitingOn = closeRequest?.peers
    .filter((peer) => peer.state !== "delivered")
    .map((peer) => props.environmentNames.get(peer.environmentId) ?? peer.environmentId);
  const thread = props.selection?.thread;
  const openWorkstreams = openPersonalWorkstreams(props.state);
  const closedWorkstreams = props.state.workstreams.filter((entry) =>
    isClosedPersonalWorkstream(entry.lifecycle),
  );
  const closedId = group && isClosedPersonalWorkstream(group.lifecycle) ? group.id : null;
  const [history, setHistory] = useState<PersonalWorkstreamHistory | null>(null);
  const controller = props.controller;
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Read history for the mounted group and discard late results on close.
  useEffect(() => {
    if (!closedId) return;
    let live = true;
    const unavailable = { members: [], complete: false };
    void (controller.readHistory?.(closedId) ?? Promise.resolve(unavailable))
      .catch(() => unavailable)
      .then((result) => {
        if (live) setHistory(result);
      });
    return () => {
      live = false;
    };
  }, [controller, closedId]);
  const writable = thread
    ? props.controller.canAssign(thread)
    : props.state.canCreate && !closeRequest;
  const run = async (action: () => Promise<PersonalResult<unknown>>) => {
    if (busy) return;
    setBusy(true);
    await Promise.resolve()
      .then(() => props.perform(action))
      .then((saved) => {
        if (saved) setName("");
      })
      .finally(() => setBusy(false));
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
    const ordered = openWorkstreams.filter((entry) => entry.id !== group.id);
    const oldIndex = openWorkstreams.findIndex((entry) => entry.id === group.id);
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
        {closeRequest ? (
          <View>
            <Text accessibilityLiveRegion="polite">
              {closeRequest.status === "stale"
                ? "Close requested · environment list changed"
                : closeRequest.reason === "unsettled_threads"
                  ? "Close requested · settle or move active threads on the authority"
                  : waitingOn?.length
                    ? `Close requested · waiting for ${waitingOn.join(", ")}`
                    : "Close requested"}
            </Text>
            {controller.cancelOperation
              ? button(
                  "Cancel close request",
                  () => void run(() => controller.cancelOperation!(closeRequest.requestId)),
                  !props.state.canCreate,
                )
              : null}
          </View>
        ) : null}
        {!closedId && (
          <TextInput
            accessibilityLabel={group ? "Workstream name" : "New Workstream name"}
            value={name}
            onChangeText={setName}
            placeholder={group?.name ?? "Workstream name"}
            maxLength={160}
            editable={!busy && writable}
            style={{ padding: 12, borderWidth: 1, borderColor: "#9CA3AF" }}
          />
        )}
        {group && closedId ? (
          <>
            <Text>{group.lifecycle === "completed" ? "Completed" : "Abandoned"}</Text>
            <Text accessibilityRole="header">Assigned threads</Text>
            {!history && <Text>Loading threads…</Text>}
            {history && !history.complete && (
              <Text>Some environments are unavailable. This list may be incomplete.</Text>
            )}
            {history?.complete && !history.members.length && <Text>No assigned threads.</Text>}
            {history?.members.map((member) => (
              <View key={personalThreadKey(member)}>
                {props.onOpenThread ? (
                  button(member.title, () => {
                    props.onOpenThread?.(member);
                    props.onClose();
                  })
                ) : (
                  <Text>{member.title}</Text>
                )}
                <Text>{member.settled ? "Settled" : "Unsettled"}</Text>
              </View>
            ))}
            {button(
              "Reopen Workstream",
              () => void run(() => props.controller.setLifecycle(group.id, "active")),
              !writable,
            )}
            {button("All Workstreams", () => props.onSelect({}))}
          </>
        ) : group ? (
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
              !writable || openWorkstreams[0]?.id === group.id,
            )}
            {button(
              "Move Workstream down",
              () => move(1),
              !writable || openWorkstreams.at(-1)?.id === group.id,
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
            {openWorkstreams.map((entry) =>
              button(
                entry.name,
                () => {
                  if (thread) void run(() => props.controller.assign(thread, entry.id));
                  else {
                    setName("");
                    props.onSelect({ groupKey: entry.id });
                  }
                },
                !!thread &&
                  (!writable || props.state.pendingCloseWorkstreamIds?.has(entry.id) === true),
              ),
            )}
            {!thread && closedWorkstreams.length > 0 && (
              <View>
                <Text accessibilityRole="header">Closed Workstreams</Text>
                {closedWorkstreams.map((entry) => (
                  <View key={entry.id}>
                    {button(entry.name, () => props.onSelect({ groupKey: entry.id }))}
                    <Text>{entry.lifecycle === "completed" ? "Completed" : "Abandoned"}</Text>
                  </View>
                ))}
              </View>
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
