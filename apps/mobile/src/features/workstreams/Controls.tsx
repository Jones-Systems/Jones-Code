import { useEffect, useRef, useState } from "react";
import { Modal, Pressable, ScrollView, Text, TextInput, View } from "react-native";
import type { WorkstreamThreadLike } from "@t3tools/client-runtime/state/workstreams";
import {
  attestedNativeThreadKey,
  nativeWorkstreamThreadKey,
  resolveWorkstreamCompletionAuthority,
} from "@t3tools/client-runtime/state/workstreams";
import type { T3WorkstreamMetadata, WorkstreamLifecycle } from "@t3tools/contracts";
import type { MobileWorkstreams } from "./types";
import type { MobileWorkstreamSnapshot } from "./projection";
import { canEditWorkstreams, planNativeMembership } from "./actions";

export function MobileWorkstreamControls({
  api,
  selection,
  onSelect,
  onClose,
}: {
  api: MobileWorkstreams;
  selection: { thread?: WorkstreamThreadLike; groupKey?: string } | null;
  onSelect: (selection: { thread?: WorkstreamThreadLike; groupKey?: string }) => void;
  onClose: () => void;
}) {
  const binding = useRef(api.bindingRevision);
  binding.current = api.bindingRevision;
  const [name, setName] = useState("");
  const [message, setMessage] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [history, setHistory] = useState<readonly string[]>([]);
  useEffect(() => {
    setMessage(null);
    setBusy(false);
    setHistory([]);
    setName("");
  }, [api.bindingRevision, selection]);
  const group = api.groups.find((item) => item.key === selection?.groupKey);
  const snapshot = selection?.thread
    ? api.snapshots.find((item) => item.prepared.environmentId === selection.thread?.environmentId)
    : (group?.snapshot ?? api.snapshots[0]);
  const editable =
    !!snapshot && canEditWorkstreams(snapshot.data) && snapshot.placements?.readiness === "ready";
  const action = (label: string, run: () => Promise<unknown>, disabled = false) => (
    <Pressable
      accessibilityRole="button"
      accessibilityLabel={label}
      disabled={disabled || busy}
      onPress={() => {
        setBusy(true);
        setMessage(null);
        void run()
          .catch(() =>
            setMessage("Could not complete this change. Refresh Workstreams before retrying."),
          )
          .finally(() => setBusy(false));
      }}
      style={{ paddingVertical: 12, opacity: disabled || busy ? 0.45 : 1 }}
    >
      <Text style={{ color: "#2563eb" }}>{label}</Text>
    </Pressable>
  );
  const update = (
    bound: MobileWorkstreamSnapshot,
    item: T3WorkstreamMetadata,
    patch: Partial<Pick<T3WorkstreamMetadata, "name" | "lifecycle" | "sortOrder">>,
  ) =>
    api.submit(bound, {
      operation: "update_workstream",
      workstream_id: item.workstreamId,
      expected_version: item.version,
      name: patch.name ?? item.name,
      lifecycle: patch.lifecycle ?? item.lifecycle,
      progress: item.progress,
      sort_order: patch.sortOrder ?? item.sortOrder,
    });
  const membership = async (destination: string | null, secondary = false) => {
    if (!snapshot || !snapshot.placements || !selection?.thread) throw new Error("Unavailable");
    if (
      !snapshot.identityKeys.has(
        nativeWorkstreamThreadKey(selection.thread!.environmentId, selection.thread!.id),
      )
    )
      throw new Error("This thread is outside the verified placement inventory.");
    const references = await api.pages(snapshot, "/references", (client, headers, cursor) =>
      client.references({ headers, payload: { limit: 50, ...(cursor ? { cursor } : {}) } }),
    );
    const target = destination
      ? snapshot.data.items.find((item) => item.workstreamId === destination)
      : undefined;
    const episodes = target
      ? await api.pages(
          snapshot,
          `/${encodeURIComponent(target.workstreamId)}/memberships`,
          (client, headers, cursor) =>
            client.memberships({
              headers,
              params: { workstreamId: target.workstreamId },
              payload: { limit: 50, ...(cursor ? { cursor } : {}) },
            }),
        )
      : undefined;
    const planned = planNativeMembership({
      data: snapshot.data,
      placements: snapshot.placements,
      references: references.items,
      destinationMemberships: episodes?.items,
      thread: selection.thread,
      destination,
      now: Date.now(),
    });
    if (secondary) {
      if (!target) throw new Error("Unavailable");
      const trust = new Map(
        snapshot.placements.trustedEnvironments.map((entry) => [entry.environmentId, entry]),
      );
      const key = nativeWorkstreamThreadKey(selection.thread!.environmentId, selection.thread!.id);
      const reference = references.items.find(
        (item) =>
          item.owner_id === snapshot.data.binding.ownerId &&
          attestedNativeThreadKey(item, Date.now(), trust) === key,
      );
      if (!reference) throw new Error("Unavailable");
      return api.submit(snapshot, {
        operation: "link_secondary",
        workstream_id: target.workstreamId,
        expected_version: target.version,
        native_reference_id: reference.native_reference_id,
      });
    }
    if (planned) return api.submit(snapshot, planned);
  };
  const showHistory = async (bound: MobileWorkstreamSnapshot, item: T3WorkstreamMetadata) => {
    const startedBinding = api.bindingRevision;
    const [detail, historyPage, members] = await Promise.all([
      api.read(bound, `/${encodeURIComponent(item.workstreamId)}`, (client, headers) =>
        client.detail({ headers, params: { workstreamId: item.workstreamId } }),
      ),
      api.pages(
        bound,
        `/${encodeURIComponent(item.workstreamId)}/history`,
        (client, headers, cursor) =>
          client.history({
            headers,
            params: { workstreamId: item.workstreamId },
            payload: { limit: 50, ...(cursor ? { cursor } : {}) },
          }),
      ),
      api.pages(
        bound,
        `/${encodeURIComponent(item.workstreamId)}/memberships`,
        (client, headers, cursor) =>
          client.memberships({
            headers,
            params: { workstreamId: item.workstreamId },
            payload: { limit: 50, ...(cursor ? { cursor } : {}) },
          }),
      ),
    ]);
    if (
      detail.context.owner_id !== bound.data.binding.ownerId ||
      detail.context.server_generation !== bound.data.binding.serverGeneration ||
      detail.context.registry_version !== bound.data.binding.registryVersion
    )
      throw new Error("Workstream detail revision changed.");
    if (historyPage.next_cursor !== null) throw new Error("Workstream history is incomplete.");
    const completion = resolveWorkstreamCompletionAuthority(detail.workstream, {
      ...historyPage,
      coverage: "complete",
      next_cursor: null,
    });
    if (binding.current !== startedBinding) return;
    setHistory([
      `Completion: ${completion.state}`,
      ...members.items.map(
        (episode) =>
          `${episode.kind}: ${episode.closed === null ? "current" : "ended"} · ${episode.native_reference_id}`,
      ),
      ...historyPage.items.map((event) => `${event.occurred_at}: ${event.operation}`),
    ]);
  };
  return (
    <View>
      <Modal
        visible={selection !== null}
        animationType="slide"
        presentationStyle="pageSheet"
        onRequestClose={onClose}
      >
        <ScrollView
          contentContainerStyle={{ padding: 24, gap: 8, backgroundColor: "#ffffff", flexGrow: 1 }}
        >
          <Pressable accessibilityRole="button" onPress={onClose}>
            <Text style={{ color: "#2563eb" }}>Close</Text>
          </Pressable>
          <Text style={{ fontSize: 22, fontWeight: "600" }}>
            {group?.name ?? (selection?.thread ? "Thread Workstreams" : "Workstreams")}
          </Text>
          {message ? <Text accessibilityRole="alert">{message}</Text> : null}
          {api.error ? <Text>{api.error}</Text> : null}
          {!snapshot ? (
            <Text>Workstreams are unavailable. Connect to an enrolled environment.</Text>
          ) : null}
          {snapshot && !editable ? <Text>Workstreams are read-only.</Text> : null}
          {snapshot?.placements?.readiness !== "ready" ? (
            <Text>Thread assignment requires current verified enrollment.</Text>
          ) : null}
          <Pressable accessibilityRole="button" onPress={api.toggleEnabled}>
            <Text>{api.enabled ? "Use native order" : "Group by Workstream"}</Text>
          </Pressable>
          <Pressable accessibilityRole="button" onPress={api.refresh}>
            <Text>Refresh</Text>
          </Pressable>
          {snapshot && !selection?.thread ? (
            <>
              <TextInput
                accessibilityLabel="Workstream name"
                placeholder={group?.name ?? "New Workstream name"}
                value={name}
                onChangeText={setName}
                style={{ borderWidth: 1, borderColor: "#9ca3af", padding: 12, color: "#111827" }}
              />
              {action(
                group ? "Rename Workstream" : "Create Workstream",
                () =>
                  group
                    ? update(snapshot, group.workstream, { name: name.trim() })
                    : api.submit(snapshot, {
                        operation: "create_workstream",
                        name: name.trim(),
                        lifecycle: "active",
                        progress: { state: "unknown" },
                        sort_order: snapshot.data.items.length,
                      }),
                !editable || !name.trim(),
              )}
            </>
          ) : null}
          {snapshot && group ? (
            <>
              <Text>
                {group.workstream.lifecycle} · {group.workstream.progress.state} ·{" "}
                {group.workstream.delivery}
              </Text>
              {action("Membership history and completion evidence", () =>
                showHistory(snapshot, group.workstream),
              )}
              {(["planned", "active", "paused", "completed", "deferred", "abandoned"] as const).map(
                (lifecycle: WorkstreamLifecycle) => (
                  <View key={lifecycle}>
                    {action(
                      `Mark ${lifecycle}`,
                      () => update(snapshot, group.workstream, { lifecycle }),
                      !editable || group.workstream.lifecycle === lifecycle,
                    )}
                  </View>
                ),
              )}
              <Text>
                Completed records an owner declaration. Membership changes do not settle or stop
                native threads.
              </Text>
              {action("Move group up", () => api.reorderGroup(group, -1), !editable)}
              {action("Move group down", () => api.reorderGroup(group, 1), !editable)}
            </>
          ) : null}
          {snapshot && selection?.thread ? (
            <>
              {action(
                "Move to Unassigned",
                () => membership(null),
                !editable ||
                  snapshot.placements?.readiness !== "ready" ||
                  !snapshot.identityKeys.has(
                    nativeWorkstreamThreadKey(
                      selection.thread!.environmentId,
                      selection.thread!.id,
                    ),
                  ),
              )}
              {snapshot.data.items.map((item) => (
                <View key={item.workstreamId}>
                  <Text style={{ fontWeight: "600" }}>{item.name}</Text>
                  {action(
                    `Assign to ${item.name}`,
                    () => membership(item.workstreamId),
                    !editable ||
                      snapshot.placements?.readiness !== "ready" ||
                      !snapshot.identityKeys.has(
                        nativeWorkstreamThreadKey(
                          selection.thread!.environmentId,
                          selection.thread!.id,
                        ),
                      ),
                  )}
                  {action(
                    `Link secondary: ${item.name}`,
                    () => membership(item.workstreamId, true),
                    !editable ||
                      snapshot.placements?.readiness !== "ready" ||
                      !snapshot.identityKeys.has(
                        nativeWorkstreamThreadKey(
                          selection.thread!.environmentId,
                          selection.thread!.id,
                        ),
                      ),
                  )}
                  {snapshot.placements?.items
                    .filter(
                      (entry) =>
                        entry.kind === "secondary" &&
                        entry.workstream_id === item.workstreamId &&
                        entry.source_instance_id === selection.thread?.environmentId &&
                        entry.native_thread_id === selection.thread?.id,
                    )
                    .map((entry) => (
                      <View key={entry.membership_id}>
                        {action(
                          `Unlink secondary: ${item.name}`,
                          () =>
                            api.submit(snapshot, {
                              operation: "remove_membership",
                              workstream_id: item.workstreamId,
                              expected_version: item.version,
                              membership_id: entry.membership_id,
                            }),
                          !editable,
                        )}
                      </View>
                    ))}
                  {action(`History: ${item.name}`, () => showHistory(snapshot, item))}
                </View>
              ))}
            </>
          ) : null}
          {!selection?.thread && !group
            ? api.groups.map((item) => (
                <Pressable
                  accessibilityRole="button"
                  key={item.key}
                  onPress={() => onSelect({ groupKey: item.key })}
                  style={{ padding: 12 }}
                >
                  <Text>{item.name}</Text>
                </Pressable>
              ))
            : null}
          {history.map((line, index) => (
            <Text key={index}>{line}</Text>
          ))}
        </ScrollView>
      </Modal>
    </View>
  );
}
