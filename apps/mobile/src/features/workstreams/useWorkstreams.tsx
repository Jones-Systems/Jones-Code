import { useAtomValue } from "@effect/atom-react";
import {
  groupPersonalThreads,
  personalThreadKey,
  type PersonalWorkstreamsState,
  type PersonalThreadRef,
  type PersonalResult,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { Atom } from "effect/reactivity";
import { useEffect, useRef, useState } from "react";
import { Pressable, Text, View } from "react-native";
import { personalMobileStorage } from "./personalStorage";
import { useEnvironments } from "../../state/environments";
import { Toast } from "../../components/ui/Toast";
import { MobileWorkstreamControls } from "./Controls";
import { mountPersonalMobile, type getPersonalMobileBinding } from "./personalBinding";
import { personalWorkstreamFailureText } from "./personalPresentation";

const emptyState: PersonalWorkstreamsState = {
  workstreams: [],
  environments: [],
  assignmentsByThreadKey: new Map(),
  pendingThreadKeys: new Set(),
  canCreate: false,
};
const empty = Atom.make(emptyState);
const COLLAPSE = "jones.workstreams.personal.collapsed.v1";
export function useMobileWorkstreams(threads: readonly PersonalThreadRef[]) {
  const { environments } = useEnvironments();
  const idsKey = JSON.stringify(environments.map((entry) => entry.environmentId).sort());
  const [binding, setBinding] = useState<Awaited<
    ReturnType<typeof getPersonalMobileBinding>
  > | null>(null);
  const [selection, setSelection] = useState<{
    thread?: PersonalThreadRef;
    groupKey?: string;
  } | null>(null);
  const [collapsedKeys, setCollapsed] = useState<ReadonlySet<string>>(new Set());
  const [notice, setNotice] = useState<{ key: string; text: string } | null>(null);
  const seen = useRef(new Set<string>());
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Attach the shared controller to mounted native screens and release its connection subscriptions on unmount.
  useEffect(() => {
    let active = true;
    let release: (() => void) | undefined;
    void mountPersonalMobile(JSON.parse(idsKey))
      .then((mounted) => {
        if (!active) {
          mounted.release();
          return;
        }
        release = mounted.release;
        setBinding(mounted);
      })
      .catch(() => {
        if (active) setNotice({ key: "storage", text: "Couldn't load Workstreams. Try again." });
      });
    return () => {
      active = false;
      release?.();
    };
  }, [idsKey]);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Read persisted native collapse preferences once, cancelling delivery to an unmounted screen.
  useEffect(() => {
    let active = true;
    void personalMobileStorage
      .getItem(COLLAPSE)
      .then((raw) => {
        const keys: unknown = raw ? JSON.parse(raw) : [];
        if (active && Array.isArray(keys) && keys.every((key) => typeof key === "string"))
          setCollapsed(new Set(keys));
      })
      .catch(() => {});
    return () => {
      active = false;
    };
  }, []);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Own and cancel the native notification dismissal timer for the displayed notice.
  useEffect(() => {
    if (!notice) return;
    const timer = setTimeout(() => {
      seen.current.delete(notice.key);
      setNotice(null);
    }, 6000);
    return () => clearTimeout(timer);
  }, [notice]);
  const state = useAtomValue(binding?.controller.state ?? empty);
  const grouped = groupPersonalThreads(state, threads);
  const groups = grouped.groups.map(({ workstream, threads: members }) => ({
    key: workstream.id,
    name: workstream.name,
    color: workstream.color ?? "#0284C7",
    ...(workstream.color ? { borderColor: workstream.color } : {}),
    threadKeys: new Set(members.map(personalThreadKey)),
  }));
  const confirmedUnassignedKeys = new Set(
    threads
      .filter((thread) =>
        state.environments.some(
          (entry) =>
            entry.environmentId === thread.environmentId &&
            entry.confirmed &&
            entry.status !== "update_required",
        ),
      )
      .map(personalThreadKey),
  );
  async function perform<A>(action: () => Promise<PersonalResult<A>>) {
    const result = await action();
    if (!result.ok && !seen.current.has(result.failure.dedupeKey)) {
      seen.current.add(result.failure.dedupeKey);
      const label =
        environments.find((entry) => entry.environmentId === result.failure.environmentId)?.label ??
        "Environment";
      setNotice({
        key: result.failure.dedupeKey,
        text: personalWorkstreamFailureText(result.failure, label),
      });
    }
    return result.ok;
  }
  return {
    enabled: state.environments.some((entry) => entry.status !== "update_required"),
    groups,
    collapsedKeys,
    confirmedUnassignedKeys,
    secondaryLabelsByKey: new Map<string, readonly string[]>(),
    bindingRevision: JSON.stringify([...state.assignmentsByThreadKey]),
    toggleGroup: (key: string) =>
      setCollapsed((previous) => {
        const next = new Set(previous);
        if (next.has(key)) next.delete(key);
        else next.add(key);
        void personalMobileStorage.setItem(COLLAPSE, JSON.stringify([...next])).catch(() => {});
        return next;
      }),
    openThread: (thread: PersonalThreadRef) => setSelection({ thread }),
    openGroup: (groupKey: string) => setSelection({ groupKey }),
    controls: (
      <Pressable
        accessibilityRole="button"
        onPress={() => setSelection({})}
        style={{ padding: 12 }}
      >
        <Text>Workstreams</Text>
      </Pressable>
    ),
    sheet: (
      <>
        {binding && (
          <MobileWorkstreamControls
            key={
              selection?.groupKey ??
              (selection?.thread ? personalThreadKey(selection.thread) : "all")
            }
            controller={binding.controller}
            state={state}
            selection={selection}
            onSelect={setSelection}
            onClose={() => setSelection(null)}
            perform={perform}
            environmentNames={
              new Map(environments.map((entry) => [entry.environmentId, entry.label]))
            }
          />
        )}
        {notice && (
          <View style={{ position: "absolute", bottom: 24, left: 16, right: 16 }}>
            <Toast
              message={notice.text}
              tone="danger"
              dismissMode="swipe"
              onDismiss={() => {
                seen.current.delete(notice.key);
                setNotice(null);
              }}
            />
          </View>
        )}
      </>
    ),
  };
}
