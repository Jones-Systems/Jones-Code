interface WorkQueueItemFields {
  id: string;
  snapshotToken: string;
  text: string | null;
  statusLabel: string;
  submittedAt: string | null;
  targetLabel: string | null;
  pause?: "manual" | "editing" | "grace" | undefined;
  graceUntil?: number | undefined;
}

export type WorkQueueItem = WorkQueueItemFields &
  ({ editability: "editable"; reason?: string } | { editability: "readonly"; reason: string });

export type WorkQueueSaveResult =
  | { kind: "saved"; item: WorkQueueItem }
  | { kind: "conflict"; current: WorkQueueItem }
  | { kind: "held"; current: WorkQueueItem; reason: string }
  | { kind: "error"; message: string; effect: "none" | "unknown" };

// This preview interface is local to the page; it grants no real queue authority.
export interface WorkQueueSource {
  mode: "mock";
  load(): Promise<readonly WorkQueueItem[]>;
  beginEdit(id: string): WorkQueueItem | undefined;
  finishEdit(id: string): WorkQueueItem | undefined;
  setManualPause(id: string, paused: boolean): WorkQueueItem | undefined;
  tick(protectedIds: readonly string[]): readonly WorkQueueItem[];
  sendNow(input: { id: string; baseToken: string }): WorkQueueSaveResult;
  save(input: { id: string; baseToken: string; text: string }): Promise<WorkQueueSaveResult>;
}

export function createMockWorkQueueSource(now: () => number = Date.now): WorkQueueSource {
  const descriptions = [
    [
      "pending",
      "Pending",
      "Review the workspace navigation and propose a simpler layout.",
      "editable",
      "",
    ],
    ["navigation", "Pending", "Add keyboard navigation to the project list.", "editable", ""],
    ["release-notes", "Pending", "Check the release notes before the next update.", "editable", ""],
    [
      "held",
      "Blocked",
      "Verify the target workspace before delivery.",
      "readonly",
      "Target verification is still required.",
    ],
    ["decisions", "Pending", "Summarize the latest design decisions.", "editable", ""],
    [
      "unknown",
      "UNKNOWN",
      "Confirm the result of the previous delivery.",
      "readonly",
      "Delivery effect is unknown. Reconcile it before further action.",
    ],
    [
      "reserved",
      "Reserved",
      "Inspect the settings search flow.",
      "readonly",
      "A worker has reserved this submission.",
    ],
    [
      "delivered",
      "Delivered",
      "Compare the current page with the accepted design.",
      "readonly",
      "This submission has already been delivered.",
    ],
  ] as const;
  const rows = new Map<string, WorkQueueItem>(
    descriptions.map(([id, statusLabel, text, editability, reason], index) => [
      id,
      {
        id,
        statusLabel,
        text,
        editability,
        reason,
        snapshotToken: `sample-${id}-1`,
        submittedAt: `2026-10-03T${String(15 - index).padStart(2, "0")}:20:00Z`,
        targetLabel:
          index % 2 ? "Design review · existing thread" : "Workspace improvements · new thread",
      },
    ]),
  );
  let revision = 1;
  function grace(item: WorkQueueItem) {
    return item.pause === "manual"
      ? { pause: "manual" as const, graceUntil: undefined }
      : { pause: "grace" as const, graceUntil: now() + 120_000 };
  }
  function change(id: string, update: (item: WorkQueueItem) => WorkQueueItem) {
    const item = rows.get(id);
    if (!item || item.editability !== "editable") return undefined;
    const next = update(item);
    rows.set(id, next);
    return { ...next };
  }
  return {
    mode: "mock",
    async load() {
      return [...rows.values()].map((item) => ({ ...item }));
    },
    beginEdit(id) {
      return change(id, (item) =>
        item.pause === "manual" ? item : { ...item, pause: "editing", graceUntil: undefined },
      );
    },
    finishEdit(id) {
      return change(id, (item) => ({ ...item, ...grace(item) }));
    },
    setManualPause(id, paused) {
      return change(id, (item) => ({
        ...item,
        pause: paused ? "manual" : undefined,
        graceUntil: undefined,
      }));
    },
    tick(protectedIds) {
      for (const [id, item] of rows) {
        if (
          item.pause === "grace" &&
          item.graceUntil !== undefined &&
          item.graceUntil <= now() &&
          !protectedIds.includes(id)
        ) {
          rows.set(id, { ...item, pause: undefined, graceUntil: undefined });
        }
      }
      return [...rows.values()].map((item) => ({ ...item }));
    },
    sendNow({ id, baseToken }) {
      const current = rows.get(id);
      if (!current)
        return { kind: "error", message: "Sample submission is unavailable.", effect: "none" };
      if (current.editability !== "editable")
        return { kind: "held", current, reason: current.reason };
      if (current.snapshotToken !== baseToken) return { kind: "conflict", current: { ...current } };
      if (current.pause === "editing")
        return {
          kind: "held",
          current: { ...current },
          reason: "Finish editing this sample before sending.",
        };
      const item: WorkQueueItem = {
        ...current,
        statusLabel: "Submitted (mock)",
        editability: "readonly",
        pause: undefined,
        graceUntil: undefined,
        reason: "Submitted in this simulation.",
        submittedAt: new Date(now()).toISOString(),
        snapshotToken: `sample-${id}-${++revision}`,
      };
      rows.set(id, item);
      return { kind: "saved", item: { ...item } };
    },
    async save({ id, baseToken, text }) {
      const current = rows.get(id);
      if (!current)
        return { kind: "error", message: "Sample submission is unavailable.", effect: "none" };
      if (current.editability === "readonly")
        return {
          kind: "held",
          current: { ...current },
          reason: current.reason ?? "Read-only submission.",
        };
      if (baseToken !== current.snapshotToken) return { kind: "conflict", current: { ...current } };
      const item = {
        ...current,
        ...grace(current),
        text,
        snapshotToken: `sample-${id}-${++revision}`,
      };
      rows.set(id, item);
      return { kind: "saved", item: { ...item } };
    },
  };
}
