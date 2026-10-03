interface WorkQueueItemFields {
  id: string;
  snapshotToken: string;
  text: string | null;
  statusLabel: string;
  submittedAt: string | null;
  targetLabel: string | null;
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
  save(input: { id: string; baseToken: string; text: string }): Promise<WorkQueueSaveResult>;
}

export function createMockWorkQueueSource(): WorkQueueSource {
  const descriptions = [
    [
      "pending",
      "Pending",
      "Review the workspace navigation and propose a simpler layout.",
      "editable",
      "",
    ],
    ["conflict", "Pending", "Add keyboard navigation to the project list.", "editable", ""],
    ["held-on-save", "Pending", "Check the release notes before the next update.", "editable", ""],
    [
      "held",
      "Held",
      "Verify the target workspace before delivery.",
      "readonly",
      "Target verification is still required.",
    ],
    ["error", "Pending", "Summarize the latest design decisions.", "editable", ""],
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
  return {
    mode: "mock",
    async load() {
      return [...rows.values()].map((item) => ({ ...item }));
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
      if (id === "conflict" && current.snapshotToken === "sample-conflict-1") {
        const changed = {
          ...current,
          text: "Add keyboard navigation and visible focus to the project list.",
          snapshotToken: "sample-conflict-2",
        };
        rows.set(id, changed);
        return { kind: "conflict", current: { ...changed } };
      }
      if (id === "held-on-save") {
        const held: WorkQueueItem = {
          ...current,
          statusLabel: "Held",
          editability: "readonly",
          reason: "Target verification is still required.",
          snapshotToken: "sample-held-on-save-2",
        };
        rows.set(id, held);
        return { kind: "held", current: { ...held }, reason: held.reason };
      }
      if (id === "error")
        return {
          kind: "error",
          message: "The sample save failed. Your draft is preserved.",
          effect: "none",
        };
      const item = { ...current, text, snapshotToken: `sample-${id}-${++revision}` };
      rows.set(id, item);
      return { kind: "saved", item: { ...item } };
    },
  };
}
