import {
  isLinkedRequestSentAt,
  LinkedRequestParent,
  LinkedRequestProject,
  LinkedRequestProjectIcon,
  LinkedRequestThread,
  type RemoteAdmissionRecord,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";

const object = (value: unknown): Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
const string = (value: unknown) => (typeof value === "string" && value.trim() ? value : null);
export const requestDisplayText = (value: string) =>
  value
    .replace(/(?:(?![\u200C\u200D])[\p{Cc}\p{Cf}\p{Cs}\p{Zl}\p{Zp}])+/gu, " ")
    .replace(/\s+/gu, " ")
    .trim();
const display = (value: unknown) => {
  const raw = string(value);
  return raw === null ? null : requestDisplayText(raw) || null;
};
const decodeThread = Schema.decodeUnknownOption(LinkedRequestThread);
const decodeParent = Schema.decodeUnknownOption(LinkedRequestParent);
const decodeProject = Schema.decodeUnknownOption(LinkedRequestProject);
const decodeIcon = Schema.decodeUnknownOption(LinkedRequestProjectIcon);
const operationLabels = new Map([
  ["t3_thread_launch", "Start a task"],
  ["t3_thread_send", "Continue a task"],
  ["schedule_task", "Schedule a task"],
  ["update_scheduled_task", "Update a scheduled task"],
  ["run_scheduled_task_now", "Run a scheduled task now"],
  ["t3_thread_link", "Link threads"],
]);

export function admissionTarget(request: RemoteAdmissionRecord) {
  const snapshot = object(object(request.envelope.payload).target);
  const scheduled = object(snapshot.scheduled);
  const threads = Array.isArray(snapshot.threads) ? snapshot.threads.map(object) : [];
  const projects = Array.isArray(snapshot.projects) ? snapshot.projects.map(object) : [];
  const threadId = request.envelope.target.threadId ?? string(scheduled.threadId);
  const thread = threads.find((entry) => entry.id === threadId);
  const projectId =
    request.envelope.target.projectId ?? string(thread?.projectId) ?? string(scheduled.projectId);
  const project = projects.find((entry) => entry.id === projectId);
  const state = (entry: Record<string, unknown> | undefined) =>
    entry?.missing === true
      ? "not found when requested"
      : entry?.deleted === true || string(entry?.deletedAt) !== null
        ? "deleted when requested"
        : null;
  return {
    threadId,
    projectId,
    threadState: state(thread),
    projectState: state(project),
    location: display(thread?.worktreePath) ?? display(project?.workspaceRoot),
  };
}

export function admissionPresentation(request: RemoteAdmissionRecord) {
  const payload = object(request.envelope.payload);
  const params = object(payload.params);
  const invocationMetadata = new Map(Object.entries(object(payload.invocation)));
  const sourceMetadata = object(invocationMetadata.get("client"));
  const target = object(payload.target);
  const advisory = object(payload.advisory);
  const reported = object(advisory.requestContext);
  const context =
    advisory.schemaVersion === "t3.remote-admission-advisory/v1" &&
    reported.schemaVersion === "t3.linked-request-context/v1"
      ? reported
      : {};
  const origin = object(sourceMetadata.remoteOrigin);
  const originThread = object(origin.thread);
  const boundOrigin =
    typeof sourceMetadata.sessionId === "string" &&
    origin.linkSessionId === sourceMetadata.sessionId;
  const legacyId = boundOrigin ? string(originThread.threadId) : null;
  const reportedThread = Option.getOrNull(decodeThread(context.thread));
  const threadId = reportedThread?.threadId ?? legacyId;
  const threadTitle =
    display(reportedThread?.title) ??
    (threadId !== null && threadId === legacyId ? display(originThread.title) : null);
  const parent = Option.getOrNull(decodeParent(context.parent));
  const projectInput = object(context.project);
  // Decode the glyph separately: one malformed icon must not erase the project.
  const project = Option.getOrNull(
    decodeProject({
      projectId: projectInput.projectId,
      ...(projectInput.title !== undefined ? { title: projectInput.title } : {}),
    }),
  );
  const icon = Option.getOrNull(decodeIcon(projectInput.icon));
  const sentAt = isLinkedRequestSentAt(context.sentAt) ? context.sentAt : null;
  return {
    title: display(params.title),
    operation:
      operationLabels.get(request.envelope.operation) ??
      `Remote request · ${requestDisplayText(request.envelope.operation)}`,
    source: display(sourceMetadata.label) ?? "Connected environment",
    sentAt,
    invalidSentAt:
      sentAt !== null || context.sentAt === undefined
        ? null
        : (display(
            typeof context.sentAt === "string" ? context.sentAt : JSON.stringify(context.sentAt),
          ) ?? "(empty)"),
    thread:
      threadId === null
        ? null
        : {
            threadId,
            title: threadTitle,
            label: threadTitle ?? `Thread ${requestDisplayText(threadId)}`,
          },
    parent:
      parent === null
        ? null
        : {
            ...parent,
            label: display(parent.title) ?? `Thread ${requestDisplayText(parent.threadId)}`,
          },
    project:
      project === null
        ? null
        : {
            ...project,
            icon,
            label: display(project.title) ?? `Project ${requestDisplayText(project.projectId)}`,
          },
    message:
      string(params.message) ??
      string(params.prompt) ??
      string(params.text) ??
      string(object(target.scheduled).prompt) ??
      "Review the requested change before continuing.",
    target: admissionTarget(request),
  };
}

export function admissionCategory(request: RemoteAdmissionRecord) {
  if (request.status !== "held") return "In progress";
  return request.category === "evaluation_unavailable" ||
    request.category === "evaluation_interrupted"
    ? "Evaluator service failure"
    : "Request review";
}

export function admissionStatus(request: RemoteAdmissionRecord) {
  switch (request.status) {
    case "held":
      return admissionCategory(request);
    case "evaluating":
      return "Evaluating";
    case "approved":
      return "Approved · awaiting dispatch";
    case "dispatching":
      return "Dispatch outcome unknown · awaiting confirmation";
    case "dispatched":
      return "Dispatched";
    case "passed":
      return "Evaluation passed · awaiting dispatch";
    case "denied":
      return "Denied";
    case "cancelled":
      return "Cancelled";
  }
}

export function groupAdmissions(records: readonly RemoteAdmissionRecord[]) {
  const groups = new Map<
    string,
    {
      key: string;
      source: string;
      category: string;
      records: RemoteAdmissionRecord[];
    }
  >();
  for (const record of records) {
    const category = admissionCategory(record);
    const source = admissionPresentation(record).source;
    const key = JSON.stringify([
      record.envelope.identity.clientId,
      record.envelope.identity.linkId,
      category,
    ]);
    let group = groups.get(key);
    if (!group) {
      group = { key, source, category, records: [] };
      groups.set(key, group);
    }
    group.records.push(record);
  }
  return [...groups.values()];
}
