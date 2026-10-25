import type { RemoteAdmissionRecord } from "@t3tools/contracts";

const object = (value: unknown): Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
const string = (value: unknown) => (typeof value === "string" && value.trim() ? value : null);
export function admissionPresentation(request: RemoteAdmissionRecord) {
  const payload = object(request.envelope.payload);
  const params = object(payload.params);
  const invocationMetadata = new Map(Object.entries(object(payload.invocation)));
  const sourceMetadata = object(invocationMetadata.get("client"));
  const target = object(payload.target);
  const projects = Array.isArray(target.projects) ? target.projects.map(object) : [];
  const threads = Array.isArray(target.threads) ? target.threads.map(object) : [];
  return {
    title:
      string(params.title) ??
      (request.envelope.operation === "t3_thread_launch"
        ? "Start a task"
        : request.envelope.operation === "t3_thread_send"
          ? "Continue a task"
          : "Remote request"),
    source: string(sourceMetadata.label) ?? "Connected environment",
    message:
      string(params.message) ??
      string(params.prompt) ??
      string(params.text) ??
      string(object(target.scheduled).prompt) ??
      "Review the requested change before continuing.",
    target:
      string(threads[0]?.worktreePath) ??
      string(projects[0]?.workspaceRoot) ??
      request.envelope.target.threadId ??
      request.envelope.target.projectId ??
      "This environment",
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
