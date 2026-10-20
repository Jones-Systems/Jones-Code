import type { RemoteAdmissionRecord } from "@t3tools/contracts";

const object = (value: unknown): Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
const string = (value: unknown) => (typeof value === "string" && value.trim() ? value : null);
export function admissionPresentation(request: RemoteAdmissionRecord) {
  const payload = object(request.envelope.payload);
  const params = object(payload.params);
  const scope = object(payload.invocation);
  const client = object(scope.client);
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
    source: string(client.label) ?? "Connected environment",
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
