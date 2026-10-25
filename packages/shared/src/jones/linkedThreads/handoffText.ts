import type { LinkedThreadHandoff, LinkedThreadId } from "@t3tools/contracts/jones/linkedThreads";

const oneLine = (value: string) =>
  value
    .replace(/[\u0000-\u001f\u007f\u2028\u2029]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
const escape = (value: string) =>
  oneLine(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/[\\`*_[\]{}()#+.!|~-]/g, "\\$&");
const code = (value: string) => {
  const fence = "`".repeat(
    Math.max(0, ...Array.from(value.matchAll(/`+/g), (m) => m[0].length)) + 1,
  );
  const pad = /^[` ]|[` ]$/.test(value) ? " " : "";
  return `${fence}${pad}${value}${pad}${fence}`;
};
const field = (label: string, value: string) => `- ${label}: ${value.replace(/\r?\n/g, "\n  ")}`;

/** Portable text carries attribution and a return route without transferring ownership. */
export function renderThreadHandoff({
  handoff: h,
  linkId,
  message,
}: {
  readonly handoff: LinkedThreadHandoff;
  readonly linkId: LinkedThreadId | null;
  readonly message?: string | undefined;
}): string {
  const origin = `${escape(h.origin.title)} — thread ${code(h.origin.threadId)} on ${escape(h.origin.environmentLabel)} (environment ${code(h.origin.environmentId)})`;
  const destination =
    h.destination === null
      ? "not assigned (portable)"
      : h.destination.threadId === null
        ? "this thread"
        : `${escape(h.destination.title ?? "Untitled thread")} — thread ${code(h.destination.threadId)} on ${escape(h.destination.environmentLabel)} (environment ${code(h.destination.environmentId)})`;
  const target = `threadId=${code(h.returnRoute.threadId)}`;
  const route =
    h.returnRoute.kind === "manual_relay"
      ? `give your report to the user to relay to ${escape(h.origin.environmentLabel)}, thread ${code(h.origin.threadId)}`
      : h.returnRoute.kind === "same_environment"
        ? `${code("t3_thread_send")} ${target}`
        : `${code("t3_thread_send")} environmentId=${code(h.returnRoute.environmentId)} ${target}. This needs a link from your environment to ${escape(h.origin.environmentLabel)}; check ${code("t3_environment_links")}. Otherwise give your report to the user to relay.`;
  return [
    "**Thread handoff** (t3.thread-handoff/v1)",
    field("Origin", origin),
    field("Destination", destination),
    field(
      "Ownership",
      `offered, not transferred. Accept with ${code("t3_thread_link")} action="accept" from=${code(JSON.stringify({ environmentId: h.origin.environmentId, threadId: h.origin.threadId }))}${linkId === null ? "" : ` linkId=${code(linkId)}`}.`,
    ),
    field("Return route", route),
    field("Goal", h.goal),
    field("Scope", h.scope),
    field("Status", h.status),
    ...h.sourceRefs.map((source) =>
      field(
        "Sources",
        `${source.kind}: ${source.ref}${source.label === undefined ? "" : ` (${source.label})`}`,
      ),
    ),
    ...(message === undefined ? [] : ["", message]),
  ].join("\n");
}
