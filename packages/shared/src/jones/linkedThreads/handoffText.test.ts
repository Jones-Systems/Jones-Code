import { expect, it } from "vite-plus/test";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import type { LinkedThreadHandoff } from "@t3tools/contracts/jones/linkedThreads";
import { renderThreadHandoff } from "./handoffText.ts";

const handoff: LinkedThreadHandoff = {
  schemaVersion: "t3.thread-handoff/v1",
  origin: {
    environmentId: EnvironmentId.make("env:origin"),
    environmentLabel: "Laptop",
    threadId: ThreadId.make("thread`id"),
    title: "[hostile](url)\n<script>",
  },
  destination: null,
  returnRoute: {
    kind: "linked_environment",
    environmentId: EnvironmentId.make("env:origin"),
    threadId: ThreadId.make("thread`id"),
  },
  goal: "Finish\nsecond line",
  scope: "One task",
  status: "Ready",
  sourceRefs: [{ kind: "file", ref: "src/a.ts", label: "source" }],
  offeredAt: "2026-10-24T12:00:00Z",
};
it("renders portable attribution, literal IDs, escaped titles and an honest return route", () => {
  const text = renderThreadHandoff({ handoff, linkId: null });
  expect(text).toContain("\\[hostile\\]\\(url\\) &lt;script&gt;");
  expect(text).toContain("thread ``thread`id`` on Laptop");
  expect(text).toContain("Destination: not assigned (portable)");
  expect(text).toContain("Ownership: offered, not transferred.");
  expect(text).toContain("environmentId=`env:origin` threadId=``thread`id``");
  expect(text).toContain("Otherwise give your report to the user to relay.");
  expect(text).toContain("- Goal: Finish\n  second line");
  expect(text).not.toContain("t3-thread://");
});
it("uses a same-environment route only when declared", () => {
  expect(
    renderThreadHandoff({
      handoff: { ...handoff, returnRoute: { ...handoff.returnRoute, kind: "same_environment" } },
      linkId: null,
    }),
  ).toContain("Return route: `t3_thread_send` threadId=``thread`id``");
});
