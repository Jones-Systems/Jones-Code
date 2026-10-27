import "../../index.css";
import { expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";
import { EnvironmentId, type RemoteAdmissionRecord } from "@t3tools/contracts";
import { act, render } from "../../../test/render";
import { TooltipProvider } from "../../components/ui/tooltip";
import { RequestCard } from "./RequestCard";
import { RequestMarkdown } from "./RequestMarkdown";
import { RequestReviewRow } from "./RequestReviewRow";
import { admissionPresentation, admissionTarget } from "./presentation";
import { admissionFixture, contextAdmissionFixture } from "./testFixtures";

const selectors = vi.hoisted(() => ({ project: vi.fn(), thread: vi.fn() }));
vi.mock("../../state/entities", () => ({
  useProject: selectors.project,
  useThreadShell: selectors.thread,
}));
vi.mock("../../hooks/useSettings", () => ({
  useClientSettings: (select: (settings: { timestampFormat: string }) => unknown) =>
    select({ timestampFormat: "12-hour" }),
}));

function card(record = contextAdmissionFixture(), blocked: string | null = null) {
  const target = admissionTarget(record);
  return (
    <TooltipProvider>
      <RequestCard
        record={record}
        view={admissionPresentation(record)}
        target={{ ...target, projectName: target.projectId, threadName: target.threadId }}
        timestampFormat="12-hour"
        nowMs={Date.parse("2026-10-24T12:00:00Z")}
        policyRevision="synthetic-policy"
        blocked={blocked}
        onDecide={() => undefined}
      />
    </TooltipProvider>
  );
}

function withContext(context: Record<string, unknown>) {
  const record = contextAdmissionFixture();
  return {
    ...record,
    envelope: {
      ...record.envelope,
      payload: {
        ...(record.envelope.payload as Record<string, unknown>),
        advisory: {
          schemaVersion: "t3.remote-admission-advisory/v1",
          requestContext: { schemaVersion: "t3.linked-request-context/v1", ...context },
        },
      },
    },
  } as unknown as RemoteAdmissionRecord;
}

it("orders the agreed slots, isolates sender labels, and keeps paths in the disclosure", async () => {
  const record = contextAdmissionFixture();
  await using screen = await render(card(record));
  const summary = screen.container.querySelector("summary")!;
  expect(
    [...summary.querySelectorAll("[data-request-slot]")].map((node) =>
      node.getAttribute("data-request-slot"),
    ),
  ).toEqual(["source-time", "breadcrumb", "operation", "title", "target", "preview"]);
  expect(summary.textContent).toContain("From Authenticated laptop");
  expect(summary.textContent).toContain("Reported by sender:");
  expect(
    [...summary.querySelectorAll('[data-request-slot="breadcrumb"] bdi')].map(
      (node) => node.textContent,
    ),
  ).toEqual(["Sender project", "Sender parent", "Sender thread"]);
  expect(summary.querySelector("time")?.getAttribute("datetime")).toBe("2026-10-24T11:59:00Z");
  expect(summary.textContent).toContain("Sent");
  expect(summary.textContent).not.toContain("/synthetic/");
  for (const element of summary.querySelectorAll("*")) {
    expect(
      ["span", "bdi", "time"].includes(element.localName) ||
        element.namespaceURI === "http://www.w3.org/2000/svg",
    ).toBe(true);
  }
  await act(() => summary.click());
  expect(screen.container.textContent).toContain("Sent (sender's clock)");
  expect(screen.container.textContent).toContain("Received (this environment)");
  expect(screen.container.textContent).toContain("/synthetic/worktree");
  const json = [...screen.container.querySelectorAll("pre")].find((node) =>
    node.textContent?.includes('"bindingDigest"'),
  )!;
  expect(JSON.parse(json.textContent!)).toEqual({
    id: record.id,
    bindingDigest: record.bindingDigest,
    requestDigest: record.requestDigest,
    envelope: record.envelope,
  });
});

it.each([undefined, "bad", "2026-02-30T00:00:00Z"])(
  "uses Received for sender time %s",
  async (sentAt) => {
    const record = withContext(sentAt === undefined ? {} : { sentAt });
    await using screen = await render(card(record));
    const summary = screen.container.querySelector("summary")!;
    expect(summary.textContent).toContain("Received");
    expect(summary.querySelector("time")?.dateTime).toBe(record.createdAt);
    await act(() => summary.click());
    expect(screen.container.textContent).toContain(
      sentAt === undefined ? "Not reported" : `Invalid value ${sentAt}`,
    );
  },
);

it.each([
  { kind: "monogram", text: "SP", color: "blue" },
  { kind: "emoji", emoji: "🏡" },
  { kind: "lucide", name: "folder-code", color: "green" },
  { kind: "future-kind" },
  { kind: "emoji", emoji: 42 },
  null,
  undefined,
])("renders local sender glyph %j without losing context or making requests", async (icon) => {
  const record = withContext({
    sentAt: "2026-10-24T11:59:00Z",
    project: { projectId: "p", title: "Sender project", ...(icon === undefined ? {} : { icon }) },
  });
  await using screen = await render(card(record));
  const breadcrumb = screen.container.querySelector('[data-request-slot="breadcrumb"]')!;
  expect(breadcrumb.textContent).toContain("Sender project");
  expect(breadcrumb.querySelector('[aria-hidden="true"]')).not.toBeNull();
  expect(screen.container.querySelector("time")?.dateTime).toBe("2026-10-24T11:59:00Z");
  expect(screen.container.querySelector("img,a,button,input")).toBeNull();
});

it("skips absent breadcrumb and title slots, retaining legacy context only for a bound origin", async () => {
  await using screen = await render(card(admissionFixture()));
  expect(screen.container.querySelector('[data-request-slot="breadcrumb"]')).toBeNull();
  await act(() => screen.container.querySelector("summary")!.click());
  expect(screen.container.textContent).toContain("The sender did not report thread context.");
  const record = admissionFixture();
  await screen.rerender(card({ ...record, envelope: { ...record.envelope, payload: {} } }));
  expect(screen.container.querySelector('[data-request-slot="title"]')).toBeNull();
  const legacy = {
    ...record,
    envelope: {
      ...record.envelope,
      payload: {
        invocation: {
          client: {
            sessionId: "legacy-session",
            remoteOrigin: {
              linkSessionId: "legacy-session",
              thread: { threadId: "legacy-thread", title: "Legacy thread title" },
            },
          },
        },
      },
    },
  };
  await screen.rerender(card(legacy));
  expect(screen.container.querySelector('[data-request-slot="breadcrumb"]')?.textContent).toContain(
    "Legacy thread title",
  );
  expect(screen.container.querySelector("summary")?.textContent).toContain("Received");
  await screen.rerender(
    card({
      ...legacy,
      envelope: {
        ...legacy.envelope,
        payload: {
          invocation: {
            client: {
              sessionId: "other-session",
              remoteOrigin: {
                linkSessionId: "legacy-session",
                thread: { threadId: "legacy-thread", title: "Legacy thread title" },
              },
            },
          },
        },
      },
    }),
  );
  expect(screen.container.querySelector('[data-request-slot="breadcrumb"]')).toBeNull();
});

it("renders body syntax as inert text and structure, including links, images, tasks and shell fences", async () => {
  const markdown = `# Heading

[Example](https://example.test/path) <https://example.test/>

![remote diagram](https://example.test/image.png)

- [ ] pending
- [x] complete

| Column | Value |
| --- | --- |
| One | Two |

<script>unsafe()</script>
<iframe src="https://example.test/frame"></iframe>
<button>Run</button>

\`\`\`jones-shell environment=untrusted
echo synthetic
\`\`\`
`;
  await using screen = await render(<RequestMarkdown>{markdown}</RequestMarkdown>);
  expect(
    screen.container.querySelector("a,img,input,script,iframe,button,h1,h2,h3,h4,h5,h6"),
  ).toBeNull();
  expect(screen.container.textContent).toContain("Example (https://example.test/path)");
  expect(screen.container.textContent).toContain(
    "[image: remote diagram] (https://example.test/image.png)",
  );
  expect(screen.container.textContent).toContain("☐Not done");
  expect(screen.container.textContent).toContain("☑Done");
  expect(screen.container.textContent).toContain("<script>unsafe()</script>");
  expect(screen.container.textContent).toContain(
    '<iframe src="https://example.test/frame"></iframe>',
  );
  expect(screen.container.querySelector("pre code")?.textContent).toContain("echo synthetic");
  expect(screen.container.querySelector("ul li")).not.toBeNull();
  expect(screen.container.querySelector("table td")?.textContent).toBe("One");
  const autolink = [...screen.container.querySelectorAll("p")].find((node) =>
    node.textContent?.startsWith("Example"),
  )!;
  expect(autolink.textContent).toBe("Example (https://example.test/path) https://example.test/");
});

it("resolves receiving-environment IDs, follows live renames and falls back to IDs without deletion claims", async () => {
  selectors.project.mockReset().mockReturnValue({ title: "Live receiver project" });
  selectors.thread.mockReset().mockReturnValue({ title: "Live receiver thread" });
  const record = contextAdmissionFixture();
  const environmentId = EnvironmentId.make("receiving-environment");
  const row = (current = record) => (
    <TooltipProvider>
      <RequestReviewRow
        environmentId={environmentId}
        record={current}
        policyRevision="synthetic-policy"
        blocked={null}
        onDecide={() => undefined}
      />
    </TooltipProvider>
  );
  await using screen = await render(row());
  expect(selectors.project).toHaveBeenCalledWith({ environmentId, projectId: "synthetic-project" });
  expect(selectors.thread).toHaveBeenCalledWith({ environmentId, threadId: "receiver-thread" });
  expect(screen.container.querySelector('[data-request-slot="target"]')?.textContent).toContain(
    "Live receiver project / Live receiver thread",
  );
  selectors.project.mockReturnValue({ title: "Renamed project" });
  selectors.thread.mockReturnValue({ title: "Renamed thread" });
  await screen.rerender(row());
  expect(screen.container.querySelector('[data-request-slot="target"]')?.textContent).toContain(
    "Renamed project / Renamed thread",
  );
  selectors.project.mockReturnValue(null);
  selectors.thread.mockReturnValue(null);
  await screen.rerender(row());
  const target = screen.container.querySelector('[data-request-slot="target"]')!;
  expect(target.textContent).toContain("synthetic-project / receiver-thread");
  expect(target.textContent).not.toContain("deleted");
  expect(target.textContent).not.toContain("not found");
  expect(
    selectors.project.mock.calls.every(
      ([ref]) => ref.environmentId === environmentId && ref.projectId === "synthetic-project",
    ),
  ).toBe(true);
  expect(
    selectors.thread.mock.calls.every(
      ([ref]) => ref.environmentId === environmentId && ref.threadId === "receiver-thread",
    ),
  ).toBe(true);
  await screen.rerender(
    row({
      ...record,
      envelope: {
        ...record.envelope,
        payload: {
          ...(record.envelope.payload as Record<string, unknown>),
          target: {
            projects: [{ id: "synthetic-project", deletedAt: "2026-10-24T00:00:00Z" }],
            threads: [{ id: "receiver-thread", missing: true }],
          },
        },
      },
    } as RemoteAdmissionRecord),
  );
  expect(screen.container.querySelector('[data-request-slot="target"]')?.textContent).toContain(
    "synthetic-project (deleted when requested) / receiver-thread (not found when requested)",
  );
  const noTarget = {
    ...record,
    envelope: {
      ...record.envelope,
      target: { ...record.envelope.target, projectId: null, threadId: null },
      payload: {},
    },
  };
  await screen.rerender(
    <RequestReviewRow
      environmentId={environmentId}
      record={noTarget}
      policyRevision="synthetic-policy"
      blocked={null}
      onDecide={() => undefined}
    />,
  );
  expect(selectors.project).toHaveBeenLastCalledWith(null);
  expect(selectors.thread).toHaveBeenLastCalledWith(null);
});

it.each([320, 900, 1440])(
  "keeps time and decisions inside %spx with 200%% text and increased spacing",
  async (width) => {
    await page.viewport(width, 1000);
    const rootStyle = document.documentElement.style.cssText;
    const style = document.createElement("style");
    style.textContent =
      "[data-request-id], [data-request-id] * { line-height: 1.5 !important; letter-spacing: .12em !important; word-spacing: .16em !important; } [data-request-id] p { margin-bottom: 2em !important; }";
    document.head.append(style);
    document.documentElement.style.fontSize = "200%";
    try {
      const record = withContext({
        sentAt: "2026-10-24T11:59:00Z",
        project: { projectId: "p".repeat(256), title: "Long project ".repeat(9) },
        thread: { threadId: "t".repeat(256), title: "Long thread ".repeat(9) },
      });
      const view = admissionPresentation(record);
      const container = document.createElement("div");
      container.style.width = "100%";
      container.style.maxWidth = `${width}px`;
      await using screen = await render(
        card({
          ...record,
          envelope: {
            ...record.envelope,
            payload: {
              ...(record.envelope.payload as Record<string, unknown>),
              params: {
                title: "Long title ".repeat(20),
                message: `${view.message}\n${"unbroken".repeat(150)}`,
              },
            },
          },
        } as RemoteAdmissionRecord),
        { container },
      );
      const details = screen.container.querySelector<HTMLElement>("[data-request-id]")!;
      const summary = details.querySelector("summary")!;
      const bounds = details.getBoundingClientRect();
      const clock = summary.querySelector("time")!.getBoundingClientRect();
      expect(getComputedStyle(summary).display).toBe("block");
      expect(details.scrollWidth).toBeLessThanOrEqual(details.clientWidth + 1);
      expect(clock.left).toBeGreaterThanOrEqual(bounds.left);
      expect(clock.right).toBeLessThanOrEqual(bounds.right + 1);
      const source = summary
        .querySelector('[data-request-slot="source-time"] > span')!
        .getBoundingClientRect();
      expect(source.width).toBeGreaterThan(0);
      expect(source.right).toBeLessThanOrEqual(clock.left);
      await act(() => summary.click());
      expect(details.scrollWidth).toBeLessThanOrEqual(details.clientWidth + 1);
      for (const label of ["Approve", "Deny"]) {
        const button = [...details.querySelectorAll("button")].find(
          (node) => node.textContent === label,
        )!;
        expect(button.getBoundingClientRect().width).toBeGreaterThan(0);
        expect(button.getBoundingClientRect().right).toBeLessThanOrEqual(bounds.right + 1);
        button.focus();
        expect(document.activeElement).toBe(button);
      }
    } finally {
      document.documentElement.style.cssText = rootStyle;
      style.remove();
      await page.viewport(1280, 900);
    }
  },
);
