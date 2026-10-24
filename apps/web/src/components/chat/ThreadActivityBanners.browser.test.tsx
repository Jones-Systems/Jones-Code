import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { expect, it, vi } from "vite-plus/test";
import { render } from "../../../test/render";
import { makeThreadFixture } from "../../test-fixtures";
import { childInputBannerItem } from "./ThreadActivityBanners";

it("opens each remote child's owning environment from its title and question or approval action", async () => {
  const question = makeThreadFixture({
    environmentId: EnvironmentId.make("remote-question-environment"),
    id: ThreadId.make("question-child"),
    title: "Question on remote A",
    hasPendingUserInput: true,
    hasPendingApprovals: false,
  });
  const approval = makeThreadFixture({
    environmentId: EnvironmentId.make("remote-approval-environment"),
    id: ThreadId.make("approval-child"),
    title: "Approval on remote B",
    hasPendingUserInput: false,
    hasPendingApprovals: true,
  });
  const openThread = vi.fn();
  const banner = (children: (typeof question)[]) => {
    const item = childInputBannerItem({
      childThreadInputs: children,
      parentAwaitingUser: false,
      onOpenThread: openThread,
    });
    return (
      <>
        {item?.description}
        {item?.actions}
      </>
    );
  };
  await using screen = await render(banner([question, approval]), { router: false });

  await screen.getByRole("button", { name: "Question on remote A", exact: true }).click();
  await screen.getByRole("button", { name: "Approval on remote B", exact: true }).click();
  await screen.getByRole("button", { name: "Open question", exact: true }).click();
  await screen.rerender(banner([approval, question]));
  await screen.getByRole("button", { name: "Open approval", exact: true }).click();

  expect(openThread.mock.calls).toEqual([
    [{ environmentId: "remote-question-environment", threadId: "question-child" }],
    [{ environmentId: "remote-approval-environment", threadId: "approval-child" }],
    [{ environmentId: "remote-question-environment", threadId: "question-child" }],
    [{ environmentId: "remote-approval-environment", threadId: "approval-child" }],
  ]);
});
