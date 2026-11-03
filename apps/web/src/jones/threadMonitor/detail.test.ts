import { expect, it } from "vite-plus/test";
import { makeThreadProjectionFixture } from "../../test-fixtures";
import { projectMonitorDetail } from "./detail";
import { testEnvironment } from "./fixtures.test-support";

it("directs child attention to the parent where approval controls or the question link can be reached", () => {
  const projection = makeThreadProjectionFixture();
  const detail = projectMonitorDetail({
    selection: {
      publisherGeneration: testEnvironment.publisherGeneration,
      environmentId: testEnvironment.environmentId,
      environmentEpoch: testEnvironment.environmentEpoch,
      threadId: projection.thread.id,
      selectionId: "selection",
    },
    revision: 1,
    projection,
    unavailableReason: null,
    planUnavailableReason: null,
    childAttention: true,
  });
  expect(detail.unavailableReason).toBe(
    "A child thread needs attention. Open this thread to answer its approval or reach its question.",
  );
  expect(detail.threadId).toBe(projection.thread.id);
  expect(detail.request).toBeNull();
});

it("preserves a current connection failure ahead of child-attention guidance", () => {
  const projection = makeThreadProjectionFixture();
  const detail = projectMonitorDetail({
    selection: {
      publisherGeneration: testEnvironment.publisherGeneration,
      environmentId: testEnvironment.environmentId,
      environmentEpoch: testEnvironment.environmentEpoch,
      threadId: projection.thread.id,
      selectionId: "selection",
    },
    revision: 1,
    projection,
    unavailableReason: "Waiting for current detail",
    planUnavailableReason: null,
    childAttention: true,
  });
  expect(detail.unavailableReason).toBe("Waiting for current detail");
});
