import { expect, it, vi } from "vite-plus/test";
import { createWordOpenTrace, type WordOpenPhase } from "./wordOpenTrace";

function target() {
  return {
    now: vi.fn(() => 10),
    uuid: vi.fn(() => "open-one"),
    mark: vi.fn<(name: string) => void>(),
    post: vi.fn<(phase: WordOpenPhase) => void>(),
  };
}

it("leaves worker timing, marks, identity and messages untouched when diagnostics are disabled", () => {
  const sink = target();
  expect(createWordOpenTrace(false, sink)).toBeUndefined();
  expect(sink.now).not.toHaveBeenCalled();
  expect(sink.uuid).not.toHaveBeenCalled();
  expect(sink.mark).not.toHaveBeenCalled();
  expect(sink.post).not.toHaveBeenCalled();
});

it("separates open attempts and preserves ordered, metadata-only native mark attribution", () => {
  const sink = target();
  sink.uuid.mockReturnValueOnce("first-open").mockReturnValueOnce("second-open");
  const first = createWordOpenTrace(true, sink)!;
  const second = createWordOpenTrace(true, sink)!;
  first("session-start");
  second("session-start");
  sink.now.mockReturnValue(25);
  first("session-ready");
  expect(sink.mark.mock.calls).toEqual([
    ["jones-word:first-open:1:session-start"],
    ["jones-word:second-open:1:session-start"],
    ["jones-word:first-open:2:session-ready"],
  ]);
  expect(sink.post.mock.calls[2]?.[0]).toEqual({
    type: "jones-word-open-phase",
    traceId: "first-open",
    phaseId: 2,
    phase: "session-ready",
    mark: "jones-word:first-open:2:session-ready",
    elapsedMs: 25,
    openElapsedMs: 15,
  });
});
