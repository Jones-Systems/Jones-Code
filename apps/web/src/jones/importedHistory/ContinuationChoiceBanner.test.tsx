import { act, create, type ReactTestRenderer } from "react-test-renderer";
import { expect, it, vi } from "vite-plus/test";
import { CommandId, EnvironmentId, MessageId, ThreadId } from "@t3tools/contracts";
import type { ImportedHistoryCorrelation } from "@t3tools/client-runtime/jones/imported-history/continuation";
import type { ComponentProps } from "react";
const h = vi.hoisted(() => ({ saved: null as ImportedHistoryCorrelation | null, start: vi.fn(), observe: vi.fn(), identity: vi.fn() }));
vi.mock("@t3tools/client-runtime/jones/imported-history/commands", () => ({ createImportedHistoryCommands: () => ({ start: "start", observe: "observe", identity: "identity" }) }));
vi.mock("../../connection/runtime", () => ({ connectionAtomRuntime: {} }));
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: (command: string) => command === "start" ? h.start : command === "observe" ? h.observe : h.identity }));
vi.mock("../../composerDraftStore", () => ({ importedHistoryCorrelationStorage: () => ({ withLock: async <A,>(operation: () => Promise<A>): Promise<A> => operation(), read: () => h.saved, reserve: (value: ImportedHistoryCorrelation) => { h.saved = value; }, remove: () => { h.saved = null; } }) }));
vi.mock("../../components/chat/ComposerBanner", () => {
  const Part = ({ children }: ComponentProps<"div">) => <div>{children}</div>;
  return { ComposerBanner: { Root: Part, Row: Part, Content: Part, Actions: Part } };
});
vi.mock("../../components/ui/button", () => ({ Button: ({ children, onClick, disabled }: ComponentProps<"button">) => <button onClick={onClick} disabled={disabled}>{children}</button> }));
import { ContinuationChoiceBanner } from "./ContinuationChoiceBanner";
it("accepted admission stays pending across remount and offers observation without clearing the draft", async () => {
  h.saved = null;
  h.start.mockReset(); h.observe.mockReset(); h.identity.mockReset();
  h.identity.mockResolvedValue({ _tag: "Success", value: { commandDigest: "a".repeat(64), deliveryDigest: "b".repeat(64) } });
  const accepted = () => ({ _tag: "Success", value: { commandId: h.saved!.command.commandId, threadId: h.saved!.command.threadId, reviewedBasis: h.saved!.command.reviewedBasis, commandDigest: h.saved!.commandDigest, deliveryDigest: h.saved!.deliveryDigest, status: "accepted", runId: null, effectId: null, reason: null } });
  h.start.mockImplementation(async () => accepted());
  h.observe.mockImplementation(async () => accepted());
  const onDismiss = vi.fn();
  const props = { environmentId: EnvironmentId.make("environment:test"), threadId: ThreadId.make("thread:test"), onDismiss,
    prepared: { reviewedBasis: "c".repeat(64), draftIdentity: "draft:test", unchanged: () => true,
      delivery: { type: "message" as const, command: { type: "message.dispatch" as const, commandId: CommandId.make("command:test"), threadId: ThreadId.make("thread:test"), messageId: MessageId.make("message:test"), createdBy: "user" as const, text: "synthetic draft", attachments: [], dispatchMode: { type: "start_immediately" as const } } } },
  };
  let tree!: ReactTestRenderer;
  await act(async () => { tree = create(<ContinuationChoiceBanner {...props} />); });
  await act(async () => { await tree.root.findAllByType("button")[0]!.props.onClick(); });
  expect(h.start).toHaveBeenCalledTimes(1);
  expect(tree.root.findByProps({ role: "status" }).children.join("")).toContain("execution is not confirmed");
  expect(onDismiss).not.toHaveBeenCalled();
  const commandId = h.saved!.command.commandId;
  await act(async () => { tree.unmount(); tree = create(<ContinuationChoiceBanner {...props} prepared={null} />); });
  await act(async () => { await tree.root.findByType("button").props.onClick(); });
  expect(h.start).toHaveBeenCalledTimes(1);
  expect(h.observe).toHaveBeenCalledWith({ environmentId: props.environmentId, input: expect.objectContaining({ commandId }) });
  expect(onDismiss).not.toHaveBeenCalled();
  await act(async () => { tree.unmount(); });
});
