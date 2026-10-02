import type { WorkstreamCommand, WorkstreamReceipt } from "@t3tools/contracts";

export async function reconcileMobileWorkstreamCommand(input: {
  readonly command: WorkstreamCommand;
  readonly ownerId: string;
  readonly principalId: string;
  readonly submit: () => Promise<WorkstreamReceipt>;
  readonly poll: (commandId: string) => Promise<WorkstreamReceipt>;
  readonly assertCurrent: () => void;
  readonly wait: (milliseconds: number) => Promise<void>;
  readonly resume?: boolean;
  readonly pollAttempts?: number;
}): Promise<WorkstreamReceipt> {
  const validate = (receipt: WorkstreamReceipt) => {
    input.assertCurrent();
    if (
      receipt.command_id !== input.command.command_id ||
      receipt.operation !== input.command.action.operation ||
      receipt.server_generation !== input.command.expected_server_generation ||
      receipt.owner_id !== input.ownerId ||
      receipt.actor.principal_id !== input.principalId
    )
      throw new Error("Workstream receipt binding changed.");
    if (
      receipt.state === "committed" &&
      receipt.registry_version < input.command.expected_registry_version
    )
      throw new Error("Workstream receipt revision moved backwards.");
  };
  input.assertCurrent();
  let receipt: WorkstreamReceipt;
  if (input.resume) receipt = await input.poll(input.command.command_id);
  else {
    try {
      receipt = await input.submit();
    } catch {
      input.assertCurrent();
      receipt = await input.poll(input.command.command_id);
    }
  }
  validate(receipt);
  for (
    let attempt = 0;
    (receipt.state === "pending" || receipt.state === "unresolved") &&
    attempt < (input.pollAttempts ?? 8);
    attempt += 1
  ) {
    await input.wait(Math.min(30_000, Math.max(250, receipt.retry_after_seconds * 1_000)));
    input.assertCurrent();
    receipt = await input.poll(input.command.command_id);
    validate(receipt);
  }
  return receipt;
}

export function waitForWorkstreamReceipt(milliseconds: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    signal.throwIfAborted();
    const abort = () => {
      clearTimeout(timer);
      reject(new Error("Workstream connection changed."));
    };
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", abort);
      resolve();
    }, milliseconds);
    signal.addEventListener("abort", abort, { once: true });
  });
}
