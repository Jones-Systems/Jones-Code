import { describe, expect, it, vi } from "vite-plus/test";
import type { WorkstreamCommand, WorkstreamReceipt } from "@t3tools/contracts";
import { reconcileMobileWorkstreamCommand } from "./commands";
const command: WorkstreamCommand = {
  command_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  expected_server_generation: 7,
  expected_registry_version: 11,
  action: {
    operation: "create_workstream",
    name: "Example",
    lifecycle: "active",
    progress: { state: "unknown" },
    sort_order: 0,
  },
};
const pending: WorkstreamReceipt = {
  command_id: command.command_id,
  owner_id: "owner",
  actor: { principal_id: "principal" },
  operation: "create_workstream",
  request_sha256: "a".repeat(64),
  server_generation: 7,
  accepted_at: "2026-09-30T12:00:00Z",
  state: "pending",
  retry_after_seconds: 1,
};
const committed: WorkstreamReceipt = {
  ...pending,
  state: "committed",
  completed_at: "2026-09-30T12:00:01Z",
  registry_version: 12,
  changed: true,
  effects: {
    workstream_versions: [{ workstream_id: "created", version: 1 }],
    native_reference_id: null,
    membership_ids: [],
    declaration_id: null,
    declaration_revision: null,
    edge_id: null,
    observation: null,
    registration: null,
    lifecycle_declaration: null,
    coordination_disposition: null,
    native_settlement: null,
  },
};
describe("mobile command reconciliation", () => {
  it("returns the verified committed receipt and stops polling", async () => {
    const poll = vi.fn(async (_commandId: string) => committed);
    expect(
      await reconcileMobileWorkstreamCommand({
        command,
        ownerId: "owner",
        principalId: "principal",
        submit: async () => pending,
        poll,
        wait: async () => {},
        assertCurrent: () => {},
      }),
    ).toEqual(committed);
    expect(poll).toHaveBeenCalledTimes(1);
  });
  it("polls the original command after a lost submit response without resubmission", async () => {
    const submit = vi.fn(async () => {
      throw new Error("lost response");
    });
    const poll = vi.fn(async (_commandId: string) => pending);
    const wait = vi.fn(async () => {});
    const result = await reconcileMobileWorkstreamCommand({
      command,
      ownerId: "owner",
      principalId: "principal",
      submit,
      poll,
      wait,
      assertCurrent: () => {},
    });
    expect(result.state).toBe("pending");
    expect(submit).toHaveBeenCalledTimes(1);
    expect(poll).toHaveBeenCalledTimes(9);
    expect(poll.mock.calls.every(([id]) => id === command.command_id)).toBe(true);
  });
  it("stops before polling when the connection changes during submission", async () => {
    let changed = false;
    const poll = vi.fn(async (_commandId: string) => pending);
    await expect(
      reconcileMobileWorkstreamCommand({
        command,
        ownerId: "owner",
        principalId: "principal",
        submit: async () => {
          changed = true;
          throw new Error("disconnected");
        },
        poll,
        wait: async () => {},
        assertCurrent: () => {
          if (changed) throw new Error("binding changed");
        },
      }),
    ).rejects.toThrow("binding changed");
    expect(poll).not.toHaveBeenCalled();
  });
  it("rejects another principal's receipt", async () => {
    const poll = vi.fn(async (_commandId: string) => pending);
    await expect(
      reconcileMobileWorkstreamCommand({
        command,
        ownerId: "owner",
        principalId: "another",
        submit: async () => pending,
        poll,
        wait: async () => {},
        assertCurrent: () => {},
      }),
    ).rejects.toThrow("receipt binding");
    expect(poll).not.toHaveBeenCalled();
  });
  it("resumes the known command through GET only", async () => {
    const submit = vi.fn(async () => pending);
    const poll = vi.fn(async (_commandId: string) => pending);
    await reconcileMobileWorkstreamCommand({
      command,
      ownerId: "owner",
      principalId: "principal",
      resume: true,
      submit,
      poll,
      wait: async () => {},
      assertCurrent: () => {},
    });
    expect(submit).not.toHaveBeenCalled();
  });
});
