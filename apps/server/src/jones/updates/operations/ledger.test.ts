// @effect-diagnostics nodeBuiltinImport:off -- Every fixture owns its exact temporary home.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import { describe, it, expect } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import type { JonesOperationStartInput } from "@t3tools/contracts/jones/jonesOperations";
import { createOperationLedger } from "./ledger.ts";

const input: JonesOperationStartInput = {
  operationId: "11111111-1111-4111-8111-111111111111",
  environmentId: EnvironmentId.make("isolated-operation-host"), expectedInstalledSource: "a".repeat(40),
  action: { kind: "download-exact", targetSource: "b".repeat(40) },
  correlation: { planId: "22222222-2222-4222-8222-222222222222", deviceKey: "host" },
};
async function home(run: (base: string, ledger: ReturnType<typeof createOperationLedger>) => Promise<void>) {
  const base = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-operation-ledger-"));
  try { await run(base, createOperationLedger(base)); }
  finally { await NodeFSP.rm(base, { recursive: true, force: true }); }
}
describe("durable target operation ledger", () => {
  it("serializes acceptance across instances, survives restart and consumes refused IDs", async () => home(async (base, ledger) => {
    const results = await Promise.all([ledger.accept(input), createOperationLedger(base).accept(input)]);
    expect(results.filter((r) => r.created)).toHaveLength(1);
    const replay = await createOperationLedger(base).accept({ ...input, action: { targetSource: "b".repeat(40), kind: "download-exact" } });
    expect(replay.receipt).toEqual(results[0]!.receipt);
    expect((await ledger.accept({ ...input, expectedInstalledSource: "c".repeat(40) })).receipt.code).toBe("operation-conflict");
    expect((await ledger.read(input.operationId))?.expectedInstalledSource).toBe(input.expectedInstalledSource);
    const refused = { ...input, operationId: NodeCrypto.randomUUID() };
    await ledger.refuse(refused, "busy", "Another operation is active.");
    expect((await ledger.accept(refused)).receipt).toMatchObject({ phase: "refused", code: "busy" });
  }));
  it("fences a late sender durably without inventing acceptance", async () => home(async (base, ledger) => {
    const fenced = await ledger.fence(input);
    expect(fenced).toMatchObject({ ...input, phase: "fenced" });
    expect(fenced.acceptedAt).toBeUndefined();
    expect((await createOperationLedger(base).accept(input)).receipt).toEqual(fenced);
  }));
  it("rejects illegal transitions and changed stable evidence without overwriting the original", async () => home(async (_base, ledger) => {
    await ledger.accept(input);
    const jobId = NodeCrypto.randomUUID();
    await ledger.transition(input.operationId, (r) => ({ ...r, phase: "running", jobId }));
    await expect(ledger.transition(input.operationId, (r) => ({ ...r, jobId: NodeCrypto.randomUUID() }))).rejects.toThrow("Stable");
    await expect(ledger.transition(input.operationId, (r) => ({ ...r, phase: "fenced" }))).rejects.toThrow("Illegal");
    expect((await ledger.read(input.operationId))?.jobId).toBe(jobId);
  }));
  it("preserves corrupt, future-protocol, oversized and linked receipts for reconciliation", async () => home(async (base, ledger) => {
    await ledger.accept(input);
    const file = NodePath.join(base, "runtime", "jones-operations", `${input.operationId}.json`);
    const bytes = await NodeFSP.readFile(file, "utf8");
    for (const content of ["{", JSON.stringify({ ...JSON.parse(bytes), protocol: 2 }), " ".repeat(16 * 1024 + 1)]) {
      await NodeFSP.writeFile(file, content);
      await expect(ledger.accept(input)).rejects.toThrow();
      expect(await NodeFSP.readFile(file, "utf8")).toBe(content);
    }
    await NodeFSP.writeFile(file, bytes);
    await NodeFSP.link(file, `${file}.link`);
    await expect(ledger.read(input.operationId)).rejects.toThrow();
  }));
  it("retains more than 256 young/referenced identities without blocking valid acceptance", async () => home(async (base, ledger) => {
    const ids: string[] = [];
    for (let i = 0; i < 260; i++) {
      const operationId = NodeCrypto.randomUUID(); ids.push(operationId);
      await ledger.refuse({ ...input, operationId }, "busy", "No effect.");
    }
    const restarted = createOperationLedger(base);
    expect((await restarted.accept({ ...input, operationId: ids[0]! })).receipt.phase).toBe("refused");
    expect((await restarted.accept(input)).receipt.phase).toBe("accepted");
    const unresolved = [];
    for await (const receipt of restarted.listUnsettled()) unresolved.push(receipt.operationId);
    expect(unresolved).toEqual([input.operationId]);
  }));
});
