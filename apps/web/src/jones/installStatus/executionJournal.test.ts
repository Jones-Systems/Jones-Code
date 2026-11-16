// @effect-diagnostics nodeBuiltinImport:off - Deterministic task-owned command digests; no native bridge, fetch or private storage is accessed.
import * as NodeCrypto from "node:crypto";
import { describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import type {
  JonesInstallPlanRequest,
  JonesInstallPlanResult,
  JonesInstallPlanState,
} from "@t3tools/contracts/jones/installPlan";
import {
  createWebJonesJournal,
  nativeJonesDispatch,
  reconcileNativeJonesDuty,
} from "./executionJournal";

const environmentId = EnvironmentId.make("native-journal-laptop");
const targetSource = "a".repeat(40),
  planId = "11111111-1111-4111-8111-111111111111";
const commandId = "22222222-2222-4222-8222-222222222222";
const digest = async (body: string) => NodeCrypto.createHash("sha256").update(body).digest("hex");
const state: JonesInstallPlanState = {
  schema: 1,
  current: null,
  last: null,
  clientCommands: 1,
  laptop: { environmentId, observedAt: "2026-10-27T04:00:00Z", activationBlocked: false },
  view: { complete: false },
};
const request = {
  type: "start",
  origin: "connections",
  target: {
    repository: "Jones-Systems/Jones-Code",
    sourceSha: targetSource,
    sourceTree: "b".repeat(40),
    version: "new",
  },
  select: [],
  laptop: { action: "download" },
} as const;

describe("native handoff duty correlation", () => {
  it("restores a lost accepted handoff and clears only the exact durable terminal proof", async () => {
    let disk: string | null = null;
    const storage = {
      getItem: () => disk,
      setItem: (_key: string, value: string) => {
        disk = value;
      },
    };
    const journal = createWebJonesJournal(storage);
    let sent!: JonesInstallPlanRequest;
    const send = vi.fn(async (input: JonesInstallPlanRequest): Promise<JonesInstallPlanResult> => {
      expect(journal.getSnapshot().duties[0]?.native?.commandId).toBe(commandId);
      sent = input;
      throw new Error("reply lost after reservation");
    });
    const dispatch = nativeJonesDispatch({
      environmentId,
      targetSource,
      action: "download",
      commandId,
      canOperate: () => true,
      journal,
      send,
      digest,
    });
    await expect(dispatch(request, state)).rejects.toThrow("reply lost");
    const restored = createWebJonesJournal(storage);
    await restored.load();
    await restored.reserve({
      id: "44444444-4444-4444-8444-444444444444",
      environmentId: EnvironmentId.make("unrelated"),
      kind: "legacy",
      action: "download",
      targetSource,
    });
    if (!("command" in sent) || sent.command === undefined) throw new Error("No captured command");
    const bodyHash = restored.getSnapshot().duties.find((d) => d.id === commandId)!
      .native!.bodyHash!;
    const proof: JonesInstallPlanResult = {
      state,
      command: {
        command: sent.command,
        bodyHash,
        requestType: "start",
        planId,
        operationId: "33333333-3333-4333-8333-333333333333",
        outcome: "reserved",
      },
    };
    expect(await reconcileNativeJonesDuty(proof, restored)).toBe(false);
    expect(
      await reconcileNativeJonesDuty(
        { ...proof, command: { ...proof.command!, bodyHash: "f".repeat(64), outcome: "terminal" } },
        restored,
      ),
    ).toBe(false);
    expect(
      await reconcileNativeJonesDuty(
        {
          ...proof,
          command: {
            ...proof.command!,
            command: { ...sent.command, environmentId: EnvironmentId.make("wrong") },
            outcome: "terminal",
          },
        },
        restored,
      ),
    ).toBe(false);
    expect(
      await reconcileNativeJonesDuty(
        { ...proof, command: { ...proof.command!, outcome: "terminal" } },
        restored,
      ),
    ).toBe(true);
    expect(restored.getSnapshot().duties.map((d) => d.environmentId)).toEqual(["unrelated"]);
    expect(send).toHaveBeenCalledTimes(1);
  });

  it("refuses old bridges and revoked control authority before any native mutation or duty", async () => {
    const journal = createWebJonesJournal({ getItem: () => null, setItem: () => {} });
    const send = vi.fn();
    const dispatch = nativeJonesDispatch({
      environmentId,
      targetSource,
      action: "download",
      commandId,
      canOperate: () => false,
      journal,
      send,
      digest,
    });
    await expect(dispatch(request, state)).rejects.toThrow("no longer authorizes");
    const { clientCommands: _supported, ...old } = state;
    await expect(
      nativeJonesDispatch({
        environmentId,
        targetSource,
        action: "download",
        commandId,
        canOperate: () => true,
        journal,
        send,
        digest,
      })(request, old),
    ).rejects.toThrow("cannot reconcile");
    await expect(
      nativeJonesDispatch({
        environmentId,
        targetSource,
        action: "download",
        commandId,
        canOperate: () => true,
        journal,
        send,
        digest,
      })(request, {
        ...state,
        readOnly: { code: "invalid", message: "The native journal is held." },
      }),
    ).rejects.toThrow("journal is held");
    expect(send).not.toHaveBeenCalled();
    expect(journal.getSnapshot().duties).toEqual([]);
  });
});
