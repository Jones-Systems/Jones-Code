// @effect-diagnostics nodeBuiltinImport:off
// This native journal is outside the application profile restored by an update rollback.
import * as Fs from "node:fs/promises";
import * as Path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { constants } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import * as Schema from "effect/Schema";
import {
  FleetDesktopState, FleetDesktopRequest,
  type FleetDesktopCampaign, type FleetPrepareCampaignInput, type FleetUpdateMemberInput,
} from "@t3tools/contracts/jones/fleet-updates";

export interface FleetInstallBinding {
  readonly campaignId: string;
  readonly stagedHandle: string;
  readonly targetSource: string;
  readonly transactionId: string;
  readonly fromGeneration: string;
}
/** Only the updater's validated journal/active-install reconciliation supplies this value. */
export interface FleetNativeOutcome {
  readonly transactionId: string;
  readonly status: "committed" | "rolled-back";
  readonly activeGeneration: string;
  readonly activeSource: string;
}
export interface DesktopFleetStore {
  readonly request: (request: FleetDesktopRequest) => Promise<FleetDesktopState>;
  readonly bindInstall: (binding: FleetInstallBinding) => Promise<void>;
  readonly recordOutcome: (proof: FleetNativeOutcome) => Promise<void>;
}
const validState = Schema.is(FleetDesktopState);
const validRequest = Schema.is(FleetDesktopRequest);
const inFlight = new Set(["dispatching", "install-blocked", "reconciling", "pending"]);
const occupied = new Set([...inFlight, "retiring"]);
const terminal = new Set(["current", "committed", "rolled-back", "blocked", "superseded"]);

function prepare(state: FleetDesktopState, input: FleetPrepareCampaignInput): FleetDesktopState {
  const existing = state.campaigns.find((campaign) => campaign.campaignId === input.campaignId);
  if (existing !== undefined) {
    if (existing.targetSource !== input.targetSource || existing.desktopStagedHandle !== input.desktopStagedHandle) throw new Error("Fleet campaign identity changed.");
    return state;
  }
  if (state.campaigns.some((campaign) => campaign.phase === "installing")) throw new Error("A desktop installation still requires reconciliation.");
  const campaign: FleetDesktopCampaign = {
    ...input, phase: "prepared",
    members: state.enrollments.filter((entry) => entry.enabled).map((enrollment) => ({
      enrollment, operationId: randomUUID(), phase: "waiting",
    })),
  };
  return { ...state, campaigns: [...state.campaigns.map((previous) => ({
    ...previous, members: previous.members.map((member) => terminal.has(member.phase) || occupied.has(member.phase)
      ? member : { ...member, phase: member.expectedInstalledSource === undefined ? "superseded" as const : "retiring" as const }),
  })), campaign] };
}

function updateMember(state: FleetDesktopState, input: FleetUpdateMemberInput): FleetDesktopState {
  const campaign = state.campaigns.find((candidate) => candidate.campaignId === input.campaignId);
  const member = campaign?.members.find((candidate) => candidate.operationId === input.operationId);
  if (campaign === undefined || member === undefined) throw new Error("Unknown fleet campaign member.");
  if (member.phase !== input.expectedPhase) throw new Error("Fleet member changed; refresh before continuing.");
  if (terminal.has(member.phase) && input.phase !== member.phase) throw new Error("A completed fleet operation cannot be reopened.");
  if (member.phase === "reconciling" && ["dispatching", "install-blocked"].includes(input.phase)) throw new Error("Uncertain native acceptance must be observed without resubmission.");
  if (inFlight.has(member.phase) && !inFlight.has(input.phase) && !terminal.has(input.phase)) throw new Error("An accepted operation must be reconciled before its state can change.");
  if ((["dispatching", "install-blocked", "reconciling", "pending"].includes(input.phase)) &&
      (campaign.phase !== "committed" || campaign.committedGeneration === undefined)) {
    throw new Error("The laptop update has not committed; remote activation is forbidden.");
  }
  if (input.phase === "staging" || input.phase === "dispatching") {
    const enrolled = state.enrollments.some((entry) => entry.enabled && entry.environmentId === member.enrollment.environmentId && entry.enrollmentId === member.enrollment.enrollmentId);
    if (!enrolled) throw new Error("Fleet enrollment was disabled or replaced.");
    if (state.campaigns.some((other) => other.members.some((entry) => entry.operationId !== member.operationId && entry.enrollment.environmentId === member.enrollment.environmentId && occupied.has(entry.phase)))) {
      throw new Error("Another operation on this host still requires reconciliation.");
    }
  }
  if (member.expectedInstalledSource !== undefined && input.expectedInstalledSource !== undefined &&
      member.expectedInstalledSource !== input.expectedInstalledSource) throw new Error("Fleet operation installed-source binding changed.");
  const { reason: _reason, ...previous } = member;
  const next = {
    ...previous, phase: input.phase,
    ...(input.expectedInstalledSource === undefined ? {} : { expectedInstalledSource: input.expectedInstalledSource }),
    ...(input.reason === undefined ? {} : { reason: input.reason }),
  };
  return { ...state, campaigns: state.campaigns.map((entry) => entry.campaignId !== campaign.campaignId ? entry : {
    ...entry, members: entry.members.map((entryMember) => entryMember.operationId === member.operationId ? next : entryMember),
  }) };
}

export function createDesktopFleetStore(options: { readonly home: string; readonly profile: string | undefined }): DesktopFleetStore {
  const runtime = Path.join(options.home, "runtime");
  const profileKey = createHash("sha256").update(options.profile ?? "").digest("hex");
  const directory = Path.join(runtime, "jones-fleet", `desktop-${profileKey}`);
  const destination = Path.join(directory, "campaigns.json");
  let queue: Promise<unknown> = Promise.resolve();
  const transact = <T>(change: (state: FleetDesktopState) => { state: FleetDesktopState; result: T }): Promise<T> => {
    const run = async () => {
      for (const path of [runtime, Path.dirname(directory), directory]) {
        await Fs.mkdir(path, { recursive: true, mode: 0o700 });
        const stat = await Fs.lstat(path);
        if (!stat.isDirectory() || stat.isSymbolicLink() || (stat.mode & 0o022) !== 0 ||
            (typeof process.getuid === "function" && stat.uid !== process.getuid())) throw new Error("Fleet storage ownership is unknown.");
        const parent = await Fs.open(Path.dirname(path), "r");
        try { await parent.sync(); } finally { await parent.close(); }
      }
      const lockPath = Path.join(directory, "lock.sqlite");
      await Fs.open(lockPath, "ax", 0o600).then((file) => file.close()).catch((error: NodeJS.ErrnoException) => {
        if (error.code !== "EEXIST") throw error;
      });
      const lockStat = await Fs.lstat(lockPath);
      if (!lockStat.isFile() || lockStat.isSymbolicLink() || lockStat.nlink !== 1 || (lockStat.mode & 0o077) !== 0 ||
          (typeof process.getuid === "function" && lockStat.uid !== process.getuid())) throw new Error("Fleet storage lock ownership is unknown.");
      const lock = new DatabaseSync(lockPath);
      try {
        lock.exec("PRAGMA busy_timeout=0; BEGIN EXCLUSIVE");
        const file = await Fs.open(destination, constants.O_RDONLY | constants.O_NOFOLLOW).catch((error: NodeJS.ErrnoException) => {
          if (error.code === "ENOENT") return null;
          throw error;
        });
        let state: FleetDesktopState = { schema: 1, enrollments: [], campaigns: [] };
        if (file !== null) {
          try {
            const stat = await file.stat();
            if (!stat.isFile() || stat.nlink !== 1 || stat.size > 8 * 1024 * 1024 || (stat.mode & 0o077) !== 0 ||
                (typeof process.getuid === "function" && stat.uid !== process.getuid())) throw new Error("Fleet campaign file ownership or size is invalid.");
            const saved: unknown = JSON.parse(await file.readFile("utf8"));
            if (!validState(saved)) throw new Error("Fleet campaign file is invalid; its evidence was retained.");
            state = saved;
          } finally { await file.close(); }
        }
        const next = change(state);
        if (!validState(next.state)) throw new Error("Invalid fleet campaign state.");
        if (next.state !== state) {
          const encoded = `${JSON.stringify(next.state)}\n`;
          if (Buffer.byteLength(encoded) > 8 * 1024 * 1024) throw new Error("Fleet campaign history reached its retained storage limit.");
          const temporary = Path.join(directory, `${randomUUID()}.tmp`);
          const output = await Fs.open(temporary, "wx", 0o600);
          try {
            try { await output.writeFile(encoded); await output.sync(); }
            finally { await output.close(); }
            await Fs.rename(temporary, destination);
            const parent = await Fs.open(directory, "r");
            try { await parent.sync(); } finally { await parent.close(); }
          } finally {
            await Fs.unlink(temporary).catch((error: NodeJS.ErrnoException) => { if (error.code !== "ENOENT") throw error; });
          }
        }
        return next.result;
      } finally { lock.close(); }
    };
    const result = queue.then(run);
    queue = result.catch(() => undefined);
    return result;
  };
  return {
    request: (request) => transact((state) => {
      if (!validRequest(request)) throw new Error("Invalid fleet request.");
      let next = state;
      switch (request.action) {
        case "read": break;
        case "enroll": next = { ...state, enrollments: [
          ...state.enrollments.filter((entry) => entry.environmentId !== request.enrollment.environmentId), request.enrollment,
        ] }; break;
        case "prepare": next = prepare(state, request.input); break;
        case "updateMember": next = updateMember(state, request.input); break;
      }
      return { state: next, result: next };
    }),
    bindInstall: (binding) => transact((state) => {
      const campaign = state.campaigns.find((entry) => entry.campaignId === binding.campaignId);
      if (campaign === undefined || campaign.desktopStagedHandle !== binding.stagedHandle ||
          campaign.targetSource !== binding.targetSource || binding.transactionId !== binding.stagedHandle ||
          (campaign.phase !== "prepared" && campaign.phase !== "installing")) throw new Error("The selected desktop update does not match its fleet campaign.");
      if (campaign.installation !== undefined && (campaign.installation.transactionId !== binding.transactionId ||
          campaign.installation.fromGeneration !== binding.fromGeneration)) throw new Error("Fleet desktop installation binding changed.");
      return { state: { ...state, campaigns: state.campaigns.map((entry) => entry.campaignId !== campaign.campaignId ? entry : {
        ...entry, phase: "installing", installation: { transactionId: binding.transactionId, fromGeneration: binding.fromGeneration },
      }) }, result: undefined };
    }),
    recordOutcome: (proof) => transact((state) => ({
      state: { ...state, campaigns: state.campaigns.map((campaign) => {
        if (campaign.installation?.transactionId !== proof.transactionId || campaign.phase !== "installing") return campaign;
        if (proof.status === "committed" && proof.activeSource === campaign.targetSource &&
            proof.activeGeneration === proof.transactionId && proof.activeGeneration !== campaign.installation.fromGeneration) {
          return { ...campaign, phase: "committed", committedGeneration: proof.activeGeneration };
        }
        if (proof.status === "rolled-back" && proof.activeGeneration === campaign.installation.fromGeneration) return { ...campaign, phase: "rolled-back" };
        return { ...campaign, phase: "blocked" };
      }) }, result: undefined,
    })),
  };
}
