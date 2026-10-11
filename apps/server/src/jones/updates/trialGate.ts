// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalTimers:off
// Native detached-helper boundary needs exact process and durable file identity.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodeNet from "node:net";
import * as NodePath from "node:path";
import * as Schema from "effect/Schema";
import type * as NetAddress from "effect/net/NetAddress";

const TrialIdentity = Schema.Struct({
  protocol: Schema.Literal(1),
  startupGateProtocol: Schema.Literal(1),
  transactionId: Schema.NonEmptyString,
  home: Schema.NonEmptyString,
  databasePath: Schema.NonEmptyString,
  profile: Schema.NonEmptyString,
  environmentId: Schema.NonEmptyString,
  version: Schema.NonEmptyString,
  sourceSha: Schema.NonEmptyString,
  sourceTree: Schema.NonEmptyString,
  listener: Schema.NonEmptyString,
});
const TrialDescriptor = Schema.Struct({
  ...TrialIdentity.fields,
  trialReceiptPath: Schema.NonEmptyString,
  commitGrantPath: Schema.NonEmptyString,
});
const CommitGrant = Schema.Struct({
  ...TrialIdentity.fields,
  generation: Schema.NonEmptyString,
});
const NativeActiveInstall = Schema.Struct({
  protocol: Schema.Literal(1),
  owner: Schema.Literal("desktop"),
  generation: Schema.NonEmptyString,
  transactionId: Schema.NonEmptyString,
  home: Schema.NonEmptyString,
  databasePath: Schema.NonEmptyString,
  profile: Schema.NonEmptyString,
  environmentId: Schema.NonEmptyString,
  version: Schema.NonEmptyString,
  sourceSha: Schema.NonEmptyString,
  sourceTree: Schema.NonEmptyString,
});
const TrialReceipt = Schema.Struct({
  ...TrialIdentity.fields,
  resumeHeld: Schema.Literal(true),
  backendProcess: Schema.Struct({ pid: Schema.Number, identity: Schema.NonEmptyString }),
});
const ResumedJournal = Schema.Struct({
  phase: Schema.Literal("resumed"),
  intent: Schema.Struct({ protocol: Schema.Literal(1), transactionId: Schema.NonEmptyString }),
});
const BuildIdentity = Schema.Struct({
  jonesSource: Schema.Struct({
    repository: Schema.Literal("Jones-Systems/Jones-Code"),
    sha: Schema.NonEmptyString,
    tree: Schema.NonEmptyString,
  }),
});
const decodeDescriptor = Schema.decodeUnknownSync(Schema.fromJsonString(TrialDescriptor));
const decodeGrant = Schema.decodeUnknownSync(Schema.fromJsonString(CommitGrant));
const decodeNativeActive = Schema.decodeUnknownSync(Schema.fromJsonString(NativeActiveInstall));
const decodeReceipt = Schema.decodeUnknownSync(Schema.fromJsonString(TrialReceipt));
const decodeResumedJournal = Schema.decodeUnknownSync(Schema.fromJsonString(ResumedJournal));
const decodeBuildIdentity = Schema.decodeUnknownSync(BuildIdentity);
const identityKeys = Object.keys(TrialIdentity.fields) as Array<keyof typeof TrialIdentity.Type>;
const activeIdentityKeys = [
  "protocol",
  "transactionId",
  "home",
  "databasePath",
  "profile",
  "environmentId",
  "version",
  "sourceSha",
  "sourceTree",
] as const;

export class JonesTrialGateError extends Schema.TaggedError<JonesTrialGateError>()(
  "JonesTrialGateError",
  {
    step: Schema.Literals([
      "identity",
      "listener",
      "process",
      "receipt",
      "grant",
      "reservation",
      "cancel",
    ]),
    uncertain: Schema.Boolean,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    if (this.uncertain)
      return "Jones trial resume reservation has an uncertain effect; activation remains held.";
    if (this.step === "cancel") return "Jones trial was cancelled before commit.";
    if (this.step === "identity") return "Jones trial identity does not match this native runtime.";
    if (this.step === "listener")
      return "Jones trial listener does not match a supported native socket.";
    if (this.step === "grant")
      return "Jones commit grant does not match the trial identity or generation.";
    return `Jones trial ${this.step} could not be proved or durably published.`;
  }
}

function cancelled(signal?: AbortSignal): void {
  if (signal?.aborted) {
    throw new JonesTrialGateError({ step: "cancel", uncertain: false, cause: signal.reason });
  }
}

class PublicationFailure {
  readonly published: boolean;
  readonly cause: unknown;

  constructor(published: boolean, cause: unknown) {
    this.published = published;
    this.cause = cause;
  }
}

async function writeDurableExclusive(
  path: string,
  value: unknown,
  signal?: AbortSignal,
): Promise<void> {
  const temp = NodePath.join(
    NodePath.dirname(path),
    `.${NodePath.basename(path)}.${process.pid}.${NodeCrypto.randomUUID()}.pending`,
  );
  let owned = false;
  let published = false;
  try {
    cancelled(signal);
    const file = await NodeFSP.open(temp, "wx", 0o600);
    owned = true;
    try {
      await file.writeFile(JSON.stringify(value) + "\n");
      await file.sync();
    } finally {
      await file.close();
    }
    cancelled(signal);
    await NodeFSP.link(temp, path);
    published = true;
    const parent = await NodeFSP.open(NodePath.dirname(path), "r");
    try {
      await parent.sync();
    } finally {
      await parent.close();
    }
    cancelled(signal);
  } catch (cause) {
    throw new PublicationFailure(published, cause);
  } finally {
    // Only this invocation's exclusive adjacent temporary name may be removed.
    if (owned) {
      try {
        await NodeFSP.unlink(temp);
      } catch (cause) {
        throw new PublicationFailure(published, cause);
      }
    }
  }
}

function assertListener(endpoint: string, observed: NetAddress.SocketAddress): void {
  const url = new URL(endpoint);
  const host = url.hostname.replace(/^\[|\]$/g, "");
  const family = NodeNet.isIP(host);
  const port = Number(url.port || "80");
  if (
    url.protocol !== "http:" ||
    url.username !== "" ||
    url.password !== "" ||
    url.pathname !== "/" ||
    url.search !== "" ||
    url.hash !== "" ||
    !((family === 4 && host.startsWith("127.")) || host === "::1") ||
    observed._tag === "UnixPathAddress" ||
    observed.port !== port
  )
    throw new Error("Unsupported trial listener endpoint or socket.");
  const bound = observed.address.toString();
  const covered =
    observed._tag === "InetAddressV4"
      ? family === 4 && (bound === host || bound === "0.0.0.0")
      : observed.scopeId === 0 && family === 6 && (bound === host || bound === "::");
  // IPv6 wildcard does not prove IPv4 coverage: the socket may be IPv6-only.
  if (!covered) throw new Error("The actual socket does not prove coverage of the trial endpoint.");
}

async function assertAbsent(path: string): Promise<void> {
  try {
    await NodeFSP.lstat(path);
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException).code === "ENOENT") return;
    throw cause;
  }
  throw new Error(`Occupied trial artifact refuses replay: ${path}`);
}

async function proveBackend(signal?: AbortSignal): Promise<{ pid: number; identity: string }> {
  const identity = await new Promise<string>((resolve, reject) =>
    NodeChildProcess.execFile(
      "/bin/ps",
      ["-p", String(process.pid), "-o", "lstart=", "-o", "command="],
      { maxBuffer: 16384, ...(signal === undefined ? {} : { signal }) },
      (error, stdout) => (error === null ? resolve(stdout.trim()) : reject(error)),
    ),
  );
  cancelled(signal);
  if (!identity) throw new Error("Empty backend process proof.");
  return { pid: process.pid, identity };
}

async function isCommittedRestart(
  descriptor: typeof TrialDescriptor.Type,
  descriptorPath: string,
  signal?: AbortSignal,
): Promise<boolean> {
  const manifestPath = NodePath.join(descriptor.home, "runtime", "jones-active-install.json");
  let raw: string;
  try {
    raw = await NodeFSP.readFile(manifestPath, "utf8");
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw cause;
  }
  cancelled(signal);
  const active = decodeNativeActive(raw);
  if (active.generation !== descriptor.transactionId) return false;
  const directory = NodePath.join(
    descriptor.home,
    "runtime",
    "jones-updates",
    "transactions",
    descriptor.transactionId,
  );
  if (
    !/^[a-zA-Z0-9_-]+$/.test(descriptor.transactionId) ||
    descriptorPath !== NodePath.join(directory, "trial-descriptor.json") ||
    descriptor.trialReceiptPath !== NodePath.join(directory, "trial-receipt.json") ||
    descriptor.commitGrantPath !== NodePath.join(directory, "commit-grant.json") ||
    activeIdentityKeys.some((key) => active[key] !== descriptor[key])
  )
    throw new Error("Committed native generation differs from the inherited descriptor.");
  const [grant, journal, receipt, reservation] = await Promise.all([
    NodeFSP.readFile(descriptor.commitGrantPath, "utf8").then(decodeGrant),
    NodeFSP.readFile(NodePath.join(directory, "journal.json"), "utf8").then(decodeResumedJournal),
    NodeFSP.readFile(descriptor.trialReceiptPath, "utf8").then(decodeReceipt),
    NodeFSP.readFile(NodePath.join(directory, "resume-dispatched.json"), "utf8").then(
      decodeReceipt,
    ),
  ]);
  cancelled(signal);
  if (
    grant.generation !== descriptor.transactionId ||
    journal.intent.transactionId !== descriptor.transactionId ||
    identityKeys.some(
      (key) =>
        grant[key] !== descriptor[key] ||
        receipt[key] !== descriptor[key] ||
        reservation[key] !== descriptor[key],
    ) ||
    !Number.isSafeInteger(receipt.backendProcess.pid) ||
    receipt.backendProcess.pid <= 0 ||
    receipt.backendProcess.pid !== reservation.backendProcess.pid ||
    receipt.backendProcess.identity !== reservation.backendProcess.identity
  )
    throw new Error("Completed native trial evidence does not bind the committed generation.");
  if ((await NodeFSP.readFile(manifestPath, "utf8")) !== raw)
    throw new Error("The committed native generation changed during startup inspection.");
  cancelled(signal);
  // A child restart uses ordinary recovery policy. The prior resume reservation
  // is retained as evidence and never replayed or republished by this path.
  return true;
}

/** Holds startup until an exact trial grant or a fully evidenced committed child restart. */
export async function awaitJonesTrialCommit(input: {
  descriptorPath?: string | undefined;
  home: string;
  databasePath: string;
  profile?: string | undefined;
  environmentId: string;
  version: string;
  buildMetadata: unknown;
  observedListener: NetAddress.SocketAddress;
  signal?: AbortSignal;
}): Promise<void> {
  if (input.descriptorPath === undefined) return;
  const signal = input.signal;
  cancelled(signal);
  let descriptor: typeof TrialDescriptor.Type;
  let descriptorPath: string;
  try {
    if (!input.descriptorPath.trim() || input.profile === undefined || !input.profile.trim()) {
      throw new Error("Trial descriptor and inherited desktop profile are required.");
    }
    descriptorPath = await NodeFSP.realpath(input.descriptorPath);
    descriptor = decodeDescriptor(await NodeFSP.readFile(descriptorPath, "utf8"));
    const build = decodeBuildIdentity(input.buildMetadata);
    const [home, databasePath, profile] = await Promise.all([
      NodeFSP.realpath(input.home),
      NodeFSP.realpath(input.databasePath),
      NodeFSP.realpath(input.profile),
    ]);
    cancelled(signal);
    const directory = NodePath.dirname(descriptorPath);
    const reservationPath = NodePath.join(directory, "resume-dispatched.json");
    if (
      descriptor.home !== home ||
      descriptor.databasePath !== databasePath ||
      descriptor.profile !== profile ||
      descriptor.environmentId !== input.environmentId ||
      descriptor.version !== input.version ||
      descriptor.sourceSha !== build.jonesSource.sha ||
      descriptor.sourceTree !== build.jonesSource.tree ||
      !/^[a-f0-9]{40}$/.test(build.jonesSource.sha) ||
      !/^[a-f0-9]{40}$/.test(build.jonesSource.tree) ||
      NodePath.dirname(descriptor.trialReceiptPath) !== directory ||
      NodePath.dirname(descriptor.commitGrantPath) !== directory ||
      !NodePath.isAbsolute(descriptor.trialReceiptPath) ||
      !NodePath.isAbsolute(descriptor.commitGrantPath) ||
      new Set([
        descriptorPath,
        descriptor.trialReceiptPath,
        descriptor.commitGrantPath,
        reservationPath,
      ]).size !== 4
    )
      throw new Error("Descriptor identity or artifact paths differ.");
  } catch (cause) {
    if (Schema.is(JonesTrialGateError)(cause)) throw cause;
    throw new JonesTrialGateError({ step: "identity", uncertain: false, cause });
  }
  try {
    assertListener(descriptor.listener, input.observedListener);
  } catch (cause) {
    throw new JonesTrialGateError({ step: "listener", uncertain: false, cause });
  }
  try {
    if (await isCommittedRestart(descriptor, descriptorPath, signal)) return;
  } catch (cause) {
    if (Schema.is(JonesTrialGateError)(cause)) throw cause;
    throw new JonesTrialGateError({ step: "identity", uncertain: false, cause });
  }
  const reservationPath = NodePath.join(NodePath.dirname(descriptorPath), "resume-dispatched.json");
  const identity = Object.fromEntries(identityKeys.map((key) => [key, descriptor[key]]));
  let backendProcess: { pid: number; identity: string };
  try {
    backendProcess = await proveBackend(signal);
  } catch (cause) {
    if (signal?.aborted) cancelled(signal);
    throw new JonesTrialGateError({ step: "process", uncertain: false, cause });
  }
  const receipt = { ...identity, resumeHeld: true, backendProcess };
  try {
    await assertAbsent(descriptor.trialReceiptPath);
    await assertAbsent(reservationPath);
    cancelled(signal);
    await writeDurableExclusive(descriptor.trialReceiptPath, receipt, signal);
    cancelled(signal);
  } catch (cause) {
    if (signal?.aborted) cancelled(signal);
    throw new JonesTrialGateError({ step: "receipt", uncertain: false, cause });
  }
  await new Promise<void>((resolve, reject) => {
    let watcher: NodeFS.FSWatcher | undefined;
    let inFlight: Promise<void> | undefined;
    let recheck: ReturnType<typeof setTimeout> | undefined;
    let inspectAgain = false;
    let terminal = false;
    let terminalCause: unknown;
    let reservationPublished = false;
    const finish = (cause?: unknown) => {
      if (terminal) {
        if (Schema.is(JonesTrialGateError)(cause) && cause.uncertain) terminalCause = cause;
        return;
      }
      terminal = true;
      terminalCause = cause;
      clearTimeout(recheck);
      watcher?.close();
      signal?.removeEventListener("abort", abort);
      // Drain an inspection before returning: cancellation can overlap durable reservation I/O.
      void (inFlight ?? Promise.resolve()).then(() => {
        if (terminalCause === undefined) resolve();
        else if (reservationPublished)
          reject(
            new JonesTrialGateError({
              step: "reservation",
              uncertain: true,
              cause: terminalCause,
            }),
          );
        else reject(terminalCause);
      });
    };
    const abort = () => {
      clearTimeout(recheck);
      watcher?.close();
      if (inFlight === undefined) {
        finish(
          new JonesTrialGateError({ step: "cancel", uncertain: false, cause: signal?.reason }),
        );
      }
    };
    const inspect = async () => {
      let step: "grant" | "reservation" = "grant";
      try {
        cancelled(signal);
        let raw: string;
        try {
          raw = await NodeFSP.readFile(descriptor.commitGrantPath, "utf8");
        } catch (cause) {
          cancelled(signal);
          if ((cause as NodeJS.ErrnoException).code === "ENOENT") return;
          throw cause;
        }
        cancelled(signal);
        const grant = decodeGrant(raw);
        if (
          identityKeys.some((key) => grant[key] !== descriptor[key]) ||
          grant.generation !== descriptor.transactionId
        ) {
          throw new Error("Commit identity or generation differs.");
        }
        if (terminal) return;
        step = "reservation";
        cancelled(signal);
        // A retained marker is an uncertain dispatch effect, never a reason to retry recovery.
        await writeDurableExclusive(reservationPath, receipt, signal);
        reservationPublished = true;
        cancelled(signal);
        finish();
      } catch (cause) {
        if (Schema.is(JonesTrialGateError)(cause)) finish(cause);
        else if (cause instanceof PublicationFailure) {
          reservationPublished = cause.published;
          finish(new JonesTrialGateError({ step, uncertain: cause.published, cause: cause.cause }));
        } else finish(new JonesTrialGateError({ step, uncertain: false, cause }));
      }
    };
    const startInspect = () => {
      if (terminal) return;
      clearTimeout(recheck);
      recheck = undefined;
      if (inFlight !== undefined) {
        inspectAgain = true;
        return;
      }
      inspectAgain = false;
      inFlight = inspect().finally(() => {
        inFlight = undefined;
        if (signal?.aborted && !terminal) abort();
        else if (inspectAgain && !terminal) startInspect();
        // Native filesystem notifications can be dropped; recheck only the bound grant file.
        else if (!terminal) recheck = setTimeout(startInspect, 250);
      });
    };
    try {
      watcher = NodeFS.watch(NodePath.dirname(descriptorPath), startInspect);
      watcher.on("error", (cause) =>
        finish(new JonesTrialGateError({ step: "grant", uncertain: false, cause })),
      );
      signal?.addEventListener("abort", abort, { once: true });
      if (signal?.aborted) abort();
      else startInspect();
    } catch (cause) {
      finish(new JonesTrialGateError({ step: "grant", uncertain: false, cause }));
    }
  });
}
