// @effect-diagnostics nodeBuiltinImport:off -- Producer-owned durable finalization records; this is not an assessment or retirement ledger.
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import { randomBytes } from "node:crypto";
import { attest, binding, type Attestation, type Binding } from "./OutputAttestation.ts";
import { participantGuard } from "./ParticipantGuard.ts";
import { canonical, exists, inside, openAbsolute, readBounded } from "./Filesystem.ts";

interface Production {
  schema: "workspace-output-obligation/v1";
  id: string;
  producer: string;
  owner: string;
  worktree: string;
  binding: Binding;
  started_at: string;
  status: "active" | Attestation["status"];
  outcome: "unresolved" | "retained" | "disposed";
  proofStatus: "deferred" | "attested" | "unavailable";
  roots: string[];
  sidecars: string[];
  reasons: string[];
  finished_at?: string;
}
export interface ProductionSession {
  record: Production;
  path: string;
}
async function save(session: ProductionSession) {
  if (canonical(await binding(session.record.worktree)) !== canonical(session.record.binding))
    throw new Error("identity_mismatch");
  const directory = await openAbsolute(Path.dirname(session.path), true);
  const temporary = Path.join(
    Path.dirname(session.path),
    "." + session.record.id + "-" + randomBytes(8).toString("hex"),
  );
  try {
    const file = await FS.open(temporary, "wx", 0o600);
    try {
      await file.writeFile(canonical(session.record) + "\n");
      await file.sync();
    } finally {
      await file.close();
    }
    await FS.rename(temporary, session.path);
    await directory.sync();
  } finally {
    await directory.close();
    await FS.rm(temporary, { force: true });
  }
}
export async function beginProduction(
  worktree: string,
  producer: string,
  roots: readonly string[],
): Promise<ProductionSession> {
  const bound = await binding(worktree);
  if (
    !/^[a-z0-9][a-z0-9._-]{0,23}$/.test(producer) ||
    roots.some(
      (root) => !inside(Path.resolve(root), bound.path) || Path.resolve(root) === bound.path,
    )
  )
    throw new Error("invalid_producer");
  const directory = Path.join(bound.git_dir, "workspace-output-obligations");
  const id = producer + "-" + randomBytes(16).toString("hex");
  const session: ProductionSession = {
    path: Path.join(directory, id + ".json"),
    record: {
      schema: "workspace-output-obligation/v1",
      id,
      producer,
      owner: producer,
      worktree: bound.path,
      binding: bound,
      started_at: new Date().toISOString(),
      status: "active",
      outcome: "unresolved",
      proofStatus: "deferred",
      roots: roots.map((root) => Path.resolve(root)),
      sidecars: [],
      reasons: ["producer_active"],
    },
  };
  await participantGuard(bound, async () => {
    await FS.mkdir(directory, { mode: 0o700 }).catch((cause) => {
      if (cause.code !== "EEXIST") throw cause;
    });
    await save(session);
  });
  return session;
}
export async function finishProduction(
  session: ProductionSession,
  status: Attestation["status"],
  extraReasons: readonly string[] = [],
  options: { verifyContent?: boolean } = {},
) {
  if (session.record.status !== "active") throw new Error("producer_already_finalized");
  const sidecars: string[] = [],
    reasons = [...extraReasons];
  let remaining = 0;
  for (const root of session.record.roots) {
    if (!(await exists(root))) continue;
    remaining++;
    if (!options.verifyContent) {
      reasons.push("content_verification_deferred");
      continue;
    }
    try {
      const record = await attest({
        worktree: session.record.worktree,
        root,
        producer: session.record.producer,
        status,
        expected: session.record.binding,
        participantGuard,
      });
      sidecars.push(record.sidecar);
      if (record.roots[0]?.verification !== "content") reasons.push("attestation_unverifiable");
    } catch {
      reasons.push("attestation_unverifiable");
    }
  }
  if (!session.record.roots.length) reasons.push("producer_outputs_unknown");
  session.record = {
    ...session.record,
    status,
    finished_at: new Date().toISOString(),
    sidecars,
    proofStatus:
      options.verifyContent && remaining
        ? reasons.includes("attestation_unverifiable")
          ? "unavailable"
          : "attested"
        : "deferred",
    reasons: [...new Set(reasons)],
    outcome: reasons.some((reason) => reason !== "content_verification_deferred")
      ? "unresolved"
      : remaining
        ? "retained"
        : "disposed",
  };
  await participantGuard(session.record.binding, () => save(session));
  return session.record;
}
export async function outputObligations(worktree: string) {
  const bound = await binding(worktree),
    directory = Path.join(bound.git_dir, "workspace-output-obligations");
  if (!(await exists(directory))) return [];
  const dir = await openAbsolute(directory, true);
  await dir.close();
  const names = await FS.readdir(directory);
  if (names.length > 10000) throw new Error("bound_exceeded");
  const results: Production[] = [];
  for (const name of names) {
    if (!/^[a-z0-9][a-z0-9._-]{0,23}-[a-f0-9]{32}\.json$/.test(name))
      throw new Error("observation_failed");
    const record: Production = JSON.parse(
      (await readBounded(Path.join(directory, name), 65536)).toString(),
    );
    if (
      record.schema !== "workspace-output-obligation/v1" ||
      record.id + ".json" !== name ||
      canonical(record.binding) !== canonical(bound) ||
      !["active", "succeeded", "failed", "cancelled"].includes(record.status)
    )
      throw new Error("identity_mismatch");
    if (record.outcome === "unresolved" || record.status === "active") results.push(record);
  }
  return results;
}
