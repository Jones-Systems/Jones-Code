// @effect-diagnostics nodeBuiltinImport:off -- X1 stream transport uses Node authenticated AES-GCM and fatal UTF-8 decoding.
import * as NodeCrypto from "node:crypto";
import {
  canonical,
  classes,
  compareKeys,
  digest,
  refuse,
  tableKeys,
  validateLine,
} from "./canonical.ts";
import type { WorkstreamsSnapshotHeader as Header } from "@t3tools/contracts/jones/workstreamsSnapshot";
export interface SnapshotRow {
  readonly class: string;
  readonly key: readonly (string | number | boolean)[];
  readonly row: Readonly<Record<string, unknown>>;
}
export interface ClassFooter {
  readonly class: string;
  readonly count: number;
  readonly sha256: string;
}
export interface Snapshot {
  readonly header: Header;
  readonly rows: readonly SnapshotRow[];
  readonly classes: readonly ClassFooter[];
  readonly snapshot_sha256: string;
}
export interface SignedPermit {
  readonly key_id: string;
  readonly header_sha256: string;
  readonly signature: string;
}
export function verifyPermit(
  header: Header,
  permit: SignedPermit,
  publicKey: NodeCrypto.KeyObject,
): void {
  if (
    permit.header_sha256 !== digest(canonical(header)) ||
    !NodeCrypto.verify(
      null,
      Buffer.from(canonical(["workstreams-snapshot-export-permit-v1", header])),
      publicKey,
      unbase64(permit.signature),
    )
  )
    refuse("signature_invalid");
}
export function unbase64(value: unknown, length?: number): Buffer {
  if (
    typeof value !== "string" ||
    !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value)
  )
    return refuse("frame_base64");
  const bytes = Buffer.from(value, "base64");
  if (bytes.toString("base64") !== value || (length !== undefined && bytes.length !== length))
    return refuse("frame_base64");
  return bytes;
}
export function encryptSnapshot(plaintext: string, keyId: string, key: Uint8Array): string {
  if (key.length !== 32 || !keyId) return refuse("key_unavailable");
  const bytes = Buffer.from(plaintext),
    frames: string[] = [];
  for (let index = 0, offset = 0; ; index++, offset += 65536) {
    const final = offset >= bytes.length,
      nonce = NodeCrypto.randomBytes(12);
    const cipher = NodeCrypto.createCipheriv("aes-256-gcm", key, nonce);
    cipher.setAAD(
      Buffer.from(canonical(["workstreams-snapshot-encryption-v1", keyId, index, final])),
    );
    const ct = Buffer.concat([
      cipher.update(final ? Buffer.alloc(0) : bytes.subarray(offset, offset + 65536)),
      cipher.final(),
    ]);
    frames.push(
      canonical({
        v: 1,
        alg: "A256GCM",
        key_id: keyId,
        index,
        final,
        nonce: nonce.toString("base64"),
        ct: ct.toString("base64"),
        tag: cipher.getAuthTag().toString("base64"),
      }),
    );
    if (final) return frames.join("\n") + "\n";
  }
}
export function decryptSnapshot(encrypted: string, keyId: string, key: Uint8Array): string {
  if (key.length !== 32) return refuse("key_unavailable");
  if (!encrypted.endsWith("\n")) return refuse("transport_incomplete");
  const plaintext: Buffer[] = [];
  let final = false,
    index = 0;
  for (const line of encrypted.slice(0, -1).split("\n")) {
    const f = JSON.parse(line) as Record<string, unknown>;
    if (
      final ||
      Object.keys(f).sort().join(",") !== "alg,ct,final,index,key_id,nonce,tag,v" ||
      f.v !== 1 ||
      f.alg !== "A256GCM" ||
      f.key_id !== keyId ||
      f.index !== index++ ||
      typeof f.final !== "boolean"
    )
      return refuse("frame_invalid");
    const ct = unbase64(f.ct),
      nonce = unbase64(f.nonce, 12),
      tag = unbase64(f.tag, 16);
    if (ct.length > 65536 || (f.final && ct.length !== 0) || (!f.final && ct.length === 0))
      return refuse("frame_size");
    const cipher = NodeCrypto.createDecipheriv("aes-256-gcm", key, nonce);
    cipher.setAAD(
      Buffer.from(canonical(["workstreams-snapshot-encryption-v1", keyId, f.index, f.final])),
    );
    cipher.setAuthTag(tag);
    plaintext.push(Buffer.concat([cipher.update(ct), cipher.final()]));
    final = f.final;
  }
  if (!final) return refuse("transport_incomplete");
  // ignoreBOM=true preserves U+FEFF as data; BOMs in private payload strings are never stripped.
  return new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(
    Buffer.concat(plaintext),
  );
}
export function encodeSnapshot(header: Header, rows: readonly SnapshotRow[]): string {
  const output = [canonical(header)],
    summaries: string[] = [];
  for (const name of classes) {
    const section = rows
      .filter((r) => r.class === name)
      .toSorted((a, b) => compareKeys(a.key, b.key));
    const bytes = section.map((r) => canonical(r) + "\n").join("");
    const hash = digest(bytes);
    output.push(
      ...section.map(canonical),
      canonical({ class: name, count: section.length, sha256: hash }),
    );
    summaries.push(`${name}:${section.length}:${hash}\n`);
  }
  output.push(
    canonical({
      complete: true,
      snapshot_sha256: digest(canonical(header) + "\n" + summaries.join("")),
    }),
  );
  return output.join("\n") + "\n";
}
export function parseSnapshot(text: string): Snapshot {
  if (!text.endsWith("\n")) return refuse("snapshot_incomplete");
  const lines: unknown[] = text
    .slice(0, -1)
    .split("\n")
    .map((l) => {
      const v: unknown = JSON.parse(l);
      validateLine(v);
      if (canonical(v) !== l) refuse("noncanonical_line");
      return v;
    });
  const header = lines.shift() as Header;
  if (
    !header ||
    header.contract !== "workstreams-snapshot/1.0.0" ||
    header.producer.schema_version !== (header.producer.system === "control-plane" ? 15 : 124) ||
    !header.encryption.key_id
  )
    return refuse("version_unsupported");
  const rows: SnapshotRow[] = [],
    footers: ClassFooter[] = [];
  let index = 0,
    count = 0,
    bytes = "",
    prior: readonly unknown[] | undefined;
  for (const value of lines.slice(0, -1)) {
    const line = value as SnapshotRow | ClassFooter;
    if (line.class !== classes[index]) return refuse("class_order");
    if ("row" in line) {
      if (
        canonical(tableKeys[line.class]!.map((k) => line.row[k])) !== canonical(line.key) ||
        (prior && compareKeys(prior, line.key) >= 0)
      )
        return refuse("key_conflict");
      prior = line.key;
      count++;
      bytes += canonical(line) + "\n";
      rows.push(line);
    } else {
      if (line.count !== count || line.sha256 !== digest(bytes)) return refuse("class_digest");
      footers.push(line);
      index++;
      count = 0;
      bytes = "";
      prior = undefined;
    }
  }
  const trailer = lines.at(-1) as { complete?: boolean; snapshot_sha256?: string };
  const expected = digest(
    canonical(header) + "\n" + footers.map((f) => `${f.class}:${f.count}:${f.sha256}\n`).join(""),
  );
  if (
    index !== classes.length ||
    trailer?.complete !== true ||
    trailer.snapshot_sha256 !== expected
  )
    return refuse("snapshot_digest");
  const owners = new Set(header.owners.map((o) => o.owner_id));
  if (owners.size !== header.owners.length) return refuse("duplicate_owner");
  for (const r of rows)
    if (r.row.owner_id !== undefined && !owners.has(String(r.row.owner_id)))
      return refuse("owner_mismatch");
  const identities = rows.filter((r) => r.class === "workstream_registry_identity");
  if (
    identities.length !== 1 ||
    identities[0]!.row.registry_instance_id !== header.registry.registry_instance_id ||
    identities[0]!.row.canonical_origin !== header.registry.canonical_origin
  )
    return refuse("registry_mismatch");
  const contexts = rows.filter((r) => r.class === "workstream_registry_context");
  if (contexts.length !== owners.size) return refuse("context_mismatch");
  for (const owner of header.owners) {
    const context = contexts.find((r) => r.row.owner_id === owner.owner_id)?.row;
    if (
      context?.server_generation !== owner.server_generation ||
      context.registry_version !== owner.registry_version
    )
      return refuse("context_mismatch");
    const events = rows.filter(
      (r) => r.class === "workstream_audit_events" && r.row.owner_id === owner.owner_id,
    );
    if (
      new Set(events.map((r) => r.row.event_sequence)).size !== events.length ||
      Math.max(0, ...events.map((r) => Number(r.row.event_sequence))) !== owner.audit_high_water
    )
      return refuse("history_conflict");
  }
  if (
    rows.filter((r) => r.class === "workstream_external_reservations" && r.row.state === "active")
      .length !== header.open_effects
  )
    return refuse("open_effect_count");
  const workstreams = new Set(
    rows
      .filter((r) => r.class === "workstreams")
      .map((r) => canonical([r.row.owner_id, r.row.workstream_id])),
  );
  const references = new Set(
    rows
      .filter((r) => r.class === "workstream_native_references")
      .map((r) => canonical([r.row.owner_id, r.row.native_reference_id])),
  );
  for (const { class: name, row: r } of rows) {
    if (
      [
        "workstream_memberships",
        "workstream_declarations",
        "workstream_lifecycle_declarations",
        "workstream_appearance",
        "workstream_rollup_manifests",
        "workstream_rollup_summaries",
        "workstream_rollup_queue",
      ].includes(name) &&
      !workstreams.has(canonical([r.owner_id, r.workstream_id]))
    )
      refuse("missing_workstream");
    if (
      [
        "workstream_memberships",
        "workstream_pr_observations",
        "workstream_external_reservations",
      ].includes(name) &&
      !references.has(canonical([r.owner_id, r.native_reference_id]))
    )
      refuse("missing_reference");
    if (
      name === "workstream_edges" &&
      [r.from_workstream_id, r.to_workstream_id].some(
        (id) => !workstreams.has(canonical([r.owner_id, id])),
      )
    )
      refuse("missing_workstream");
  }
  const primary = new Set<string>(),
    pairs = new Set<string>(),
    edges = new Map<string, Set<string>>(),
    revisions = new Map<string, number[]>();
  for (const { class: name, row: r } of rows) {
    if (name === "workstream_memberships" && r.closed_at === null) {
      const key = canonical([r.owner_id, r.native_reference_id]),
        pair = canonical([r.owner_id, r.workstream_id, r.native_reference_id]);
      if (r.kind === "primary") {
        if (primary.has(key)) refuse("primary_conflict");
        primary.add(key);
      }
      if (pairs.has(pair)) refuse("association_conflict");
      pairs.add(pair);
    }
    if (name === "workstream_edges" && r.closed_at === null) {
      const from = canonical([r.owner_id, r.from_workstream_id]),
        to = canonical([r.owner_id, r.to_workstream_id]);
      const seen = new Set<string>(),
        pending = [to];
      while (pending.length) {
        const x = pending.pop()!;
        if (x === from) refuse("cycle_conflict");
        if (!seen.has(x)) {
          seen.add(x);
          pending.push(...(edges.get(x) ?? []));
        }
      }
      const targets = edges.get(from) ?? new Set<string>();
      targets.add(to);
      edges.set(from, targets);
    }
    const field: Record<string, string> = {
      workstream_owner_grants: "grant_version",
      workstream_source_bindings: "binding_version",
      workstream_declarations: "revision",
      workstream_lifecycle_declarations: "revision",
      workstream_pr_observations: "observation_version",
    };
    if (field[name]) {
      const f = field[name]!,
        group =
          name === "workstream_lifecycle_declarations"
            ? [r.owner_id, r.workstream_id]
            : tableKeys[name]!.filter((k) => k !== f).map((k) => r[k]);
      const k = canonical([name, group]);
      const v = revisions.get(k) ?? [];
      v.push(Number(r[f]));
      revisions.set(k, v);
    }
  }
  for (const v of revisions.values())
    if (v.sort((a, b) => a - b).some((n, i) => n !== i + 1)) refuse("revision_conflict");
  return { header, rows, classes: footers, snapshot_sha256: expected };
}
