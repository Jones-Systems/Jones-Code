// @effect-diagnostics nodeBuiltinImport:off -- Random process-local handles and exact memory accounting; no persistence.
import * as NodeCrypto from "node:crypto";
import { RegistryError } from "../registry/session.ts";

interface Lease<A> {
  readonly snapshot: A;
  readonly partition: string;
  readonly query: string;
  readonly expires: number;
  readonly tokens: Map<string, string>;
  bytes: number;
}
interface Continuation<A> {
  readonly lease: Lease<A>;
  readonly inner: string;
}

/** Service-lifetime storage. Handles confer no authority: callers must freshly authorize
 * and prove the complete content fingerprint before using a retained observation. */
export class CoherentSnapshots<A extends { readonly fingerprint: string }> {
  private readonly leases = new Set<Lease<A>>();
  private readonly handles = new Map<string, Continuation<A>>();
  private bytes = 0;
  private readonly limits;

  constructor(
    limits: {
      readonly entries?: number;
      readonly bytes?: number;
      readonly tokens?: number;
      readonly ttlMs?: number;
    } = {},
  ) {
    this.limits = {
      entries: limits.entries ?? 16,
      bytes: limits.bytes ?? 16 * 1024 * 1024,
      tokens: limits.tokens ?? 1000,
      ttlMs: limits.ttlMs ?? 900_000,
    };
    for (const [name, value] of Object.entries(this.limits)) {
      const maximum = { entries: 16, bytes: 16 * 1024 * 1024, tokens: 1000, ttlMs: 900_000 }[name]!;
      if (!Number.isSafeInteger(value) || value < 1 || value > maximum)
        throw new RegistryError({ code: "invalid_request" });
    }
  }

  private remove(lease: Lease<A>): void {
    for (const token of lease.tokens.values()) this.handles.delete(token);
    this.leases.delete(lease);
    this.bytes -= lease.bytes;
  }

  purge(partition: string): void {
    for (const lease of this.leases) if (lease.partition === partition) this.remove(lease);
  }

  private expire(now: number): void {
    if (!Number.isFinite(now)) throw new RegistryError({ code: "cursor_invalid" });
    for (const lease of this.leases) if (lease.expires <= now) this.remove(lease);
  }

  resume(
    token: string,
    partition: string,
    query: string,
    fingerprint: string,
    now: number,
  ): Continuation<A> {
    this.expire(now);
    const found = /^[A-Za-z0-9_-]{43}$/.test(token) ? this.handles.get(token) : undefined;
    if (!found || found.lease.partition !== partition || found.lease.query !== query)
      throw new RegistryError({ code: "cursor_invalid" });
    if (found.lease.snapshot.fingerprint !== fingerprint) {
      this.purge(partition);
      throw new RegistryError({ code: "cursor_stale" });
    }
    return found;
  }

  /** Retries of the same inner position reuse one handle; final pages retain no new token. */
  next(
    snapshot: A,
    partition: string,
    query: string,
    inner: string | null,
    now: number,
    prior?: Continuation<A>,
  ): string | null {
    this.expire(now);
    let lease = prior?.lease;
    if (
      lease &&
      (!this.leases.has(lease) ||
        lease.partition !== partition ||
        lease.query !== query ||
        lease.snapshot.fingerprint !== snapshot.fingerprint)
    )
      throw new RegistryError({ code: "cursor_invalid" });
    if (inner === null) return null;
    if (inner.length > 2048) throw new RegistryError({ code: "backpressure" });
    const existing = lease?.tokens.get(inner);
    if (existing) return existing;
    const tokenBytes = Buffer.byteLength(inner) + 43;
    const material = lease ? null : JSON.stringify({ snapshot, partition, query });
    const added = tokenBytes + (material === null ? 0 : Buffer.byteLength(material));
    if (added > this.limits.bytes) throw new RegistryError({ code: "backpressure" });
    // Pressure evicts only this authenticated partition. Another caller cannot clear it.
    for (const candidate of this.leases) {
      if (
        this.bytes + added <= this.limits.bytes &&
        this.handles.size < this.limits.tokens &&
        (lease || this.leases.size < this.limits.entries)
      )
        break;
      if (candidate.partition === partition && candidate !== lease) this.remove(candidate);
    }
    if (
      this.bytes + added > this.limits.bytes ||
      this.handles.size >= this.limits.tokens ||
      (!lease && this.leases.size >= this.limits.entries)
    )
      throw new RegistryError({ code: "backpressure" });
    if (!lease) {
      lease = {
        snapshot: structuredClone(snapshot),
        partition,
        query,
        expires: now + this.limits.ttlMs,
        tokens: new Map(),
        bytes: added - tokenBytes,
      };
      this.bytes += lease.bytes;
      this.leases.add(lease);
    }
    let token: string;
    do {
      token = NodeCrypto.randomBytes(32).toString("base64url");
    } while (this.handles.has(token));
    lease.tokens.set(inner, token);
    lease.bytes += tokenBytes;
    this.bytes += tokenBytes;
    this.handles.set(token, { lease, inner });
    return token;
  }
}
