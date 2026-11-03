import type { PreparedConnection } from "../../connection/model.ts";
import { RemoteEnvironmentAuthorization } from "../../authorization/service.ts";
import { ManagedRelayDpopSigner } from "../../relay/managedRelay.ts";
import {
  NativeReadError,
  NativeWorkstreamReader,
  validateNativeReadScope,
  type NativeReadScope,
  type NativeReadSha256,
  type NativeReadRoute,
  type NativeCompleteRead,
} from "../../state/workstreams/nativeRead.ts";
import type { WorkstreamsHomeReadResultRequest } from "@t3tools/contracts/jones/workstreamsHomeReadResult";
import {
  fetchEnvironmentNativeWorkstreamRead,
  fetchEnvironmentNativeWorkstreamExport,
  type EnvironmentNativeCompletedExport,
} from "./nativeHttp.ts";
import * as Effect from "effect/Effect";
import type { HttpClient } from "effect/http";

export type NativeWorkstreamsRequestError = Effect.Error<
  | ReturnType<typeof fetchEnvironmentNativeWorkstreamRead>
  | ReturnType<typeof fetchEnvironmentNativeWorkstreamExport>
>;

export type NativeWorkstreamsEffectRunner = <A, E extends NativeWorkstreamsRequestError>(
  effect: Effect.Effect<A, E, HttpClient.HttpClient>,
  signal: AbortSignal,
) => Promise<A>;

export interface EnvironmentNativeWorkstreamsLifetimeOptions {
  readonly prepared: PreparedConnection;
  readonly scope: NativeReadScope;
  readonly sha256: NativeReadSha256;
  readonly run: NativeWorkstreamsEffectRunner;
  /** Checks the application's original connection, session, generation and binding. */
  readonly isCurrent: () => boolean;
  readonly signal?: AbortSignal;
}

/** One application lifetime. Disposal purges memory and aborts every owned read and export. */
export class EnvironmentNativeWorkstreamsLifetime {
  readonly #options: EnvironmentNativeWorkstreamsLifetimeOptions;
  readonly #scope: NativeReadScope;
  readonly #pending = new Set<AbortController>();
  readonly #reader: NativeWorkstreamReader;
  readonly #onParentAbort = () => this.dispose();
  #disposed = false;

  constructor(options: EnvironmentNativeWorkstreamsLifetimeOptions) {
    this.#options = options;
    this.#scope = validateNativeReadScope(options.scope);
    this.#reader = new NativeWorkstreamReader({
      scope: this.#scope,
      sha256: options.sha256,
      read: (request, signal) =>
        this.#guard(
          (owned) =>
            options.run(
              this.#authorized().pipe(
                Effect.flatMap((input) =>
                  fetchEnvironmentNativeWorkstreamRead({ ...input, request }),
                ),
              ),
              owned,
            ),
          signal,
        ),
    });
    options.signal?.addEventListener("abort", this.#onParentAbort, { once: true });
    if (options.signal?.aborted) this.dispose();
  }

  #authorized() {
    const prepared = this.#options.prepared;
    const scope = this.#scope;
    const sha256 = this.#options.sha256;
    return Effect.gen(function* () {
      const signer = yield* Effect.serviceOption(ManagedRelayDpopSigner);
      const remoteAuthorization = yield* Effect.serviceOption(RemoteEnvironmentAuthorization);
      return { prepared, scope, sha256, signer, remoteAuthorization };
    });
  }

  #check(): void {
    if (this.#disposed || !this.#options.isCurrent()) {
      this.dispose();
      throw new NativeReadError(
        "scope_changed",
        "Refresh the native Workstreams connection and access.",
      );
    }
  }

  async #guard<A>(use: (signal: AbortSignal) => Promise<A>, signal?: AbortSignal): Promise<A> {
    this.#check();
    if (signal?.aborted)
      throw new NativeReadError("cancelled", "The native Workstreams request was cancelled.");
    const owned = new AbortController();
    const abort = () => owned.abort();
    this.#pending.add(owned);
    signal?.addEventListener("abort", abort, { once: true });
    try {
      const result = await use(owned.signal);
      this.#check();
      if (owned.signal.aborted)
        throw new NativeReadError("cancelled", "The native Workstreams request was cancelled.");
      return result;
    } catch (cause) {
      this.#check();
      if (owned.signal.aborted)
        throw new NativeReadError("cancelled", "The native Workstreams request was cancelled.");
      throw cause;
    } finally {
      this.#pending.delete(owned);
      signal?.removeEventListener("abort", abort);
    }
  }

  read(request: WorkstreamsHomeReadResultRequest, signal?: AbortSignal) {
    return this.#guard((owned) => this.#reader.read(request, owned), signal);
  }

  complete<R extends NativeReadRoute>(
    request: WorkstreamsHomeReadResultRequest & { readonly request: { readonly route: R } },
    signal?: AbortSignal,
  ): Promise<NativeCompleteRead<R>> {
    return this.#guard((owned) => this.#reader.complete<R>(request, owned), signal);
  }

  export(signal?: AbortSignal, pageSize?: number): Promise<EnvironmentNativeCompletedExport> {
    return this.#guard(
      (owned) =>
        this.#options.run(
          this.#authorized().pipe(
            Effect.flatMap((input) =>
              fetchEnvironmentNativeWorkstreamExport({
                ...input,
                ...(pageSize === undefined ? {} : { pageSize }),
              }),
            ),
          ),
          owned,
        ),
      signal,
    );
  }

  dispose(): void {
    if (this.#disposed) return;
    this.#disposed = true;
    this.#options.signal?.removeEventListener("abort", this.#onParentAbort);
    for (const pending of this.#pending) pending.abort();
    this.#pending.clear();
    this.#reader.invalidate();
  }
}
