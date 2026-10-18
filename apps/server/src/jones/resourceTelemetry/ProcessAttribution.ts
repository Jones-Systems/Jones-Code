import type { PhysicalProcessIdentity, ResourceTelemetryProcessOwner } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";

export interface ProcessAttributionRecord {
  readonly pid: number;
  readonly category: "provider-root";
  readonly owner: ResourceTelemetryProcessOwner;
  readonly registeredAtMs: number;
}

interface RegisteredProcessAttribution extends ProcessAttributionRecord {
  readonly token: object;
}

export class ProcessAttribution extends Context.Service<
  ProcessAttribution,
  {
    readonly registerProviderRoot: (input: {
      readonly pid: number;
      readonly threadId: string;
      readonly provider: string;
    }) => Effect.Effect<void, never, Scope.Scope>;
    readonly bindLauncher: (
      client: object,
      identity: PhysicalProcessIdentity | null,
    ) => Effect.Effect<void, never, Scope.Scope>;
    readonly launcherFor: (client: object) => Effect.Effect<PhysicalProcessIdentity | null>;
    readonly snapshot: Effect.Effect<ReadonlyMap<number, ProcessAttributionRecord>>;
  }
>()("t3/jones/resourceTelemetry/ProcessAttribution") {}

export const make = Effect.fn("resourceTelemetry.processAttribution.make")(function* () {
  const launchers = new WeakMap<
    object,
    { identity: PhysicalProcessIdentity | null; token: object }
  >();
  const registrations = yield* Ref.make(new Map<number, RegisteredProcessAttribution>());

  const registerProviderRoot: ProcessAttribution["Service"]["registerProviderRoot"] = (input) =>
    Effect.gen(function* () {
      const registeredAtMs = DateTime.toEpochMillis(yield* DateTime.now);
      const token = {};
      const registration: RegisteredProcessAttribution = {
        pid: input.pid,
        category: "provider-root",
        owner: {
          kind: "provider",
          threadId: input.threadId,
          provider: input.provider,
        },
        registeredAtMs,
        token,
      };
      yield* Ref.update(registrations, (current) => {
        const next = new Map(current);
        next.set(input.pid, registration);
        return next;
      });
      yield* Effect.addFinalizer(() =>
        Ref.update(registrations, (current) => {
          if (current.get(input.pid)?.token !== token) return current;
          const next = new Map(current);
          next.delete(input.pid);
          return next;
        }),
      );
    });

  return ProcessAttribution.of({
    registerProviderRoot,
    bindLauncher: (client, identity) =>
      Effect.gen(function* () {
        const token = {};
        launchers.set(client, { identity, token });
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (launchers.get(client)?.token === token) launchers.delete(client);
          }),
        );
      }),
    launcherFor: (client) => Effect.sync(() => launchers.get(client)?.identity ?? null),
    snapshot: Ref.get(registrations).pipe(
      Effect.map(
        (current) =>
          new Map(
            [...current].map(([pid, { token: _token, ...registration }]) => [pid, registration]),
          ),
      ),
    ),
  });
});

export const layer = Layer.effect(ProcessAttribution, make());
