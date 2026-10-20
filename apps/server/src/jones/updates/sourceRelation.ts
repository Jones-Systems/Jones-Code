// @effect-diagnostics globalDate:off globalTimers:off - Promise-shared GitHub lookups need a wall-clock backoff and a timer that outlives any one request fiber; both are injectable for tests.
import {
  JonesActionsClient,
  type JonesSourceRelationKind,
} from "@t3tools/shared/jones/jonesActions";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

const REPOSITORY = "Jones-Systems/Jones-Code";
const SHA = /^[a-f0-9]{40}$/;

export interface JonesSourceRelationResult {
  readonly repository: typeof REPOSITORY;
  readonly base: string;
  readonly head: string;
  readonly relation: JonesSourceRelationKind | "unknown";
  readonly observedAt: string;
}

/** Ancestry of a host's installed commit (`base`) against this laptop's (`head`). */
export class JonesSourceRelation extends Context.Service<
  JonesSourceRelation,
  {
    /** Never fails: an unproven relation, including a timeout or missing gh, is "unknown". */
    readonly relation: (pair: {
      readonly base: string;
      readonly head: string;
    }) => Effect.Effect<JonesSourceRelationResult>;
  }
>()("t3/jones/updates/sourceRelation/JonesSourceRelation") {}

interface Observed {
  readonly relation: JonesSourceRelationKind;
  readonly observedAt: string;
}

/**
 * Two exact commits keep one ancestry, so proven relations are cached. Failures only back off
 * briefly; concurrent requests for one pair share one lookup, and distinct pairs are capped so a
 * caller cannot fan out GitHub processes.
 */
export function make(options: {
  readonly compare: (base: string, head: string) => Promise<JonesSourceRelationKind>;
  readonly now?: () => number;
  readonly timeoutMs?: number;
  readonly failureBackoffMs?: number;
  readonly cacheLimit?: number;
  readonly concurrentLimit?: number;
}): JonesSourceRelation["Service"] {
  const now = options.now ?? Date.now;
  const timeoutMs = options.timeoutMs ?? 15_000;
  // Covers the gh transport's own 30 second bound, so a timed-out lookup is not re-spawned early.
  const failureBackoffMs = options.failureBackoffMs ?? 30_000;
  const cacheLimit = options.cacheLimit ?? 256;
  const concurrentLimit = options.concurrentLimit ?? 4;
  const proven = new Map<string, Observed>();
  const backoff = new Map<string, number>();
  const pending = new Map<string, Promise<Observed | undefined>>();

  const remember = <V>(map: Map<string, V>, key: string, value: V) => {
    map.delete(key);
    map.set(key, value);
    if (map.size > cacheLimit) map.delete(map.keys().next().value as string);
  };

  const lookup = (key: string, base: string, head: string): Promise<Observed | undefined> => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<undefined>((resolve) => {
      timer = setTimeout(() => resolve(undefined), timeoutMs);
      timer.unref?.();
    });
    const compared = options
      .compare(base, head)
      .then((relation): Observed => ({
        relation,
        observedAt: DateTime.formatIso(DateTime.makeUnsafe(now())),
      }))
      .catch(() => undefined);
    return Promise.race([compared, timeout]).then((observed) => {
      clearTimeout(timer);
      pending.delete(key);
      if (observed === undefined) remember(backoff, key, now() + failureBackoffMs);
      else {
        backoff.delete(key);
        remember(proven, key, observed);
      }
      return observed;
    });
  };

  const observe = (base: string, head: string): Promise<Observed | undefined> => {
    const key = `${base}:${head}`;
    const cached = proven.get(key);
    if (cached !== undefined) {
      remember(proven, key, cached);
      return Promise.resolve(cached);
    }
    const shared = pending.get(key);
    if (shared !== undefined) return shared;
    const until = backoff.get(key);
    if (until !== undefined) {
      if (until > now()) return Promise.resolve(undefined);
      backoff.delete(key);
    }
    if (pending.size >= concurrentLimit) return Promise.resolve(undefined);
    const started = lookup(key, base, head);
    pending.set(key, started);
    return started;
  };

  return JonesSourceRelation.of({
    relation: ({ base, head }) =>
      Effect.promise(async (): Promise<JonesSourceRelationResult> => {
        const result = (observed: Observed | undefined): JonesSourceRelationResult => ({
          repository: REPOSITORY,
          base,
          head,
          relation: observed?.relation ?? "unknown",
          observedAt: observed?.observedAt ?? DateTime.formatIso(DateTime.makeUnsafe(now())),
        });
        if (!SHA.test(base) || !SHA.test(head)) return result(undefined);
        if (base === head)
          return result({
            relation: "identical",
            observedAt: DateTime.formatIso(DateTime.makeUnsafe(now())),
          });
        return result(await observe(base, head));
      }),
  });
}

export const layer = Layer.sync(JonesSourceRelation, () => {
  // Fixed repository and gh transport; the client spawns nothing until a lookup runs.
  const client = new JonesActionsClient();
  return make({ compare: (base, head) => client.relation(base, head) });
});
