export interface WordOpenPhase {
  readonly type: "jones-word-open-phase";
  readonly traceId: string;
  readonly phaseId: number;
  readonly mark: string;
  readonly phase: string;
  readonly elapsedMs: number;
  readonly openElapsedMs: number;
}

interface WordOpenTraceTarget {
  readonly now: () => number;
  readonly uuid: () => string;
  readonly mark: (name: string) => void;
  readonly post: (phase: WordOpenPhase) => void;
}

/** Opt-in worker diagnostics carry phase metadata only, never document contents. */
export function createWordOpenTrace(enabled: boolean, target?: WordOpenTraceTarget) {
  if (!enabled) return undefined;
  const sink = target ?? {
    now: () => performance.now(),
    // @effect-diagnostics-next-line cryptoRandomUUID:off -- Diagnostic token only; not a production identity or authority.
    uuid: () => crypto.randomUUID(),
    mark: (name: string) => {
      performance.mark(name);
    },
    post: (phase: WordOpenPhase) => postMessage(phase),
  };
  const startedAt = sink.now();
  const traceId = sink.uuid();
  let phaseId = 0;
  return (phase: string) => {
    const mark = `jones-word:${traceId}:${++phaseId}:${phase}`;
    sink.mark(mark);
    sink.post({
      type: "jones-word-open-phase",
      traceId,
      phaseId,
      mark,
      phase,
      elapsedMs: Math.round(sink.now()),
      openElapsedMs: Math.round(sink.now() - startedAt),
    });
  };
}
