import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import type * as Duration from "effect/Duration";
import * as Tracer from "effect/Tracer";
import * as Statement from "effect/sql/Statement";

export type Query = { text: string; parameters?: ReadonlyArray<unknown> };
export type Measurement = { operation: string; milliseconds: number; statements: Query[] };

export function measure<A, E, R>(
  operation: string,
  effect: Effect.Effect<A, E, R>,
  measurements: Measurement[],
): Effect.Effect<A, E, R> {
  return Effect.suspend(() => {
    const statements: Query[] = [];
    const bindings = new WeakMap<Tracer.Span, Query>();
    const capture: Statement.Transformer = (statement, _sql, _fiber, span) => {
      const [text, parameters] = statement.compile();
      bindings.set(span, { text, parameters: [...parameters] });
      return Effect.succeed(statement);
    };
    const tracer = Tracer.make({
      span(options) {
        const span = new Tracer.NativeSpan(options);
        const end = span.end.bind(span);
        span.end = (endTime, exit) => {
          end(endTime, exit);
          const text = span.attributes.get("db.query.text");
          if (typeof text === "string") {
            const captured = bindings.get(span);
            statements.push(captured?.text === text ? captured : { text });
          }
        };
        return span;
      },
    });
    const start = performance.now();
    return effect.pipe(
      Effect.provideService(Statement.CurrentTransformer, capture),
      Effect.withTracer(tracer),
      Effect.ensuring(
        Effect.sync(() => {
          measurements.push({ operation, milliseconds: performance.now() - start, statements });
        }),
      ),
    );
  });
}

// Owns each diagnostic phase origin so observations use the same native monotonic clock.
export class NativeTimeline {
  readonly origins = new Map<string, number>();
  mark(phase: string) { this.origins.set(phase, performance.now()); }
  elapsed(phase: string) {
    const origin = this.origins.get(phase);
    if (origin === undefined) throw new Error(`Diagnostic phase ${phase} has not started`);
    return performance.now() - origin;
  }
}

// Only the watchdog uses wall time; the operation retains its controlled stream clock.
export function withNativeWatchdog<A, E, R>(
  operation: Effect.Effect<A, E, R>,
  deadline: Duration.Input,
  failure: () => Error,
): Effect.Effect<A, E, R> {
  return operation.pipe(
    Effect.raceFirst(
      Effect.sleep(deadline).pipe(
        Effect.andThen(() => Effect.die(failure())),
        Effect.provideService(Clock.Clock, Clock.Clock.defaultValue()),
      ),
    ),
  );
}
