// @effect-diagnostics nodeBuiltinImport:off -- This adapter observes physical directory identity outside all SQL transactions.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { AdmissionError, canonical, ScopeObservation, type PhysicalScope } from "./Protocol.ts";

const isAdmissionError = Schema.is(AdmissionError);
const sameIdentity = (
  a: PhysicalScope["ancestors"][number],
  b: PhysicalScope["ancestors"][number],
) => canonical(a.identity) === canonical(b.identity);
const containsPath = (ancestor: string, child: string) => {
  const relative = NodePath.relative(ancestor, child);
  return (
    relative === "" ||
    (relative !== ".." &&
      !relative.startsWith(`..${NodePath.sep}`) &&
      !NodePath.isAbsolute(relative))
  );
};

/** Missing names conservatively occupy their nearest existing parent. This
 * avoids guessing case folding or Unicode equivalence on the mounted volume. */
export const overlaps = (a: PhysicalScope, b: PhysicalScope): boolean => {
  const left = a.ancestors[0];
  const right = b.ancestors[0];
  return (
    a.ancestors.some((item) => sameIdentity(item, right)) ||
    b.ancestors.some((item) => sameIdentity(item, left)) ||
    containsPath(a.unresolved.length === 0 ? a.canonicalPath : left.path, b.canonicalPath) ||
    containsPath(b.unresolved.length === 0 ? b.canonicalPath : right.path, a.canonicalPath)
  );
};

const isMissing = (cause: unknown) =>
  typeof cause === "object" && cause !== null && "code" in cause && cause.code === "ENOENT";
const observe = async (requestedPath: string): Promise<PhysicalScope> => {
  if (!NodePath.isAbsolute(requestedPath) || NodePath.normalize(requestedPath) !== requestedPath)
    throw new AdmissionError({ reason: "scope_requires_normalized_absolute_path" });
  let cursor = requestedPath;
  const unresolved: string[] = [];
  let nearest: string;
  for (;;) {
    try {
      nearest = await NodeFSP.realpath(cursor);
      break;
    } catch (cause) {
      if (!isMissing(cause)) throw cause;
      // A dangling symlink is not a supported absent directory observation.
      try {
        await NodeFSP.lstat(cursor);
        throw new AdmissionError({ reason: "scope_dangling_or_unresolvable_alias" });
      } catch (entryCause) {
        if (!isMissing(entryCause)) throw entryCause;
      }
      const parent = NodePath.dirname(cursor);
      if (parent === cursor) throw cause;
      unresolved.unshift(NodePath.basename(cursor));
      cursor = parent;
    }
  }
  const ancestors: PhysicalScope["ancestors"][number][] = [];
  cursor = nearest;
  for (;;) {
    const stat = await NodeFSP.stat(cursor, { bigint: true });
    if (!stat.isDirectory()) throw new AdmissionError({ reason: "scope_not_directory" });
    ancestors.push({
      path: cursor,
      identity: {
        version: 1,
        device: String(stat.dev),
        inode: String(stat.ino),
        birth: String(stat.birthtimeNs),
      },
    });
    const parent = NodePath.dirname(cursor);
    if (parent === cursor) break;
    cursor = parent;
  }
  const first = ancestors[0];
  if (first === undefined) throw new AdmissionError({ reason: "scope_ancestor_missing" });
  return {
    requestedPath,
    canonicalPath: NodePath.join(nearest, ...unresolved),
    ancestors: [first, ...ancestors.slice(1)],
    unresolved,
  };
};

const capture = Effect.fn("SharedPhysicalScope.capture")((path: string) =>
  Effect.tryPromise({
    try: () => observe(path),
    catch: (cause) =>
      isAdmissionError(cause)
        ? cause
        : new AdmissionError({ reason: "scope_observation_unavailable", cause }),
  }),
);
/** @public An observation adapter only; never grants participation or exclusion. */
export const layer = Layer.succeed(ScopeObservation, {
  capture,
  revalidate: Effect.fn("SharedPhysicalScope.revalidate")(function* (scope) {
    const observed = yield* capture(scope.requestedPath);
    if (canonical(observed) !== canonical(scope))
      return yield* new AdmissionError({ reason: "scope_observation_changed" });
  }),
});
