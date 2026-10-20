// @effect-diagnostics nodeBuiltinImport:off
// Owned scratch for native staging. Extracted Darwin payloads contain `*.asar` files, and
// Electron's patched `fs` treats those as directories, so recursive removal must use the
// raw filesystem that qualified staging already uses for copying and hashing.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { bundleFileSystem } from "./qualifiedRuntime.ts";

export type OwnedScratchRemover = (scratchPath: string) => Promise<void>;

/** Selection is deferred to each removal so tests can inject the Electron/Node choice. */
export const ownedScratchRemover =
  (select: () => { readonly promises: Pick<typeof NodeFSP, "rm"> } = bundleFileSystem) =>
  (scratchPath: string): Promise<void> =>
    select().promises.rm(scratchPath, { recursive: true, force: true });

/** The scratch that cleanup could not remove, left in place for the owner. */
export interface OwnedScratchLeftover {
  readonly path: string;
  readonly cause: unknown;
}

/** Primary failure preserved verbatim; the cleanup failure rides alongside it. */
export class OwnedScratchCleanupError extends AggregateError {
  readonly leftover: OwnedScratchLeftover;
  constructor(primary: unknown, leftover: OwnedScratchLeftover) {
    super(
      [primary, leftover.cause],
      primary instanceof Error ? primary.message : "Scratch work failed.",
      { cause: primary },
    );
    this.name = "OwnedScratchCleanupError";
    this.leftover = leftover;
  }
}

/**
 * Creates one scratch directory under `parent` and removes exactly that directory afterwards.
 *
 * `body` settling successfully means its result is already durable (for staging, after the
 * atomic rename), so a removal failure must not discard it: the result is returned together
 * with the leftover path. A removal failure after `body` failed is attached to the original
 * error instead of replacing it. Earlier leftovers are never swept.
 */
export async function withOwnedScratch<A>(
  parent: string,
  prefix: string,
  body: (scratchPath: string) => Promise<A>,
  remove: OwnedScratchRemover = ownedScratchRemover(),
): Promise<{ readonly value: A; readonly leftover?: OwnedScratchLeftover }> {
  const scratchPath = await NodeFSP.mkdtemp(NodePath.join(parent, prefix));
  let value: A;
  try {
    value = await body(scratchPath);
  } catch (primary) {
    try {
      await remove(scratchPath);
    } catch (cause) {
      throw new OwnedScratchCleanupError(primary, { path: scratchPath, cause });
    }
    throw primary;
  }
  try {
    await remove(scratchPath);
  } catch (cause) {
    return { value, leftover: { path: scratchPath, cause } };
  }
  return { value };
}
