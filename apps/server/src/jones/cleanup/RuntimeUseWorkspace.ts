// @effect-diagnostics nodeBuiltinImport:off -- Captured bigint directory identity must match the actual canonical physical workspace, outside SQL.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeProcess from "node:process";
import * as Effect from "effect/Effect";
import { canonical, RuntimeUseError, runtimeUseError } from "./RuntimeUseProtocol.ts";

export const captureRuntimeUseWorkspace = Effect.fn("RuntimeUseWorkspace.capture")(function* (
  path: string,
) {
  const observe = Effect.tryPromise({
    try: async () => {
      if (!NodePath.isAbsolute(path) || NodePath.normalize(path) !== path)
        throw new RuntimeUseError({ reason: "runtime_use_workspace_not_canonical" });
      const realPath = await NodeFSP.realpath(path);
      // Alias support needs a complete cross-path writer fence. An alias cannot
      // silently obtain the canonical workspace's cleanup authority.
      if (realPath !== path) throw new RuntimeUseError({ reason: "runtime_use_workspace_alias" });
      const before = await NodeFSP.lstat(realPath, { bigint: true });
      const again = await NodeFSP.realpath(path);
      const after = await NodeFSP.lstat(again, { bigint: true });
      if (
        !before.isDirectory() ||
        before.ino === 0n ||
        realPath !== again ||
        before.dev !== after.dev ||
        before.ino !== after.ino ||
        before.birthtimeNs !== after.birthtimeNs
      )
        throw new RuntimeUseError({ reason: "runtime_use_workspace_changed" });
      return {
        canonicalPath: realPath,
        directoryIdentity: {
          version: 1 as const,
          device: String(before.dev),
          inode: String(before.ino),
          birth: String(before.birthtimeNs),
        },
      };
    },
    catch: runtimeUseError,
  });
  const captured = yield* observe;
  const revalidate = observe.pipe(
    Effect.flatMap((current) =>
      canonical(current) === canonical(captured)
        ? Effect.void
        : Effect.fail(new RuntimeUseError({ reason: "runtime_use_workspace_replaced" })),
    ),
  );
  return { ...captured, revalidate };
});

/** Nullable provider cwd means the native server process default, not a path
 * supplied by its client. Prove that default names the intended physical target. */
export const assertRuntimeUseDefaultWorkspace = Effect.fn("RuntimeUseWorkspace.assertDefault")(
  function* (
    expected: Pick<
      Effect.Success<ReturnType<typeof captureRuntimeUseWorkspace>>,
      "canonicalPath" | "directoryIdentity"
    >,
  ) {
    const current = yield* captureRuntimeUseWorkspace(NodeProcess.cwd());
    if (
      current.canonicalPath !== expected.canonicalPath ||
      canonical(current.directoryIdentity) !== canonical(expected.directoryIdentity)
    )
      return yield* new RuntimeUseError({ reason: "runtime_use_default_workspace_mismatch" });
  },
);
