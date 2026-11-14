// The managed route and checked-in pack config share their mutation root.
// Extra config/CLI overrides belong to the explicitly unmanaged vp pack route.
export const serverBundlePackProfile = { outDir: "dist", clean: true } as const;

export function assertManagedServerBundle(
  args: readonly string[],
  packMode: string | undefined,
): void {
  if (args.length !== 0)
    throw new Error("Managed build:bundle accepts no arguments; direct vp pack is unmanaged.");
  if (packMode)
    throw new Error(
      "Executable pack modes require build:exe; managed build:bundle uses the default profile.",
    );
}
