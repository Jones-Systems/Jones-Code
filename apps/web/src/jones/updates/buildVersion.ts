const BUILD_VERSION = /^(\d+\.\d+\.\d+)(?:-preview\.\d{8}\.(\d+)(?:\.\d+)?)?$/;

export interface CompactJonesVersion {
  /** Release core, with a leading `0.` dropped from `0.0.x` releases. */
  readonly core: string;
  /** Last four digits of the preview build's workflow run, which tell nearby builds apart. */
  readonly run?: string;
}

/**
 * Display form of a build version, such as `0.45 · 5620` for
 * `0.0.45-preview.20261016.38974945620.1`. Callers keep the full version reachable;
 * an unrecognized version is returned whole.
 */
export function compactJonesVersion(version: string): CompactJonesVersion {
  const match = BUILD_VERSION.exec(version);
  if (match === null) return { core: version };
  const [, release, run] = match;
  const core = release!.startsWith("0.0.") ? release!.slice(2) : release!;
  return run === undefined ? { core } : { core, run: run.slice(-4) };
}

export function compactJonesVersionText(version: string): string {
  const { core, run } = compactJonesVersion(version);
  return run === undefined ? core : `${core} · ${run}`;
}
