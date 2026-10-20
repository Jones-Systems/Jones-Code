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

const PREVIEW_BUILD = /^\d+\.\d+\.\d+-preview\.\d{8}\.(\d+)(?:\.(\d+))?$/;
const PLAIN_RELEASE = /^\d+\.\d+\.\d+$/;

/**
 * Short label for the running build, such as `2507.1` for
 * `0.1.0-preview.20261020.39438472507.1`: the last four digits of the workflow run, then the run
 * attempt when one is present. A plain release uses its compact form, and an unknown `0.0.0` or
 * unrecognized version has no label because it would tell builds apart no better than nothing.
 */
export function shortJonesBuildLabel(version: string): string | undefined {
  const bare = version.split("+", 1)[0]!;
  const preview = PREVIEW_BUILD.exec(bare);
  if (preview !== null) {
    const [, run, attempt] = preview;
    return attempt === undefined ? run!.slice(-4) : `${run!.slice(-4)}.${attempt}`;
  }
  if (PLAIN_RELEASE.test(bare) && bare !== "0.0.0") return compactJonesVersionText(bare);
  return undefined;
}
