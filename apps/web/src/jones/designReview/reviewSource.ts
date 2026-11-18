import { APP_BUILD_STARTED_AT, APP_SOURCE_SHA } from "../../branding";

const DESIGN_REVIEW_REPOSITORY = "Jones-Systems/Jones-Code";
const SOURCE_SHA = /^[a-f0-9]{40}$/;

export interface DesignReviewSource {
  readonly repository: typeof DESIGN_REVIEW_REPOSITORY;
  /** Null when neither the build nor the page names a full commit. */
  readonly sha: string | null;
  readonly builtAt: string | null;
}

/**
 * The build's stamped commit, or the one the packaging shell writes to
 * `<div id="root" data-review-source="…">` when it builds from a confirmed checkout.
 */
export function resolveDesignReviewSource(root: HTMLElement | null): DesignReviewSource {
  const declared = root?.dataset.reviewSource?.trim().toLowerCase();
  const sha = APP_SOURCE_SHA ?? (declared && SOURCE_SHA.test(declared) ? declared : null);
  return {
    repository: DESIGN_REVIEW_REPOSITORY,
    sha,
    builtAt: APP_BUILD_STARTED_AT ?? null,
  };
}

export function formatDesignReviewSource(source: DesignReviewSource): string {
  return source.sha === null
    ? `${source.repository} (source commit not stamped)`
    : `${source.repository}@${source.sha}`;
}
