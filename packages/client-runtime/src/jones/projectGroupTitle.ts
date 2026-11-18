export function sharedSavedProjectTitle(
  members: ReadonlyArray<{
    readonly title: string;
    readonly hasCustomTitle?: boolean | undefined;
  }>,
): string | null {
  const title = members[0]?.title.trim();
  if (!title || !members.every((member) => member.title.trim() === title)) return null;
  // Older servers did not send provenance. Only an entirely legacy group may
  // treat an unmarked saved name as custom; modern automatic names use upstream fallback.
  return members.some((member) => member.hasCustomTitle === true) ||
    members.every((member) => member.hasCustomTitle === undefined)
    ? title
    : null;
}
