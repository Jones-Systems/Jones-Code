export function sharedSavedProjectTitle(
  members: ReadonlyArray<{ readonly title: string }>,
): string | null {
  const title = members[0]?.title.trim();
  return title && members.every((member) => member.title.trim() === title) ? title : null;
}
