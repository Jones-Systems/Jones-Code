/** Navigate only after the main process has installed the assignment's guest guards. */
export async function registerCompanionGuest(input: {
  readonly ready: Promise<unknown>;
  readonly isCurrent: () => boolean;
  readonly register: () => Promise<unknown>;
  readonly url: () => string | null;
  readonly navigate: (url: string) => void;
}): Promise<boolean> {
  await input.ready;
  if (!input.isCurrent()) return false;
  await input.register();
  if (!input.isCurrent()) return false;
  const url = input.url();
  if (url) input.navigate(url);
  return true;
}
