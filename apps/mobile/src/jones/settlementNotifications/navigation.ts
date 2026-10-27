export function encodeThreadDeepLink(input: {
  readonly environmentId: string;
  readonly threadId: string;
}): string {
  return `/threads/${encodeURIComponent(input.environmentId)}/${encodeURIComponent(input.threadId)}`;
}
