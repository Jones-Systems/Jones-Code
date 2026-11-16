export const LAPTOP_RECEIVER = "8750edf7-486b-41ac-81ce-cd50dfeb71eb";
export function requestHref(receiver: string, id?: string, digest?: string, access?: string) {
  const search = new URLSearchParams({ receiver });
  if (id) search.set("request", id);
  if (digest) search.set("binding", digest);
  if (access) search.set("access", access);
  return `/requests?${search}`;
}
export const threadHref = (receiver: string, thread: string) => `/${encodeURIComponent(receiver)}/${encodeURIComponent(thread)}`;
