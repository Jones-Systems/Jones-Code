export const LAPTOP_RECEIVER = "8750edf7-486b-41ac-81ce-cd50dfeb71eb";

export interface RequestsSearch {
  receiver?: string | undefined;
  request?: string | undefined;
  binding?: string | null | undefined;
  access?: string | undefined;
}

export function requestLink(receiver: string, request?: string, binding?: string) {
  return { to: "/requests" as const, search: { receiver, request, binding } };
}

export const threadLink = (environmentId: string, threadId: string) => ({
  to: "/$environmentId/$threadId" as const,
  params: { environmentId, threadId },
});
