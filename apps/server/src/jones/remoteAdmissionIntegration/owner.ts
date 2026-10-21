import {
  AuthAccessWriteScope,
  AuthSettingsWriteScope,
  type EnvironmentSessionPrincipalShape,
} from "@t3tools/contracts";

export const isRemoteAdmissionOwner = (principal: EnvironmentSessionPrincipalShape): boolean =>
  principal.method === "browser-session-cookie" &&
  principal.subject !== "mcp-client" &&
  principal.scopes.has(AuthAccessWriteScope) &&
  principal.scopes.has(AuthSettingsWriteScope);
