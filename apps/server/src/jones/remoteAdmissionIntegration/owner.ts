import {
  AuthAccessWriteScope,
  AuthSettingsWriteScope,
  type EnvironmentSessionPrincipalShape,
} from "@t3tools/contracts";

export const isRemoteAdmissionOwner = (principal: EnvironmentSessionPrincipalShape): boolean =>
  (principal.method === "browser-session-cookie" ||
    (principal.method === "bearer-access-token" && principal.subject === "desktop-bootstrap")) &&
  principal.subject !== "mcp-client" &&
  principal.scopes.has(AuthAccessWriteScope) &&
  principal.scopes.has(AuthSettingsWriteScope);
