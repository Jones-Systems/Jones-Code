import type { AuthSessionState, DesktopCompanionTicketResponse } from "@t3tools/contracts";
import {
  AuthOrchestrationReadScope,
  AuthPreviewOperateScope,
  PREVIEW_COMPANION_WS_PATH,
  sessionGrantsScope,
} from "@t3tools/contracts";
import type { PreparedConnection } from "@t3tools/client-runtime/connection";
import {
  resolveDeviceHubAccess,
  withDeviceHubQuery,
} from "@t3tools/client-runtime/state/deviceHubAccess";
import * as Effect from "effect/Effect";

type TicketResult = DesktopCompanionTicketResponse["result"];

export function companionTicketEligibility(input: {
  readonly registered: boolean;
  readonly enabled: boolean;
  readonly prepared: PreparedConnection | null;
  readonly session: AuthSessionState | null;
}): Exclude<TicketResult, { readonly _tag: "ready" }> | null {
  if (!input.registered || !input.enabled || !input.prepared) return { _tag: "unavailable" };
  const { prepared, session } = input;
  try {
    const url = new URL(prepared.httpBaseUrl);
    if (
      prepared.target._tag === "RelayConnectionTarget" ||
      url.pathname !== "/" ||
      url.search ||
      url.hash ||
      (url.protocol !== "http:" && url.protocol !== "https:")
    )
      return { _tag: "unsupported" };
  } catch {
    return { _tag: "unsupported" };
  }
  if (
    !session ||
    !sessionGrantsScope(session, AuthPreviewOperateScope) ||
    !sessionGrantsScope(session, AuthOrchestrationReadScope) ||
    prepared.httpAuthorization === null
  ) {
    return { _tag: "auth_required" };
  }
  return null;
}

export const resolveCompanionTicket = (prepared: PreparedConnection) =>
  resolveDeviceHubAccess({ prepared, hubBasePath: PREVIEW_COMPANION_WS_PATH }).pipe(
    Effect.map((access): TicketResult =>
      access.credentials || !access.query.wsTicket
        ? { _tag: "auth_required" }
        : { _tag: "ready", url: withDeviceHubQuery(access.wsBase, access) },
    ),
    Effect.catch((cause) =>
      Effect.succeed<TicketResult>({
        _tag:
          typeof cause === "object" &&
          cause !== null &&
          "_tag" in cause &&
          [
            "EnvironmentAuthInvalidError",
            "EnvironmentScopeRequiredError",
            "EnvironmentOperationForbiddenError",
          ].includes(String(cause._tag))
            ? "auth_required"
            : "unavailable",
      }),
    ),
  );
