// @effect-diagnostics nodeBuiltinImport:off - compares a private digest in native provider callbacks.
import * as NodeCrypto from "node:crypto";
import type { CommandId, ProviderInstanceId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export interface AccountBinding {
  readonly instanceId: ProviderInstanceId;
  readonly digest: string;
}

export interface AccountLaunchAdmission {
  readonly commandId: CommandId;
  readonly binding: AccountBinding;
}

export const accountIdentityDigest = (family: "codex" | "claudeAgent", identity: string): string =>
  NodeCrypto.createHash("sha256")
    .update(JSON.stringify(["jones-native-account-v1", family, identity]))
    .digest("hex");

export const AccountLaunchIntent = Context.Reference<AccountLaunchAdmission | undefined>(
  "jones/accountRouting/AccountLaunchIntent",
  { defaultValue: () => undefined },
);

export const withAccountLaunchBinding = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  commandId: CommandId,
  binding: AccountBinding | undefined,
) =>
  binding === undefined
    ? effect
    : Effect.provideService(effect, AccountLaunchIntent, { commandId, binding });
