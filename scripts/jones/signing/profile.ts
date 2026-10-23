import Plist from "plist";
import { JONES_MACOS_APP_ID, JONES_MACOS_TEAM, nativePasskeysEnabled, productionSigning } from "./policy.ts";

export function requireProductionProfile(
  env: Readonly<Record<string, string | undefined>>,
  profile: string,
  rpDomains: readonly string[],
): void {
  if (!productionSigning(env) || !nativePasskeysEnabled(env)) return;
  const start = profile.indexOf("<?xml");
  const end = profile.indexOf("</plist>", start);
  if (start < 0 || end < 0) throw new Error("Native passkey profile cannot be decoded.");
  const parsed = Plist.parse(profile.slice(start, end + "</plist>".length));
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed) || !("Entitlements" in parsed)) throw new Error("Native passkey profile has no entitlements.");
  const entitlements = parsed.Entitlements;
  if (!entitlements || typeof entitlements !== "object" || Array.isArray(entitlements)) throw new Error("Native passkey profile entitlements are invalid.");
  const fields = entitlements as Record<string, unknown>;
  const associated = fields["com.apple.developer.associated-domains"];
  if (fields["com.apple.application-identifier"] !== `${JONES_MACOS_TEAM}.${JONES_MACOS_APP_ID}` ||
      fields["com.apple.developer.team-identifier"] !== JONES_MACOS_TEAM ||
      !Array.isArray(associated) || !rpDomains.every((domain) => associated.includes("*") || associated.includes(`webcredentials:${domain}`))) {
    throw new Error("Native passkey profile does not authorize the production application and RP domains.");
  }
}
