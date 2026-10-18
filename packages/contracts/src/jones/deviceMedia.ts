import * as Schema from "effect/Schema";
import { TrimmedNonEmptyString } from "../baseSchemas.ts";

export const DesktopDeviceMediaTunnelInputSchema = Schema.Struct({
  target: Schema.String.check(Schema.isPattern(/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,254}$/)),
  gatewayPort: Schema.Int.check(Schema.isBetween({ minimum: 1024, maximum: 65535 })),
  owner: TrimmedNonEmptyString.check(Schema.isMaxLength(256)),
  generation: TrimmedNonEmptyString.check(Schema.isMaxLength(256)),
});
export type DesktopDeviceMediaTunnelInput = typeof DesktopDeviceMediaTunnelInputSchema.Type;

export const DesktopDeviceMediaTunnelSchema = Schema.Struct({
  id: Schema.String,
  httpBase: Schema.String,
});
export type DesktopDeviceMediaTunnel = typeof DesktopDeviceMediaTunnelSchema.Type;

/** An opaque grant for one device. expiresAt is epoch milliseconds. */
export const DeviceDirectAccess = Schema.Struct({
  target: TrimmedNonEmptyString.check(Schema.isPattern(/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,254}$/u)),
  gatewayPort: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 65535 })),
  owner: TrimmedNonEmptyString,
  generation: TrimmedNonEmptyString,
  grant: TrimmedNonEmptyString,
  expiresAt: Schema.Number,
});
export type DeviceDirectAccess = typeof DeviceDirectAccess.Type;
