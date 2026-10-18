import type { BootServiceIdentity } from "../../cloud/bootService.ts";

export const JONES_BOOT_SERVICE_IDENTITY: BootServiceIdentity = {
  systemdUnitFile: "jones-code.service",
  launchdLabel: "com.jones-systems.jones-code.service",
  description: "Jones Code server",
};
