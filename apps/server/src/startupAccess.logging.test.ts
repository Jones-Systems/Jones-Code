import { expect, it } from "@effect/vitest";

import { formatHeadlessServeOutput } from "./startupAccess.ts";

it("withholds startup credentials from headless output", () => {
  const output = formatHeadlessServeOutput({
    connectionString: "http://192.168.1.42:3773",
    token: "SYNTHETIC_STARTUP_CREDENTIAL",
    pairingUrl: "http://192.168.1.42:3773/pair#token=SYNTHETIC_STARTUP_CREDENTIAL",
  });

  expect(output).not.toContain("SYNTHETIC_STARTUP_CREDENTIAL");
  expect(output).not.toMatch(/Token:|Pairing URL:|#token=|[█▀▄]/);
  expect(output).toContain("Connection string: http://192.168.1.42:3773");
  expect(output).toContain("Startup pairing credential issued");
  expect(output).toContain("t3 pair");
  expect(output).toContain("--base-dir");
});
