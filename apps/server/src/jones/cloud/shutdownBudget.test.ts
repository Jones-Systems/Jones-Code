import { expect, it } from "@effect/vitest";
import {
  PROVIDER_SCOPE_CLOSE_TIMEOUT_MS,
  SERVER_CHILD_SHUTDOWN_GRACE_MS,
  SERVICE_MANAGER_STOP_TIMEOUT_SECONDS,
} from "./shutdownBudget.ts";

it("keeps both provider close windows inside launcher and service-manager shutdown budgets", () => {
  expect(PROVIDER_SCOPE_CLOSE_TIMEOUT_MS).toBe(30_000);
  expect(SERVER_CHILD_SHUTDOWN_GRACE_MS).toBe(75_000);
  expect(SERVER_CHILD_SHUTDOWN_GRACE_MS).toBeGreaterThan(2 * PROVIDER_SCOPE_CLOSE_TIMEOUT_MS);
  expect(SERVICE_MANAGER_STOP_TIMEOUT_SECONDS * 1000).toBeGreaterThan(
    SERVER_CHILD_SHUTDOWN_GRACE_MS,
  );
});
