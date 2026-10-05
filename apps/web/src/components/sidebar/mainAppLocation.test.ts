import { describe, expect, it } from "vite-plus/test";

import { isSidebarUtilityPage } from "./mainAppLocation";

describe("sidebar utility locations", () => {
  it("keeps the conversation library out of main-app return history", () => {
    expect(isSidebarUtilityPage("/conversations")).toBe(true);
    expect(isSidebarUtilityPage("/pull-requests")).toBe(true);
    expect(isSidebarUtilityPage("/work-queue")).toBe(true);
    expect(isSidebarUtilityPage("/")).toBe(false);
  });
});
