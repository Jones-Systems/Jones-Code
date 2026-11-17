import "~/index.css";
import { ProviderDriverKind, ProviderInstanceId, type ServerProvider } from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import type { ComponentProps } from "react";
import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";

import { render } from "../../../test/render";
import { deriveProviderInstanceEntries } from "../../providerInstances";
import { DefaultModelPicker } from "./DefaultModelPicker";

const codex = ProviderInstanceId.make("codex");
const claude = ProviderInstanceId.make("claude");
const model = "gpt-6.2-sol";
const saved = { instanceId: claude, model: "claude-opus-5-5" };
const baseProvider = {
  enabled: true,
  installed: true,
  version: null,
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: "2026-01-01T00:00:00Z",
  slashCommands: [],
  skills: [],
} as const;
const providers: ReadonlyArray<ServerProvider> = [
  {
    ...baseProvider,
    instanceId: claude,
    driver: ProviderDriverKind.make("claudeAgent"),
    models: [],
  },
  {
    ...baseProvider,
    instanceId: codex,
    driver: ProviderDriverKind.make("codex"),
    models: [
      {
        slug: model,
        name: "GPT-6.2-Sol",
        isCustom: false,
        capabilities: createModelCapabilities({
          optionDescriptors: [
            {
              id: "reasoningEffort",
              label: "Reasoning",
              type: "select",
              role: "reasoning",
              currentValue: "medium",
              options: [
                { id: "medium", label: "Medium" },
                { id: "high", label: "High" },
              ],
            },
            {
              id: "serviceTier",
              label: "Speed",
              type: "select",
              currentValue: "standard",
              options: [
                { id: "standard", label: "Standard" },
                { id: "ultrafast", label: "Ultrafast" },
              ],
            },
          ],
        }),
      },
    ],
  },
];

function picker(onSave: ComponentProps<typeof DefaultModelPicker>["onSave"]) {
  return (
    <DefaultModelPicker
      environmentId={null}
      triggerLabel="Mixed"
      selection={saved}
      instanceEntries={deriveProviderInstanceEntries(providers)}
      modelOptionsByInstance={new Map([[codex, [{ slug: model, name: "GPT-6.2-Sol" }]]])}
      onSave={onSave}
    />
  );
}

async function pickCodex() {
  await page.getByRole("button", { name: "Mixed", exact: true }).click();
  await page.getByRole("button", { name: "Codex", exact: true }).click();
  await page.getByRole("option", { name: /GPT-6\.2-Sol/ }).click();
}

it("keeps the Composer picker open and submits the whole default only on Done", async () => {
  const save = vi.fn();
  await using _screen = await render(picker(save));

  await pickCodex();
  await page.getByRole("radio", { name: "High", exact: true }).click();
  await page.getByRole("radio", { name: "Ultrafast", exact: true }).click();
  expect(save).not.toHaveBeenCalled();

  await page.getByRole("button", { name: "Done", exact: true }).click();
  expect(save).toHaveBeenCalledTimes(1);
  expect(save).toHaveBeenCalledWith({
    instanceId: codex,
    model,
    options: expect.arrayContaining([
      { id: "reasoningEffort", value: "high" },
      { id: "serviceTier", value: "ultrafast" },
    ]),
  });
  await expect.element(page.getByPlaceholder("Search models...")).not.toBeInTheDocument();
});

it("discards a pending model and options on Escape", async () => {
  const save = vi.fn();
  await using _screen = await render(picker(save));

  await pickCodex();
  await page.getByRole("radio", { name: "High", exact: true }).click();
  await userEvent.keyboard("{Escape}");
  await expect.element(page.getByPlaceholder("Search models...")).not.toBeInTheDocument();
  expect(save).not.toHaveBeenCalled();

  await pickCodex();
  await expect
    .element(page.getByRole("radio", { name: "High", exact: true }))
    .toHaveAttribute("aria-checked", "false");
});
