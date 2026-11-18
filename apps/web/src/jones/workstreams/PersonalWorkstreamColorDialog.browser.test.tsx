import "~/index.css";
import { useState } from "react";
import { expect, it } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import { render } from "../../../test/render";
import { PersonalWorkstreamColorDialog } from "./PersonalWorkstreamColorDialog";

function Example() {
  const [color, setColor] = useState<string | null>("#ABCDE0");
  const [open, setOpen] = useState(true);
  return (
    <>
      <div role="status">{color ?? "Automatic"}</div>
      <button type="button" onClick={() => setOpen(true)}>
        Change color
      </button>
      {open ? (
        <PersonalWorkstreamColorDialog
          color={color}
          automaticColor="#14B8A6"
          pending={false}
          onOpenChange={setOpen}
          onSave={(selection) => {
            setColor(selection);
            setOpen(false);
          }}
        />
      ) : null}
    </>
  );
}

it("saves a palette color, reads it back, and restores Automatic", async () => {
  await using _screen = await render(<Example />);
  const dialog = page.getByRole("dialog", { name: "Workstream color" });
  const hex = dialog.getByRole("textbox", { name: "Hex color" });
  const rose = dialog.getByRole("button", { name: "Rose", exact: true });
  const save = dialog.getByRole("button", { name: "Save", exact: true });
  const automatic = dialog.getByRole("button", { name: "Automatic", exact: true });

  await expect.element(hex).toHaveValue("#ABCDE0");
  await userEvent.click(rose);
  await expect.element(rose).toHaveAttribute("aria-pressed", "true");
  await userEvent.click(save);
  await expect.element(page.getByRole("status")).toHaveTextContent("#F43F5E");

  await userEvent.click(page.getByRole("button", { name: "Change color" }));
  await expect.element(rose).toHaveAttribute("aria-pressed", "true");
  await userEvent.click(automatic);
  await expect.element(automatic).toHaveAttribute("aria-pressed", "true");
  await expect.element(dialog.getByLabelText("Custom color")).toHaveValue("#14b8a6");
  await userEvent.click(save);
  await expect.element(page.getByRole("status")).toHaveTextContent("Automatic");
});

it("requires a complete hex color and stores it in the existing uppercase format", async () => {
  await using _screen = await render(<Example />);
  const dialog = page.getByRole("dialog", { name: "Workstream color" });
  const hex = dialog.getByRole("textbox", { name: "Hex color" });
  const save = dialog.getByRole("button", { name: "Save", exact: true });

  await userEvent.fill(hex, "#abc");
  await expect.element(save).toBeDisabled();
  await expect.element(hex).toHaveAttribute("aria-invalid", "true");
  await userEvent.fill(hex, "#abc123");
  await expect.element(dialog.getByLabelText("Custom color")).toHaveValue("#abc123");
  await userEvent.click(save);
  await expect.element(page.getByRole("status")).toHaveTextContent("#ABC123");
});

it("updates the hex value when the native color picker changes", async () => {
  await using _screen = await render(<Example />);
  const dialog = page.getByRole("dialog", { name: "Workstream color" });
  await userEvent.fill(dialog.getByLabelText("Custom color"), "#365487");
  await expect.element(dialog.getByRole("textbox", { name: "Hex color" })).toHaveValue("#365487");
  await userEvent.click(dialog.getByRole("button", { name: "Save", exact: true }));
  await expect.element(page.getByRole("status")).toHaveTextContent("#365487");
});
