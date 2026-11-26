import { describe, expect, it } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";

import { ConfirmDialogHost } from "~/components/ConfirmDialogHost";
import { render } from "../../test/render";
import { confirmTerminalClose } from "./terminalCloseConfirm";

describe("terminal close confirmation", () => {
  it.each([
    { key: "Enter", confirmed: true },
    { key: "Escape", confirmed: false },
  ])("resolves to $confirmed on $key after opening the dialog", async ({ key, confirmed }) => {
    let confirmation: Promise<boolean> | undefined;
    await using screen = await render(
      <>
        <button
          type="button"
          onClick={() => {
            confirmation = confirmTerminalClose(["Terminal 1"]);
          }}
        >
          Close terminal
        </button>
        <ConfirmDialogHost />
      </>,
      { router: false },
    );

    await screen.getByRole("button", { name: "Close terminal" }).click();
    const dialog = page.getByRole("alertdialog", { name: 'Close terminal "Terminal 1"?' });
    await expect.element(dialog.getByRole("button", { name: "Confirm" })).toHaveFocus();

    await userEvent.keyboard(`{${key}}`);
    await expect(confirmation).resolves.toBe(confirmed);
  });
});
