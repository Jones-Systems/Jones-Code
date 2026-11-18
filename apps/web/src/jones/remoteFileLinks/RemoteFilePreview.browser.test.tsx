import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { expect, it, vi } from "vite-plus/test";

import { render } from "../../../test/render";
import "~/index.css";

import ChatMarkdown from "~/components/ChatMarkdown";
import { selectActiveRightPanelSurface, useRightPanelStore } from "~/rightPanelStore";
import { parseRemoteFileHref } from "./remoteFileLink";
import RemoteFilePreview from "./RemoteFilePreview";

const origin = {
  environmentId: EnvironmentId.make("workstation"),
  threadId: ThreadId.make("remote-file-test"),
};
const destination = EnvironmentId.make("vps:example");
const path = "/home/example/release notes.txt";
const href = `t3-file://v1/${encodeURIComponent(destination)}/${encodeURIComponent(path)}`;

const asset = vi.hoisted(() => ({
  allowed: true,
  read: vi.fn(),
}));
vi.mock(import("~/components/files/projectFilesQueryState"), async (original) => ({
  ...(await original()),
  useProjectFileQuery: (environmentId, cwd, relativePath) => {
    asset.read(environmentId, cwd, relativePath);
    return {
      data: {
        contents: "Content from the VPS",
        truncated: false,
        byteLength: 20,
        relativePath: relativePath ?? "",
      },
      error: null,
      readError: null,
      isFile: true,
      isNotFile: false,
      isPending: false,
      refresh: () => {},
    };
  },
}));
vi.mock(import("~/state/filesystem"), async (original) => ({
  ...(await original()),
  useFilesystemReadAccess: () => ({ canReadFiles: asset.allowed, isPending: false, error: null }),
}));

function OpenReference() {
  const surface = useRightPanelStore((state) =>
    selectActiveRightPanelSurface(state.byThreadKey, origin),
  );
  return (
    <>
      <ChatMarkdown text={`[Release notes](${href})`} cwd="/same/repo" threadRef={origin} />
      {surface?.kind === "remote-file" ? (
        <aside aria-label="Files">
          <RemoteFilePreview {...surface.target} />
        </aside>
      ) : null}
    </>
  );
}

it("opens the cited computer's file on click without fetching it in the conversation", async () => {
  asset.allowed = true;
  asset.read.mockClear();
  useRightPanelStore.setState({ byThreadKey: {} });
  await using screen = await render(<OpenReference />);

  expect(asset.read).not.toHaveBeenCalled();
  await screen.getByRole("button", { name: "Release notes" }).click();
  await expect.element(screen.getByRole("complementary", { name: "Files" })).toBeVisible();
  await expect.element(screen.getByText("Content from the VPS")).toBeVisible();
  expect(asset.read).toHaveBeenCalledWith(destination, "/", path);
  expect(asset.read.mock.calls.every(([environmentId]) => environmentId === destination)).toBe(
    true,
  );
});

it("does not request an asset when the target computer withholds filesystem access", async () => {
  asset.allowed = false;
  asset.read.mockClear();
  useRightPanelStore.setState({ byThreadKey: {} });
  await using screen = await render(<OpenReference />);

  await screen.getByRole("button", { name: "Release notes" }).click();
  await expect
    .element(screen.getByRole("alert"))
    .toHaveTextContent("This computer does not grant file read access.");
  expect(asset.read).not.toHaveBeenCalled();
});

it("accepts encoded absolute paths and refuses relative, malformed and network paths", () => {
  expect(parseRemoteFileHref(href)).toEqual({ environmentId: destination, absolutePath: path });
  for (const absolutePath of ["/a", "C:\\work\\file.md", "/home/a#b.md"]) {
    expect(
      parseRemoteFileHref(`t3-file://v1/exact/${encodeURIComponent(absolutePath)}`),
    ).toMatchObject({ environmentId: "exact", absolutePath });
  }
  for (const path of [
    "../file",
    "C:file.md",
    "//server/share/file",
    "\\\\server\\share\\file",
    "%",
    "/directory/",
    "/path\0bad",
  ]) {
    expect(parseRemoteFileHref(`t3-file://v1/exact/${encodeURIComponent(path)}`)).toBeNull();
  }
  expect(parseRemoteFileHref("t3-file://v1/exact/%2Ffile%ZZ")).toBeNull();
});
