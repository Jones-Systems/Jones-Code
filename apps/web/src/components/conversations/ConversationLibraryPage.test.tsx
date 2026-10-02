import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vite-plus/test";

import { ConversationLibraryMessage } from "./ConversationLibraryPage";

describe("ConversationLibraryMessage", () => {
  it("renders imported content as escaped plain text", () => {
    const markup = renderToStaticMarkup(
      <ConversationLibraryMessage
        message={{
          id: "message-1",
          parentId: null,
          messageId: null,
          role: "assistant",
          text: "<img src=x onerror=alert(1)>",
          createdAt: null,
          hidden: false,
          unsupportedParts: 0,
        }}
      />,
    );

    expect(markup).toContain("&lt;img src=x onerror=alert(1)&gt;");
    expect(markup).not.toContain("<img");
  });
});
