import { Schema } from "@tiptap/pm/model";
import { expect, it, vi } from "vite-plus/test";

import { serializeEditorDoc } from "~/composer-rich-text-doc";
import { createEditorSerializationCache } from "./editorSerializationCache";

vi.mock(import("~/composer-rich-text-doc"), { spy: true });

const schema = new Schema({
  nodes: {
    doc: { content: "paragraph+" },
    paragraph: { content: "text*" },
    text: {},
  },
});
const doc = (text: string) =>
  schema.node("doc", null, [schema.node("paragraph", null, schema.text(text))]);

it("serializes once per document identity and retains only the current document", () => {
  const read = createEditorSerializationCache();
  const first = doc("first");
  const second = doc("second");
  vi.mocked(serializeEditorDoc).mockClear();
  const firstMap = read(first);
  expect(read(first)).toBe(firstMap);
  expect(serializeEditorDoc).toHaveBeenCalledTimes(1);
  expect(read(second).value).toBe("second");
  expect(read(first)).toEqual(firstMap);
  expect(serializeEditorDoc).toHaveBeenCalledTimes(3);
  expect(read(doc("first"))).toEqual(firstMap);
  expect(serializeEditorDoc).toHaveBeenCalledTimes(4);
});

it("does not share cached maps between editors even for the same document", () => {
  const first = createEditorSerializationCache();
  const second = createEditorSerializationCache();
  const sharedDoc = doc("shared");
  vi.mocked(serializeEditorDoc).mockClear();
  expect(first(sharedDoc)).not.toBe(second(sharedDoc));
  expect(serializeEditorDoc).toHaveBeenCalledTimes(2);
});
