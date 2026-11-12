import type { Node as ProseMirrorNode } from "@tiptap/pm/model";

import { serializeEditorDoc, type ReadonlyRichDocMap } from "~/composer-rich-text-doc";

export function createEditorSerializationCache(
  serialize: (doc: ProseMirrorNode) => ReadonlyRichDocMap = serializeEditorDoc,
) {
  let entry: { doc: ProseMirrorNode; map: ReadonlyRichDocMap } | undefined;
  return (doc: ProseMirrorNode): ReadonlyRichDocMap => {
    if (entry?.doc === doc) return entry.map;
    const map = serialize(doc);
    entry = { doc, map };
    return map;
  };
}
