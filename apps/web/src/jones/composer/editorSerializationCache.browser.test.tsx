import type { Editor } from "@tiptap/core";
import { closeHistory } from "@tiptap/pm/history";
import { act, createRef, useState } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";

import { serializeEditorDoc } from "~/composer-rich-text-doc";
import {
  ComposerPromptEditor,
  type ComposerPromptEditorHandle,
  type ComposerPromptEditorProps,
} from "~/components/ComposerPromptEditor";

const counts = vi.hoisted(() => ({ serialize: vi.fn() }));
vi.mock(import("./editorSerializationCache"), async (original) => {
  const actual = await original();
  return {
    ...actual,
    createEditorSerializationCache: () => actual.createEditorSerializationCache((doc) => {
      counts.serialize(doc);
      return serializeEditorDoc(doc);
    }),
  };
});

let root: Root;
let container: HTMLDivElement;

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});

afterEach(async () => {
  try {
    await act(async () => root.unmount());
  } finally {
    container.remove();
    vi.unstubAllGlobals();
    vi.clearAllMocks();
  }
});

async function mount(initial: string, options: Partial<ComposerPromptEditorProps> = {}) {
  const editorRef = createRef<ComposerPromptEditorHandle>();
  const changed = vi.fn<ComposerPromptEditorProps["onChange"]>();
  const visibleSelection = vi.fn();
  let replace: (value: string) => void = () => {};
  function Harness() {
    const [value, setValue] = useState(initial);
    const [cursor, setCursor] = useState(0);
    replace = setValue;
    return (
      <ComposerPromptEditor
        editorRef={editorRef}
        value={value}
        cursor={cursor}
        contextRecords={new Map()}
        skills={[]}
        disabled={false}
        placeholder=""
        richTextEnabled
        onPaste={() => {}}
        onVisibleSelectionChange={visibleSelection}
        onChange={(...args) => {
          changed(...args);
          setValue(args[0]);
          setCursor(args[1]);
        }}
        {...options}
      />
    );
  }
  await act(async () => root.render(<Harness />));
  const element = container.querySelector<HTMLElement & { editor: Editor }>(".tiptap");
  if (!element) throw new Error("Composer did not mount");
  return {
    editor: element.editor,
    editorRef,
    changed,
    visibleSelection,
    replace: (value: string) => replace(value),
  };
}

it.each([true, false])("reuses serialization for selection and focus with rich text %s", async (richTextEnabled) => {
  const { editor, editorRef, changed, visibleSelection } = await mount("hello world", {
    richTextEnabled,
  });
  await act(async () => editorRef.current?.focusAt(0));
  expect(counts.serialize).toHaveBeenCalled();
  counts.serialize.mockClear();
  await act(async () => {
    editor.commands.setTextSelection(3);
  });
  expect(changed).toHaveBeenLastCalledWith("hello world", 2, 2, false, []);
  await act(async () => {
    editor.commands.setTextSelection({ from: 3, to: 6 });
  });
  expect(visibleSelection).toHaveBeenCalled();
  expect(editorRef.current?.readSelectionRange()).toEqual({ start: 2, end: 5 });
  await act(async () => editorRef.current?.focusAt(5));
  expect(editorRef.current?.readSelectionRange()).toEqual({ start: 5, end: 5 });
  expect(counts.serialize).not.toHaveBeenCalled();
});

it("invalidates for content, marks, skill chips, undo, redo and controlled replacement", async () => {
  const { editor, editorRef, changed, replace } = await mount("hello world");
  async function changesDoc(change: () => unknown, expected: string) {
    const before = editor.state.doc;
    counts.serialize.mockClear();
    await act(async () => {
      editor.view.dispatch(closeHistory(editor.state.tr));
      change();
    });
    expect(editor.state.doc).not.toBe(before);
    expect(counts.serialize).toHaveBeenCalledWith(editor.state.doc);
    expect(counts.serialize).toHaveBeenCalledTimes(1);
    expect(changed.mock.lastCall?.[0]).toBe(expected);
    counts.serialize.mockClear();
    await act(async () => editorRef.current?.focusAt(0));
    editorRef.current?.readSelectionRange();
    expect(counts.serialize).not.toHaveBeenCalled();
  }
  await changesDoc(() => editor.commands.insertContent("x"), "xhello world");
  await act(async () => {
    editor.commands.setTextSelection({ from: 2, to: 7 });
  });
  await changesDoc(() => editor.commands.setMark("bold"), "x**hello** world");
  await changesDoc(
    () => editor.commands.insertContent({ type: "composer-skill", attrs: { skillName: "review" } }),
    "$review" + "x**hello** world",
  );
  await changesDoc(() => editor.commands.undo(), "x**hello** world");
  await changesDoc(() => editor.commands.redo(), "$review" + "x**hello** world");
  counts.serialize.mockClear();
  await act(async () => replace("replacement"));
  expect(counts.serialize).toHaveBeenCalledWith(editor.state.doc);
  expect(editor.state.doc.textContent).toBe("replacement");
  counts.serialize.mockClear();
  await act(async () => {
    editor.commands.setTextSelection(4);
  });
  expect(changed).toHaveBeenLastCalledWith("replacement", 3, 3, false, []);
  expect(counts.serialize).not.toHaveBeenCalled();
});

it("keeps literal mode trigger suppression with a reused map", async () => {
  const { editor, changed } = await mount("$review **plain**", { literalText: true });
  counts.serialize.mockClear();
  await act(async () => {
    editor.commands.setTextSelection(4);
  });
  expect(changed).toHaveBeenLastCalledWith("$review **plain**", 3, 3, true, []);
  expect(counts.serialize).not.toHaveBeenCalled();
});

it("recomputes chip and Markdown cursor coordinates from the cached map", async () => {
  const { editor, editorRef, changed } = await mount("$review **bold**");
  counts.serialize.mockClear();
  await act(async () => {
    editor.commands.setTextSelection(2);
  });
  expect(changed).toHaveBeenLastCalledWith("$review **bold**", 1, 7, true, []);
  await act(async () => {
    editor.commands.setTextSelection(4);
  });
  expect(changed).toHaveBeenLastCalledWith("$review **bold**", 5, 11, false, []);
  expect(editorRef.current?.readSelectionRange()).toEqual({ start: 11, end: 11 });
  expect(counts.serialize).not.toHaveBeenCalled();
});

it("preserves code-block trigger suppression on selection cache hits", async () => {
  const value = "```\ncode\n```";
  const { editor, changed } = await mount(value);
  counts.serialize.mockClear();
  await act(async () => {
    editor.commands.setTextSelection(3);
  });
  expect(changed).toHaveBeenLastCalledWith(value, 6, 6, true, []);
  expect(counts.serialize).not.toHaveBeenCalled();
});
