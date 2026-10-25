import type { ServerProviderSkill } from "@t3tools/contracts";
import type { Editor } from "@tiptap/core";
import { closeHistory } from "@tiptap/pm/history";
import { createRef, useImperativeHandle, useState } from "react";
import { expect, it, vi } from "vite-plus/test";

import { act, render } from "../../../test/render.tsx";

import { serializeEditorDoc } from "~/composer-rich-text-doc";
import {
  ComposerPromptEditor,
  type ComposerPromptEditorHandle,
  type ComposerPromptEditorProps,
} from "~/components/ComposerPromptEditor";
import { formatInlineContextReference } from "~/lib/composerContextReferences";

const review: ServerProviderSkill = {
  name: "review",
  path: "/synthetic/skills/review/SKILL.md",
  enabled: true,
};

const counts = vi.hoisted(() => ({ serialize: vi.fn(), boundary: vi.fn() }));
vi.mock(import("~/composer-rich-text-doc"), async (original) => {
  const actual = await original();
  return {
    ...actual,
    serializeEditorDoc: (...args: Parameters<typeof actual.serializeEditorDoc>) => {
      counts.boundary(...args);
      return actual.serializeEditorDoc(...args);
    },
  };
});
vi.mock(import("./editorSerializationCache"), async (original) => {
  const actual = await original();
  return {
    ...actual,
    createEditorSerializationCache: () =>
      actual.createEditorSerializationCache((doc) => {
        counts.serialize(doc);
        return serializeEditorDoc(doc);
      }),
  };
});

function createHarness(initial: string, options: Partial<ComposerPromptEditorProps> = {}) {
  const editorRef = createRef<ComposerPromptEditorHandle>();
  const changed = vi.fn<ComposerPromptEditorProps["onChange"]>();
  const visibleSelection = vi.fn();
  const controlsRef = createRef<{
    replace: (value: string) => void;
    replaceSkills: (skills: ReadonlyArray<ServerProviderSkill>) => void;
  }>();
  function Harness() {
    const [value, setValue] = useState(initial);
    const [cursor, setCursor] = useState(0);
    const [skills, setSkills] = useState(options.skills ?? []);
    useImperativeHandle(controlsRef, () => ({ replace: setValue, replaceSkills: setSkills }), []);
    return (
      <ComposerPromptEditor
        editorRef={editorRef}
        value={value}
        cursor={cursor}
        contextRecords={new Map()}
        disabled={false}
        placeholder=""
        richTextEnabled
        onPaste={() => {}}
        onVisibleSelectionChange={visibleSelection}
        {...options}
        skills={skills}
        onChange={(...args) => {
          changed(...args);
          setValue(args[0]);
          setCursor(args[1]);
          options.onChange?.(...args);
        }}
      />
    );
  }
  const controls = () => {
    if (!controlsRef.current) throw new Error("Composer harness did not commit");
    return controlsRef.current;
  };
  return {
    ui: <Harness />,
    editorRef,
    changed,
    visibleSelection,
    replace: (value: string) => controls().replace(value),
    replaceSkills: (skills: ReadonlyArray<ServerProviderSkill>) => controls().replaceSkills(skills),
    [Symbol.dispose]() {
      vi.clearAllMocks();
    },
  };
}

function readMountedEditor(container: HTMLElement): Editor {
  const element = container.querySelector<HTMLElement & { editor: Editor }>(".tiptap");
  if (!element) throw new Error("Composer did not mount");
  return element.editor;
}

it.each([true, false])(
  "reuses serialization for selection and focus with rich text %s",
  async (richTextEnabled) => {
    using harness = createHarness("hello world", {
      richTextEnabled,
    });
    await using screen = await render(harness.ui, { router: false });
    const { editorRef, changed, visibleSelection } = harness;
    const editor = readMountedEditor(screen.container);
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
  },
);

it("invalidates for content, marks, skill chips, undo, redo and controlled replacement", async () => {
  using harness = createHarness("hello world", { skills: [review] });
  await using screen = await render(harness.ui, { router: false });
  const { editorRef, changed, replace } = harness;
  const editor = readMountedEditor(screen.container);
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
  using harness = createHarness("$review **plain**", { literalText: true });
  await using screen = await render(harness.ui, { router: false });
  const { changed } = harness;
  const editor = readMountedEditor(screen.container);
  counts.serialize.mockClear();
  await act(async () => {
    editor.commands.setTextSelection(4);
  });
  expect(changed).toHaveBeenLastCalledWith("$review **plain**", 3, 3, true, []);
  expect(counts.serialize).not.toHaveBeenCalled();
});

it("recomputes chip and Markdown cursor coordinates from the cached map", async () => {
  using harness = createHarness("$review **bold**", { skills: [review] });
  await using screen = await render(harness.ui, { router: false });
  const { editorRef, changed } = harness;
  const editor = readMountedEditor(screen.container);
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

it("isolates callback and snapshot context arrays from a warmed cache", async () => {
  const contextId = "file_cache-test";
  const value = `See ${formatInlineContextReference({ kind: "file", contextId, label: "notes.txt" })} now`;
  const received: string[][] = [];
  using harness = createHarness(value, {
    onChange: (_value, _cursor, _expandedCursor, _suppressTrigger, contextIds) => {
      received.push([...contextIds]);
      contextIds.splice(0, contextIds.length, "callback-mutation");
    },
  });
  await using screen = await render(harness.ui, { router: false });
  const { editorRef } = harness;
  const editor = readMountedEditor(screen.container);
  await act(async () => editorRef.current?.focusAt(0));
  const cachedDoc = editor.state.doc;
  counts.serialize.mockClear();
  received.length = 0;
  await act(async () => {
    editor.commands.setTextSelection(2);
  });
  expect(received).toEqual([[contextId]]);
  await act(async () => {
    editor.commands.setTextSelection({ from: 2, to: 3 });
  });
  expect(received).toEqual([[contextId]]);
  const snapshot = editorRef.current!.readSnapshot();
  expect(snapshot.contextIds).toEqual([contextId]);
  snapshot.contextIds.splice(0, snapshot.contextIds.length, "snapshot-mutation");
  await act(async () => {
    editor.commands.setTextSelection({ from: 2, to: 4 });
  });
  expect(received).toEqual([[contextId]]);
  await act(async () => editorRef.current?.focusAt(2));
  expect(received).toEqual([[contextId], [contextId]]);
  expect(editorRef.current!.readSnapshot().contextIds).toEqual([contextId]);
  expect(editor.state.doc).toBe(cachedDoc);
  expect(counts.serialize).not.toHaveBeenCalled();
});

it("invalidates a warmed cache when provider skills reconcile text and chips", async () => {
  const value = "$review tail";
  using harness = createHarness(value);
  await using screen = await render(harness.ui, { router: false });
  const { editorRef, changed, replaceSkills } = harness;
  const editor = readMountedEditor(screen.container);
  await act(async () => editorRef.current?.focusAt(0));
  const textDoc = editor.state.doc;
  expect(textDoc.textContent).toBe(value);
  counts.serialize.mockClear();
  await act(async () => replaceSkills([review]));
  const chipDoc = editor.state.doc;
  expect(chipDoc).not.toBe(textDoc);
  expect(chipDoc.firstChild?.firstChild?.type.name).toBe("composer-skill");
  expect(counts.serialize).toHaveBeenCalledWith(chipDoc);
  expect(counts.serialize).toHaveBeenCalledTimes(1);
  counts.serialize.mockClear();
  await act(async () => editorRef.current?.focusAt(1));
  expect(changed).toHaveBeenLastCalledWith(value, 1, 7, true, []);
  expect(editorRef.current!.readSnapshot().value).toBe(value);
  expect(counts.serialize).not.toHaveBeenCalled();

  await act(async () => replaceSkills([]));
  const restoredTextDoc = editor.state.doc;
  expect(restoredTextDoc).not.toBe(chipDoc);
  expect(restoredTextDoc.textContent).toBe(value);
  expect(restoredTextDoc.firstChild?.firstChild?.isText).toBe(true);
  expect(counts.serialize).toHaveBeenCalledWith(restoredTextDoc);
  expect(counts.serialize).toHaveBeenCalledTimes(1);
  counts.serialize.mockClear();
  await act(async () => {
    editor.commands.setTextSelection(4);
  });
  expect(changed).toHaveBeenLastCalledWith(value, 1, 3, false, []);
  expect(editorRef.current!.readSnapshot()).toMatchObject({ value, cursor: 1, expandedCursor: 3 });
  expect(counts.serialize).not.toHaveBeenCalled();
});

it("preserves code-block trigger suppression on selection cache hits", async () => {
  const value = "```\ncode\n```";
  using harness = createHarness(value);
  await using screen = await render(harness.ui, { router: false });
  const { changed } = harness;
  const editor = readMountedEditor(screen.container);
  counts.serialize.mockClear();
  await act(async () => {
    editor.commands.setTextSelection(3);
  });
  expect(changed).toHaveBeenLastCalledWith(value, 6, 6, true, []);
  expect(counts.serialize).not.toHaveBeenCalled();
});

it.each([true, false])(
  "reuses serialization across warmed snapshot and selection-range reads: %s",
  async (richTextEnabled) => {
    using harness = createHarness("hello world", { richTextEnabled });
    await using screen = await render(harness.ui, { router: false });
    const { editorRef } = harness;
    const editor = readMountedEditor(screen.container);
    await act(async () => editorRef.current?.focusAt(0));
    const document = editor.state.doc;
    expect(counts.boundary).toHaveBeenCalledWith(document);
    await act(async () => editor.commands.setTextSelection({ from: 3, to: 6 }));
    // Selection decorations have their own serializer; measure the warmed imperative reads separately.
    counts.boundary.mockClear();
    expect(editorRef.current?.readSelectionRange()).toEqual({ start: 2, end: 5 });
    expect(editorRef.current?.readSnapshot()).toMatchObject({
      value: "hello world",
      cursor: 2,
      expandedCursor: 2,
    });
    expect(editorRef.current?.readSelectionRange()).toEqual({ start: 2, end: 5 });
    expect(counts.boundary).not.toHaveBeenCalled();
    await act(async () => editorRef.current?.focusAt(5));
    counts.boundary.mockClear();
    expect(editorRef.current?.readSelectionRange()).toEqual({ start: 5, end: 5 });
    expect(editorRef.current?.readSnapshot()).toMatchObject({
      value: "hello world",
      cursor: 5,
      expandedCursor: 5,
    });
    expect(editor.state.doc).toBe(document);
    expect(counts.boundary).not.toHaveBeenCalled();
  },
);
