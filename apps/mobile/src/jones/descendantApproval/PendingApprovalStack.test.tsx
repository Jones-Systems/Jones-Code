import { act, type ReactNode } from "react";
import { createRoot } from "react-dom/client";
import { RuntimeRequestId } from "@t3tools/contracts";
import { describe, expect, it, vi } from "vite-plus/test";
import type { DerivedValue } from "react-native-reanimated";

type NativeProps = {
  children?: ReactNode;
  className?: string;
  style?: { maxHeight?: number; height?: number };
  accessibilityLabel?: string;
  onPress?: () => void;
};
vi.mock("react-native", () => ({
  Platform: { OS: "ios" },
  View: ({ children, className, style }: NativeProps) => (
    <div className={className} style={style}>
      {children}
    </div>
  ),
  ScrollView: ({ children }: NativeProps) => <div>{children}</div>,
  Pressable: ({ children, onPress, accessibilityLabel }: NativeProps) => (
    <button onClick={onPress} aria-label={accessibilityLabel}>
      {children}
    </button>
  ),
}));
vi.mock("react-native-reanimated", async () => {
  const { useRef } = await import("react");
  const transition = { duration: () => ({ easing: () => undefined }) };
  return {
    default: {
      View: ({ children, className, style }: NativeProps) => (
        <div className={className} style={style}>
          {children}
        </div>
      ),
      ScrollView: ({ children, style, accessibilityLabel }: NativeProps) => (
        <div role="region" aria-label={accessibilityLabel} style={{ ...style, overflowY: "auto" }}>
          {children}
        </div>
      ),
    },
    useAnimatedStyle: (read: () => unknown) => read(),
    useSharedValue: (value: number) =>
      useRef({
        value,
        set(next: number) {
          this.value = next;
        },
      }).current,
    Easing: { out: () => undefined, cubic: undefined },
    FadeInUp: transition,
    FadeOutDown: transition,
  };
});
vi.mock("../../components/ui/AppText", () => ({
  AppText: ({ children }: NativeProps) => <span>{children}</span>,
}));
vi.mock("../../components/ui/AppSymbol", () => ({ SymbolView: () => null }));
vi.mock("../../components/ui/ControlPill", () => ({ ControlPill: () => null }));
vi.mock("../../components/ui/Button", () => ({
  Button: ({
    label,
    onPress,
    disabled,
  }: {
    label: string;
    onPress: () => void;
    disabled?: boolean;
  }) => (
    <button onClick={onPress} disabled={disabled}>
      {label}
    </button>
  ),
}));
vi.mock("../../jones/questionAnswer/QuestionProgress", () => ({ QuestionProgress: () => null }));
vi.mock("../../features/threads/QuestionAttachments", () => ({
  QuestionAttachments: ({
    value,
    onChangeText,
  }: {
    value: string;
    onChangeText: (value: string) => void;
  }) => <input value={value} onChange={(event) => onChangeText(event.target.value)} />,
}));
vi.mock("../../native/SelectableMarkdownText", () => ({
  SelectableMarkdownText: ({ markdown }: { markdown: string }) => <span>{markdown}</span>,
}));
vi.mock("../../native/useOpenPullRequest", () => ({ useOpenPullRequest: () => vi.fn() }));
vi.mock("../../features/files/FileMarkdownPreview", () => ({
  useMarkdownPreviewStyles: () => ({ nativeTextStyle: {} }),
}));

import {
  PendingUserInputCard,
  type PendingUserInputCardProps,
} from "../../features/threads/PendingUserInputCard";
import { PendingApprovalStack, descendantApprovalStackHeight } from "./PendingApprovalStack";

const derived = (value: number) => ({ value }) as DerivedValue<number>;
function questionProps(): PendingUserInputCardProps {
  return {
    canOperateThread: true,
    pendingUserInput: {
      requestId: RuntimeRequestId.make("parent-question"),
      createdAt: "2026-10-27T00:00:00Z",
      responseCapability: "live",
      dismissible: false,
      questions: [
        {
          id: "question",
          header: "Parent question",
          question: "How should the parent continue?",
          options: [],
          multiSelect: false,
          allowCustomAnswer: true,
        },
      ],
    },
    maxHeight: derived(560),
    collapsed: false,
    expandedInFlow: true,
    onToggleCollapsed: vi.fn(),
    drafts: { question: { customAnswer: "Preserved parent draft" } },
    answers: { question: "Preserved parent draft" },
    respondingUserInputId: null,
    onSelectOption: vi.fn(),
    onChangeCustomAnswer: vi.fn(),
    onSubmit: vi.fn(async () => undefined),
    onDismiss: vi.fn(async () => undefined),
  };
}

describe("combined pending approval layout", () => {
  it.each([
    [844, 0, 96, 180, 556],
    [844, 336, 96, 180, 220],
    [667, 336, 64, 180, 75],
    [667, 336, 64, 300, 0],
    [1200, 0, 96, 180, 560],
  ])(
    "bounds the whole request stack for viewport %s, keyboard %s, header %s, composer %s",
    (windowHeight, keyboardHeight, navigationHeaderHeight, composerOverlapHeight, expected) => {
      expect(
        descendantApprovalStackHeight({
          windowHeight,
          keyboardHeight,
          navigationHeaderHeight,
          composerOverlapHeight,
        }),
      ).toBe(expected);
    },
  );

  it("keeps an expanded iOS parent question in the same bounded scroll area as child actions", async () => {
    (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
    const container = document.createElement("div");
    const root = createRoot(container);
    using _cleanup = {
      [Symbol.dispose]() {
        act(() => root.unmount());
      },
    };
    const approve = vi.fn();
    const props = questionProps();
    act(() =>
      root.render(
        <PendingApprovalStack enabled maxHeight={derived(220)}>
          <button onClick={approve}>Approve child</button>
          <button>Next</button>
          <button>Open</button>
          <PendingUserInputCard {...props} />
        </PendingApprovalStack>,
      ),
    );
    const stack = container.querySelector('[aria-label="Pending requests"]') as HTMLElement;
    expect(stack.style.maxHeight).toBe("220px");
    expect(stack.style.overflowY).toBe("auto");
    expect(stack.textContent).toContain("How should the parent continue?");
    expect(stack.querySelector(".absolute")).toBeNull();
    expect(stack.querySelector("input")?.value).toBe("Preserved parent draft");
    await act(async () => (stack.querySelector("button") as HTMLButtonElement).click());
    expect(approve).toHaveBeenCalledOnce();
    expect(props.onSubmit).not.toHaveBeenCalled();
    expect(stack.querySelector("input")?.value).toBe("Preserved parent draft");
    const send = [...stack.querySelectorAll("button")].find(
      (button) => button.textContent === "Send",
    )!;
    await act(async () => send.click());
    expect(props.onSubmit).toHaveBeenCalledOnce();
  });

  it("retains the ordinary iOS overlay when no combined stack is requested", () => {
    const container = document.createElement("div");
    const root = createRoot(container);
    using _cleanup = {
      [Symbol.dispose]() {
        act(() => root.unmount());
      },
    };
    const { expandedInFlow: _expandedInFlow, ...props } = questionProps();
    act(() => root.render(<PendingUserInputCard {...props} />));
    expect(container.querySelector(".absolute")).not.toBeNull();
  });
});
