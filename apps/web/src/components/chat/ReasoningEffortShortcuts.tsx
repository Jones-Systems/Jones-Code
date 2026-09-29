import { useLayoutEffect, useRef, useState } from "react";
import { getProviderOptionCurrentValue } from "@t3tools/shared/model";
import { ComposerControl } from "./ComposerControl";
import {
  type TraitsMenuContentProps,
  type TraitsPersistence,
  useTraitsSelection,
} from "./TraitsPicker";

export function ReasoningEffortShortcuts(props: TraitsMenuContentProps & TraitsPersistence) {
  const traits = useTraitsSelection(props);
  const descriptor = traits.selectDescriptors.find(
    ({ id }) =>
      id === "reasoningEffort" || id === "effort" || id === "variant" || id === "reasoning",
  );
  const groupRef = useRef<HTMLDivElement>(null);
  const [height, setHeight] = useState(0);
  const options = traits.modelIsUnavailable ? undefined : descriptor?.options;

  useLayoutEffect(() => {
    const group = groupRef.current;
    if (!group || !options?.length) return;
    const measure = () => {
      const rows = new Set(Array.from(group.children, (child) => (child as HTMLElement).offsetTop));
      setHeight(
        rows.size > 0 && rows.size <= 2 && group.scrollWidth <= group.clientWidth
          ? group.offsetHeight
          : 0,
      );
    };
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(group);
    return () => observer.disconnect();
  }, [options]);

  if (!descriptor || !options?.length) return null;
  const promptControlled =
    descriptor.id === traits.primarySelectDescriptor?.id && traits.ultrathinkPromptControlled;
  const disabled =
    descriptor.id === traits.primarySelectDescriptor?.id && traits.ultrathinkInBodyText;
  const selectedValue = promptControlled ? "ultrathink" : getProviderOptionCurrentValue(descriptor);

  return (
    <div className="relative min-w-0 flex-1" style={{ height }}>
      {/* Retain the available width while hidden so the measurement cannot oscillate. */}
      <div
        ref={groupRef}
        role="group"
        aria-label={descriptor.label}
        aria-hidden={height === 0}
        inert={height === 0}
        className="absolute inset-x-0 top-0 flex flex-wrap justify-end gap-1"
        style={{ visibility: height === 0 ? "hidden" : "visible" }}
      >
        {options.map((option) => (
          <ComposerControl
            key={option.id}
            size="xs"
            aria-pressed={selectedValue === option.id}
            disabled={
              disabled ||
              (props.allowPromptInjectedEffort === false &&
                !!descriptor.promptInjectedValues?.includes(option.id))
            }
            title={
              disabled
                ? 'Your prompt contains "ultrathink" in the text. Remove it to change this option.'
                : option.description
            }
            onClick={() => traits.handleSelectChange(descriptor, option.id)}
          >
            {option.label}
          </ComposerControl>
        ))}
      </div>
    </div>
  );
}
