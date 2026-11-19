import type { ProviderOptionDescriptor } from "@t3tools/contracts";
import type { EffortDisplay } from "@t3tools/contracts/settings";
import type { ReactNode } from "react";
import type { ComposerControlSize } from "../../components/chat/ComposerControl";
import { EffortGlyph, resolveEffortGlyph } from "../../components/chat/EffortGlyph";

// Quick controls sit to the left of this badge. Reserve the option widths so an immediate
// selection cannot move the button before the second click lands.
export function QuickEffortLabelSlot(props: {
  descriptor: Extract<ProviderOptionDescriptor, { type: "select" }>;
  effortDisplay: EffortDisplay;
  size: ComposerControlSize;
  children: ReactNode;
}) {
  const { descriptor, effortDisplay, size } = props;
  const sizingLabels = new Set<string>();
  let barCount = 0;
  for (const option of descriptor.options) {
    const glyph = resolveEffortGlyph(descriptor, option.id);
    if (effortDisplay === "bars" && glyph && !glyph.special) barCount = glyph.count;
    else sizingLabels.add(glyph?.special ?? option.label);
  }
  return (
    <span className="inline-grid items-center [&>*]:col-start-1 [&>*]:row-start-1">
      {[...sizingLabels].map((label) => (
        <span
          key={label}
          aria-hidden="true"
          data-width-label={label}
          className="invisible max-w-24 truncate after:content-[attr(data-width-label)]"
        />
      ))}
      {barCount > 0 ? (
        <span aria-hidden="true" className="invisible">
          <EffortGlyph count={barCount} filled={0} size={size} />
        </span>
      ) : null}
      {props.children}
    </span>
  );
}
