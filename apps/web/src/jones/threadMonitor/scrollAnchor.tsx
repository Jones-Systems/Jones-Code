/* eslint-disable shadcn/no-unknown-classes -- This isolated native peer uses the dedicated monitor.css stylesheet instead of Tailwind utilities. */
import { Component, createRef, type ReactNode } from "react";
export interface MonitorScrollSnapshot {
  readonly scrollTop: number;
  readonly anchors: ReadonlyArray<{ readonly key: string; readonly offset: number }>;
}
export function captureMonitorAnchor(element: HTMLElement): MonitorScrollSnapshot {
  const top = element.getBoundingClientRect().top;
  const candidates = [...element.querySelectorAll<HTMLElement>("[data-anchor]")].filter(
    (entry) => entry.getClientRects().length > 0 && entry.getBoundingClientRect().bottom >= top,
  );
  return {
    scrollTop: element.scrollTop,
    anchors: candidates.slice(0, 8).map((entry) => ({
      key: entry.dataset.anchor!,
      offset: entry.getBoundingClientRect().top - top,
    })),
  };
}
export function restoreMonitorAnchor(element: HTMLElement, snapshot: MonitorScrollSnapshot) {
  const top = element.getBoundingClientRect().top;
  for (const anchor of snapshot.anchors) {
    const target = [...element.querySelectorAll<HTMLElement>("[data-anchor]")].find(
      (entry) => entry.dataset.anchor === anchor.key && entry.getClientRects().length > 0,
    );
    if (target) {
      element.scrollTop += target.getBoundingClientRect().top - top - anchor.offset;
      return;
    }
  }
  element.scrollTop = snapshot.scrollTop;
}
export class MonitorAnchoredScroll extends Component<{
  children: ReactNode;
  hidden?: boolean;
  className?: string;
}> {
  readonly element = createRef<HTMLDivElement>();
  override getSnapshotBeforeUpdate() {
    return this.element.current ? captureMonitorAnchor(this.element.current) : null;
  }
  override componentDidUpdate(
    _props: unknown,
    _state: unknown,
    snapshot: MonitorScrollSnapshot | null,
  ) {
    if (snapshot && this.element.current) restoreMonitorAnchor(this.element.current, snapshot);
  }
  override render() {
    return (
      <div
        ref={this.element}
        hidden={this.props.hidden}
        className={this.props.className ?? "list-scroll"}
      >
        {this.props.children}
      </div>
    );
  }
}
