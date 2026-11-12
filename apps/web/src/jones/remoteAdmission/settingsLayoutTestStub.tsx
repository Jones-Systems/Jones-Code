// Light settingsLayout stand-ins for remote-admission browser tests. The real
// SettingsRow reads session and primary-environment state these tests mock away.
import type { ReactNode } from "react";

export const SETTINGS_PICKER_TRIGGER_CLASSNAME = "";

export function SettingsSection({
  title,
  headerAction,
  children,
  ...props
}: {
  title: string;
  headerAction?: ReactNode;
  children: ReactNode;
  "aria-label"?: string;
}) {
  return (
    <section aria-label={props["aria-label"]}>
      <h2>{title}</h2>
      {headerAction}
      {children}
    </section>
  );
}

export function SettingsRow({
  title,
  description,
  status,
  control,
}: {
  title: ReactNode;
  description?: ReactNode;
  status?: ReactNode;
  control?: ReactNode;
}) {
  return (
    <div data-slot="settings-row">
      <h3>{title}</h3>
      {description ? <p>{description}</p> : null}
      {status ? <div>{status}</div> : null}
      {control}
    </div>
  );
}
