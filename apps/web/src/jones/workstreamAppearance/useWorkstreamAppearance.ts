import type {
  T3WorkstreamListResult,
  WorkstreamAppearance,
  WorkstreamAppearanceWrite,
} from "@t3tools/contracts";
import {
  validateAppearanceBinding,
  workstreamBindingKey,
} from "@t3tools/client-runtime/state/workstreams";
import * as Effect from "effect/Effect";
import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { canEditWorkstreams } from "../../components/workstreams/nativeWorkstreamActions";
import { PrimaryEnvironmentHttpClient } from "../../environments/primary/httpClient";
import { runPrimaryHttp } from "../../lib/runtime";
import { WorkstreamActionError } from "../workstreams/workstreamActionSnapshot";

export function useWorkstreamAppearance(data: T3WorkstreamListResult | null) {
  const key = data
    ? workstreamBindingKey(data.binding) +
      JSON.stringify(data.items.map((item) => item.workstreamId))
    : null;
  const [revision, setRevision] = useState(0);
  const current = useRef({ key, revision });
  const latest = useRef(data);
  useLayoutEffect(() => {
    current.current = { key, revision };
    latest.current = data;
  }, [key, data, revision]);
  const [state, setState] = useState<{
    key: string | null;
    writable: boolean;
    colors: ReadonlyMap<string, WorkstreamAppearance>;
  }>({ key: null, writable: false, colors: new Map() });
  useEffect(() => {
    const data = latest.current;
    if (!data || !key) return;
    const abort = new AbortController();
    let loading = false;
    const load = async () => {
      if (loading || document.hidden) return;
      loading = true;
      await (async () => {
        const colors = new Map<string, WorkstreamAppearance>();
        let writable = data.binding.permissions.includes("workstreams:write");
        const ids = data.items.map((item) => item.workstreamId);
        for (let start = 0; start < Math.max(1, ids.length); start += 100) {
          const batch = ids.slice(start, start + 100);
          const result = await runPrimaryHttp(
            PrimaryEnvironmentHttpClient.pipe(
              Effect.flatMap((client) =>
                client.workstreams.appearanceRead({
                  headers: {},
                  payload: { workstream_ids: batch },
                }),
              ),
            ),
            { signal: abort.signal },
          );
          if (!result.supported) {
            writable = false;
            break;
          }
          validateAppearanceBinding(result.page, data.binding, batch);
          writable = writable && result.page.permissions.includes("workstreams:write");
          for (const item of result.page.items) colors.set(item.workstream_id, item);
        }
        if (
          !abort.signal.aborted &&
          current.current.key === key &&
          current.current.revision === revision
        )
          setState({ key, writable, colors });
      })()
        .catch(() => {
          if (
            !abort.signal.aborted &&
            current.current.key === key &&
            current.current.revision === revision
          )
            setState((previous) => ({ ...previous, writable: false }));
        })
        .finally(() => {
          loading = false;
        });
    };
    void load();
    const timer = window.setInterval(() => void load(), 30_000);
    const focus = () => void load();
    window.addEventListener("focus", focus);
    document.addEventListener("visibilitychange", focus);
    return () => {
      abort.abort();
      window.clearInterval(timer);
      window.removeEventListener("focus", focus);
      document.removeEventListener("visibilitychange", focus);
    };
  }, [key, revision]);
  return {
    writable: state.key === key && state.writable,
    colors: state.key === key ? state.colors : new Map<string, WorkstreamAppearance>(),
    save: async (input: WorkstreamAppearanceWrite) => {
      const started = key;
      // An open dialog keeps an earlier save; check the snapshot as it is now, before any request.
      if (!canEditWorkstreams(latest.current)) throw new WorkstreamActionError("stale");
      if (!data || state.key !== key || !state.writable)
        throw new Error("Color editing is unavailable. Refresh before retrying.");
      const result = await runPrimaryHttp(
        PrimaryEnvironmentHttpClient.pipe(
          Effect.flatMap((client) =>
            client.workstreams.appearanceSave({ headers: {}, payload: input }),
          ),
        ),
      );
      if (current.current.key !== started) throw new Error("Workstream access changed.");
      setRevision((value) => value + 1);
      return result;
    },
  };
}
