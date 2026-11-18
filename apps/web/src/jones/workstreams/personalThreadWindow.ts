import * as Schema from "effect/Schema";
import { readLocalStorageValue, useLocalStorage } from "../../hooks/useLocalStorage";

export const PERSONAL_THREAD_LIMITS = [5, 10, 15] as const;
export type PersonalThreadLimit = (typeof PERSONAL_THREAD_LIMITS)[number];

const PersonalThreadLimitSchema = Schema.Literals(PERSONAL_THREAD_LIMITS);
const DEFAULT_LIMIT: PersonalThreadLimit = 5;

export const personalThreadLimitKey = (id: string) => `jones:workstreams:thread-limit:${id}`;
export const personalThreadExpandedKey = (id: string) => `jones:workstreams:thread-expanded:${id}`;

export function usePersonalThreadWindow(id: string) {
  const [limit, setLimit] = useLocalStorage(
    personalThreadLimitKey(id),
    DEFAULT_LIMIT,
    PersonalThreadLimitSchema,
  );
  const [expanded, setExpanded] = useLocalStorage(
    personalThreadExpandedKey(id),
    false,
    Schema.Boolean,
  );
  return { limit, setLimit, expanded, setExpanded };
}

export function readPersonalThreadWindow(id: string): {
  limit: PersonalThreadLimit;
  expanded: boolean;
} {
  return {
    limit: readLocalStorageValue(
      personalThreadLimitKey(id),
      DEFAULT_LIMIT,
      PersonalThreadLimitSchema,
    ),
    expanded: readLocalStorageValue(personalThreadExpandedKey(id), false, Schema.Boolean),
  };
}

export function windowPersonalThreads<T>(
  threads: readonly T[],
  limit: PersonalThreadLimit,
  expanded: boolean,
): readonly T[] {
  return expanded || threads.length <= limit ? threads : threads.slice(0, limit);
}
