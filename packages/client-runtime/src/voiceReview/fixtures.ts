import type {
  VoiceReviewRecentList,
  ThreadRegistryComposedSnapshot,
  ThreadRegistryWorkstreams,
} from "@t3tools/contracts";

const now = "2026-10-02T12:00:00Z";
const baseDraft = {
  source_id: "fixture-source",
  revision: 1,
  transcript_provider: "fixture",
  language: "en",
  edited: false,
  created_at: now,
  updated_at: now,
  due_at: null,
  remaining_ms: null,
  expires_at: "2026-10-03T12:00:00Z",
  command_status: null,
  reason: null,
  server_now: now,
} as const;
export const voiceReviewRecentFixture: VoiceReviewRecentList = {
  schema: "voice.recent-prompts/v1",
  server_now: now,
  partial: false,
  unavailable: [],
  entries: [
    {
      draft: {
        ...baseDraft,
        id: "released-1",
        state: "released",
        text: null,
        command_id: "command-1",
        routing_state: "frozen",
        routing_target: "fixture-thread",
      },
      thread_key: '["fixture-host","fixture-environment","fixture-thread"]',
      text: "Keep the recent prompts readable.\nPreserve literal command text after release.",
      original_source_text:
        "Keep the recent prompts readable. Preserve literal command text after release.",
      text_state: "available",
      text_origin: "retained_command",
      command_id: "command-1",
      associations: [
        {
          subject: "prompt:command-1",
          workstream_ref: "inferred:voice-review",
          state: "active",
          revision: 1,
          origin: "owner",
          job_id: null,
          command_id: "command-1",
        },
      ],
      workstream_refs: ["inferred:voice-review"],
    },
    {
      draft: {
        ...baseDraft,
        id: "paused-1",
        state: "paused",
        text: "Route the next prompt to the same thread.",
        command_id: null,
        routing_state: "proposed",
        routing_target: "fixture-thread",
      },
      thread_key: '["fixture-host","fixture-environment","fixture-thread"]',
      text: "Route the next prompt to the same thread.",
      original_source_text: "Route the next prompt to the same thread.",
      text_state: "available",
      text_origin: "draft",
      command_id: null,
      workstream_refs: ["inferred:voice-review"],
    },
    {
      draft: { ...baseDraft, id: "deleted-1", state: "deleted", text: null, command_id: null },
      text: null,
      original_source_text: null,
      text_state: "deleted",
      text_origin: null,
      command_id: null,
      workstream_refs: [],
    },
  ],
};
export const threadRegistryWorkstreamsFixture: ThreadRegistryWorkstreams = {
  schema: "voice.registry-read/v1",
  snapshot_revision: 3,
  workstreams: [
    {
      label_id: "inferred:voice-review",
      name: "Voice review",
      description: "Recent prompts and routing",
      state: "active",
      revision: 1,
      origin: "owner",
    },
  ],
};
export const threadRegistrySnapshotFixture: ThreadRegistryComposedSnapshot = {
  schema: "voice.registry-read/v1",
  snapshot_revision: 3,
  next_cursor: null,
  partial: true,
  unavailable: ["native_memberships"],
  threads: [
    {
      thread_key: '["fixture-host","fixture-environment","fixture-thread"]',
      registration: { purpose: "Voice review bridge", revision: 1 },
      summary: {
        text: "Implementing recent prompt review and routing metadata.",
        generated_at: now,
        model: "fixture",
        effort: "fixture",
        coverage: "synthetic",
        digest: "fixture",
      },
      activity: { last_activity_at: now, observed_at: now },
      freshness: { stale: false, summary_age_seconds: 0 },
      associations: [
        {
          subject: 'thread:["fixture-host","fixture-environment","fixture-thread"]',
          workstream_ref: "inferred:voice-review",
          state: "active",
          revision: 1,
          origin: "owner",
          job_id: null,
        },
      ],
      native_memberships: [],
    },
  ],
};
