import { createFileRoute } from "@tanstack/react-router";
import { VoiceReviewPage } from "../components/voiceReview/VoiceReviewPage";

export const Route = createFileRoute("/voice-review")({ component: VoiceReviewPage });
