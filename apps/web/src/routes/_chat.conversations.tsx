import { createFileRoute } from "@tanstack/react-router";

import { ConversationLibraryPage } from "../components/conversations/ConversationLibraryPage";

export const Route = createFileRoute("/_chat/conversations")({
  component: ConversationLibraryPage,
});
