import React from "react";
import ReactDOM from "react-dom/client";
import "./index.css";
import { WorkQueuePreview } from "./components/workQueue/WorkQueuePreview";

document.title = "Submitted work — Jones Code";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <WorkQueuePreview />
  </React.StrictMode>,
);
