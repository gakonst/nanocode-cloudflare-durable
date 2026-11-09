import { createRoot } from "react-dom/client";
import { Demo } from "./agent.tsx";
import "./page.css";

createRoot(document.getElementById("root")!).render(<Demo theme="claude" title="Claude-style demo" />);
