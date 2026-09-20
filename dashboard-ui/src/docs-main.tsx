import { createRoot } from "react-dom/client";
import { DocumentationSite } from "./docs";
import "./styles.css";
import "./docs.css";

createRoot(document.getElementById("root")!).render(<DocumentationSite />);
