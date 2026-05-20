import React from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import { createBackendApi } from "./backend";
import "./styles.css";

createRoot(document.getElementById("root")!).render(<App api={createBackendApi()} />);
