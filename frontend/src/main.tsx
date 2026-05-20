import React from "react";
import { createRoot } from "react-dom/client";
import "./styles.css";

const sections = ["Import", "Collection", "Decks / Compare", "Lending"];

function App() {
  return (
    <main className="app-shell">
      <header>
        <h1>MTG Collection</h1>
        <p>Go + Wails rewrite shell. Backend bindings will power each workflow.</p>
      </header>
      <nav aria-label="Application sections">
        {sections.map((section) => (
          <button key={section} type="button">
            {section}
          </button>
        ))}
      </nav>
      <section className="panel">
        <h2>Implementation Ready</h2>
        <p>
          The UI shell is split into the four primary workflows from the Python app:
          import, collection, deck compare/build, and lending.
        </p>
      </section>
    </main>
  );
}

createRoot(document.getElementById("root")!).render(<App />);
