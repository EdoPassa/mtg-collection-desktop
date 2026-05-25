import React, { useEffect, useState } from "react";
import type { BackendApi } from "./backend";
import { StatusPill } from "./components/StatusPill";
import { defaultSectionId, getSection, navigation, type SectionId } from "./navigation";
import { sectionPanels } from "./panels";

export function App({ api }: { api: BackendApi }) {
  const [active, setActive] = useState<SectionId>(defaultSectionId);
  const [status, setStatus] = useState("loading");
  const [message, setMessage] = useState("");

  useEffect(() => {
    api.ResolverStatus().then(setStatus).catch((error) => setStatus(`error: ${String(error)}`));
  }, [api]);

  const section = getSection(active);
  const ActivePanel = sectionPanels[active];
  const messageIsError = message.toLowerCase().includes("error") || message.startsWith("TypeError");

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark" aria-hidden="true">
            MTG
          </div>
          <p className="brand-title">MTG Collection</p>
          <p>Local tracker · Scryfall</p>
        </div>

        <nav aria-label="Application sections" className="tabs">
          {navigation.map((entry) =>
            entry.type === "item" ? (
              <NavButton key={entry.section.id} section={entry.section} active={active} onSelect={setActive} onClearMessage={() => setMessage("")} />
            ) : (
              <div key={entry.label} className="nav-group">
                <p className="nav-group-label">{entry.label}</p>
                {entry.sections.map((sectionConfig) => (
                  <NavButton
                    key={sectionConfig.id}
                    section={sectionConfig}
                    active={active}
                    onSelect={setActive}
                    onClearMessage={() => setMessage("")}
                  />
                ))}
              </div>
            )
          )}
        </nav>

        <div className="sidebar-footer">
          <StatusPill status={status} />
        </div>
      </aside>

      <div className="app-main">
        <header className="app-header">
          <div>
            <h2>{section.title}</h2>
            <p className="subtitle">{section.description}</p>
          </div>
        </header>

        {message && <p className={`message${messageIsError ? " message--error" : ""}`}>{message}</p>}

        <ActivePanel api={api} setMessage={setMessage} />
      </div>
    </div>
  );
}

function NavButton({
  section,
  active,
  onSelect,
  onClearMessage
}: {
  section: ReturnType<typeof getSection>;
  active: SectionId;
  onSelect: (id: SectionId) => void;
  onClearMessage: () => void;
}) {
  return (
    <button
      type="button"
      className="tab-btn"
      aria-current={active === section.id ? "page" : undefined}
      onClick={() => {
        onSelect(section.id);
        onClearMessage();
      }}
    >
      <span className="tab-icon" aria-hidden="true">
        {section.icon}
      </span>
      {section.label}
    </button>
  );
}
