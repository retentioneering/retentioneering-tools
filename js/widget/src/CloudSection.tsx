import * as React from "react";
import { type AuthSession } from "./AuthGate";

interface Props {
  widgetId: string;
  cloudStatus: string;
  session: AuthSession | null;
  enabled: boolean;
  onOpen: () => void;
  onAuthNeeded: (afterAuth: () => void) => void;
}

export function CloudSection({ widgetId, cloudStatus, session, enabled, onOpen, onAuthNeeded }: Props) {
  if (!enabled) return null;

  const isActive = cloudStatus === "saving" || cloudStatus === "loading"
                || cloudStatus === "saved"  || cloudStatus === "loaded";
  const isError  = cloudStatus.startsWith("error:");

  const [activeYellow, setActiveYellow] = React.useState(false);
  const yellowTimer = React.useRef<ReturnType<typeof setTimeout>>();
  React.useEffect(() => {
    if (isActive) {
      clearTimeout(yellowTimer.current);
      setActiveYellow(true);
      // Keep yellow for at least 1.5s so even a fast save is noticeable
      yellowTimer.current = setTimeout(() => setActiveYellow(false), 800);
    }
    return () => clearTimeout(yellowTimer.current);
  }, [cloudStatus]); // react to every status change

  const handleIconClick = () => {
    if (!session) { onAuthNeeded(onOpen); return; }
    onOpen();
  };

  return (
    <div style={{ position: "relative" }}>
      <button
        onClick={handleIconClick}
        title={session ? "Save / load widget state" : "Sign in to save widget state"}
        style={{
          display: "flex", alignItems: "center", justifyContent: "center",
          width: 26, height: 26,
          background: "none", border: "none",
          cursor: "pointer",
          color: activeYellow ? "var(--retentioneering-yellow)" : "#9ca3af",
          transition: "color 0.15s",
        }}
      >
        <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <polyline points="16 16 12 12 8 16"/>
          <line x1="12" y1="12" x2="12" y2="21"/>
          <path d="M20.39 18.39A5 5 0 0 0 18 9h-1.26A8 8 0 1 0 3 16.3"/>
        </svg>
      </button>
    </div>
  );
}

// ── Standalone save/load modal (rendered in the widget root) ──────────────

interface ModalProps {
  widgetId: string;
  nameExists: boolean;
  manageUrl?: string;
  onCheckName: (name: string) => void;
  onSave: (name: string) => void;
  onLoad: () => void;
  onClose: () => void;
}

export function CloudModal({ widgetId, nameExists, manageUrl, onCheckName, onSave, onLoad, onClose }: ModalProps) {
  const [saveName, setSaveName] = React.useState(widgetId || "");
  const [confirmOverwrite, setConfirmOverwrite] = React.useState(false);
  const [checking, setChecking] = React.useState(false);
  const inputRef = React.useRef<HTMLInputElement>(null);
  const checkTimer = React.useRef<ReturnType<typeof setTimeout>>();

  React.useEffect(() => { setTimeout(() => inputRef.current?.focus(), 0); }, []);

  // Debounced name check
  const handleNameChange = (val: string) => {
    setSaveName(val);
    setConfirmOverwrite(false);
    clearTimeout(checkTimer.current);
    const trimmed = val.trim();
    if (trimmed) {
      setChecking(true);
      checkTimer.current = setTimeout(() => {
        onCheckName(trimmed);
        setChecking(false);
      }, 500);
    }
  };

  const handleSave = () => {
    const name = saveName.trim();
    if (!name) return;
    if (nameExists && !confirmOverwrite) {
      setConfirmOverwrite(true);
      return;
    }
    onSave(name);
    onClose();
  };

  React.useEffect(() => { setConfirmOverwrite(false); }, [saveName]);

  return (
    <div
      onClick={onClose}
      style={{ position: "absolute", inset: 0, zIndex: 50, background: "rgba(0,0,0,0.25)", display: "flex", alignItems: "center", justifyContent: "center" }}
    >
      <div
        onClick={e => e.stopPropagation()}
        style={{ background: "#fff", borderRadius: 12, padding: "24px 28px", width: 380, boxShadow: "0 20px 40px rgba(0,0,0,0.18)", display: "flex", flexDirection: "column", gap: 16 }}
      >
        {/* Header */}
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
          <span style={{ fontSize: 15, fontWeight: 600, color: "#111827" }}>Save widget state</span>
          <button onClick={onClose} style={{ background: "none", border: "none", cursor: "pointer", color: "#9ca3af", padding: 2, lineHeight: 1 }}>
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M18 6 6 18M6 6l12 12"/></svg>
          </button>
        </div>

        {/* Note */}
        <div style={{ fontSize: 12, color: "#6b7280", lineHeight: 1.5, background: "#f9fafb", borderRadius: 8, padding: "8px 12px" }}>
          Only the widget configuration (filters, diff settings, layout) is saved — not the eventstream data itself.
        </div>

        {/* Save row */}
        <div>
          <label style={{ fontSize: 12, fontWeight: 500, color: "#374151", display: "block", marginBottom: 6 }}>Save name</label>
          <div style={{ display: "flex", gap: 8 }}>
            <input
              ref={inputRef}
              value={saveName}
              onChange={e => handleNameChange(e.target.value)}
              onKeyDown={e => { if (e.key === "Enter") handleSave(); if (e.key === "Escape") onClose(); }}
              placeholder="e.g. my-analysis"
              style={{ flex: 1, minWidth: 0, boxSizing: "border-box" as const, border: "1px solid #d1d5db", borderRadius: 7, padding: "7px 10px", fontSize: 13, outline: "none", color: "#111827" }}
            />
            <button
              onClick={handleSave}
              disabled={!saveName.trim()}
              style={{ padding: "7px 16px", borderRadius: 7, background: saveName.trim() ? "hsl(45,93%,58%)" : "#f3f4f6", border: "none", cursor: saveName.trim() ? "pointer" : "default", fontSize: 13, fontWeight: 600, color: saveName.trim() ? "#1a1a1a" : "#9ca3af", flexShrink: 0 }}
            >
              Save
            </button>
          </div>

          {/* Overwrite confirmation */}
          {confirmOverwrite && (
            <div style={{ marginTop: 10, background: "#fffbeb", border: "1px solid #fde68a", borderRadius: 8, padding: "10px 12px" }}>
              <div style={{ fontSize: 12, color: "#92400e", marginBottom: 8 }}>
                <strong>"{saveName.trim()}"</strong> already exists. Overwrite it?
              </div>
              <div style={{ display: "flex", gap: 8 }}>
                <button onClick={() => { onSave(saveName.trim()); onClose(); }}
                  style={{ flex: 1, padding: "5px 0", background: "#ef4444", border: "none", borderRadius: 6, cursor: "pointer", fontSize: 12, fontWeight: 600, color: "#fff" }}>
                  Overwrite
                </button>
                <button onClick={() => setConfirmOverwrite(false)}
                  style={{ flex: 1, padding: "5px 0", background: "#f3f4f6", border: "1px solid #d1d5db", borderRadius: 6, cursor: "pointer", fontSize: 12, color: "#374151" }}>
                  Cancel
                </button>
              </div>
            </div>
          )}
        </div>

        {/* Load + manage */}
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", borderTop: "1px solid #f3f4f6", paddingTop: 12 }}>
          {widgetId ? (
            <button onClick={() => { onLoad(); onClose(); }} style={{ background: "none", border: "none", cursor: "pointer", padding: 0, fontSize: 12, color: "#6b7280" }}>
              ↓ Load "{widgetId}"
            </button>
          ) : <span />}
          {manageUrl && (
            <span onClick={() => window.open(manageUrl, "_blank")}
              style={{ fontSize: 11, color: "#9ca3af", cursor: "pointer" }}
              onMouseEnter={e => (e.currentTarget.style.color = "#6b7280")}
              onMouseLeave={e => (e.currentTarget.style.color = "#9ca3af")}>
              Manage saved widgets ↗
            </span>
          )}
        </div>
      </div>
    </div>
  );
}

// ── Cloud error modal ──────────────────────────────────────────────────────

interface ErrorModalProps {
  message: string;
  onClose: () => void;
}

export function CloudErrorModal({ message, onClose }: ErrorModalProps) {
  return (
    <div onClick={onClose}
      style={{ position: "absolute", inset: 0, zIndex: 50, background: "rgba(0,0,0,0.25)", display: "flex", alignItems: "center", justifyContent: "center" }}>
      <div onClick={e => e.stopPropagation()}
        style={{ background: "#fff", borderRadius: 12, padding: "24px 28px", width: 380, boxShadow: "0 20px 40px rgba(0,0,0,0.18)", display: "flex", flexDirection: "column", gap: 16 }}>
        {/* Header */}
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="#ef4444" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/>
          </svg>
          <span style={{ fontSize: 15, fontWeight: 600, color: "#111827" }}>Cloud storage error</span>
        </div>
        {/* Message */}
        <p style={{ fontSize: 13, color: "#6b7280", margin: 0, lineHeight: 1.5 }}>{message}</p>
        {/* OK button */}
        <button onClick={onClose}
          style={{ padding: "8px 0", background: "#f3f4f6", border: "1px solid #e5e7eb", borderRadius: 8, cursor: "pointer", fontSize: 13, fontWeight: 500, color: "#111827" }}>
          OK
        </button>
      </div>
    </div>
  );
}

