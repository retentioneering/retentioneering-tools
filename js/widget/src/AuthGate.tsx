import * as React from "react";
import { LoginForm, loadSession, clearSession, refreshSession, type AuthSession } from "@retentioneering/viz-core";

export { loadSession, clearSession, refreshSession };
export type { AuthSession };

interface AuthGateProps {
  session: AuthSession | null;
  onLogin: (s: AuthSession) => void;
  onClose?: () => void;
  disabled?: boolean;
  title?: string;
  description?: React.ReactNode;
  style?: React.CSSProperties;
  children: React.ReactNode;
}

export function AuthGate({ session, onLogin, onClose, disabled, title, description, style, children }: AuthGateProps) {
  const showOverlay = !disabled && !session;

  React.useEffect(() => {
    if (!showOverlay || !onClose) return;
    const handler = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [showOverlay, onClose]);

  // Always render the same wrapper div to keep DOM structure stable for Cytoscape
  return (
    <div style={{ display: "flex", position: "relative", ...style }}>
      <div style={{ display: "flex", flex: 1, filter: showOverlay ? "blur(4px)" : "none", pointerEvents: showOverlay ? "none" : undefined, minWidth: 0, minHeight: 0 }}>
        {children}
      </div>

      {showOverlay && (
        <div style={{
          position: "absolute", inset: 0,
          background: "rgba(255,255,255,0.88)",
          backdropFilter: "blur(2px)",
          display: "flex", flexDirection: "column",
          alignItems: "center", justifyContent: "center",
          gap: 20, zIndex: 40, padding: "0 24px",
        }}>
          {onClose && (
            <button onClick={onClose} style={{ position: "absolute", top: 12, right: 12, background: "none", border: "none", cursor: "pointer", color: "#9ca3af", fontSize: 20, lineHeight: 1, padding: 4 }}>×</button>
          )}
          <div style={{ textAlign: "center", maxWidth: 340 }}>
            <div style={{ fontSize: 22, marginBottom: 8 }}>🔓</div>
            <div style={{ color: "#111827", fontSize: 16, fontWeight: 700, marginBottom: 6 }}>
              {title ?? "Sign in to Retentioneering"}
            </div>
            {description && (
              <div style={{ color: "#6b7280", fontSize: 13, lineHeight: 1.6 }}>
                {description}
              </div>
            )}
          </div>
          <LoginForm onSuccess={onLogin} isDark={false} />
        </div>
      )}
      <style>{`@keyframes retentioneering-spin { to { transform: rotate(360deg); } }`}</style>
    </div>
  );
}
