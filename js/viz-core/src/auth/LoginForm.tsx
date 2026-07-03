import * as React from "react";
import { sendOtp, verifyOtp, type AuthSession } from "./authClient";

export interface LoginFormProps {
  onSuccess: (session: AuthSession) => void;
  isDark?: boolean;
}

type Step = "email" | "otp" | "loading";

export function LoginForm({ onSuccess, isDark = true }: LoginFormProps) {
  const [step, setStep]       = React.useState<Step>("email");
  const [email, setEmail]     = React.useState("");
  const [code, setCode]       = React.useState("");
  const [error, setError]     = React.useState("");

  const bg      = isDark ? "#1f2937" : "#f9fafb";
  const border  = isDark ? "#374151" : "#e5e7eb";
  const text    = isDark ? "#f3f4f6" : "#111827";
  const muted   = isDark ? "#9ca3af" : "#6b7280";
  const accent  = "hsl(45, 93%, 58%)";

  const inputStyle: React.CSSProperties = {
    width: "100%",
    boxSizing: "border-box",
    background: isDark ? "#111827" : "#fff",
    border: `1px solid ${border}`,
    borderRadius: 8,
    color: text,
    fontSize: 14,
    padding: "10px 14px",
    outline: "none",
  };

  const btnPrimary: React.CSSProperties = {
    width: "100%",
    padding: "10px 0",
    background: accent,
    border: "none",
    borderRadius: 8,
    color: "hsl(0, 0%, 10%)",
    fontSize: 14,
    fontWeight: 700,
    cursor: "pointer",
  };

  const btnSecondary: React.CSSProperties = {
    background: "transparent",
    border: "none",
    color: muted,
    fontSize: 12,
    cursor: "pointer",
    padding: 0,
    marginTop: 8,
  };

  async function handleSendOtp(e: React.FormEvent) {
    e.preventDefault();
    setError("");
    setStep("loading");
    try {
      await sendOtp(email.trim().toLowerCase());
      setStep("otp");
    } catch (err) {
      setError((err as Error).message);
      setStep("email");
    }
  }

  async function handleVerifyOtp(e: React.FormEvent) {
    e.preventDefault();
    setError("");
    setStep("loading");
    try {
      const session = await verifyOtp(email.trim().toLowerCase(), code.trim());
      onSuccess(session);
    } catch (err) {
      setError((err as Error).message);
      setStep("otp");
    }
  }

  return (
    <div
      style={{
        background: bg,
        border: `1px solid ${border}`,
        borderRadius: 12,
        padding: "24px 28px",
        width: 320,
        display: "flex",
        flexDirection: "column",
        gap: 16,
      }}
    >
      {/* Header */}
      <div>
        <div style={{ fontSize: 16, fontWeight: 700, color: text, marginBottom: 4 }}>
          {step === "otp" ? "Enter the code" : "Sign in to Retentioneering"}
        </div>
        <div style={{ fontSize: 12, color: muted }}>
          {step === "otp"
            ? `We sent a 6-digit code to ${email}`
            : "Enter your email to receive a one-time code"}
        </div>
      </div>

      {/* Email form */}
      {(step === "email" || step === "loading") && step !== "otp" && (
        <form onSubmit={handleSendOtp} style={{ display: "flex", flexDirection: "column", gap: 10 }}>
          <input
            type="email"
            placeholder="you@company.com"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            required
            autoFocus
            disabled={step === "loading"}
            style={inputStyle}
          />
          {error && <div style={{ fontSize: 12, color: "#ef4444" }}>{error}</div>}
          <button type="submit" disabled={step === "loading"} style={btnPrimary}>
            {step === "loading" ? "Sending…" : "Send code"}
          </button>
        </form>
      )}

      {/* OTP form */}
      {step === "otp" && (
        <form onSubmit={handleVerifyOtp} style={{ display: "flex", flexDirection: "column", gap: 10 }}>
          <input
            type="text"
            inputMode="numeric"
            pattern="[0-9]{6}"
            placeholder="123456"
            value={code}
            onChange={(e) => setCode(e.target.value.replace(/\D/g, "").slice(0, 6))}
            required
            autoFocus
            style={{ ...inputStyle, letterSpacing: "0.2em", fontSize: 20, textAlign: "center" }}
          />
          {error && <div style={{ fontSize: 12, color: "#ef4444" }}>{error}</div>}
          <button type="submit" style={btnPrimary}>Verify</button>
          <button
            type="button"
            style={btnSecondary}
            onClick={() => { setStep("email"); setCode(""); setError(""); }}
          >
            ← Use a different email
          </button>
        </form>
      )}

      {step === "loading" && step !== "email" && (
        <div style={{ display: "flex", alignItems: "center", gap: 8, color: muted, fontSize: 13 }}>
          <div style={{ width: 16, height: 16, border: `2px solid ${border}`, borderTop: `2px solid ${accent}`, borderRadius: "50%", animation: "retentioneering-spin 0.8s linear infinite" }} />
          Verifying…
        </div>
      )}
    </div>
  );
}
