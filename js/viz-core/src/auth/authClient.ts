import { createClient } from "@supabase/supabase-js";

// Build-time constants injected by Vite define. Empty unless whoever builds
// the wheel sets RETENTIONEERING_CLOUD_SUPABASE_URL and
// RETENTIONEERING_CLOUD_SUPABASE_ANON_KEY — no backend ships by default.
declare const __SUPABASE_URL__: string;
declare const __SUPABASE_ANON_KEY__: string;

// ── types ─────────────────────────────────────────────────────────────────

export interface AuthUser {
  id: string;
  email: string;
  name: string;
  role: string;
}

export interface AuthSession {
  user: AuthUser;
  access_token: string;
  refresh_token: string;
  expires_at: number; // unix timestamp (seconds) — actual Supabase JWT expiry
}

// ── localStorage ──────────────────────────────────────────────────────────

const LS_KEY = "retentioneering_auth_session";

function jwtExpiry(token: string): number {
  try {
    const part = token.split(".")[1].replace(/-/g, "+").replace(/_/g, "/");
    return JSON.parse(atob(part)).exp ?? 0;
  } catch { return 0; }
}

export function loadSession(): AuthSession | null {
  if (typeof window === "undefined") return null;
  try {
    const raw = window.localStorage.getItem(LS_KEY);
    if (!raw) return null;
    const s: AuthSession = JSON.parse(raw);
    // Check both stored expires_at and actual JWT expiry (60s buffer)
    const exp = Math.min(s.expires_at ?? Infinity, jwtExpiry(s.access_token));
    if (Date.now() / 1000 > exp - 60) {
      window.localStorage.removeItem(LS_KEY);
      return null;
    }
    return s;
  } catch {
    return null;
  }
}

export async function refreshSession(): Promise<AuthSession | null> {
  if (typeof window === "undefined") return null;
  try {
    const raw = window.localStorage.getItem(LS_KEY);
    if (!raw) return null;
    const s: AuthSession = JSON.parse(raw);
    if (!s.refresh_token) return null;
    const { data, error } = await getSupabase().auth.refreshSession({ refresh_token: s.refresh_token });
    if (error || !data.session) { clearSession(); return null; }
    const refreshed: AuthSession = {
      ...s,
      access_token: data.session.access_token,
      refresh_token: data.session.refresh_token,
      expires_at: data.session.expires_at,
    };
    saveSession(refreshed);
    return refreshed;
  } catch {
    return null;
  }
}

function saveSession(session: AuthSession) {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(LS_KEY, JSON.stringify(session));
}

export function clearSession() {
  if (typeof window === "undefined") return;
  window.localStorage.removeItem(LS_KEY);
}

// ── Supabase client ───────────────────────────────────────────────────────

function getSupabase() {
  // Use build-time constants; fall back to empty string (will produce a clear error)
  const url = typeof __SUPABASE_URL__ !== "undefined" ? __SUPABASE_URL__ : "";
  const key = typeof __SUPABASE_ANON_KEY__ !== "undefined" ? __SUPABASE_ANON_KEY__ : "";

  if (!url || !key) {
    throw new Error(
      "This build of retentioneering was compiled without Supabase credentials. " +
      "Please rebuild with RETENTIONEERING_CLOUD_SUPABASE_URL and RETENTIONEERING_CLOUD_SUPABASE_ANON_KEY set.",
    );
  }

  return createClient(url, key, {
    auth: { persistSession: false, autoRefreshToken: false },
  });
}

// ── auth operations ───────────────────────────────────────────────────────

export async function sendOtp(email: string): Promise<void> {
  const { error } = await getSupabase().auth.signInWithOtp({
    email,
    options: { shouldCreateUser: false },
  });
  if (error) throw new Error(error.message);
}

export async function verifyOtp(email: string, token: string): Promise<AuthSession> {
  const { data, error } = await getSupabase().auth.verifyOtp({
    email,
    token,
    type: "email",
  });

  if (error || !data.user || !data.session) {
    throw new Error(error?.message ?? "Invalid or expired code");
  }

  const user = data.user;
  const role = user.app_metadata?.roles?.[0] || user.app_metadata?.role || "user";

  const session: AuthSession = {
    user: {
      id: user.id,
      email: user.email!,
      name: user.user_metadata?.name || user.email!.split("@")[0],
      role,
    },
    access_token: data.session.access_token,
    refresh_token: data.session.refresh_token,
    expires_at: data.session.expires_at,
  };

  saveSession(session);
  return session;
}
