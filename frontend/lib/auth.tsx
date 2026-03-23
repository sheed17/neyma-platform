"use client";

import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from "react";
import { usePathname, useRouter } from "next/navigation";
import type { AuthChangeEvent, Session } from "@supabase/supabase-js";

import { bootstrapGuestSession, getAccessState } from "@/lib/api";
import { hasSupabaseEnv, getSupabaseClient } from "@/lib/supabase";
import type { AccessState } from "@/lib/types";

type User = { email: string; name: string };

type AuthContextType = {
  user: User | null;
  access: AccessState | null;
  loading: boolean;
  accessLoading: boolean;
  login: (email: string, password: string) => Promise<void>;
  register: (name: string, email: string, password: string) => Promise<void>;
  requestPasswordReset: (email: string) => Promise<void>;
  updatePassword: (password: string) => Promise<void>;
  loginAsTestUser: () => Promise<void>;
  logout: () => Promise<void>;
  refreshAccess: () => Promise<AccessState | null>;
};

const STORAGE_KEY = "neyma_user";
const ACCESS_TOKEN_KEY = "neyma_access_token";
const TEST_USER: User = { email: "test@neyma.local", name: "Neyma Test" };
const AUTH_EMAIL_COOKIE = "neyma_auth_email";
const AUTH_NAME_COOKIE = "neyma_auth_name";

const AuthContext = createContext<AuthContextType | null>(null);

const PUBLIC_PATHS = new Set(["/", "/login", "/register"]);
const GUEST_ALLOWED_PATTERNS = [
  /^\/territory\/new$/,
  /^\/territory\/[^/]+$/,
  /^\/diagnostic\/new$/,
  /^\/diagnostic\/\d+$/,
];

function cookieString(name: string, value: string, maxAge: number) {
  return `${name}=${encodeURIComponent(value)}; Max-Age=${maxAge}; Path=/; SameSite=Lax`;
}

function persistLocalUser(user: User | null) {
  if (typeof window === "undefined") return;
  if (user) {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(user));
    return;
  }
  window.localStorage.removeItem(STORAGE_KEY);
}

function persistAccessToken(token: string | null) {
  if (typeof window === "undefined") return;
  if (token) {
    window.localStorage.setItem(ACCESS_TOKEN_KEY, token);
    return;
  }
  window.localStorage.removeItem(ACCESS_TOKEN_KEY);
}

function syncAuthCookies(user: User | null) {
  if (typeof document === "undefined") return;
  if (user) {
    document.cookie = cookieString(AUTH_EMAIL_COOKIE, user.email, 60 * 60 * 24 * 365);
    document.cookie = cookieString(AUTH_NAME_COOKIE, user.name, 60 * 60 * 24 * 365);
    return;
  }
  document.cookie = cookieString(AUTH_EMAIL_COOKIE, "", 0);
  document.cookie = cookieString(AUTH_NAME_COOKIE, "", 0);
}

function clearGuestSession() {
  if (typeof window === "undefined") return;
  window.localStorage.removeItem("neyma_guest_session");
}

function readStoredUser(): User | null {
  if (typeof window === "undefined") return null;
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    return raw ? (JSON.parse(raw) as User) : null;
  } catch {
    return null;
  }
}

function mapSessionUser(session: Session | null): User | null {
  const sessionUser = session?.user;
  if (!sessionUser?.email) return null;
  const metadataName =
    typeof sessionUser.user_metadata?.name === "string" ? sessionUser.user_metadata.name :
    typeof sessionUser.user_metadata?.full_name === "string" ? sessionUser.user_metadata.full_name :
    null;
  return {
    email: sessionUser.email,
    name: metadataName?.trim() || sessionUser.email.split("@")[0],
  };
}

function applyUserState(setUser: (user: User | null) => void, user: User | null) {
  persistLocalUser(user);
  syncAuthCookies(user);
  setUser(user);
}

function applySessionState(setUser: (user: User | null) => void, session: Session | null) {
  persistAccessToken(session?.access_token ?? null);
  applyUserState(setUser, mapSessionUser(session));
}

function isGuestAllowedPath(pathname: string | null): boolean {
  const path = pathname || "/";
  return GUEST_ALLOWED_PATTERNS.some((pattern) => pattern.test(path));
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [access, setAccess] = useState<AccessState | null>(null);
  const [loading, setLoading] = useState(true);
  const [accessLoading, setAccessLoading] = useState(true);

  const refreshAccess = useCallback(async () => {
    setAccessLoading(true);
    try {
      const nextAccess = user ? await getAccessState() : await bootstrapGuestSession();
      setAccess(nextAccess);
      return nextAccess;
    } catch {
      setAccess(null);
      return null;
    } finally {
      setAccessLoading(false);
    }
  }, [user]);

  useEffect(() => {
    let mounted = true;
    const fallbackUser = readStoredUser();
    if (fallbackUser) {
      applyUserState((next) => mounted && setUser(next), fallbackUser);
    } else {
      syncAuthCookies(null);
    }

    if (!hasSupabaseEnv()) {
      setLoading(false);
      return;
    }

    const supabase = getSupabaseClient();

    supabase.auth.getSession().then(({ data, error }) => {
      if (!mounted) return;
      if (error) {
        setLoading(false);
        return;
      }
      applySessionState(setUser, data.session);
      setLoading(false);
    });

    const { data: listener } = supabase.auth.onAuthStateChange(
      (_event: AuthChangeEvent, session: Session | null) => {
        if (!mounted) return;
        applySessionState(setUser, session);
        setLoading(false);
      },
    );

    return () => {
      mounted = false;
      listener.subscription.unsubscribe();
    };
  }, []);

  useEffect(() => {
    if (loading) return;
    void refreshAccess();
  }, [loading, refreshAccess]);

  const login = useCallback(async (email: string, password: string) => {
    if (!hasSupabaseEnv()) {
      throw new Error("Supabase is not configured yet. Add NEXT_PUBLIC_SUPABASE_URL and NEXT_PUBLIC_SUPABASE_ANON_KEY to frontend/.env.local.");
    }
    const supabase = getSupabaseClient();
    const { data, error } = await supabase.auth.signInWithPassword({
      email: email.trim(),
      password,
    });
    if (error) throw new Error(error.message);
    applySessionState(setUser, data.session);
    setAccessLoading(true);
    try {
      const nextAccess = await getAccessState();
      setAccess(nextAccess);
      if (nextAccess.viewer.is_guest) {
        throw new Error("We couldn't finish signing you in. Please try again.");
      }
    } finally {
      setAccessLoading(false);
    }
  }, []);

  const register = useCallback(async (name: string, email: string, password: string) => {
    if (!hasSupabaseEnv()) {
      throw new Error("Supabase is not configured yet. Add NEXT_PUBLIC_SUPABASE_URL and NEXT_PUBLIC_SUPABASE_ANON_KEY to frontend/.env.local.");
    }
    const supabase = getSupabaseClient();
    const { data, error } = await supabase.auth.signUp({
      email: email.trim(),
      password,
      options: {
        data: {
          name: name.trim(),
          full_name: name.trim(),
        },
      },
    });
    if (error) throw new Error(error.message);
    const mapped = mapSessionUser(data.session);
    if (mapped) {
      applySessionState(setUser, data.session);
      return;
    }
    persistAccessToken(null);
    applyUserState(setUser, null);
  }, []);

  const requestPasswordReset = useCallback(async (email: string) => {
    if (!hasSupabaseEnv()) {
      throw new Error("Supabase is not configured yet. Add NEXT_PUBLIC_SUPABASE_URL and NEXT_PUBLIC_SUPABASE_ANON_KEY to frontend/.env.local.");
    }
    const supabase = getSupabaseClient();
    const origin =
      typeof window !== "undefined" && window.location.origin
        ? window.location.origin
        : "http://localhost:3000";
    const { error } = await supabase.auth.resetPasswordForEmail(email.trim(), {
      redirectTo: `${origin}/reset-password`,
    });
    if (error) throw new Error(error.message);
  }, []);

  const updatePassword = useCallback(async (password: string) => {
    if (!hasSupabaseEnv()) {
      throw new Error("Supabase is not configured yet. Add NEXT_PUBLIC_SUPABASE_URL and NEXT_PUBLIC_SUPABASE_ANON_KEY to frontend/.env.local.");
    }
    const supabase = getSupabaseClient();
    const { data, error } = await supabase.auth.updateUser({ password });
    if (error) throw new Error(error.message);
    applySessionState(setUser, data.user ? await supabase.auth.getSession().then(({ data: s }) => s.session) : null);
    setAccessLoading(true);
    try {
      const nextAccess = await getAccessState();
      setAccess(nextAccess);
    } finally {
      setAccessLoading(false);
    }
  }, []);

  const loginAsTestUser = useCallback(async () => {
    await new Promise((r) => setTimeout(r, 250));
    applyUserState(setUser, TEST_USER);
    setAccessLoading(true);
    try {
      const nextAccess = await getAccessState();
      setAccess(nextAccess);
    } finally {
      setAccessLoading(false);
    }
  }, []);

  const logout = useCallback(async () => {
    if (hasSupabaseEnv()) {
      const supabase = getSupabaseClient();
      await supabase.auth.signOut();
    }
    persistAccessToken(null);
    clearGuestSession();
    applyUserState(setUser, null);
    setAccess(null);
  }, []);

  const value = useMemo<AuthContextType>(
    () => ({
      user,
      access,
      loading,
      accessLoading,
      login,
      register,
      requestPasswordReset,
      updatePassword,
      loginAsTestUser,
      logout,
      refreshAccess,
    }),
    [user, access, loading, accessLoading, login, register, requestPasswordReset, updatePassword, loginAsTestUser, logout, refreshAccess],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within AuthProvider");
  return ctx;
}

export function AuthGuard({ children }: { children: ReactNode }) {
  const { user, loading } = useAuth();
  const router = useRouter();
  const pathname = usePathname();
  const canEnterAsGuest = PUBLIC_PATHS.has(pathname || "/") || isGuestAllowedPath(pathname);

  useEffect(() => {
    if (!loading && !user && !canEnterAsGuest) {
      router.replace("/login");
    }
  }, [user, loading, canEnterAsGuest, router]);

  if (loading) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-zinc-50">
        <div className="text-sm text-zinc-400">Loading…</div>
      </div>
    );
  }

  if (!user && !canEnterAsGuest) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-zinc-50">
        <div className="text-sm text-zinc-400">Redirecting to login…</div>
      </div>
    );
  }

  return <>{children}</>;
}

export function isGuestEntryPath(pathname: string | null) {
  return isGuestAllowedPath(pathname);
}
