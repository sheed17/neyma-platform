"use client";

import { createClient, type SupabaseClient } from "@supabase/supabase-js";

let client: SupabaseClient | null = null;

function readEnv() {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const anonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;
  return {
    url: typeof url === "string" ? url.trim() : "",
    anonKey: typeof anonKey === "string" ? anonKey.trim() : "",
  };
}

export function hasSupabaseEnv() {
  const { url, anonKey } = readEnv();
  return Boolean(url && anonKey);
}

export function getSupabaseClient() {
  if (client) return client;
  const { url, anonKey } = readEnv();
  if (!url || !anonKey) {
    throw new Error("Supabase env vars are missing. Add NEXT_PUBLIC_SUPABASE_URL and NEXT_PUBLIC_SUPABASE_ANON_KEY to frontend/.env.local.");
  }
  client = createClient(url, anonKey, {
    auth: {
      persistSession: true,
      autoRefreshToken: true,
      detectSessionInUrl: true,
    },
  });
  return client;
}
