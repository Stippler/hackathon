import { createClient, type Session, type SupabaseClient } from "@supabase/supabase-js";

let cachedClient: SupabaseClient | null = null;
let cachedKey = "";

type SafeSessionResult = {
  session: Session | null;
  error: Error | null;
};

function isInvalidRefreshTokenError(message: string): boolean {
  const normalized = message.toLowerCase();
  return normalized.includes("invalid refresh token") || normalized.includes("refresh token not found");
}

function normalizeError(error: unknown): Error {
  if (error instanceof Error) {
    return error;
  }
  return new Error(typeof error === "string" ? error : "Unknown auth error");
}

export async function getSessionSafe(supabase: SupabaseClient): Promise<SafeSessionResult> {
  try {
    const { data, error } = await supabase.auth.getSession();
    if (error) {
      if (isInvalidRefreshTokenError(error.message)) {
        await supabase.auth.signOut({ scope: "local" });
      }
      return { session: null, error };
    }
    return { session: data.session ?? null, error: null };
  } catch (error: unknown) {
    const authError = normalizeError(error);
    if (isInvalidRefreshTokenError(authError.message)) {
      await supabase.auth.signOut({ scope: "local" });
    }
    return { session: null, error: authError };
  }
}

export function getSupabaseAuthClient(url: string, anonKey: string): SupabaseClient {
  const currentKey = `${url}::${anonKey}`;

  if (!cachedClient || cachedKey !== currentKey) {
    cachedClient = createClient(url, anonKey, {
      auth: {
        persistSession: true,
        autoRefreshToken: true,
      },
    });
    cachedKey = currentKey;
  }

  return cachedClient;
}
