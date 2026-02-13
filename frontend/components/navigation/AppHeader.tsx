"use client";

import Image from "next/image";
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useEffect, useMemo, useState } from "react";
import { clearAuthCookie } from "@/lib/auth-cookie";
import { getSessionSafe, getSupabaseAuthClient } from "@/lib/supabase-auth";

export function AppHeader() {
  const pathname = usePathname();
  const router = useRouter();
  const [isAuthenticated, setIsAuthenticated] = useState(false);

  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL || "";
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || "";

  const hasSupabaseConfig = Boolean(supabaseUrl && supabaseAnonKey);
  const isChatRoute = pathname.startsWith("/chat");

  const supabase = useMemo(() => {
    if (!hasSupabaseConfig) {
      return null;
    }
    return getSupabaseAuthClient(supabaseUrl, supabaseAnonKey);
  }, [hasSupabaseConfig, supabaseAnonKey, supabaseUrl]);

  useEffect(() => {
    if (!supabase) {
      return;
    }

    let isMounted = true;

    async function syncSession() {
      const { session } = await getSessionSafe(supabase);
      if (isMounted) {
        const hasSession = Boolean(session);
        setIsAuthenticated(hasSession);
        if (!hasSession) {
          clearAuthCookie();
        }
      }
    }

    void syncSession();

    const {
      data: { subscription },
    } = supabase.auth.onAuthStateChange((_event, session) => {
      const hasSession = Boolean(session);
      setIsAuthenticated(hasSession);
      if (!hasSession) {
        clearAuthCookie();
      }
    });

    return () => {
      isMounted = false;
      subscription.unsubscribe();
    };
  }, [supabase]);

  async function handleLogout() {
    if (!supabase) {
      return;
    }
    await supabase.auth.signOut();
    clearAuthCookie();
    setIsAuthenticated(false);
    router.replace("/login");
  }

  return (
    <header className="sticky top-0 z-30 h-16 shrink-0 border-b border-white/10 bg-black/50 px-6 backdrop-blur">
      <div className="mx-auto flex h-full w-full max-w-6xl items-center justify-between">
        <Link href="/" className="inline-flex items-center gap-2">
          <Image src="/icon.png" alt="Grablin logo" width={36} height={36} className="h-9 w-9 object-cover" />
          <span className="text-sm font-semibold tracking-wide text-gray-100">Grablin</span>
        </Link>

        <div className="flex items-center gap-2">
          {isChatRoute && isAuthenticated ? (
            <button
              type="button"
              onClick={handleLogout}
              className="rounded-lg border border-white/20 bg-white/5 px-3 py-1.5 text-xs font-medium text-gray-100 transition hover:bg-white/10"
            >
              Logout
            </button>
          ) : null}
        </div>
      </div>
    </header>
  );
}
