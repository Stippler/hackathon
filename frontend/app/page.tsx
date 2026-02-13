"use client";

import Image from "next/image";
import { useRouter } from "next/navigation";
import { useMemo, useState } from "react";
import { AppHeader } from "@/components/navigation/AppHeader";
import { getSessionSafe, getSupabaseAuthClient } from "@/lib/supabase-auth";

export default function LandingPage() {
  const router = useRouter();
  const [isCheckingSession, setIsCheckingSession] = useState(false);
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL || "";
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || "";
  const supabase = useMemo(() => {
    if (!supabaseUrl || !supabaseAnonKey) {
      return null;
    }
    return getSupabaseAuthClient(supabaseUrl, supabaseAnonKey);
  }, [supabaseAnonKey, supabaseUrl]);

  async function handleGetStarted() {
    if (isCheckingSession) {
      return;
    }

    setIsCheckingSession(true);
    if (!supabase) {
      router.push("/register");
      return;
    }

    const { session } = await getSessionSafe(supabase);
    router.push(session ? "/chat" : "/register");
  }

  return (
    <main className="relative flex h-dvh flex-col overflow-hidden bg-[#0b1020] text-gray-100 transition-colors duration-500">
      <div className="pointer-events-none absolute inset-0 overflow-hidden">
        <div className="landing-orb landing-orb-a bg-blue-500/20" aria-hidden="true" />
        <div className="landing-orb landing-orb-b bg-purple-500/20" aria-hidden="true" />
        <div className="landing-orb landing-orb-c bg-cyan-400/20" aria-hidden="true" />
      </div>

      <AppHeader />

      <div className="relative z-10 min-h-0 flex-1 overflow-y-auto overflow-x-hidden">
        <section className="mx-auto grid max-w-6xl items-center gap-10 px-6 py-20 sm:py-28 lg:grid-cols-[1fr_360px]">
          <div className="relative">
            <div className="hero-aurora-overlay" aria-hidden="true" />
            <div className="hero-grid-overlay" aria-hidden="true" />

            <p className="inline-flex rounded-full border border-white/20 px-3 py-1 text-xs font-medium text-gray-300">
              New: AI-first acquisition workflows
            </p>

            <h1 className="mt-6 max-w-4xl text-4xl font-semibold leading-tight sm:text-6xl">
              Grablin is the new growth engine for modern acquisition.
            </h1>

            <p className="mt-6 max-w-2xl text-base text-gray-300 sm:text-lg">
              Build campaigns faster, qualify better leads, and scale outreach with AI-assisted
              decisioning. Grablin helps teams move from manual prospecting to predictable pipeline.
            </p>

            <div className="mt-10 flex flex-wrap gap-3">
              <button
                type="button"
                onClick={handleGetStarted}
                disabled={isCheckingSession}
                className="inline-flex items-center justify-center rounded-lg bg-white px-5 py-3 text-sm font-medium text-gray-900 transition hover:bg-gray-200 disabled:cursor-not-allowed disabled:opacity-70"
              >
                {isCheckingSession ? "Checking..." : "Get started"}
              </button>
            </div>
          </div>

          <div className="pointer-events-none relative hidden justify-center lg:flex" aria-hidden="true">
            <div className="hero-icon-glow" />
            <Image src="/icon.png" alt="Grablin hero icon" width={320} height={320} className="hero-icon-image h-72 w-72 object-contain" />
          </div>
        </section>

        <section className="border-t border-white/10 bg-black/20 transition-colors">
          <div className="mx-auto grid max-w-6xl gap-6 px-6 py-14 sm:grid-cols-3">
            <article className="rounded-xl border border-white/10 bg-white/5 p-5 transition-colors">
              <h2 className="text-lg font-semibold">AI Prospect Discovery</h2>
              <p className="mt-2 text-sm text-gray-300">
                Identify high-intent accounts and prioritize who to contact first.
              </p>
            </article>
            <article className="rounded-xl border border-white/10 bg-white/5 p-5 transition-colors">
              <h2 className="text-lg font-semibold">Automated Qualification</h2>
              <p className="mt-2 text-sm text-gray-300">
                Score leads in real-time so your team focuses on opportunities that convert.
              </p>
            </article>
            <article className="rounded-xl border border-white/10 bg-white/5 p-5 transition-colors">
              <h2 className="text-lg font-semibold">Faster Revenue Cycles</h2>
              <p className="mt-2 text-sm text-gray-300">
                Shorten time-to-pipeline with repeatable acquisition playbooks.
              </p>
            </article>
          </div>
        </section>
      </div>
    </main>
  );
}
