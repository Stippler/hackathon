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

  const intelligenceSources = [
    {
      title: "WKO Company Search",
      description:
        "WKO stands for Wirtschaftskammer Osterreich (Austrian Economic Chamber). We use WKO company data to identify and enrich relevant Austrian businesses.",
      image: "/wko.png",
      alt: "WKO data source integration preview",
    },
    {
      title: "Project Facts Dashboard",
      description:
        "Project facts exports help check whether a company has already been contacted and when the last contact happened.",
      image: "/projectfacts.png",
      alt: "Project facts dashboard preview",
    },
    {
      title: "Open Firmenbuch (OFB)",
      description:
        "Open Firmenbuch gives us capabilities to access Austrian company register and balance sheet related financial data.",
      image: "/ofb.png",
      alt: "Open Firmenbuch integration preview",
    },
    {
      title: "EVI Enrichment",
      description:
        "EVI provides full balance sheet documents as PDFs, making detailed financial review possible directly in the workflow.",
      image: "/evi.png",
      alt: "EVI data enrichment preview",
    },
  ];

  return (
    <main className="relative flex h-dvh flex-col overflow-hidden bg-[#0b1020] text-gray-100 transition-colors duration-500">
      <div className="pointer-events-none absolute inset-0 overflow-hidden">
        <div className="landing-orb landing-orb-a bg-blue-500/20" aria-hidden="true" />
        <div className="landing-orb landing-orb-b bg-purple-500/20" aria-hidden="true" />
        <div className="landing-orb landing-orb-c bg-cyan-400/20" aria-hidden="true" />
      </div>

      <AppHeader />

      <div className="relative z-10 min-h-0 flex-1 overflow-y-auto overflow-x-hidden">
        <section className="relative mx-auto grid min-h-[calc(100dvh-4rem)] max-w-7xl items-center gap-12 px-6 py-10 sm:py-14 lg:grid-cols-[1fr_520px]">
          <div className="relative z-20">
            <div className="hero-aurora-overlay" aria-hidden="true" />
            <div className="hero-grid-overlay" aria-hidden="true" />

            <p className="inline-flex rounded-full border border-white/20 px-3 py-1 text-xs font-medium text-gray-300">
              New: AI-first acquisition workflows
            </p>

            <h1 className="mt-6 max-w-4xl text-5xl font-semibold leading-tight sm:text-6xl lg:text-7xl">
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
                className="inline-flex items-center justify-center rounded-xl bg-white px-8 py-4 text-lg font-semibold text-gray-900 shadow-lg transition hover:bg-gray-200 disabled:cursor-not-allowed disabled:opacity-70"
              >
                {isCheckingSession ? "Checking..." : "Get started"}
              </button>
            </div>

          </div>

          <div className="pointer-events-none relative z-20 flex justify-center" aria-hidden="true">
            <div className="hero-icon-glow" />
            <Image
              src="/icon.png"
              alt="Grablin hero icon"
              width={520}
              height={520}
              className="hero-icon-image relative z-20 h-72 w-72 object-contain sm:h-96 sm:w-96 lg:h-[30rem] lg:w-[30rem]"
            />
          </div>

        </section>

        <section className="border-t border-white/10 bg-gradient-to-b from-black/30 via-[#0e152e]/70 to-[#0b1020]">
          <div className="mx-auto max-w-6xl px-6 py-14 sm:py-16">
            <div className="max-w-3xl">
              <p className="inline-flex rounded-full border border-white/15 bg-white/5 px-3 py-1 text-xs font-medium text-gray-300">
                Landing page facts and tooling overview
              </p>
              <h2 className="mt-4 text-2xl font-semibold text-white sm:text-3xl">
                Built to combine project intelligence with actionable lead data.
              </h2>
              <p className="mt-3 text-sm text-gray-300 sm:text-base">
                The modules below show the current tooling and information surfaces that power the
                acquisition workflow inside Grablin.
              </p>
            </div>

            <div className="mt-8 grid gap-5 md:grid-cols-2">
              {intelligenceSources.map((source) => (
                <article
                  key={source.title}
                  className="group overflow-hidden rounded-2xl border border-white/15 bg-white/5 shadow-[0_0_0_1px_rgba(255,255,255,0.03)] backdrop-blur-sm transition hover:border-white/25 hover:bg-white/10"
                >
                  <div className="relative aspect-[16/10] w-full overflow-hidden bg-[#060a16]">
                    <Image
                      src={source.image}
                      alt={source.alt}
                      fill
                      className="object-cover transition duration-500 group-hover:scale-[1.02]"
                      sizes="(max-width: 768px) 100vw, 50vw"
                    />
                  </div>
                  <div className="p-5">
                    <h3 className="text-lg font-semibold text-white">{source.title}</h3>
                    <p className="mt-2 text-sm leading-6 text-gray-300">{source.description}</p>
                  </div>
                </article>
              ))}
            </div>
          </div>
        </section>

        <section className="border-t border-white/10 bg-black/20 transition-colors">
          <div className="mx-auto max-w-7xl px-6 py-16 sm:py-20">
            <div className="max-w-4xl">
              <p className="inline-flex rounded-full border border-white/15 bg-white/5 px-3 py-1 text-xs font-medium text-gray-300">
                Platform details
              </p>
              <h2 className="mt-4 text-3xl font-semibold text-white sm:text-4xl">
                Everything below explains how Grablin works in real acquisition operations.
              </h2>
              <p className="mt-4 text-sm leading-7 text-gray-300 sm:text-base">
                Grablin is built to remove friction between research, financial validation, and
                action. Instead of checking separate tools, your team can work inside one flow that
                combines account discovery, prior-contact checks, and financial documents.
              </p>
            </div>

            <div className="mt-8 grid gap-5 md:grid-cols-2">
              <article className="rounded-xl border border-white/10 bg-white/5 p-6">
                <h3 className="text-sm font-semibold uppercase tracking-wide text-gray-300">Tooling</h3>
                <p className="mt-3 text-sm leading-7 text-gray-200">
                  The platform combines a modern web frontend with API-driven backend services and
                  AI-assisted workflows. Teams can identify prospects, inspect company context, and
                  prepare outreach in a single workspace with shared visibility.
                </p>
                <ul className="mt-4 space-y-2 text-sm text-gray-300">
                  <li>- Centralized acquisition workspace for team collaboration.</li>
                  <li>- Fast workflow transitions from discovery to qualification.</li>
                  <li>- Structured data surfaces for consistent decision-making.</li>
                </ul>
              </article>

              <article className="rounded-xl border border-white/10 bg-white/5 p-6">
                <h3 className="text-sm font-semibold uppercase tracking-wide text-gray-300">Data Sources</h3>
                <ul className="mt-3 space-y-3 text-sm leading-7 text-gray-200">
                  <li>
                    <strong>WKO (Wirtschaftskammer Osterreich):</strong> Austrian Economic Chamber
                    data that helps identify and enrich relevant company targets.
                  </li>
                  <li>
                    <strong>Project Facts export:</strong> validates whether companies were already
                    contacted and when the latest touchpoint happened.
                  </li>
                  <li>
                    <strong>Open Firmenbuch (OFB):</strong> company-register context plus balance
                    sheet related data for better qualification.
                  </li>
                  <li>
                    <strong>EVI:</strong> full balance sheet PDFs to support deeper financial checks
                    before prioritizing outreach.
                  </li>
                </ul>
              </article>

              <article className="rounded-xl border border-white/10 bg-white/5 p-6">
                <h3 className="text-sm font-semibold uppercase tracking-wide text-gray-300">Outcome</h3>
                <p className="mt-3 text-sm leading-7 text-gray-200">
                  Teams reduce duplicate outreach, spend less time on low-quality targets, and move
                  faster from first research to high-confidence contact decisions.
                </p>
                <ul className="mt-4 space-y-2 text-sm text-gray-300">
                  <li>- Higher relevance in initial outreach messaging.</li>
                  <li>- Better prioritization based on financial context.</li>
                  <li>- More predictable progression from prospect to pipeline.</li>
                </ul>
              </article>

              <article className="rounded-xl border border-white/10 bg-white/5 p-6">
                <h3 className="text-sm font-semibold uppercase tracking-wide text-gray-300">How Grablin Helps</h3>
                <p className="mt-3 text-sm leading-7 text-gray-200">
                  Grablin links company discovery, history checks, and financial review in one
                  sequence. Your team can quickly understand a company, avoid repeated contact, and
                  choose the next best step with full context.
                </p>
                <ul className="mt-4 space-y-2 text-sm text-gray-300">
                  <li>- Start with discovery and enrichment.</li>
                  <li>- Validate prior contact status from exports.</li>
                  <li>- Confirm financial profile via OFB and EVI documents.</li>
                  <li>- Execute with higher confidence and clearer timing.</li>
                </ul>
              </article>
            </div>

            <div className="mt-8 rounded-2xl border border-white/10 bg-white/5 p-6">
              <h3 className="text-lg font-semibold text-white">Why this matters for acquisition teams</h3>
              <p className="mt-3 text-sm leading-7 text-gray-200">
                In most teams, valuable context is spread across spreadsheets, portals, and documents.
                Grablin brings that context together so reps and managers can work from the same facts.
                The result is faster cycle time, better outreach quality, and a clearer path from
                target list to pipeline contribution.
              </p>
            </div>
          </div>
        </section>
      </div>
    </main>
  );
}
