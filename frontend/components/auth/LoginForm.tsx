"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { FormEvent, useEffect, useMemo, useState } from "react";
import { setAuthCookie } from "@/lib/auth-cookie";
import { getSessionSafe, getSupabaseAuthClient } from "@/lib/supabase-auth";

type LoginFormProps = {
  supabaseUrl: string;
  supabaseAnonKey: string;
};

export function LoginForm({ supabaseUrl, supabaseAnonKey }: LoginFormProps) {
  const router = useRouter();
  const supabase = useMemo(
    () => getSupabaseAuthClient(supabaseUrl, supabaseAnonKey),
    [supabaseUrl, supabaseAnonKey],
  );

  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState("");
  const [status, setStatus] = useState("");

  const isConfigMissing = !supabaseUrl || !supabaseAnonKey;

  useEffect(() => {
    let isMounted = true;

    async function redirectIfAuthenticated() {
      const { session } = await getSessionSafe(supabase);
      if (isMounted && session) {
        setAuthCookie();
        router.replace("/chat");
      }
    }

    void redirectIfAuthenticated();

    return () => {
      isMounted = false;
    };
  }, [router, supabase]);

  async function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError("");
    setStatus("");
    setIsLoading(true);

    const { error: signInError } = await supabase.auth.signInWithPassword({
      email: email.trim(),
      password,
    });

    if (signInError) {
      setError(signInError.message);
      setIsLoading(false);
      return;
    }

    setStatus("Logged in successfully. Redirecting...");
    setAuthCookie();
    router.push("/chat");
    router.refresh();
  }

  return (
    <section className="w-full max-w-md rounded-2xl border border-white/10 bg-black/30 p-5 shadow-sm backdrop-blur sm:p-8">
      <h1 className="text-2xl font-semibold text-gray-100">Login</h1>
      <p className="mt-2 text-sm text-gray-300">Sign in with your email and password.</p>

      {isConfigMissing ? (
        <p className="mt-4 rounded-md border border-red-500/30 bg-red-900/30 p-3 text-sm text-red-200" role="alert">
          Supabase auth is not configured. Add `SUPABASE_URL` and `SUPABASE_ANON_KEY` to your environment.
        </p>
      ) : null}

      <form className="mt-6 space-y-4" onSubmit={onSubmit} noValidate autoComplete="off">
        <div>
          <label htmlFor="login-email" className="mb-1 block text-sm font-medium text-gray-200">
            Email
          </label>
          <input
            id="login-email"
            name="email"
            type="email"
            autoComplete="off"
            required
            value={email}
            onChange={(event) => setEmail(event.target.value)}
            className="w-full rounded-lg border border-white/20 bg-white/5 px-3 py-2.5 text-sm text-gray-100 outline-none transition placeholder:text-gray-500 focus:border-blue-400 focus:ring-2 focus:ring-blue-500/20"
          />
        </div>

        <div>
          <label htmlFor="login-password" className="mb-1 block text-sm font-medium text-gray-200">
            Password
          </label>
          <input
            id="login-password"
            name="password"
            type="password"
            autoComplete="off"
            required
            minLength={8}
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            className="w-full rounded-lg border border-white/20 bg-white/5 px-3 py-2.5 text-sm text-gray-100 outline-none transition placeholder:text-gray-500 focus:border-blue-400 focus:ring-2 focus:ring-blue-500/20"
          />
        </div>

        <button
          type="submit"
          disabled={isLoading || isConfigMissing}
          className="inline-flex w-full items-center justify-center rounded-lg bg-white px-4 py-2.5 text-sm font-medium text-gray-900 transition hover:bg-gray-200 disabled:cursor-not-allowed disabled:bg-gray-500 disabled:text-gray-200"
        >
          {isLoading ? "Logging in..." : "Login"}
        </button>
      </form>

      <div className="mt-4 min-h-6">
        {error ? (
          <p className="text-sm text-red-300" role="alert" aria-live="assertive">
            {error}
          </p>
        ) : null}
        {status ? (
          <p className="text-sm text-green-300" aria-live="polite">
            {status}
          </p>
        ) : null}
      </div>

      <p className="mt-2 text-sm text-gray-300">
        New here?{" "}
        <Link href="/register" className="font-medium text-blue-300 underline underline-offset-2 hover:text-blue-200">
          Create an account
        </Link>
      </p>
    </section>
  );
}
