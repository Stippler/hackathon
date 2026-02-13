import { ChatInterface } from "@/components/chat/ChatInterface";
import { AppHeader } from "@/components/navigation/AppHeader";
import { headers } from "next/headers";

function resolveBackendUrl(hostname: string): string {
  const normalizedHost = hostname.split(":")[0].toLowerCase();

  if (normalizedHost === "app.christian-stippel.com" && process.env.PRODUCTION_API_URL) {
    return process.env.PRODUCTION_API_URL;
  }

  if (normalizedHost === "app.ponyo.christian-stippel.com" && process.env.API_URL) {
    return process.env.API_URL;
  }

  if (process.env.NEXT_PUBLIC_BACKEND_URL) {
    return process.env.NEXT_PUBLIC_BACKEND_URL;
  }

  if (process.env.API_URL) {
    return process.env.API_URL;
  }

  if (process.env.PRODUCTION_API_URL) {
    return process.env.PRODUCTION_API_URL;
  }

  return "http://localhost:8010";
}

export default async function ChatPage() {
  const requestHeaders = await headers();
  const host = requestHeaders.get("host") || "";
  const backendUrl = resolveBackendUrl(host);
  const supabaseUrl = process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL || "";
  const supabaseAnonKey = process.env.SUPABASE_ANON_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || "";

  if (!supabaseUrl || !supabaseAnonKey) {
    return (
      <main className="flex min-h-screen flex-col bg-[#0b1020]">
        <AppHeader />
        <div className="flex flex-1 items-center justify-center px-6">
          <p className="rounded-md border border-red-500/30 bg-red-900/30 px-4 py-3 text-sm text-red-200">
            Supabase auth is not configured. Please set `SUPABASE_URL` and `SUPABASE_ANON_KEY`.
          </p>
        </div>
      </main>
    );
  }

  return <ChatInterface backendUrl={backendUrl} supabaseUrl={supabaseUrl} supabaseAnonKey={supabaseAnonKey} />;
}
