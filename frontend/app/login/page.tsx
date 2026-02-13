import { LoginForm } from "@/components/auth/LoginForm";
import { AppHeader } from "@/components/navigation/AppHeader";

export default function LoginPage() {
  const supabaseUrl = process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL || "";
  const supabaseAnonKey = process.env.SUPABASE_ANON_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || "";

  return (
    <main className="min-h-screen bg-[#0b1020] text-gray-100">
      <AppHeader />

      <div className="mx-auto flex min-h-[calc(100dvh-65px)] w-full max-w-5xl items-center justify-center px-4 py-8 sm:px-6 sm:py-10">
        <LoginForm supabaseUrl={supabaseUrl} supabaseAnonKey={supabaseAnonKey} />
      </div>
    </main>
  );
}
