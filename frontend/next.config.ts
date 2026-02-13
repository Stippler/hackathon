import type { NextConfig } from "next";
import { config as loadDotEnv } from "dotenv";

loadDotEnv({ path: "../.env" });

const backendPort = process.env.BACKEND_PORT || "8010";
const resolvedBackendUrl =
  process.env.NEXT_PUBLIC_BACKEND_URL ||
  process.env.API_URL ||
  `http://localhost:${backendPort}`;
const resolvedSupabaseUrl =
  process.env.NEXT_PUBLIC_SUPABASE_URL || process.env.SUPABASE_URL || "";
const resolvedSupabaseAnonKey =
  process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || process.env.SUPABASE_ANON_KEY || "";

const nextConfig: NextConfig = {
  allowedDevOrigins: [
    "http://localhost:3010",
    "http://127.0.0.1:3010",
    "https://app.christian-stippel.com",
    "https://app.ponyo.christian-stippel.com",
  ],
  env: {
    NEXT_PUBLIC_BACKEND_URL: resolvedBackendUrl,
    NEXT_PUBLIC_SUPABASE_URL: resolvedSupabaseUrl,
    NEXT_PUBLIC_SUPABASE_ANON_KEY: resolvedSupabaseAnonKey,
  },
};

export default nextConfig;
