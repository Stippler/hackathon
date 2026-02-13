"use client";

import {
  AssistantIf,
  AssistantRuntimeProvider,
  ComposerPrimitive,
  MessagePrimitive,
  ThreadPrimitive,
  useLocalRuntime,
} from "@assistant-ui/react";
import { MarkdownTextPrimitive } from "@assistant-ui/react-markdown";
import Image from "next/image";
import { useRouter } from "next/navigation";
import remarkGfm from "remark-gfm";
import { memo, useEffect, useMemo, useRef, useState } from "react";
import { clearAuthCookie } from "@/lib/auth-cookie";
import { AppHeader } from "@/components/navigation/AppHeader";
import { createFastAPIAdapter } from "@/lib/fastapi-runtime";
import { getSessionSafe, getSupabaseAuthClient } from "@/lib/supabase-auth";

type ChatInterfaceProps = {
  backendUrl: string;
  supabaseUrl: string;
  supabaseAnonKey: string;
};

const MarkdownText = memo(function MarkdownText() {
  return (
    <MarkdownTextPrimitive
      remarkPlugins={[remarkGfm]}
      components={{
        a: ({ href, children, ...props }) => (
          <a href={href} target="_blank" rel="noopener noreferrer" {...props}>
            {children}
          </a>
        ),
      }}
    />
  );
});

function CopyButton({ contentRef }: { contentRef: React.RefObject<HTMLDivElement | null> }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    const text = contentRef.current?.textContent || "";
    await navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <button
      onClick={handleCopy}
      className="inline-flex items-center gap-1.5 rounded-md px-2 py-1 text-xs text-gray-300 transition-colors hover:bg-white/10"
      title="Copy to clipboard"
      type="button"
    >
      {copied ? (
        <>
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor">
            <polyline points="20 6 9 17 4 12" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
          Copied!
        </>
      ) : (
        <>
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <rect x="9" y="9" width="13" height="13" rx="2" ry="2" />
            <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
          </svg>
          Copy
        </>
      )}
    </button>
  );
}

export function ChatInterface({ backendUrl, supabaseUrl, supabaseAnonKey }: ChatInterfaceProps) {
  const router = useRouter();
  const [isCheckingAuth, setIsCheckingAuth] = useState(true);
  const [sessionError, setSessionError] = useState("");

  const supabase = useMemo(
    () => getSupabaseAuthClient(supabaseUrl, supabaseAnonKey),
    [supabaseUrl, supabaseAnonKey],
  );
  const adapter = useMemo(
    () => createFastAPIAdapter({ backendUrl, supabaseUrl, supabaseAnonKey }),
    [backendUrl, supabaseUrl, supabaseAnonKey],
  );
  const runtime = useLocalRuntime(adapter);

  useEffect(() => {
    let isMounted = true;

    async function validateSession() {
      const sessionResult = await Promise.race([
        getSessionSafe(supabase),
        new Promise<{
          session: null;
          error: Error;
        }>((resolve) =>
          setTimeout(
            () =>
              resolve({
                session: null,
                error: new Error("Session check timed out"),
              }),
            4000,
          ),
        ),
      ]);

      const { session, error } = sessionResult;
      if (!isMounted) {
        return;
      }

      if (error || !session) {
        clearAuthCookie();
        setSessionError(error?.message || "Please login to continue.");
        setIsCheckingAuth(false);
        router.replace("/login");
        return;
      }

      setSessionError("");
      setIsCheckingAuth(false);
    }

    void validateSession();

    const {
      data: { subscription },
    } = supabase.auth.onAuthStateChange((_event, session) => {
      if (!session) {
        clearAuthCookie();
        router.replace("/login");
      }
    });

    return () => {
      isMounted = false;
      subscription.unsubscribe();
    };
  }, [router, supabase]);

  if (isCheckingAuth) {
    return (
      <main className="flex h-dvh flex-col overflow-hidden bg-[#0b1020]">
        <AppHeader />
        <div className="flex flex-1 items-center justify-center px-6">
          <p className="text-sm text-gray-300">Checking your session...</p>
        </div>
      </main>
    );
  }

  if (sessionError) {
    return (
      <main className="flex h-dvh flex-col overflow-hidden bg-[#0b1020]">
        <AppHeader />
        <div className="flex flex-1 items-center justify-center px-6">
          <p className="rounded-md border border-red-500/30 bg-red-900/30 px-4 py-3 text-sm text-red-200">{sessionError}</p>
        </div>
      </main>
    );
  }

  return (
    <AssistantRuntimeProvider runtime={runtime}>
      <main className="chat-dark flex h-dvh flex-col overflow-hidden bg-[#0b1020] text-gray-100">
        <AppHeader />

        <ThreadPrimitive.Root className="flex min-h-0 flex-1 flex-col overflow-hidden">
          <ThreadPrimitive.Viewport className="min-h-0 flex-1 overflow-x-hidden overflow-y-auto scroll-smooth">
            <ThreadPrimitive.Messages
              components={{
                UserMessage: () => (
                  <div className="mx-auto w-full max-w-5xl px-6 py-3">
                    <div className="ml-auto w-fit max-w-[88%] rounded-2xl border border-blue-300/20 bg-blue-500/10 py-3 pl-4 pr-6 text-left text-[15px] leading-7 text-gray-100">
                      <MessagePrimitive.Content />
                    </div>
                  </div>
                ),
                AssistantMessage: () => {
                  const contentRef = useRef<HTMLDivElement>(null);
                  const [agentState, setAgentState] = useState("manager");

                  useEffect(() => {
                    const readStateFromContent = () => {
                      const text = contentRef.current?.textContent || "";
                      const match = text.match(/status:\s*([^\n]+)/i);
                      if (match?.[1]) {
                        setAgentState(match[1].trim());
                      } else {
                        setAgentState("manager");
                      }
                    };

                    readStateFromContent();
                    const observer = new MutationObserver(readStateFromContent);
                    if (contentRef.current) {
                      observer.observe(contentRef.current, {
                        childList: true,
                        subtree: true,
                        characterData: true,
                      });
                    }
                    return () => observer.disconnect();
                  }, []);

                  return (
                    <div className="mx-auto w-full max-w-5xl px-6 py-4">
                      <div className="mr-0 flex w-auto flex-col items-start gap-3 sm:mr-6 sm:flex-row">
                        <div className="mt-0 flex w-full shrink-0 flex-row items-center gap-2 sm:mt-1 sm:w-[88px] sm:flex-col sm:items-center sm:gap-0">
                          <Image
                            src="/manager.png"
                            alt="manager"
                            width={52}
                            height={52}
                            className="hidden rounded-full object-contain opacity-95 sm:block"
                            style={{
                              width: "clamp(42px, 4.2vw, 56px)",
                              height: "clamp(42px, 4.2vw, 56px)",
                            }}
                            priority={false}
                          />
                          <div
                            title={agentState}
                            className="mt-0 max-w-full truncate text-left text-[10px] leading-4 text-gray-300 sm:mt-1 sm:max-w-[88px] sm:text-center"
                          >
                            {agentState}
                          </div>
                        </div>
                        <div className="min-w-0 flex-1 overflow-hidden rounded-2xl border border-emerald-300/20 bg-emerald-500/10 px-4 py-3">
                          <div className="overflow-x-auto">
                            <div
                              ref={contentRef}
                              className="prose prose-sm min-w-0 max-w-none text-left text-[15px] leading-7 text-gray-100 [&_*]:!text-left [&_table]:w-max [&_table]:min-w-full"
                            >
                              <MessagePrimitive.If assistant last hasContent={false}>
                                <span className="inline-flex items-center" aria-label="Assistant is responding">
                                  <span className="thinking-dot h-2.5 w-2.5" aria-hidden="true" />
                                </span>
                              </MessagePrimitive.If>
                              <MessagePrimitive.Content components={{ Text: MarkdownText }} />
                            </div>
                          </div>
                          <div className="mt-2 flex items-center gap-1">
                            <CopyButton contentRef={contentRef} />
                          </div>
                        </div>
                      </div>
                    </div>
                  );
                },
              }}
            />
          </ThreadPrimitive.Viewport>

          <div className="shrink-0 border-t border-white/10 bg-black/30">
            <div className="m-auto max-w-5xl px-5 py-4">
              <ComposerPrimitive.Root className="relative flex items-center">
                <ComposerPrimitive.Input
                  autoFocus
                  placeholder="Ask Grablin..."
                  className="max-h-[200px] w-full resize-none overflow-hidden rounded-3xl border border-white/20 bg-white/5 px-5 py-3 pr-12 text-[15px] text-gray-100 shadow-sm outline-none placeholder:text-gray-400 focus:border-blue-400 disabled:bg-white/5"
                  rows={1}
                />
                <AssistantIf condition={({ thread }) => !thread.isRunning}>
                  <ComposerPrimitive.Send className="absolute right-2 inline-flex h-9 w-9 items-center justify-center rounded-full bg-white text-gray-900 transition-colors hover:bg-gray-200 disabled:bg-gray-400 disabled:text-gray-600">
                    <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor">
                      <path d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z" />
                    </svg>
                  </ComposerPrimitive.Send>
                </AssistantIf>
                <AssistantIf condition={({ thread }) => thread.isRunning}>
                  <ComposerPrimitive.Cancel className="absolute right-2 inline-flex h-9 w-9 items-center justify-center rounded-full bg-white text-gray-900 transition-colors hover:bg-gray-200">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor">
                      <rect x="6" y="6" width="12" height="12" rx="1" />
                    </svg>
                  </ComposerPrimitive.Cancel>
                </AssistantIf>
              </ComposerPrimitive.Root>
            </div>
          </div>
        </ThreadPrimitive.Root>
      </main>
    </AssistantRuntimeProvider>
  );
}
