"use client";

import type { ChatModelAdapter } from "@assistant-ui/react";
import { fetchEventSource } from "@microsoft/fetch-event-source";
import { getSessionSafe, getSupabaseAuthClient } from "@/lib/supabase-auth";

type FastAPIAdapterConfig = {
  backendUrl: string;
  supabaseUrl: string;
  supabaseAnonKey: string;
};

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

function isBackendOfflineError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false;
  }
  const message = error.message.toLowerCase();
  return (
    message.includes("failed to fetch") ||
    message.includes("fetch failed") ||
    message.includes("networkerror") ||
    message.includes("network error") ||
    message.includes("network request failed") ||
    message.includes("load failed") ||
    message.includes("econnrefused") ||
    message.includes("connection refused")
  );
}

export function createFastAPIAdapter({
  backendUrl,
  supabaseUrl,
  supabaseAnonKey,
}: FastAPIAdapterConfig): ChatModelAdapter {
  return {
    async *run({ messages, abortSignal }) {
      const lastMessage = messages[messages.length - 1];

      if (lastMessage.role !== "user") {
        throw new Error("Last message must be from user");
      }

      const supabase = getSupabaseAuthClient(supabaseUrl, supabaseAnonKey);
      const { session, error: sessionError } = await getSessionSafe(supabase);

      if (sessionError || !session?.access_token) {
        throw new Error("You must be logged in to use chat.");
      }

      const userMessage = lastMessage.content
        .filter((part) => part.type === "text")
        .map((part) => (part as { type: "text"; text: string }).text)
        .join("\n");

      const history = messages
        .slice(0, -1)
        .map((message) => ({
          role: message.role,
          content: message.content
            .filter((part) => part.type === "text")
            .map((part) => (part as { type: "text"; text: string }).text)
            .join("\n")
            .trim(),
        }))
        .filter((message) => message.content.length > 0);

      let fullText = "";
      const chunks: string[] = [];
      let streamEnded = false;
      let streamError: Error | null = null;
      let wasAborted = false;
      let finalAnswer = "";
      let receivedAnswerToken = false;
      let animatingFinalFallback = false;
      let latestStatusLine = "";
      let thinkingText = "";
      let hasUiUpdate = false;
      let shouldShowActivity = true;
      let lastHeartbeatAt = Date.now();
      const streamStartedAt = Date.now();

      const setLatestStatus = (line: string) => {
        const text = line.trim();
        if (!text) return;
        latestStatusLine = text;
      };

      const formatCursiveThinking = (text: string) =>
        text
          .split("\n")
          .map((line) => (line.trim() ? `_${line}_` : ""))
          .join("\n")
          .trim();

      // Start the SSE connection
      const streamPromise = fetchEventSource(`${backendUrl}/chat/stream`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${session.access_token}`,
        },
        body: JSON.stringify({ message: userMessage, history }),
        signal: abortSignal,

        onmessage(ev) {
          let data: unknown;
          try {
            data = JSON.parse(ev.data);
          } catch {
            // Ignore malformed/non-JSON SSE frames.
            return;
          }
          if (typeof data !== "object" || data === null) {
            return;
          }
          const payload = data as Record<string, unknown>;

          if (payload.type === "answer_token" && typeof payload.data === "object" && payload.data !== null) {
            const text = (payload.data as Record<string, unknown>).text;
            if (typeof text !== "string") {
              return;
            }
            receivedAnswerToken = true;
            shouldShowActivity = false;
            latestStatusLine = "";
            thinkingText = "";
            chunks.push(text);
          }

          if (payload.type === "tool_start" && typeof payload.data === "object" && payload.data !== null) {
            const info = payload.data as Record<string, unknown>;
            const tool = typeof info.tool === "string" ? info.tool : "tool";
            setLatestStatus(`tool start: ${tool}`);
            hasUiUpdate = true;
          }

          if (payload.type === "tool_end" && typeof payload.data === "object" && payload.data !== null) {
            const info = payload.data as Record<string, unknown>;
            const rowsCount = typeof info.rows_count === "number" ? ` (rows: ${info.rows_count})` : "";
            setLatestStatus(`tool end${rowsCount}`);
            hasUiUpdate = true;
          }

          if (payload.type === "retrieve" && typeof payload.data === "object" && payload.data !== null) {
            const info = payload.data as Record<string, unknown>;
            const table = typeof info.table === "string" ? info.table : "source";
            const rowsCount = typeof info.rows_count === "number" ? info.rows_count : 0;
            setLatestStatus(`retrieve: ${table} -> ${rowsCount} rows`);
            hasUiUpdate = true;
          }

          if (payload.type === "trace_token" && typeof payload.data === "object" && payload.data !== null) {
            const info = payload.data as Record<string, unknown>;
            const text = typeof info.text === "string" ? info.text : "";
            const source = typeof info.source === "string" ? info.source : "";
            if (text) {
              if (source === "next_thought" || source === "reasoning") {
                thinkingText += text;
              } else {
                setLatestStatus(text);
              }
            }
            hasUiUpdate = true;
          }

          if (payload.type === "final" && typeof payload.data === "object" && payload.data !== null) {
            const info = payload.data as Record<string, unknown>;
            const answer = typeof info.answer === "string" ? info.answer : "";
            finalAnswer = answer;
            if (!receivedAnswerToken && chunks.length === 0 && fullText.length === 0) {
              const syntheticChunks = answer.match(/\S+\s*|\n/g) ?? [answer];
              chunks.push(...syntheticChunks);
              animatingFinalFallback = true;
            }
            hasUiUpdate = true;
          }

          if (payload.type === "error" && typeof payload.data === "object" && payload.data !== null) {
            const info = payload.data as Record<string, unknown>;
            const message = typeof info.message === "string" ? info.message : "Stream error";
            streamError = new Error(message);
            streamEnded = true;
          }

          if (payload.type === "done") {
            streamEnded = true;
          }
        },
        onclose() {
          streamEnded = true;
        },

        onerror(err) {
          if (isAbortError(err)) {
            wasAborted = true;
            streamEnded = true;
            return;
          }
          console.error("SSE error:", err);
          streamError = err instanceof Error ? err : new Error(String(err));
          streamEnded = true;
          throw err;
        },
      });

      // Yield chunks as they arrive
      try {
        while (!streamEnded || chunks.length > 0 || hasUiUpdate) {
          if (abortSignal.aborted) {
            wasAborted = true;
            streamEnded = true;
            if (chunks.length === 0 && !hasUiUpdate) {
              break;
            }
          }

          // Process any new chunks
          if (chunks.length > 0) {
            const maxChunks = animatingFinalFallback ? 1 : chunks.length;
            for (let i = 0; i < maxChunks; i += 1) {
              const chunk = chunks.shift();
              if (!chunk) break;
              fullText += chunk;
              hasUiUpdate = true;
            }
            if (animatingFinalFallback && chunks.length === 0) {
              animatingFinalFallback = false;
            }
          }

          if (hasUiUpdate) {
            let displayText = fullText;
            if (shouldShowActivity) {
              const thoughtBlock = thinkingText.trim();
              const cursiveThoughtBlock = formatCursiveThinking(thoughtBlock);
              const activityBlockParts = [
                ...(latestStatusLine ? [`status: ${latestStatusLine}`] : []),
                ...(cursiveThoughtBlock ? [`thinking:\n${cursiveThoughtBlock}`] : []),
              ];
              const activityBlock =
                activityBlockParts.length > 0 ? activityBlockParts.map((line) => `- ${line}`).join("\n") : "";
              if (activityBlock) {
                displayText = `Agent activity (live):\n${activityBlock}\n\n${fullText}`;
              }
            }
            yield {
              content: [{ type: "text", text: displayText }],
            };
            hasUiUpdate = false;
          }

          // Check for errors
          if (streamError) {
            throw streamError;
          }

          // Keep UI alive during long LM/tool waits with heartbeat updates.
          if (!streamEnded && shouldShowActivity && !receivedAnswerToken && !hasUiUpdate) {
            const now = Date.now();
            if (now - lastHeartbeatAt >= 2000) {
              const elapsedSeconds = Math.max(1, Math.floor((now - streamStartedAt) / 1000));
              setLatestStatus(`[working] still thinking... (${elapsedSeconds}s)`);
              hasUiUpdate = true;
              lastHeartbeatAt = now;
            }
          }

          // Wait a bit before checking for more chunks
          if (!streamEnded || chunks.length > 0 || hasUiUpdate) {
            await new Promise((resolve) => setTimeout(resolve, 10));
          }
        }

        // Wait for stream to fully complete
        await streamPromise;

        if (wasAborted || abortSignal.aborted) {
          return;
        }

        if (!fullText && finalAnswer) {
          fullText = finalAnswer;
        }

        // Final yield with complete text (in case we missed any)
        yield {
          content: [{ type: "text", text: fullText }],
        };
      } catch (error) {
        // Don't throw on abort - that's a normal cancellation
        if (isAbortError(error)) {
          return;
        }
        if (isBackendOfflineError(error)) {
          yield {
            content: [
              {
                type: "text",
                text:
                  "The backend seems to be offline right now. Please wait a moment and try again.",
              },
            ],
          };
          return;
        }
        throw error;
      }
    },
  };
}
