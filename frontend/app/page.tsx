"use client";

import { useState } from "react";
import { fetchEventSource } from "@microsoft/fetch-event-source";

type Msg = { role: "user" | "assistant"; text: string };

export default function Page() {
  const [input, setInput] = useState("");
  const [messages, setMessages] = useState<Msg[]>([]);
  const [busy, setBusy] = useState(false);

  async function send() {
    const msg = input.trim();
    if (!msg || busy) return;

    setMessages((m) => [...m, { role: "user", text: msg }, { role: "assistant", text: "" }]);
    setInput("");
    setBusy(true);

    const backend = process.env.NEXT_PUBLIC_BACKEND_URL || "http://localhost:8000";

    try {
      await fetchEventSource(`${backend}/chat/stream`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: msg }),
        onmessage(ev) {
          const data = JSON.parse(ev.data);
          console.log(`[${new Date().toISOString()}] Received event:`, data.type, data);

          if (data.type === "token") {
            setMessages((m) => {
              const copy = [...m];
              const last = copy[copy.length - 1];
              copy[copy.length - 1] = { ...last, text: last.text + data.text };
              return copy;
            });
          }

          // Handle final prediction (for cached results or complete responses)
          if (data.type === "final" && data.prediction?.answer) {
            console.log("Got final prediction");
            setMessages((m) => {
              const copy = [...m];
              const last = copy[copy.length - 1];
              // Only set if text is still empty (cached result without streaming)
              if (!last.text) {
                copy[copy.length - 1] = { ...last, text: data.prediction.answer };
              }
              return copy;
            });
          }

          if (data.type === "error") {
            console.error("Stream error:", data.message);
            setMessages((m) => {
              const copy = [...m];
              const last = copy[copy.length - 1];
              if (!last.text) {
                copy[copy.length - 1] = { 
                  ...last, 
                  text: `Error: ${data.message}` 
                };
              }
              return copy;
            });
            setBusy(false);
          }

          if (data.type === "end") {
            console.log("Stream ended");
            setBusy(false);
          }

          // Ignore ping events (keep-alive)
          if (data.type === "ping") {
            console.log("Received ping");
          }
        },
        onerror(err) {
          console.error("SSE error:", err);
          setBusy(false);
          setMessages((m) => {
            const copy = [...m];
            const last = copy[copy.length - 1];
            copy[copy.length - 1] = { 
              ...last, 
              text: last.text || "Error: Could not connect to backend. Make sure the backend is running on " + backend 
            };
            return copy;
          });
          throw err;
        },
      });
    } catch (error) {
      console.error("Failed to send message:", error);
      setBusy(false);
    }
  }

  return (
    <main className="min-h-screen p-6">
      <div className="mx-auto max-w-2xl space-y-4">
        <h1 className="text-2xl font-semibold">DSPy Streaming Chat</h1>

        <div className="rounded-lg border p-4 min-h-[420px] space-y-3">
          {messages.map((m, i) => (
            <div key={i} className={m.role === "user" ? "text-right" : "text-left"}>
              <div className="inline-block rounded-lg border px-3 py-2">
                <div className="text-xs opacity-60">{m.role}</div>
                <div className="whitespace-pre-wrap">{m.text}</div>
              </div>
            </div>
          ))}
        </div>

        <div className="flex gap-2">
          <input
            className="flex-1 rounded-md border px-3 py-2"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && send()}
            placeholder="Type a message…"
          />
          <button className="rounded-md border px-4 py-2 disabled:opacity-50" onClick={send} disabled={busy}>
            Send
          </button>
        </div>
      </div>
    </main>
  );
}
