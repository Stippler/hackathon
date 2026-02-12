# DSPy Streaming Chat

A real-time streaming chat application demonstrating DSPy's streaming capabilities with FastAPI backend and Next.js frontend.

## Features

- ✨ **Token-by-token streaming** - See responses appear in real-time
- 🔄 **Server-Sent Events (SSE)** - Efficient uni-directional streaming
- 🚀 **Production-ready** - Proper error handling, disconnect detection, and concurrency support
- 🎯 **No caching issues** - Every request streams properly

## Prerequisites

- Python 3.8+ with pip
- Node.js 18+ with npm
- OpenAI API key

## Setup

### 1. Configure Environment Variables

Create/update `.env` in the project root:
```bash
OPENAI_API_KEY=your_openai_api_key_here
BACKEND_PORT=8010
FRONTEND_PORT=3010
```

Create `frontend/.env.local`:
```bash
NEXT_PUBLIC_BACKEND_URL=http://localhost:8010
```

### 2. Install Backend Dependencies

```bash
cd backend
pip install fastapi uvicorn dspy-ai python-multipart
```

Or if you have a `requirements.txt`:
```bash
pip install -r requirements.txt
```

### 3. Install Frontend Dependencies

```bash
cd frontend
npm install
```

## Running the Application

### Start Backend (Terminal 1)

```bash
cd backend
uvicorn main:app --reload --host 0.0.0.0 --port 8010
```

You should see:
```
✓ DSPy cache disabled
✓ DSPy configured
INFO:     Uvicorn running on http://0.0.0.0:8010
```

### Start Frontend (Terminal 2)

```bash
cd frontend
npm run dev -- --hostname 0.0.0.0 --port 3010
```

Access the app at: **http://localhost:3010**

## Architecture

### Backend (`backend/main.py`)

**Key Features:**
- **Per-request StreamListener** - Creates fresh listener for each request (critical for proper streaming)
- **Cache completely disabled** - Ensures every request streams tokens
- **SSE with proper headers** - Prevents proxy buffering
- **Disconnect detection** - Stops streaming if client disconnects
- **Error handling** - Graceful error messages sent via SSE
- **Keep-alive pings** - Periodic pings prevent idle connection timeouts

**SSE Event Types:**
- `start` - Stream initialization
- `token` - Individual token chunks
- `final` - Complete prediction (fallback for cached results)
- `status` - Status messages (optional)
- `error` - Error messages
- `end` - Stream completion
- `ping` - Keep-alive (every 10 seconds)

### Frontend (`frontend/app/page.tsx`)

**Key Features:**
- **Real-time rendering** - Updates UI as tokens arrive
- **Error handling** - Displays error messages gracefully
- **Connection management** - Handles disconnects and errors
- **Console logging** - Timestamped events for debugging

## How Streaming Works

1. **User sends message** → Frontend POSTs to `/chat/stream`
2. **Backend creates fresh components** → New `StreamListener` and `stream_program` per request
3. **DSPy streams tokens** → OpenAI streams to DSPy → DSPy yields `StreamResponse` chunks
4. **Backend emits SSE events** → Each token sent as SSE `data: {"type":"token",...}`
5. **Frontend appends tokens** → React state updates trigger re-renders
6. **Stream completes** → `end` event closes stream, UI shows complete message

## Troubleshooting

### Streaming Only Works for First Request

**Fixed!** This was caused by reusing `StreamListener` across requests. The current implementation creates a fresh listener per request.

### "Expected content-type to be text/event-stream"

**Cause:** Frontend can't reach backend
**Fix:**
1. Check backend is running: `curl http://localhost:8010/health`
2. Verify `NEXT_PUBLIC_BACKEND_URL` in `frontend/.env.local`
3. Check ports match (8010 in this setup)

### CORS Errors

**Cause:** Backend doesn't allow frontend origin
**Fix:** Update CORS origins in `backend/main.py`:
```python
allow_origins=[
    "http://localhost:3010",  # Add your frontend port
    # ...
]
```

### Hydration Error in Next.js

**Cause:** Font variable class mismatch between server/client
**Fix:** Already fixed with `suppressHydrationWarning` in `layout.tsx`

### No Tokens Appearing

**Check:**
1. Browser console for SSE events (should see timestamped logs)
2. Backend terminal for chunk logs
3. Network tab - check SSE connection stays open
4. OpenAI API key is valid and has credits

## Development Tips

### Debug Backend Streaming

The backend logs each chunk:
```
Chunk type: StreamResponse, Chunk: StreamResponse(predict_name='self', ...)
```

### Debug Frontend Events

Check browser console for:
```
[2026-02-12T...] Received event: token {type: 'token', text: 'Hello'}
```

### Test Backend Directly

```bash
curl -N -X POST http://localhost:8010/chat/stream \
  -H "Content-Type: application/json" \
  -d '{"message":"Hello"}'
```

Should see SSE events streaming.

## Project Structure

```
.
├── backend/
│   └── main.py          # FastAPI + DSPy streaming server
├── frontend/
│   ├── app/
│   │   ├── layout.tsx   # Root layout with fonts
│   │   ├── page.tsx     # Main chat UI
│   │   └── globals.css  # Styles
│   └── .env.local       # Frontend env vars
├── .env                 # Backend env vars (OPENAI_API_KEY)
└── README.md
```

## Tech Stack

- **Backend:** FastAPI, DSPy, Uvicorn
- **Frontend:** Next.js 16, React, TypeScript
- **LLM:** OpenAI GPT-4o-mini
- **Streaming:** Server-Sent Events (SSE)

## License

MIT
