# DSPy Streaming Chat Hackathon

This project demonstrates a streaming chat application using DSPy for the backend and Next.js for the frontend.

## Setup

### Backend (FastAPI + DSPy)
```bash
cd backend
pip install -r requirements.txt  # Make sure you have fastapi, uvicorn, dspy installed
```

### Frontend (Next.js)
```bash
cd frontend
npm install
```

### Environment Variables
- Root `.env` contains `OPENAI_API_KEY` and port configurations
- Frontend `.env.local` contains `NEXT_PUBLIC_BACKEND_URL=http://localhost:8000`

## Running the Application

### Option 1: Run Both Servers Separately

**Terminal 1 - Backend:**
```bash
cd backend
uvicorn main:app --reload --port 8000
```

**Terminal 2 - Frontend:**
```bash
cd frontend
npm run dev
```

### Option 2: Check if servers are already running
- Backend health check: http://localhost:8000/health
- Frontend: http://localhost:3000

## How It Works

1. **Frontend** (`frontend/app/page.tsx`):
   - User types a message
   - Sends POST request to `http://localhost:8000/chat/stream`
   - Receives Server-Sent Events (SSE) stream
   - Displays tokens as they arrive

2. **Backend** (`backend/main.py`):
   - Receives message via `/chat/stream` endpoint
   - Uses DSPy with `dspy.streamify()` to stream LLM responses
   - Returns SSE stream with `text/event-stream` content type
   - CORS enabled for localhost:3000

## Troubleshooting

### Error: "Expected content-type to be text/event-stream"
- **Cause**: Frontend can't reach backend or backend isn't running
- **Fix**: Make sure backend is running on port 8000
- **Test**: Visit http://localhost:8000/health - should return `{"ok": true}`

### CORS Errors
- Backend is configured to accept requests from `http://localhost:3000` and `http://127.0.0.1:3000`
- If using different ports, update `backend/main.py` CORS settings

### Environment Variables Not Loading
- Frontend: Restart Next.js dev server after changing `.env.local`
- Backend: Restart uvicorn after changing `.env`
