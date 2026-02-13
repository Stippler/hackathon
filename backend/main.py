import os
import json
import asyncio
import traceback
import sys
import uuid
import contextlib
from pathlib import Path
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from dotenv import load_dotenv
from supabase import Client, create_client

# Ensure repo root is importable when running from backend/.
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from mas.agent import stream_question_answer_async

# Load environment variables from .env file
load_dotenv()

# Get configuration from environment variables
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
BACKEND_PORT = int(os.getenv("BACKEND_PORT", "8010"))
BACKEND_HOST = os.getenv("BACKEND_HOST", "0.0.0.0")
FRONTEND_PORT = int(os.getenv("FRONTEND_PORT", "3010"))

# Optional: Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

SUPABASE_CLIENT: Optional[Client] = None
if SUPABASE_URL and SUPABASE_ANON_KEY:
    SUPABASE_CLIENT = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

# Validate required environment variables
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY environment variable is required")

# Set OpenAI API key for DSPy
os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY

print(f"✓ Configuration loaded:")
print(f"  - Backend: {BACKEND_HOST}:{BACKEND_PORT}")
print(f"  - Frontend expected on port: {FRONTEND_PORT}")
print(f"  - OpenAI API key configured: {'yes' if OPENAI_API_KEY else 'no'}")
if SUPABASE_URL:
    print(f"  - Supabase: {SUPABASE_URL}")
else:
    print(f"  - Supabase: Not configured")

app = FastAPI()

# Add CORS middleware - permissive configuration for public access
# Allow all origins so people can access from anywhere
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=False,  # Must be False when allow_origins is ["*"]
    allow_methods=["*"],
    allow_headers=["*"],
)
print("✓ CORS enabled for all origins (permissive mode)")

class ChatMessage(BaseModel):
    role: str
    content: str


class ChatReq(BaseModel):
    message: str
    history: Optional[List[ChatMessage]] = None

def sse_event(obj: dict) -> str:
    """Format object as Server-Sent Event"""
    return f"data: {json.dumps(obj, ensure_ascii=False)}\n\n"


PING = ": ping\n\n"


def _verify_supabase_token_sync(access_token: str) -> Optional[dict]:
    if SUPABASE_CLIENT is None:
        return None

    try:
        user_response = SUPABASE_CLIENT.auth.get_user(jwt=access_token)
        user = getattr(user_response, "user", None)
        if user is None:
            return None

        if hasattr(user, "model_dump"):
            user_dict = user.model_dump()
        elif hasattr(user, "dict"):
            user_dict = user.dict()
        else:
            user_dict = {}

        return {
            "id": user_dict.get("id"),
            "email": user_dict.get("email"),
            "raw_user": user_dict,
        }
    except Exception:
        return None


async def require_authenticated_user(request: Request) -> dict:
    auth_header = request.headers.get("authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing bearer token",
        )

    token = auth_header.replace("Bearer ", "", 1).strip()
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing bearer token",
        )

    if SUPABASE_CLIENT is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Supabase auth verification is not configured",
        )

    user_context = await asyncio.to_thread(_verify_supabase_token_sync, token)
    if not user_context:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        )
    # Build a request-scoped Supabase client authorized with the caller's JWT.
    request_supabase = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    request_supabase.postgrest.auth(token)
    user_context["supabase_client"] = request_supabase
    return user_context


def _to_qa_history(history_messages: Optional[List[ChatMessage]]) -> List[dict]:
    qa_history: List[dict] = []
    pending_question: Optional[str] = None

    for message in history_messages or []:
        text = message.content.strip()
        if not text:
            continue

        if message.role == "user":
            pending_question = text
            continue

        if message.role == "assistant" and pending_question:
            qa_history.append({"question": pending_question, "answer": text})
            pending_question = None

    return qa_history

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/chat/stream")
async def chat_stream(req: ChatReq, request: Request):
    """
    Stream DSPy responses with proper SSE, error handling, and disconnect detection.
    Creates fresh StreamListener per request to avoid reuse issues.
    """
    user_context = await require_authenticated_user(request)
    run_id = uuid.uuid4().hex
    default_agent_id = "rag"

    async def gen():
        stream = None
        client_disconnected = False
        try:
            yield sse_event(
                {"type": "start", "run_id": run_id, "agent_id": default_agent_id, "data": {}}
            )

            qa_history = _to_qa_history(req.history)
            stream = stream_question_answer_async(
                question=req.message,
                history=qa_history,
                user_context=user_context,
            )

            # Periodic ping helps keep some proxies from buffering.
            last_ping = asyncio.get_event_loop().time()
            async for event in stream:
                if await request.is_disconnected():
                    client_disconnected = True
                    break

                event_type = str(event.get("type", "")).strip() or "trace_token"
                agent_id = str(event.get("agent_id", default_agent_id)).strip() or default_agent_id
                data = event.get("data", {})
                if not isinstance(data, dict):
                    data = {"value": data}

                payload = {
                    "type": event_type,
                    "run_id": run_id,
                    "agent_id": agent_id,
                    "data": data,
                }
                yield sse_event(payload)
                await asyncio.sleep(0)

                current_time = asyncio.get_event_loop().time()
                if current_time - last_ping > 10:
                    yield PING
                    last_ping = current_time

        except asyncio.CancelledError:
            client_disconnected = True
            
        except Exception as e:
            # Handle any errors during streaming
            print(f"Error during streaming: {e}")
            print(f"Full traceback:\n{traceback.format_exc()}")
            
            # Handle ExceptionGroup (Python 3.11+) from TaskGroup
            if hasattr(e, '__cause__') and e.__cause__:
                print(f"Caused by: {e.__cause__}")
            if hasattr(e, 'exceptions'):
                print(f"Sub-exceptions: {e.exceptions}")
                for i, sub_e in enumerate(e.exceptions):
                    print(f"  Sub-exception {i}: {sub_e}")
                    print(f"  Traceback: {''.join(traceback.format_exception(type(sub_e), sub_e, sub_e.__traceback__))}")
            
            yield sse_event(
                {
                    "type": "error",
                    "run_id": run_id,
                    "agent_id": default_agent_id,
                    "data": {"message": str(e)},
                }
            )
            
        finally:
            if stream is not None:
                with contextlib.suppress(Exception):
                    await stream.aclose()
            if not client_disconnected:
                yield sse_event(
                    {"type": "done", "run_id": run_id, "agent_id": default_agent_id, "data": {}}
                )
    
    # Return streaming response with proper headers
    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable nginx buffering
        }
    )

# Allow running directly with: python main.py
if __name__ == "__main__":
    import uvicorn
    print(f"\n🚀 Starting server on {BACKEND_HOST}:{BACKEND_PORT}")
    uvicorn.run(
        "main:app",
        host=BACKEND_HOST,
        port=BACKEND_PORT,
        reload=True,
        reload_dirs=["./"],
        log_level="info"
    )

