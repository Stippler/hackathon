import os
import json
import dspy
import asyncio
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from dotenv import load_dotenv

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

# Validate required environment variables
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY environment variable is required")

# Set OpenAI API key for DSPy
os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY

print(f"✓ Configuration loaded:")
print(f"  - Backend: {BACKEND_HOST}:{BACKEND_PORT}")
print(f"  - Frontend expected on port: {FRONTEND_PORT}")
if SUPABASE_URL:
    print(f"  - Supabase: {SUPABASE_URL}")
else:
    print(f"  - Supabase: Not configured")

# 1) Completely disable caching
dspy.cache = None
dspy.configure_cache(
    enable_memory_cache=False,
    enable_disk_cache=False
)
print("✓ DSPy cache disabled")

# 2) Configure LM and create base program (global, reusable)
lm = dspy.LM("openai/gpt-4o-mini")
dspy.configure(lm=lm)
program = dspy.Predict("question -> answer")
print("✓ DSPy configured")

# DO NOT create StreamListener or stream_program here!
# They must be created per-request to avoid reuse issues

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

class ChatReq(BaseModel):
    message: str

def sse_event(obj: dict) -> str:
    """Format object as Server-Sent Event"""
    return f"data: {json.dumps(obj, ensure_ascii=False)}\n\n"

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/chat/stream")
async def chat_stream(req: ChatReq, request: Request):
    """
    Stream DSPy responses with proper SSE, error handling, and disconnect detection.
    Creates fresh StreamListener per request to avoid reuse issues.
    """
    
    async def gen():
        try:
            # Emit start event
            yield sse_event({"type": "start"})
            
            # Create fresh StreamListener for THIS request only
            listener = dspy.streaming.StreamListener(signature_field_name="answer")
            
            # Create stream_program for THIS request only
            stream_program = dspy.streamify(
                program,
                stream_listeners=[listener],
            )
            
            # Get the output stream
            output_stream = stream_program(question=req.message)
            
            # Track last ping time for keep-alive
            last_ping = asyncio.get_event_loop().time()
            
            async for chunk in output_stream:
                # Check if client disconnected
                if await request.is_disconnected():
                    print(f"Client disconnected, stopping stream")
                    break
                
                # Handle different chunk types
                if isinstance(chunk, dspy.streaming.StreamResponse):
                    # Token chunk - emit immediately
                    yield sse_event({
                        "type": "token",
                        "field": chunk.signature_field_name,
                        "text": chunk.chunk,
                    })
                    
                elif isinstance(chunk, dspy.streaming.StatusMessage):
                    # Status message
                    yield sse_event({
                        "type": "status",
                        "text": str(chunk)
                    })
                    
                elif isinstance(chunk, dspy.Prediction):
                    # Final prediction
                    yield sse_event({
                        "type": "final",
                        "prediction": chunk.toDict()
                    })
                
                # Optional: Send periodic ping to prevent proxy buffering
                current_time = asyncio.get_event_loop().time()
                if current_time - last_ping > 10:
                    yield sse_event({"type": "ping"})
                    last_ping = current_time
                    
        except asyncio.CancelledError:
            # Client disconnected or cancelled
            print("Stream cancelled by client")
            yield sse_event({"type": "error", "message": "Stream cancelled"})
            
        except Exception as e:
            # Handle any errors during streaming
            print(f"Error during streaming: {e}")
            yield sse_event({
                "type": "error",
                "message": str(e)
            })
            
        finally:
            # Always emit end event
            yield sse_event({"type": "end"})
    
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

