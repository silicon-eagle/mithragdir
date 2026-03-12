# Cirdan API Implementation Plan (Python / FastAPI)

This document defines the implementation plan for the `cirdan-api` backend at the module and function level, structured like tasks on a sprint board.

## 1. Project Overview & Scope
`cirdan-api` is a FastAPI application that acts as the RAG orchestration layer.
It reads from a SQLite database and a Qdrant vector store to assemble contexts, and streams responses to the UI using LangChain.

---

## 2. Target Project Structure
```text
cirdan-api/
├── main.py
├── config.py
├── domain/
│   ├── models.py
│   └── exceptions.py
├── api/
│   ├── health.py
│   └── chat.py
├── services/
│   ├── rag_orchestrator.py
│   ├── retrieval_service.py
│   ├── llm_service.py
│   └── prompt_builder.py
└── data/
    ├── sqlite_repository.py
    └── qdrant_repository.py
```

---

## 3. Sprint Tasks: Function-Level Breakdown

### EPIC 1: Skeleton, Config, & Health (Sprint Board Setup)

**Task 1.1: Initialize Project & Config Parsing**
*   **What:** Create the FastAPI skeleton and strongly-typed configuration.
*   **Functions/Classes to implement:**
    *   `uv init` (with basic dependencies)
    *   `config.py`: Create `Settings` class (via `pydantic-settings`) with properties for `sqlite_connection_string`, `qdrant_url`, `qdrant_collection`, `llm_provider`, `llm_api_key`, `llm_model`, `embedding_model`, `top_k`.
    *   `main.py`: Add startup logic to validate config values (fail fast if missing API keys).

**Task 1.2: Global Exception Handling**
*   **What:** Catch unhandled exceptions and format them into predictable JSON structures.
*   **Functions/Classes to implement:**
    *   Register exception handlers in `main.py` using `@app.exception_handler()`.
    *   Map custom domain exceptions (e.g., `ValidationException`, `DependencyOfflineException`) to 400 or 503 HTTP status codes.

**Task 1.3: Health & Readiness Probes**
*   **What:** Provide endpoints for SRE/Docker to hit.
*   **Functions/Classes to implement:**
    *   `api/health.py`: Create `GET /health` (basic process up) and `GET /ready` (checks external dependencies).
    *   Implement basic checks for SQLite (`SELECT 1`) and Qdrant database pings.

### EPIC 2: Data Access Adapters (Retrieval Layer)

**Task 2.1: SQLite Repository**
*   **What:** Hydrate chunk data.
*   **Functions/Classes to implement:**
    *   `data/sqlite_repository.py`: Initialize database connection.
    *   `async def get_chunks_by_ids(chunk_ids: list[str]) -> list[RetrievedChunk]`: Executes a SQL query against `tolkien.db` returning content mapping for the exact chunk IDs.

**Task 2.2: Qdrant Repository**
*   **What:** Semantic search over chunks.
*   **Functions/Classes to implement:**
    *   `data/qdrant_repository.py`: Initialize `QdrantClient`.
    *   `async def search(query_embedding: list[float], top_k: int) -> list[VectorHit]`: Issues search to Qdrant collection returning IDs and similarity scores.

### EPIC 3: AI Abstractions & Prompts (LangChain)

**Task 3.1: LLM Service Integration**
*   **What:** Wraps the exact AI provider (OpenAI/Gemini/Ollama) using LangChain.
*   **Functions/Classes to implement:**
    *   `services/llm_service.py`
    *   `async def generate_embedding(text: str) -> list[float]`: Calls the embedding model via LangChain.
    *   `async def stream_chat(system_prompt: str, user_message: str) -> AsyncGenerator[str, None]`: Calls the text generation model with streaming enabled via `astream`.

**Task 3.2: Retrieval Service Orchestration**
*   **What:** Unifies Search/Embedding/SQL hydration.
*   **Functions/Classes to implement:**
    *   `services/retrieval_service.py`
    *   `async def search_relevant_chunks(query: str, top_k: int) -> list[RetrievedChunk]`:
        1. Calls `generate_embedding` for the query.
        2. Calls `QdrantRepository.search`.
        3. Calls `SQLiteRepository.get_chunks_by_ids`.
        4. Merges SQL text mapped onto Vector hits and orders by score.

**Task 3.3: Prompt Builder**
*   **What:** Structures the final context window for the LLM.
*   **Functions/Classes to implement:**
    *   `services/prompt_builder.py`
    *   `def build_prompt(user_message: str, chunks: list[RetrievedChunk]) -> str`: Converts chunks to a formatted string block.
    *   `def get_system_prompt() -> str`: Returns the static Middle-earth RAG persona rules.

### EPIC 4: Final Orchestration & API Endpoints

**Task 4.1: Rag Orchestrator Workflow**
*   **What:** Glues all the components together into a single pipeline.
*   **Functions/Classes to implement:**
    *   `services/rag_orchestrator.py`
    *   `async def execute_chat_pipeline(req: ChatRequest) -> AsyncGenerator[ChatChunk, None]`:
        1. Fetch chunks (`search_relevant_chunks`).
        2. Build prompt bundle (`build_prompt`).
        3. Call streaming LLM text generator (`stream_chat`).
        4. `yield` elements wrapped in custom `ChatChunk` Pydantic models (map text tokens, then at the very end `yield` a `ChatChunk` containing the metadata Sources).

**Task 4.2: Chat Endpoint (SSE Streaming)**
*   **What:** Defines the POST route and handles Server-Sent Events output.
*   **Functions/Classes to implement:**
    *   `api/chat.py`
    *   `POST /api/chat`: Binds to `ChatRequest` Pydantic model.
    *   `async def chat_stream(req: ChatRequest)`: Returns a `StreamingResponse` with media type `text/event-stream` and yields `data: {json}\n\n` for every token and chunk emitted from the orchestrator.

### EPIC 5: Hardening & Testing

**Task 5.1: Unit & Contract Testing**
*   **What:** Ensure core logic stability.
*   **Functions/Classes to implement:**
    *   Use `pytest`.
    *   `test_prompt_builder.py`: Verify tokens and rules appear correctly.
    *   `test_retrieval_service.py`: Mock LLM and Qdrant to ensure SQL chunks are merged and ranked accurately based on synthetic scores.
    *   `test_chat_endpoint.py`: Ensure `ChatChunk` models serialize into valid SSE string formats predictably.

**Task 5.2: Main Wiring**
*   **What:** Final application configuration and router dependencies.
*   **Functions/Classes to implement:**
    *   Initialize FastAPI app and add CORS middlewares.
    *   Wire up FastAPI `Depends` for providing Repositories and Services.
    *   Include routers (`app.include_router(...)`).

---

## 4. Work Flow Guidelines (Sprint Rules)
- **Do not mix concerns:** Routers ONLY do HTTP bindings (Epic 4). Services ONLY orchestrate business logic (Epic 3 & 4). Repositories ONLY execute DB queries (Epic 2).
- **Asynchronous Execution:** Ensure `async/await` is used fully across `sqlite3` or `SQLAlchemy`, Qdrant, and LangChain to not block the event loop.
- **Data Models:** Use Pydantic `BaseModel` types for internal data transfer objects (`ChatChunk`, `RetrievedChunk`) to get easy validation and serialization.
