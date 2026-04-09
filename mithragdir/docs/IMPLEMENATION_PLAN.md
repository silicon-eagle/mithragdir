# Tolkien RAG Chatbot Implementation Plan (Python Architecture)

## 1. High-Level Architecture Overview

The system is divided into two distinct lifecycles:
1. **The Data Pipeline (Python):** A script you run locally *once* (or periodically) to scrape the Tolkien Gateway, chunk the text, generate embeddings, and populate your databases.
2. **The Runtime Application (Python + React):** The continuous web service. The Python (FastAPI) backend handles live API requests and LLM orchestration (via LangChain), while React provides the user interface.

---

## 2. Component Breakdown & Tech Stack

### A. Data Ingestion & Pipeline (Python)
Python handles all the messy string manipulation and data preparation.
* **HTTP Client:** `httpx` or `requests` to query the MediaWiki API.
* **Chunking:** `tiktoken` (for accurate token counting) and standard Python string splitting to intelligently divide paragraphs.
* **Embedding:** The official Python SDK for your chosen provider (e.g., `voyageai` or `openai`).
* **Database Clients:** Standard `sqlite3` for local text storage and `qdrant-client` for pushing vectors.

### B. Storage Layer (Shared)
* **Shared Library:** **gndlf-core**. Contains shared database connection logic and Pydantic models (e.g., `Chunk`) to ensure consistency between pipeline and backend.
* **Relational Database:** **SQLite**. The Python script creates a `redbook.db` file containing `articles` and `chunks` tables. The FastAPI backend reads from this exact same file.
* **Vector Store:** **Qdrant** (running via Docker). Python inserts the vectors, and the FastAPI backend queries them.

### C. Backend API & RAG Pipeline (Python)
This is where you handle API processing and AI orchestration.
* **Web Framework:** **FastAPI** to serve the REST API.
* **Database Client:** Standard **`sqlite3`** or **SQLAlchemy** to asynchronously query the SQLite database.
* **Vector Client:** **`qdrant-client`** (Python package) to perform semantic searches.
* **AI Orchestration:** **LangChain**. This handles the Gemini/OpenAI LLM integration, prompt building, and streaming the response back via SSE.

### D. Frontend UI (Typescript + React)
* **Framework:** **Typescript + React** (scaffolded with Vite).
* **Styling:** **TailwindCSS**.

---

## 3. Step-by-Step Implementation Plan

### Phase 1: The Gwaihir Data Pipeline (`/gndlf-pipeline`)
1. **Initialize Environment:** Configure `uv` workspace and dependencies.
2. **Setup Databases:** Use `gndlf-core` to initialize the SQLite schema (`articles` and `chunks` tables).
3. **Scrape & Parse:** Hit the Tolkien Gateway MediaWiki API (`prop=extracts`). Extract the clean text.
4. **Chunk & Embed:** Split the text into ~500-token chunks with ~50-token overlaps. Prepend the article title to each chunk. Send chunks to your embedding API.
    * Use different embedding models: sparse dense and late interaction. Store the resulting vectors separately in Qdrant with metadata.
5. **Store:** Insert the raw text/metadata into SQLite, and insert the vectors (with the SQLite `chunk.id` as the vector ID) into Qdrant.

### Phase 2: The Cirdan Backend Server (`/gndlf-workflow`)
1. **Initialize Python Project:** Using `uv` workspace, ensure `gndlf-workflow` depends on `gndlf-core`.
2. **Setup State:** Initialize your FastAPI app and load standard configurations. Use `gndlf-core` for DB connections.
3. **Write the Agentic RAG Logic:** * Create a LangChain/ LangGraph retriever that takes a query, embeds it, and searches Qdrant for the top 5 vector IDs.
    * Use self-hosted LLM for the retriever via Ollama.
    * **Define the State:** Create a state schema (e.g., `TypedDict`) to track the current `question`, retrieved `documents`, and the overall conversation `messages`.
    * **Create Nodes (The Actors):**
        * *Retrieve Node:* Extracts the current question, fetches the top 5 Qdrant vectors, and overwrites the `documents` state with the results.
        * *Grade Node:* Prompts Ollama to output a strict binary "yes/no" to evaluate if the retrieved `documents` actually contain the answer to the `question`.
        * *Rewrite Node:* Prompts Ollama to analyze the original question and generate a better-optimized search query to replace the current `question` state.
        * *Generate Node:* Uses Ollama to synthesize the final answer based entirely on the validated `documents` state.
    * **Define Edges (The Logic):** * Connect `Start` -> *Retrieve*.
        * Connect *Retrieve* -> *Grade*.
        * Add a conditional edge from *Grade*: route to *Generate* if context is relevant, or route to *Rewrite* if irrelevant.
        * Connect *Rewrite* -> *Retrieve* to complete the self-correction loop.
4. **Expose the API:** Create the `POST /api/chat` FastAPI endpoint. Accept the message history, map it to the expected LangChain schema, run the RAG logic, and stream the LLM response back using FastAPI's `StreamingResponse` for SSE.


### Phase 3: The React Frontend (`/shire-ui`)
1. **Scaffold:** `npm create vite@latest shire-ui -- --template react-ts`.
2. **Proxy Route:** Configure `vite.config.ts` to securely proxy API requests to your FastAPI backend (`http://localhost:8000/api/chat`).
3. **Build the UI:** Implement the `useChat` hook in `App.tsx`. Build a clean, Tailwind-styled chat interface that iterates over `messages`.

---

## 4. Revised Repository Structure

```text
lembas-service/
├── pyproject.toml             # Workspace root
├── .env                       # Shared environment variables
├── docker-compose.yml         # Runs Qdrant, Cirdan, and Frontend
│
├── gndlf-core/               # Shared Python Library
│   ├── pyproject.toml
│   └── src/core/
│       ├── db.py              # Shared SQLite connection logic
│       └── models.py          # Shared Pydantic models
│
├── gndlf-pipeline/                   # Data Pipeline
│   ├── pyproject.toml         # Depends on gndlf-core
│   ├── tolkien.db             # Generated SQLite file
│   └── src/pipeline/          # Ingestion logic
│
├── gndlf-workflow/                    # FastAPI Web API Project
│   ├── pyproject.toml         # Depends on gndlf-core
│   ├── main.py                # FastAPI server and endpoints
│   └── src/workflow/
│
└── frontend/                  # SvelteKit Application
    ├── package.json
    ├── Dockerfile
    ├── svelte.config.js       # Node adapter config
    └── src/
        ├── routes/+page.svelte       # Chat UI
        └── routes/api/chat/+server.ts # API Proxy
```
