# Tolkien RAG Chatbot Implementation Plan (Hybrid Architecture)

## 1. High-Level Architecture Overview

The system is divided into two distinct lifecycles:
1. **The Data Pipeline (Python):** A script you run locally *once* (or periodically) to scrape the Tolkien Gateway, chunk the text, generate embeddings, and populate your databases.
2. **The Runtime Application (Rust + Svelte):** The continuous web service. The Rust backend handles live API requests and LLM orchestration, while SvelteKit provides the user interface.

---

## 2. Component Breakdown & Tech Stack

### A. Data Ingestion & Pipeline (Python)
Python handles all the messy string manipulation and data preparation.
* **HTTP Client:** `httpx` or `requests` to query the MediaWiki API.
* **Chunking:** `tiktoken` (for accurate token counting) and standard Python string splitting to intelligently divide paragraphs.
* **Embedding:** The official Python SDK for your chosen provider (e.g., `voyageai` or `openai`).
* **Database Clients:** Standard `sqlite3` for local text storage and `qdrant-client` for pushing vectors.

### B. Storage Layer (Shared)
* **Relational Database:** **SQLite**. The Python script creates a `tolkien.db` file containing `articles` and `chunks` tables. The Rust backend reads from this exact same file.
* **Vector Store:** **Qdrant** (running via Docker). Python inserts the vectors, and Rust queries them.

### C. Backend API & RAG Pipeline (Rust)
This is where you focus your Rust learning on networking and AI orchestration.
* **Web Framework:** **Axum** (backed by Tokio) to serve the REST API.
* **Database Client:** **`sqlx`** to asynchronously query the SQLite database.
* **Vector Client:** **`qdrant-client`** (Rust crate) to perform semantic searches.
* **AI Orchestration:** **`rig`**. This crate handles the Gemini LLM integration, prompt building, and streaming the response back to Axum.

### D. Frontend UI (SvelteKit)
* **Framework:** **SvelteKit** (using `@sveltejs/adapter-node`).
* **Chat Library:** **Vercel AI SDK** (`@ai-sdk/svelte`) for out-of-the-box streaming and state management.
* **Styling:** **TailwindCSS**.

---

## 3. Step-by-Step Implementation Plan

### Phase 1: The Python Data Pipeline (`/data-pipeline`)
1. **Initialize Environment:** Create a `requirements.txt` with `httpx`, `tiktoken`, `qdrant-client`, and `python-dotenv`.
2. **Setup Databases:** Write a function to initialize the SQLite schema (`articles` and `chunks` tables) and the Qdrant collection.
3. **Scrape & Parse:** Hit the Tolkien Gateway MediaWiki API (`prop=extracts`). Extract the clean text.
4. **Chunk & Embed:** Split the text into ~500-token chunks with ~50-token overlaps. Prepend the article title to each chunk. Send chunks to your embedding API.
5. **Store:** Insert the raw text/metadata into SQLite, and insert the vectors (with the SQLite `chunk.id` as the vector ID) into Qdrant.

### Phase 2: The Rust Backend Server (`/backend`)
1. **Initialize Rust Project:** `cargo new backend` and add `axum`, `tokio`, `sqlx`, `rig`, and `qdrant-client` to your `Cargo.toml`.
2. **Setup State:** Create an Axum `AppState` struct that holds the `sqlx::SqlitePool` (pointing to the `.db` file Python created) and the Qdrant client.
3. **Write the RAG Logic:** * Create a function that takes a query, embeds it (using `rig` or an HTTP client), and searches Qdrant for the top 5 vector IDs.
   * Query SQLite for the actual text content matching those 5 IDs.
   * Initialize a `rig` agent with Gemini, a Middle-earth system prompt, and the retrieved context.
4. **Expose the API:** Create the `POST /api/chat` route. Accept the message history, run the RAG logic, and stream the LLM response back using Axum's SSE (Server-Sent Events).

### Phase 3: The SvelteKit Frontend (`/frontend`)
1. **Scaffold:** `npx sv create frontend` and configure the Node adapter.
2. **Proxy Route:** Create `src/routes/api/chat/+server.ts` to securely forward requests to your Rust backend (`http://localhost:8080/api/chat`).
3. **Build the UI:** Implement the `useChat` hook in `+page.svelte`. Build a clean, Tailwind-styled chat interface that iterates over `$messages`.

---

## 4. Revised Repository Structure

```text
tolkien-rag/
├── .env                       # Shared environment variables
├── docker-compose.yml         # Runs Qdrant, Rust Backend, and Svelte Frontend
│
├── data-pipeline/             # Python (Run locally, once)
│   ├── requirements.txt
│   ├── tolkien.db             # Generated SQLite file (Rust will read this!)
│   └── ingest.py              # Main scraping and embedding script
│
├── backend/                   # Rust Cargo Project
│   ├── Cargo.toml
│   ├── Dockerfile
│   └── src/
│       ├── main.rs            # Axum server and route handlers
│       └── rag.rs             # Qdrant queries and `rig` LLM logic
│
└── frontend/                  # SvelteKit Application
    ├── package.json
    ├── Dockerfile
    ├── svelte.config.js       # Node adapter config
    └── src/
        ├── routes/+page.svelte       # Chat UI
        └── routes/api/chat/+server.ts # API Proxy
