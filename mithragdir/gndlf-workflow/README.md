# Gndlf Workflow

Middle-earth RAG orchestration API

## Agentic RAG Architecture

This project utilizes [LangGraph](https://python.langchain.com/docs/langgraph/) to orchestrate an agentic, self-correcting Retrieval-Augmented Generation (RAG) pipeline. It operates as a strict closed-domain system, ensuring answers are grounded entirely in the provided original texts rather than pre-trained LLM knowledge.

### Core Nodes

* **`guardrail_check`**: The entry point. Evaluates the raw input to ensure it is safe and strictly relevant to the domain.
* **`route_query`**: Determines if the user input requires a database lookup or can be handled as standard conversation (e.g., greetings).
* **`conversational_llm`**: Handles general, non-lore chat without triggering the retrieval pipeline.
* **`generate_query`**: Rewrites the user's prompt into optimized search queries tailored for Qdrant's sparse, dense, and late-interaction (ColBERT) vectors.
* **`retrieve_document`**: Executes the hybrid search against the Qdrant vector database.
* **`grade_documents`**: Acts as an LLM-as-a-judge to evaluate if the retrieved context is genuinely relevant to the user's query.
* **`generate_answer`**: Drafts the final response based *strictly* on the retrieved context.
* **`grade_generation`**: The final self-reflection node. Checks the drafted answer for hallucinations (groundedness) and helpfulness.
* **`refuse_answer`**: A graceful failure node that handles out-of-bounds questions or exhausted search retries.

### Execution Flow

1. **Start:** The user's input is passed directly to the `guardrail_check` node.
2. **Guardrail Check (Conditional):**
   * *If Irrelevant or Unsafe:* Routes to the `refuse_answer` node and **Ends**.
   * *If Relevant and Safe:* Routes forward to the `route_query` node.
3. **Query Routing (Conditional):**
   * *If General Chat:* Routes to `conversational_llm`, provides a standard response, and **Ends**.
   * *If Knowledge Lookup Needed:* Routes to `generate_query`.
4. **Query Generation & Retrieval:** The `generate_query` node optimizes the search terms and passes them to `retrieve_document`, which fetches context from the Qdrant database.
5. **Document Grading (Conditional):** The `grade_documents` node evaluates the retrieved text.
   * *If Documents are Irrelevant:* * Checks if the maximum retry limit is reached.
     * If *No*: Loops back to `generate_query` to try a different search.
     * If *Yes*: Routes to `refuse_answer` and **Ends**.
   * *If Documents are Relevant:* Routes to `generate_answer`.
6. **Answer Generation:** The `generate_answer` node drafts a response using only the retrieved documents, then passes the draft to `grade_generation`.
7. **Generation Grading & Self-Correction (Conditional):**
   * *If Hallucinated (includes outside knowledge):* Loops back to `generate_answer` to rewrite the response strictly using the context.
   * *If Unhelpful (missed the user's point):* Loops back to `generate_query` to fetch better context.
   * *If Good Answer:* The final response is delivered to the user and the graph **Ends**.
