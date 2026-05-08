# How RAG Is Implemented in AstroLlama (and Why It Matters)

Retrieval-Augmented Generation, or RAG, is one of the most practical upgrades you can make to an AI assistant.

Without RAG, your model can only answer from:
- pretraining knowledge
- whatever is in the active chat context window

With RAG, the assistant can also pull in relevant local knowledge at runtime from your own documents, then use that material to answer more accurately.

In AstroLlama, RAG is designed to stay simple, local-first, and operationally reliable. This post walks through the architecture and the implementation details that make it work.

## RAG in Plain Language

At a high level, RAG has two phases:

1. Indexing phase
You ingest source documents, split them into chunks, embed them as vectors, and store them in a vector database.

2. Query phase
When a user asks a question, you embed the question, retrieve the most relevant chunks, and inject them into the prompt before generation.

That sounds straightforward, but quality depends on details: chunking, metadata, failure handling, context injection strategy, and system behavior when retrieval is unavailable.

## AstroLlama RAG Architecture

AstroLlama’s RAG flow is centered around a Retriever service plus two ingestion scripts:

- Local document ingestion for files on disk
- Web crawling ingestion for dynamic websites

Core runtime pieces:
- ChromaDB persistent collection for vector storage
- Sentence-transformers embedding function
- FastAPI app lifecycle initialization
- Prompt-time context injection in the chat orchestrator

The objective is clear: RAG should improve responses without destabilizing normal chat behavior.

## Data Sources and Ingestion Paths

AstroLlama supports two ingestion routes.

### 1) Local file ingestion

The local ingestion script processes supported file types:
- txt
- md
- csv
- pdf
- docx

For PDFs, extraction uses text parsing first, with optional OCR for image-bearing pages. There is also a column-aware OCR mode to improve extraction for multi-column layouts like newsletters and journals.

Chunking is character-based with overlap:
- default chunk size: 500 characters
- default overlap: 50 characters

Each chunk gets:
- deterministic ID (stable hash based on source and chunk index)
- metadata including source path and chunk number

Deterministic IDs matter because repeated ingestion updates existing chunks instead of creating uncontrolled duplicates.

### 2) Web ingestion with Crawl4AI

The web ingestion script uses Crawl4AI (headless Chromium/Playwright) to crawl JavaScript-heavy sites and convert pages into clean markdown-like text before chunking and indexing.

It includes practical crawling controls:
- depth and max page limits
- URL exclusion filters
- anti-bot retry and proxy escalation
- optional stealth mode
- optional login flow for authenticated sites

It can also collect linked PDF and DOCX files and ingest them into the same collection, which is useful for observatory handbooks, newsletters, and archive-style sites.

## Storage and Embeddings

At startup, AstroLlama initializes a ChromaDB persistent client and opens or creates the configured collection.

Embeddings are generated through a sentence-transformers embedding function (default model: all-MiniLM-L6-v2).

Configuration is environment-driven and includes:
- database path
- collection name
- embedding model
- retrieval top-k
- RAG on or off toggle

This keeps deployment simple while allowing easy tuning.

## Reliability Features in the Retriever

A subtle but important design choice: AstroLlama does a query smoke test when the collection has data.

Why this matters:
- Some database issues only surface on first read.
- If retrieval crashes inside request handling, it can take down the app process.

AstroLlama probes the collection during startup and disables RAG if corruption is detected, rather than failing unpredictably at chat time.

Additional safeguards:
- If ChromaDB is unavailable, the app still runs.
- Query failures are caught and logged.
- Retrieval returns an empty list on failure, allowing normal generation to continue.

The goal is graceful degradation, not all-or-nothing behavior.

## Query-Time Retrieval and Prompt Injection

During chat, AstroLlama does not mutate stored conversation history for RAG context. Instead, it builds a one-shot working message list for the current LLM call.

Flow at runtime:

1. Take the latest user message as retrieval query.
2. Retrieve up to top-k relevant chunks from ChromaDB.
3. Join chunks into a context block.
4. Append that context block to the system message for this turn.
5. Send the augmented message list to the model.

This approach keeps conversation persistence clean while still giving the model relevant context on each turn.

In other words, RAG context is injected ephemerally per request, not permanently written into chat history.

## How RAG Coexists with Tool Use

AstroLlama supports both RAG and MCP tool calling. The orchestrator sets a policy that prioritizes:
- direct model knowledge first
- then retrieved local context
- tool calls only when explicitly requested by the user

That policy prevents over-tooling and keeps normal Q&A fast and stable.

RAG is treated as passive context enhancement, while tools are explicit actions.

## Operational Controls

The system includes practical controls for development and operations:

- RAG can be disabled via configuration.
- Collection can be cleared and rebuilt.
- Test-run mode for ingestion scripts extracts and chunks without writing to ChromaDB.
- PDF extraction test mode can dump extracted text for manual inspection.

These controls make it easier to debug corpus quality and avoid blind indexing.

## Why This Implementation Works Well

Several decisions in AstroLlama improve real-world behavior:

1. Persistent local vector store
No cloud dependency required for retrieval.

2. Deterministic chunk IDs
Idempotent re-indexing and cleaner updates.

3. Defensive startup checks
RAG issues are surfaced early, with graceful fallback.

4. One-shot prompt augmentation
Context is fresh per query, without polluting conversation history.

5. Multi-source ingestion
Both local files and crawled web content can feed one retrieval layer.

## Limits and Future Improvements

Like most first-generation RAG systems, AstroLlama currently uses simple character chunking. That is robust and easy, but not always semantically optimal.

Potential upgrades include:
- semantic chunking based on headings/sections
- reranking retrieved chunks before injection
- source citation formatting in final answers
- chunk deduplication and freshness policies
- hybrid retrieval (vector plus keyword)

The current baseline is intentionally practical: reliable ingestion, retrieval that degrades safely, and clear integration into the chat loop.

## Closing

RAG is often described as a research pattern. In AstroLlama, it is an engineering pattern: ingest your knowledge, index it once, retrieve it per question, and keep the system resilient when components fail.

That combination, local corpus plus runtime retrieval plus graceful fallback, is what turns a generic model into a useful assistant for your own domain.