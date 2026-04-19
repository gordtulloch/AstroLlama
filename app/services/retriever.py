from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class Retriever:
    """
    Wraps a ChromaDB persistent collection and a sentence-transformers
    embedding function to provide semantic document retrieval for RAG.

    Import of chromadb/sentence_transformers is deferred to __init__ so the
    app still starts (with RAG disabled) even if the packages are not yet
    installed.
    """

    def __init__(
        self,
        db_path: str,
        collection_name: str,
        embedding_model: str,
        top_k: int = 3,
        hf_token: str = "",
    ) -> None:
        self.db_path = db_path
        self.collection_name = collection_name
        self.embedding_model = embedding_model
        self.top_k = top_k
        self._collection = None
        self._available = False
        if hf_token:
            os.environ.setdefault("HF_TOKEN", hf_token)

    def start(self) -> None:
        """Initialise ChromaDB client and embedding function.  Call once at startup."""
        try:
            import chromadb
            from chromadb.utils.embedding_functions import (
                SentenceTransformerEmbeddingFunction,
            )

            Path(self.db_path).mkdir(parents=True, exist_ok=True)
            client = chromadb.PersistentClient(path=self.db_path)
            ef = SentenceTransformerEmbeddingFunction(
                model_name=self.embedding_model
            )
            self._collection = client.get_or_create_collection(
                name=self.collection_name,
                embedding_function=ef,
            )
            count = self._collection.count()
            self._available = True
            logger.info(
                "ChromaDB ready — collection '%s' has %d document(s)",
                self.collection_name,
                count,
            )
        except Exception as exc:
            logger.warning("ChromaDB unavailable: %s", exc)
            self._available = False

    @property
    def available(self) -> bool:
        return self._available

    @property
    def document_count(self) -> int:
        if not self._available or self._collection is None:
            return 0
        return self._collection.count()

    def query(self, text: str) -> list[str]:
        """
        Return up to self.top_k relevant text chunks for *text*.
        Returns an empty list if RAG is unavailable or the collection is empty.
        """
        if not self._available or self._collection is None:
            return []
        if self._collection.count() == 0:
            return []

        logger.debug("RAG query: %r", text[:120])
        try:
            results = self._collection.query(
                query_texts=[text],
                n_results=min(self.top_k, self._collection.count()),
                include=["documents", "metadatas"],
            )
            docs: list[str] = results.get("documents", [[]])[0]
            metas: list[dict] = results.get("metadatas", [[]])[0]
            for i, (doc, meta) in enumerate(zip(docs, metas)):
                source = meta.get("source", "unknown") if meta else "unknown"
                logger.debug("RAG result [%d]: %s (%d chars)", i + 1, source, len(doc))
            return docs
        except Exception as exc:
            logger.warning("ChromaDB query failed: %s", exc)
            return []

    _CHROMA_MAX_BATCH = 5000  # ChromaDB's hard limit is ~5461; stay safely below it

    def add_documents(
        self,
        documents: list[str],
        ids: list[str],
        metadatas: list[dict[str, Any]] | None = None,
    ) -> None:
        """Add or update documents in the collection (used by the ingest script)."""
        if not self._available or self._collection is None:
            raise RuntimeError("ChromaDB is not available")
        metas = metadatas or [{} for _ in documents]
        for start in range(0, len(documents), self._CHROMA_MAX_BATCH):
            end = start + self._CHROMA_MAX_BATCH
            self._collection.upsert(
                documents=documents[start:end],
                ids=ids[start:end],
                metadatas=metas[start:end],
            )
