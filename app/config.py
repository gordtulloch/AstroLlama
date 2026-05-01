from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    llama_server_url: str = "http://localhost:8081"
    mcp_server_url: str = "http://localhost:8000/mcp"

    app_host: str = "localhost"
    app_port: int = 8080

    default_system_prompt: str = (
        "You are a helpful astronomical assistant. "
        "Answer questions from your own training knowledge. "
        "Answer concisely. "
        "When asked to show or generate a chart or map for any star, object, or constellation use generate_constellation_map. "
        "Only use generate_aavso_map when the user explicitly asks for a variable star finder chart or an AAVSO chart for a known variable star such as Mira, SS Cyg, RR Lyr, Delta Cep, or similar. "
        "Do not call generate_aavso_map for regular (non-variable) stars."
    )
    default_max_tokens: int = 1024
    default_context_size: int = 8192

    # ChromaDB / RAG settings
    chroma_db_path: str = "data/chromadb"
    chroma_collection: str = "documents"
    embedding_model: str = "all-MiniLM-L6-v2"
    rag_top_k: int = 3
    rag_enabled: bool = True

    # MCP tool use
    mcp_enabled: bool = True

    # Hugging Face token (used by sentence-transformers / huggingface_hub)
    hf_token: str = ""

    # Microsoft Entra authentication
    entra_auth_enabled: bool = False
    entra_tenant_id: str = ""
    entra_spa_client_id: str = ""
    entra_api_client_id: str = ""
    entra_api_scope: str = ""
    entra_redirect_uri: str = ""


settings = Settings()
