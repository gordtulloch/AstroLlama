from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    llama_server_url: str = "http://localhost:8081"
    mcp_server_url: str = "http://localhost:8000/mcp/"

    app_host: str = "localhost"
    app_port: int = 8080

    default_system_prompt: str = (
        "You are a helpful astronomical assistant. "
        "Do not use emoji or emoticons in your responses. "
        "Always prefer answering from your own training knowledge first. "
        "If additional context has been provided from the local knowledge base, use it to answer. "
        "Only call a tool when the user explicitly requests a live lookup, a chart, the current weather, "
        "or current time — not simply because the topic is astronomical. "
        "For explicit latitude/longitude or coordinate requests, call get_lat_long/get_latlong when available and use its output. "
        "Do not claim lack of database access when a matching tool is available. "
        "Use simbad_lookup_object for one specific object lookup (coordinates/properties of a named object). "
        "Use simbad_search only for multi-result list/browse queries. "
        "Answer concisely. "
        "When asked to show or generate a chart or map for any star, object, or constellation use generate_constellation_map. "
        "Only use generate_aavso_map when the user explicitly asks for a variable star finder chart or an AAVSO chart for a known variable star such as Mira, SS Cyg, RR Lyr, Delta Cep, or similar. "
        "Do not call generate_aavso_map for regular (non-variable) stars. "
        "OBSERVATION PLANNING — you MUST follow these rules without exception: "
        "NEVER answer questions about which planets are visible, what objects are up tonight, "
        "or what to observe from your training knowledge — always use a Telescopius tool. "
        "Call telescopius_solar_system_times for ANY question about planet, Sun or Moon "
        "visibility, rise, transit or set times — parameters latitude/longitude/timezone may "
        "be omitted and the observer defaults will be used automatically. "
        "Call telescopius_tonight_highlights for ANY question about what to observe or what "
        "is best visible tonight — all parameters are optional. "
        "Call telescopius_search_targets for filtered DSO searches by type, magnitude or altitude. "
        "Call telescopius_astronomy_news for recent astronomy or space news. "
        "Call telescopius_quote_of_the_day for an astronomy quote. "
        "Call telescopius_observation_lists / telescopius_observation_list_targets for the user's saved lists. "
        "Call telescopius_my_equipment for the user's registered telescope/camera gear."
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

    # Telescopius API key — used by telescopius_tool.py for observation planning.
    # Get a free key at https://api.telescopius.com/
    telescopius_api_key: str = ""

    # Response language — the AI will be instructed to reply in this language.
    # Set to "English" (the default) to keep responses in English regardless of
    # the language used by the user.  Set to "" to let the model decide.
    response_language: str = "English"

    # Observer / user context injected into every new conversation's system prompt.
    # Coordinates default to Winnipeg, Manitoba, Canada.
    observer_location: str = "Winnipeg, Manitoba, Canada"
    observer_timezone: str = "America/Winnipeg"  # IANA timezone name
    observer_lat: float = 49.8951
    observer_lon: float = -97.1384
    # First name shown in the opening greeting.  Leave empty to skip the greeting.
    user_first_name: str = "Gord"

    # Microsoft Entra authentication
    entra_auth_enabled: bool = False
    entra_tenant_id: str = ""
    entra_spa_client_id: str = ""
    entra_api_client_id: str = ""
    entra_api_scope: str = ""
    entra_redirect_uri: str = ""


settings = Settings()
