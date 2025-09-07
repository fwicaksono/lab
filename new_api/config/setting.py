from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # Application
    app_name: str
    app_version: str
    app_env: str
    
    # Server
    fastapi_host: str
    fastapi_port: int
    
    # CORS
    allowed_origins: str
    
    # Rate Limiting
    rate_limit_enabled: bool
    rate_limit_requests: int
    rate_limit_window: int
    
    # Search
    default_max_results: int
    max_search_results: int
    search_timeout: int
    
    # Sales-specific settings
    default_sales_max_results: int
    max_sales_results: int
    
    # Join operation settings
    max_join_results: int
    join_timeout: int
    
    # JWT Settings
    secret_key: str
    
    # Redis Settings (for rate limiting)
    redis_host: str
    redis_port: int
    redis_db: int
    
    # APM Settings
    apm_service_name: str
    apm_server_url: str
    
    # AI/Gemini Vertex AI Settings
    gemini_model: str
    project_name: str
    location_name: str
    service_account_file: str
    
    # ClickHouse Settings for medical search
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_database: str
    clickhouse_username: str
    clickhouse_password: str
    clickhouse_table_name: str
    
    # Google BigQuery Settings (if using BigQuery instead of ClickHouse)
    bigquery_project_id: str
    bigquery_dataset_id: str
    bigquery_table_name: str

    model_config = SettingsConfigDict(env_file=".env")

    def __getitem__(self, item):
        return getattr(self, item)

env = Settings()