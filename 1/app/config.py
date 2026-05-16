import os


class AppConfig:
    def __init__(self):
        self.host = os.getenv("DB_HOST", "db")
        self.port = int(os.getenv("DB_PORT", "5432"))
        self.database = os.getenv("DB_NAME", "shop_db")
        self.username = os.getenv("DB_USER", "shop_user")
        self.password = os.getenv("DB_PASSWORD", "shop_password")
        self.logging_level = os.getenv("LOG_LEVEL", "INFO")
        self.connection_pool_size = int(os.getenv("DB_POOL_SIZE", "5"))
        self.connection_max_overflow = int(os.getenv("DB_MAX_OVERFLOW", "10"))
        self.connection_pool_timeout = int(os.getenv("DB_POOL_TIMEOUT", "30"))
        self.retry_count = int(os.getenv("DB_CONNECT_RETRIES", "15"))
        self.retry_delay = int(os.getenv("DB_CONNECT_RETRY_DELAY_SEC", "2"))

    def get_database_url(self):
        return (
            f"postgresql+psycopg2://{self.username}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


app_config = AppConfig()
