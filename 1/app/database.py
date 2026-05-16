import time
import logging
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.config import app_config

log = logging.getLogger(__name__)


def build_engine() -> Engine:
    db_url = app_config.get_database_url()
    return create_engine(
        db_url,
        pool_size=app_config.connection_pool_size,
        max_overflow=app_config.connection_max_overflow,
        pool_timeout=app_config.connection_pool_timeout,
        pool_pre_ping=True,
        future=True,
    )


db_engine = build_engine()
SessionFactory = sessionmaker(bind=db_engine, autocommit=False, autoflush=False, future=True)


def check_database_connection() -> None:
    retries = app_config.retry_count
    delay = app_config.retry_delay

    for i in range(1, retries + 1):
        try:
            with db_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            log.info("Подключение к базе данных установлено успешно")
            return
        except SQLAlchemyError as e:
            log.warning(
                "Попытка подключения %s из %s не удалась: %s",
                i,
                retries,
                e,
            )
            time.sleep(delay)

    raise RuntimeError("Не удалось установить соединение с базой данных")


@contextmanager
def get_db_session() -> Iterator[Session]:
    db_session = SessionFactory()
    try:
        yield db_session
    finally:
        db_session.close()
