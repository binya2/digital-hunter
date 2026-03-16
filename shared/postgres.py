import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from typing import Any

from psycopg2.extras import RealDictCursor

from shared.config import settings

table_init = '''
             CREATE TABLE IF NOT EXISTS targets
             (
                 entity_id          VARCHAR PRIMARY KEY,
                 last_lat           FLOAT,
                 last_lon           FLOAT,
                 priority_level     INT,
                 last_attack_id     VARCHAR,
                 last_weapon_type   VARCHAR,
                 damage_status      VARCHAR,
                 date_time_creation TIMESTAMP,
                 date_time_updating TIMESTAMP,
                 distance           FLOAT,
                 avg_speed          FLOAT
             ) \
             '''


class Postgres:
    def __init__(self):
        self._client = None
        self.is_db_initialized = False

    @property
    def client(self) -> Any:
        if self._client is None or self._client.closed != 0:
            if not self.is_db_initialized:
                self._create_database_if_missing()

            self._client = psycopg2.connect(
                host=settings.POSTGRES_HOST,
                port=settings.POSTGRES_PORT,
                user=settings.POSTGRES_USER,
                password=settings.POSTGRES_PASSWORD,
                dbname=settings.POSTGRES_DATABASE
            )
            if not self.is_db_initialized:
                with self._client.cursor() as cursor:
                    cursor.execute(table_init)
                self._client.commit()
                self.is_db_initialized = True

        return self._client

    def _create_database_if_missing(self):
        conn = psycopg2.connect(
            host=settings.POSTGRES_HOST,
            port=settings.POSTGRES_PORT,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD,
            dbname="postgres"
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (settings.POSTGRES_DATABASE,))
            if not cur.fetchone():
                cur.execute(f"CREATE DATABASE {settings.POSTGRES_DATABASE}")
        conn.close()

    def execute_query(self, query: str, params: tuple = None, fetch: bool = False) -> Any:
        with self.client.cursor() as cursor:
            cursor.execute(query, params or ())
            if fetch:
                return cursor.fetchall()
            else:
                self.client.commit()
                return cursor.rowcount

postgres_service = Postgres()
postgres_service.client
