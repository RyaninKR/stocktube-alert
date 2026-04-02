"""
공유 데이터베이스 모듈
web 서비스와 screener 서비스가 함께 사용
"""

import os
import logging
from contextlib import asynccontextmanager

import asyncpg

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "")

# ─── Schema ───
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS users (
    chat_id VARCHAR PRIMARY KEY,
    first_name VARCHAR,
    username VARCHAR,
    registered_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS watchlists (
    id SERIAL PRIMARY KEY,
    chat_id VARCHAR REFERENCES users(chat_id) ON DELETE CASCADE,
    name VARCHAR NOT NULL,
    filters JSONB NOT NULL,
    source_video_url VARCHAR,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS alert_history (
    id SERIAL PRIMARY KEY,
    watchlist_id INT REFERENCES watchlists(id) ON DELETE CASCADE,
    ticker VARCHAR NOT NULL,
    stock_name VARCHAR,
    matched_data JSONB,
    matched_at DATE NOT NULL,
    notified_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(watchlist_id, ticker, matched_at)
);

CREATE TABLE IF NOT EXISTS notification_settings (
    chat_id VARCHAR PRIMARY KEY REFERENCES users(chat_id) ON DELETE CASCADE,
    notify_on_match BOOLEAN DEFAULT TRUE,
    notify_on_analyze BOOLEAN DEFAULT TRUE,
    notify_on_new_video BOOLEAN DEFAULT TRUE,
    quiet_start TIME DEFAULT '23:00',
    quiet_end TIME DEFAULT '08:00',
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_watchlists_chat_id ON watchlists(chat_id);
CREATE INDEX IF NOT EXISTS idx_watchlists_active ON watchlists(is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_alert_history_watchlist ON alert_history(watchlist_id, matched_at);
"""


async def get_pool() -> asyncpg.Pool:
    """커넥션 풀 생성"""
    return await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)


async def init_db(pool: asyncpg.Pool):
    """스키마 초기화"""
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL)
    logger.info("Database schema initialized")


# ─── User Operations ───
async def upsert_user(pool, chat_id: str, first_name: str = None, username: str = None):
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (chat_id, first_name, username)
            VALUES ($1, $2, $3)
            ON CONFLICT (chat_id) DO UPDATE SET first_name = $2, username = $3
        """, chat_id, first_name, username)

        # 알림 설정 기본값 생성
        await conn.execute("""
            INSERT INTO notification_settings (chat_id)
            VALUES ($1)
            ON CONFLICT (chat_id) DO NOTHING
        """, chat_id)


async def get_notification_settings(pool, chat_id: str) -> dict:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM notification_settings WHERE chat_id = $1", chat_id
        )
        if row:
            return dict(row)
        return {
            "notify_on_match": True,
            "notify_on_analyze": True,
            "notify_on_new_video": True,
        }


async def update_notification_settings(pool, chat_id: str, settings: dict):
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO notification_settings (chat_id, notify_on_match, notify_on_analyze, notify_on_new_video, updated_at)
            VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (chat_id) DO UPDATE SET
                notify_on_match = $2, notify_on_analyze = $3, notify_on_new_video = $4, updated_at = NOW()
        """, chat_id,
            settings.get("notify_on_match", True),
            settings.get("notify_on_analyze", True),
            settings.get("notify_on_new_video", True),
        )


# ─── Watchlist Operations ───
async def create_watchlist(pool, chat_id: str, name: str, filters: dict, source_video_url: str = None) -> int:
    import json
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO watchlists (chat_id, name, filters, source_video_url)
            VALUES ($1, $2, $3::jsonb, $4)
            RETURNING id
        """, chat_id, name, json.dumps(filters), source_video_url)
        return row["id"]


async def get_watchlists(pool, chat_id: str) -> list:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM watchlists WHERE chat_id = $1 ORDER BY created_at DESC", chat_id
        )
        return [dict(r) for r in rows]


async def get_watchlist(pool, watchlist_id: int) -> dict:
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM watchlists WHERE id = $1", watchlist_id)
        return dict(row) if row else None


async def update_watchlist(pool, watchlist_id: int, name: str = None, filters: dict = None, is_active: bool = None):
    import json
    async with pool.acquire() as conn:
        if name is not None:
            await conn.execute("UPDATE watchlists SET name = $1, updated_at = NOW() WHERE id = $2", name, watchlist_id)
        if filters is not None:
            await conn.execute("UPDATE watchlists SET filters = $1::jsonb, updated_at = NOW() WHERE id = $2", json.dumps(filters), watchlist_id)
        if is_active is not None:
            await conn.execute("UPDATE watchlists SET is_active = $1, updated_at = NOW() WHERE id = $2", is_active, watchlist_id)


async def delete_watchlist(pool, watchlist_id: int):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM watchlists WHERE id = $1", watchlist_id)


async def get_active_watchlists(pool) -> list:
    """모든 활성 워치리스트 조회 (screener용)"""
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT w.*, ns.notify_on_match
            FROM watchlists w
            JOIN notification_settings ns ON w.chat_id = ns.chat_id
            WHERE w.is_active = TRUE AND ns.notify_on_match = TRUE
        """)
        return [dict(r) for r in rows]


# ─── Alert History ───
async def get_today_alerted_tickers(pool, watchlist_id: int, today: str) -> set:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT ticker FROM alert_history WHERE watchlist_id = $1 AND matched_at = $2",
            watchlist_id, today
        )
        return {r["ticker"] for r in rows}


async def save_alert(pool, watchlist_id: int, ticker: str, stock_name: str, matched_data: dict, matched_at: str):
    import json
    async with pool.acquire() as conn:
        try:
            await conn.execute("""
                INSERT INTO alert_history (watchlist_id, ticker, stock_name, matched_data, matched_at)
                VALUES ($1, $2, $3, $4::jsonb, $5)
                ON CONFLICT (watchlist_id, ticker, matched_at) DO NOTHING
            """, watchlist_id, ticker, stock_name, json.dumps(matched_data), matched_at)
        except Exception as e:
            logger.error(f"Save alert error: {e}")


async def get_alert_history(pool, watchlist_id: int, limit: int = 50) -> list:
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT * FROM alert_history
            WHERE watchlist_id = $1
            ORDER BY notified_at DESC
            LIMIT $2
        """, watchlist_id, limit)
        return [dict(r) for r in rows]
