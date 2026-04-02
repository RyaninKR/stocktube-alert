"""
StockTube Alert Screener - Cron Job
활성 워치리스트를 KRX 데이터로 스크리닝하여 매칭 종목 알림 전송

Railway Cron Schedule: */5 * * * 1-5 (평일 5분 간격, 서버 시간)
실제로는 KRX 장 시간(09:00~16:00 KST)에만 유의미한 데이터 변동
"""

import os
import sys
import json
import asyncio
import logging
from datetime import datetime, timezone, timedelta

import httpx

# Add parent dir to path for shared module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.database import (
    get_pool, init_db,
    get_active_watchlists, get_today_alerted_tickers, save_alert,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
WEBAPP_URL = os.getenv("WEBAPP_URL", "")
KST = timezone(timedelta(hours=9))


def is_market_hours() -> bool:
    """KST 기준 장 시간(08:50~16:10) 여부 확인"""
    now_kst = datetime.now(KST)
    # 주말 체크
    if now_kst.weekday() >= 5:
        logger.info(f"Weekend ({now_kst.strftime('%A')}), skipping.")
        return False
    hour = now_kst.hour
    minute = now_kst.minute
    # 08:50 ~ 16:10
    if (hour == 8 and minute >= 50) or (9 <= hour <= 15) or (hour == 16 and minute <= 10):
        return True
    logger.info(f"Outside market hours ({now_kst.strftime('%H:%M')} KST), skipping.")
    return False


def fetch_market_data() -> dict:
    """KRX 시장 데이터 가져오기"""
    from pykrx import stock

    today = datetime.now(KST).strftime("%Y%m%d")
    df = stock.get_market_fundamental(today, market="ALL")

    if df.empty:
        from pykrx.stock import get_nearest_business_day_in_a_week
        today = get_nearest_business_day_in_a_week(today)
        df = stock.get_market_fundamental(today, market="ALL")

    if df.empty:
        logger.warning("No market data available")
        return {"date": today, "data": None}

    return {"date": today, "data": df}


def apply_filters(df, filters: dict) -> list:
    """필터를 적용하여 매칭 종목 반환"""
    from pykrx import stock

    results = df.copy()
    for key, value in filters.items():
        parts = key.rsplit("_", 1)
        if len(parts) != 2:
            continue
        col, op = parts
        col = col.upper()
        if col not in results.columns:
            continue
        try:
            if op == "lte":
                results = results[(results[col] <= value) & (results[col] > 0)]
            elif op == "gte":
                results = results[results[col] >= value]
            elif op == "eq":
                results = results[results[col] == value]
            elif op == "lt":
                results = results[(results[col] < value) & (results[col] > 0)]
            elif op == "gt":
                results = results[results[col] > value]
        except Exception as e:
            logger.warning(f"Filter error {key}={value}: {e}")

    matched = []
    for ticker in results.index.tolist()[:100]:  # 최대 100개
        try:
            name = stock.get_market_ticker_name(ticker)
        except:
            name = ticker
        row = results.loc[ticker]
        matched.append({
            "ticker": ticker,
            "name": name,
            "data": {col: float(row[col]) if hasattr(row[col], 'item') else row[col]
                     for col in results.columns if col != "종목명"},
        })

    return matched


async def send_alert(chat_id: str, watchlist_name: str, new_matches: list):
    """텔레그램 알림 전송"""
    if not TELEGRAM_BOT_TOKEN:
        logger.warning("No TELEGRAM_BOT_TOKEN, skipping notification")
        return

    # 종목 목록 포맷
    stocks_text = "\n".join([
        f"• {m['name']} ({m['ticker']})"
        for m in new_matches[:10]
    ])
    remaining = len(new_matches) - 10
    if remaining > 0:
        stocks_text += f"\n... 외 {remaining}개"

    message = (
        f"🔔 <b>검색식 매칭 알림!</b>\n\n"
        f"📋 검색식: <b>{watchlist_name}</b>\n"
        f"📊 새로 매칭된 종목 {len(new_matches)}개:\n\n"
        f"{stocks_text}"
    )

    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
    }
    if WEBAPP_URL:
        payload["reply_markup"] = {
            "inline_keyboard": [[{
                "text": "📈 상세 결과 보기",
                "web_app": {"url": WEBAPP_URL}
            }]]
        }

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json=payload,
                timeout=10,
            )
            if resp.status_code == 200:
                logger.info(f"Alert sent to {chat_id} for '{watchlist_name}' ({len(new_matches)} matches)")
            else:
                logger.error(f"Telegram API error: {resp.status_code} {resp.text}")
        except Exception as e:
            logger.error(f"Send alert failed: {e}")


async def run_screening():
    """메인 스크리닝 루프"""
    logger.info("=== Screener started ===")

    # 장 시간 체크
    if not is_market_hours():
        return

    # DB 연결
    pool = await get_pool()
    await init_db(pool)

    try:
        # 활성 워치리스트 조회
        watchlists = await get_active_watchlists(pool)
        if not watchlists:
            logger.info("No active watchlists, done.")
            return

        logger.info(f"Processing {len(watchlists)} active watchlists")

        # 시장 데이터 가져오기
        market = fetch_market_data()
        if market["data"] is None:
            logger.warning("No market data, aborting.")
            return

        today = market["date"]
        df = market["data"]

        # 각 워치리스트 처리
        alerts_sent = 0
        for wl in watchlists:
            try:
                filters = wl["filters"]
                if isinstance(filters, str):
                    filters = json.loads(filters)

                # 스크리닝
                matched = apply_filters(df, filters)
                if not matched:
                    continue

                # 오늘 이미 알림 보낸 종목 제외
                already_alerted = await get_today_alerted_tickers(pool, wl["id"], today)
                new_matches = [m for m in matched if m["ticker"] not in already_alerted]

                if not new_matches:
                    continue

                logger.info(f"Watchlist '{wl['name']}' (id={wl['id']}): {len(new_matches)} new matches")

                # 알림 이력 저장
                for m in new_matches:
                    await save_alert(pool, wl["id"], m["ticker"], m["name"], m["data"], today)

                # 텔레그램 알림 전송
                await send_alert(wl["chat_id"], wl["name"], new_matches)
                alerts_sent += 1

            except Exception as e:
                logger.error(f"Error processing watchlist {wl['id']}: {e}")
                continue

        logger.info(f"=== Screener done: {alerts_sent} alerts sent ===")

    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(run_screening())
