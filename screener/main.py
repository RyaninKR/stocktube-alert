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
from shared.kis_api import screen_stocks_hybrid, KIS_APP_KEY

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
WEBAPP_URL = os.getenv("WEBAPP_URL", "")
KST = timezone(timedelta(hours=9))


def is_market_hours() -> bool:
    """KST 기준 장 시간(08:50~16:10) 여부 확인"""
    now_kst = datetime.now(KST)
    if now_kst.weekday() >= 5:
        logger.info(f"Weekend ({now_kst.strftime('%A')}), skipping.")
        return False
    hour = now_kst.hour
    minute = now_kst.minute
    if (hour == 8 and minute >= 50) or (9 <= hour <= 15) or (hour == 16 and minute <= 10):
        return True
    logger.info(f"Outside market hours ({now_kst.strftime('%H:%M')} KST), skipping.")
    return False


async def send_alert(chat_id: str, watchlist_name: str, new_matches: list):
    """텔레그램 알림 전송"""
    if not TELEGRAM_BOT_TOKEN:
        logger.warning("No TELEGRAM_BOT_TOKEN, skipping notification")
        return

    stocks_text = "\n".join([
        f"• {m.get('name', m['ticker'])} ({m['ticker']})"
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

    if not is_market_hours():
        return

    pool = await get_pool()
    await init_db(pool)

    try:
        watchlists = await get_active_watchlists(pool)
        if not watchlists:
            logger.info("No active watchlists, done.")
            return

        logger.info(f"Processing {len(watchlists)} active watchlists")

        # #2: datetime.date 객체 사용
        today_date = datetime.now(KST).date()
        today_str = today_date.strftime("%Y%m%d")

        alerts_sent = 0
        for wl in watchlists:
            try:
                filters = wl["filters"]
                if isinstance(filters, str):
                    filters = json.loads(filters)

                # #1: 하이브리드 스크리닝 사용
                result = await screen_stocks_hybrid(filters)
                matched = []
                if "error" not in result and result.get("stocks"):
                    matched = result["stocks"]
                    # ticker 키 보장
                    for m in matched:
                        if "ticker" not in m:
                            continue
                    logger.info(f"Hybrid screening for '{wl['name']}': {len(matched)} matches")

                if not matched:
                    continue

                # #2: datetime.date 전달
                already_alerted = await get_today_alerted_tickers(pool, wl["id"], today_date)
                new_matches = [m for m in matched if m.get("ticker") not in already_alerted]

                if not new_matches:
                    continue

                logger.info(f"Watchlist '{wl['name']}' (id={wl['id']}): {len(new_matches)} new matches")

                # #2: datetime.date 전달
                for m in new_matches:
                    await save_alert(
                        pool, wl["id"],
                        m.get("ticker", ""),
                        m.get("name", m.get("ticker", "")),
                        m,
                        today_date,
                    )

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
