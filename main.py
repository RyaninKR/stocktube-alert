"""
StockTube Alert MVP v0.4.0
YouTube 투자 영상 → AI 검색식 자동 생성 → 실시간 종목 스크리닝
웹앱 + 텔레그램 미니앱 동시 지원 + PostgreSQL + 워치리스트
"""

import os
import json
import hmac
import hashlib
import logging
from urllib.parse import parse_qs, unquote
from datetime import datetime
from typing import Optional
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from dotenv import load_dotenv
from shared.database import (
    get_pool, init_db, upsert_user,
    get_notification_settings, update_notification_settings,
    create_watchlist, get_watchlists, get_watchlist,
    update_watchlist, delete_watchlist, get_alert_history,
)

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── Config ───
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
WEBAPP_URL = os.getenv("WEBAPP_URL", "")

# ─── DB Pool ───
db_pool = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    db_pool = await get_pool()
    await init_db(db_pool)
    logger.info("DB pool initialized")
    yield
    await db_pool.close()


app = FastAPI(title="StockTube Alert MVP", version="0.4.0", lifespan=lifespan)


# ─── Pydantic Models ───
class AnalyzeUrlRequest(BaseModel):
    youtube_url: str

class ParseRequest(BaseModel):
    transcript: str

class ScreenRequest(BaseModel):
    filters: dict

class NotifyRequest(BaseModel):
    bot_token: Optional[str] = None
    chat_id: str
    message: str

class TelegramRegisterRequest(BaseModel):
    init_data: str

class NotificationSettingsRequest(BaseModel):
    chat_id: str
    notify_on_match: bool = True
    notify_on_analyze: bool = True
    notify_on_new_video: bool = True

class WatchlistCreateRequest(BaseModel):
    chat_id: str
    name: str
    filters: dict
    source_video_url: Optional[str] = None

class WatchlistUpdateRequest(BaseModel):
    name: Optional[str] = None
    filters: Optional[dict] = None
    is_active: Optional[bool] = None


# ─── Telegram initData 검증 ───
def verify_telegram_init_data(init_data: str, bot_token: str) -> dict:
    parsed = parse_qs(init_data)
    received_hash = parsed.get("hash", [None])[0]
    if not received_hash:
        raise ValueError("hash not found")

    data_pairs = []
    for key, values in parsed.items():
        if key == "hash":
            continue
        data_pairs.append(f"{key}={unquote(values[0])}")
    data_pairs.sort()
    data_check_string = "\n".join(data_pairs)

    secret_key = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
    computed_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

    if computed_hash != received_hash:
        raise ValueError("Invalid hash")

    user_data = parsed.get("user", [None])[0]
    if user_data:
        return json.loads(unquote(user_data))
    return {}


# ─── HTML ───
@app.get("/", response_class=HTMLResponse)
async def read_index():
    return """<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>StockTube Alert - YouTube 투자 영상 분석</title>
    <meta name="description" content="YouTube 투자 영상 → AI 검색식 자동 생성 → 실시간 종목 스크리닝">
    <script src="https://telegram.org/js/telegram-web-app.js"></script>
    <style>
        :root { --bg: #fff; --text: #1a1a2e; --card: #f8f9fa; --primary: #0088cc; --border: #e0e0e0; --danger: #dc3545; --success: #28a745; }
        .tg-theme { --bg: var(--tg-theme-bg-color,#fff); --text: var(--tg-theme-text-color,#1a1a2e); --card: var(--tg-theme-secondary-bg-color,#f8f9fa); --primary: var(--tg-theme-button-color,#0088cc); }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; background: var(--bg); color: var(--text); padding: 16px; max-width: 600px; margin: 0 auto; }
        h1 { font-size: 1.5rem; margin-bottom: 4px; }
        .subtitle { color: #666; margin-bottom: 16px; font-size: 0.85rem; }
        .tabs { display: flex; gap: 4px; margin-bottom: 16px; }
        .tab { flex: 1; padding: 10px; text-align: center; border: 1px solid var(--border); border-radius: 8px; cursor: pointer; font-weight: 600; font-size: 0.9rem; background: var(--card); }
        .tab.active { background: var(--primary); color: #fff; border-color: var(--primary); }
        .panel { display: none; }
        .panel.active { display: block; }
        .card { background: var(--card); border-radius: 12px; padding: 16px; margin-bottom: 12px; border: 1px solid var(--border); }
        .card h2 { font-size: 1.05rem; margin-bottom: 10px; }
        input[type="text"], input[type="url"] { width: 100%; padding: 10px; border: 1px solid var(--border); border-radius: 8px; font-size: 0.95rem; margin-bottom: 10px; background: var(--bg); color: var(--text); }
        .btn { width: 100%; padding: 10px; border: none; border-radius: 8px; font-size: 0.95rem; cursor: pointer; font-weight: 600; }
        .btn-primary { background: var(--primary); color: #fff; }
        .btn-danger { background: var(--danger); color: #fff; }
        .btn-success { background: var(--success); color: #fff; }
        .btn-sm { width: auto; padding: 6px 12px; font-size: 0.8rem; }
        .btn:disabled { opacity: 0.5; }
        .result { margin-top: 12px; white-space: pre-wrap; font-size: 0.85rem; max-height: 300px; overflow-y: auto; }
        .toggle-row { display: flex; justify-content: space-between; align-items: center; padding: 8px 0; }
        .toggle-row + .toggle-row { border-top: 1px solid var(--border); }
        .switch { position: relative; width: 44px; height: 24px; }
        .switch input { opacity: 0; width: 0; height: 0; }
        .slider { position: absolute; cursor: pointer; inset: 0; background: #ccc; border-radius: 24px; transition: 0.3s; }
        .slider:before { content: ""; position: absolute; height: 18px; width: 18px; left: 3px; bottom: 3px; background: #fff; border-radius: 50%; transition: 0.3s; }
        input:checked + .slider { background: var(--primary); }
        input:checked + .slider:before { transform: translateX(20px); }
        .wl-item { display: flex; justify-content: space-between; align-items: center; padding: 10px 0; }
        .wl-item + .wl-item { border-top: 1px solid var(--border); }
        .wl-info { flex: 1; }
        .wl-name { font-weight: 600; font-size: 0.95rem; }
        .wl-filters { font-size: 0.8rem; color: #666; margin-top: 2px; }
        .wl-actions { display: flex; gap: 6px; }
        .badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.75rem; font-weight: 600; }
        .badge-on { background: #d4edda; color: #155724; }
        .badge-off { background: #f8d7da; color: #721c24; }
        .badge-mode { background: #e2e3e5; color: #383d41; }
        .empty { text-align: center; color: #999; padding: 24px; font-size: 0.9rem; }
        #register-banner { display: none; background: #fff3cd; color: #856404; padding: 12px; border-radius: 8px; margin-bottom: 12px; text-align: center; font-size: 0.85rem; }
    </style>
</head>
<body>
    <h1>📺 StockTube Alert</h1>
    <p class="subtitle">YouTube 투자 영상 → AI 검색식 → 실시간 스크리닝 <span id="mode-badge" class="badge badge-mode">웹</span></p>

    <div id="register-banner">텔레그램에서 접속하면 맞춤 알림을 받을 수 있습니다!</div>

    <div class="tabs">
        <div class="tab active" onclick="switchTab('analyze')">🔍 분석</div>
        <div class="tab" onclick="switchTab('watchlist')">📋 워치리스트</div>
        <div class="tab" id="tab-settings" style="display:none" onclick="switchTab('settings')">⚙️ 설정</div>
    </div>

    <!-- 분석 탭 -->
    <div id="panel-analyze" class="panel active">
        <div class="card">
            <h2>YouTube 영상 분석</h2>
            <input type="url" id="youtube-url" placeholder="YouTube URL을 입력하세요">
            <button class="btn btn-primary" id="analyze-btn" onclick="analyzeVideo()">분석 시작</button>
            <div id="analyze-result" class="result"></div>
        </div>
        <div id="save-watchlist-section" style="display:none" class="card">
            <h2>🔔 검색식을 워치리스트에 등록</h2>
            <input type="text" id="wl-name" placeholder="검색식 이름 (예: 저PER 고ROE)">
            <button class="btn btn-success" onclick="saveToWatchlist()">워치리스트에 추가</button>
        </div>
        <div class="card">
            <h2>📊 종목 스크리닝</h2>
            <input type="text" id="screen-filters" placeholder='필터 JSON (예: {"per_lte": 10})'>
            <button class="btn btn-primary" onclick="screenStocks()">스크리닝 실행</button>
            <div id="screen-result" class="result"></div>
        </div>
    </div>

    <!-- 워치리스트 탭 -->
    <div id="panel-watchlist" class="panel">
        <div class="card">
            <h2>📋 내 워치리스트</h2>
            <div id="watchlist-container"><div class="empty">워치리스트가 없습니다.<br>영상 분석 후 검색식을 등록하세요!</div></div>
        </div>
    </div>

    <!-- 설정 탭 (텔레그램만) -->
    <div id="panel-settings" class="panel">
        <div class="card">
            <h2>🔔 알림 설정</h2>
            <div class="toggle-row">
                <span>검색식 매칭 알림</span>
                <label class="switch"><input type="checkbox" id="notify-match" checked onchange="saveSettings()"><span class="slider"></span></label>
            </div>
            <div class="toggle-row">
                <span>분석 완료 알림</span>
                <label class="switch"><input type="checkbox" id="notify-analyze" checked onchange="saveSettings()"><span class="slider"></span></label>
            </div>
            <div class="toggle-row">
                <span>신규 영상 알림</span>
                <label class="switch"><input type="checkbox" id="notify-video" checked onchange="saveSettings()"><span class="slider"></span></label>
            </div>
        </div>
    </div>

    <script>
        const tg = window.Telegram?.WebApp;
        const isTelegram = !!(tg && tg.initData);
        let chatId = null;
        let lastAnalysis = null;

        // ─── Init ───
        if (isTelegram) {
            tg.ready(); tg.expand();
            document.body.classList.add('tg-theme');
            document.getElementById('mode-badge').textContent = '미니앱';
            document.getElementById('mode-badge').className = 'badge badge-on';
            document.getElementById('tab-settings').style.display = '';
            chatId = String(tg.initDataUnsafe?.user?.id || '');

            fetch('/api/telegram/register', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({init_data: tg.initData})
            }).then(r => r.json()).then(d => {
                chatId = d.chat_id;
                if (d.settings) {
                    document.getElementById('notify-match').checked = d.settings.notify_on_match;
                    document.getElementById('notify-analyze').checked = d.settings.notify_on_analyze;
                    document.getElementById('notify-video').checked = d.settings.notify_on_new_video;
                }
                loadWatchlists();
            });
        } else {
            document.getElementById('register-banner').style.display = 'block';
            // 웹에서는 localStorage로 임시 chat_id
            chatId = localStorage.getItem('st_chat_id') || 'web_' + Math.random().toString(36).substr(2, 9);
            localStorage.setItem('st_chat_id', chatId);
        }

        // ─── Tabs ───
        function switchTab(name) {
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
            event.target.classList.add('active');
            document.getElementById('panel-' + name).classList.add('active');
            if (name === 'watchlist') loadWatchlists();
        }

        // ─── Analyze ───
        async function analyzeVideo() {
            const url = document.getElementById('youtube-url').value;
            if (!url) return alert('YouTube URL을 입력하세요');
            const btn = document.getElementById('analyze-btn');
            btn.disabled = true; btn.textContent = '분석 중...';
            document.getElementById('analyze-result').textContent = '';
            document.getElementById('save-watchlist-section').style.display = 'none';
            try {
                const res = await fetch('/api/analyze', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({youtube_url:url})});
                const data = await res.json();
                lastAnalysis = data;
                document.getElementById('analyze-result').textContent = JSON.stringify(data, null, 2);
                if (data.screen_filters) {
                    document.getElementById('save-watchlist-section').style.display = 'block';
                    document.getElementById('wl-name').value = data.strategy_summary?.substring(0, 30) || '새 검색식';
                    document.getElementById('screen-filters').value = JSON.stringify(data.screen_filters);
                }
            } catch(e) { document.getElementById('analyze-result').textContent = '오류: ' + e.message; }
            finally { btn.disabled = false; btn.textContent = '분석 시작'; }
        }

        // ─── Screen ───
        async function screenStocks() {
            const s = document.getElementById('screen-filters').value;
            let filters = {};
            try { if (s) filters = JSON.parse(s); } catch { return alert('올바른 JSON 형식'); }
            try {
                const res = await fetch('/api/screen', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({filters})});
                document.getElementById('screen-result').textContent = JSON.stringify(await res.json(), null, 2);
            } catch(e) { document.getElementById('screen-result').textContent = '오류: ' + e.message; }
        }

        // ─── Watchlist ───
        async function saveToWatchlist() {
            if (!chatId) return alert('로그인이 필요합니다');
            const name = document.getElementById('wl-name').value || '새 검색식';
            const filters = lastAnalysis?.screen_filters || {};
            const sourceUrl = document.getElementById('youtube-url').value;
            try {
                const res = await fetch('/api/watchlist', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({chat_id:chatId, name, filters, source_video_url:sourceUrl})});
                const data = await res.json();
                if (data.id) { alert('워치리스트에 추가됐습니다!'); document.getElementById('save-watchlist-section').style.display = 'none'; }
                else alert('저장 실패: ' + JSON.stringify(data));
            } catch(e) { alert('오류: ' + e.message); }
        }

        async function loadWatchlists() {
            if (!chatId) return;
            try {
                const res = await fetch('/api/watchlist/' + chatId);
                const data = await res.json();
                const container = document.getElementById('watchlist-container');
                if (!data.watchlists?.length) { container.innerHTML = '<div class="empty">워치리스트가 없습니다.<br>영상 분석 후 검색식을 등록하세요!</div>'; return; }
                container.innerHTML = data.watchlists.map(w => `
                    <div class="wl-item">
                        <div class="wl-info">
                            <div class="wl-name">${w.name} <span class="badge ${w.is_active?'badge-on':'badge-off'}">${w.is_active?'활성':'비활성'}</span></div>
                            <div class="wl-filters">${JSON.stringify(w.filters)}</div>
                        </div>
                        <div class="wl-actions">
                            <button class="btn btn-sm ${w.is_active?'btn-danger':'btn-success'}" onclick="toggleWatchlist(${w.id},${!w.is_active})">${w.is_active?'OFF':'ON'}</button>
                            <button class="btn btn-sm btn-danger" onclick="deleteWatchlist(${w.id})">삭제</button>
                        </div>
                    </div>
                `).join('');
            } catch(e) { console.error(e); }
        }

        async function toggleWatchlist(id, active) {
            await fetch('/api/watchlist/' + id, {method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify({is_active:active})});
            loadWatchlists();
        }

        async function deleteWatchlist(id) {
            if (!confirm('삭제하시겠습니까?')) return;
            await fetch('/api/watchlist/' + id, {method:'DELETE'});
            loadWatchlists();
        }

        // ─── Settings ───
        async function saveSettings() {
            if (!isTelegram || !chatId) return;
            await fetch('/api/telegram/settings', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({
                chat_id: chatId,
                notify_on_match: document.getElementById('notify-match').checked,
                notify_on_analyze: document.getElementById('notify-analyze').checked,
                notify_on_new_video: document.getElementById('notify-video').checked,
            })});
        }
    </script>
</body>
</html>"""


# ─── Telegram API ───
@app.post("/api/telegram/register")
async def register_telegram_user(req: TelegramRegisterRequest):
    if not TELEGRAM_BOT_TOKEN:
        raise HTTPException(500, "TELEGRAM_BOT_TOKEN not configured")
    try:
        user_info = verify_telegram_init_data(req.init_data, TELEGRAM_BOT_TOKEN)
    except ValueError as e:
        raise HTTPException(400, str(e))

    chat_id = str(user_info.get("id", ""))
    if not chat_id:
        raise HTTPException(400, "user id not found")

    await upsert_user(db_pool, chat_id, user_info.get("first_name"), user_info.get("username"))
    settings = await get_notification_settings(db_pool, chat_id)
    logger.info(f"Telegram user registered: {chat_id}")

    return {"status": "ok", "chat_id": chat_id, "settings": settings}


@app.post("/api/telegram/settings")
async def update_settings(req: NotificationSettingsRequest):
    await update_notification_settings(db_pool, req.chat_id, {
        "notify_on_match": req.notify_on_match,
        "notify_on_analyze": req.notify_on_analyze,
        "notify_on_new_video": req.notify_on_new_video,
    })
    return {"status": "ok"}


@app.get("/api/telegram/settings/{chat_id}")
async def get_settings(chat_id: str):
    settings = await get_notification_settings(db_pool, chat_id)
    return {"settings": settings}


# ─── Watchlist API ───
@app.post("/api/watchlist")
async def create_watchlist_api(req: WatchlistCreateRequest):
    wl_id = await create_watchlist(db_pool, req.chat_id, req.name, req.filters, req.source_video_url)
    return {"id": wl_id, "status": "created"}


@app.get("/api/watchlist/{chat_id}")
async def get_watchlists_api(chat_id: str):
    watchlists = await get_watchlists(db_pool, chat_id)
    # Convert filters from string to dict if needed, and serialize dates
    result = []
    for w in watchlists:
        item = {**w}
        if isinstance(item.get("filters"), str):
            item["filters"] = json.loads(item["filters"])
        for k in ("created_at", "updated_at"):
            if item.get(k):
                item[k] = item[k].isoformat()
        result.append(item)
    return {"watchlists": result}


@app.put("/api/watchlist/{watchlist_id}")
async def update_watchlist_api(watchlist_id: int, req: WatchlistUpdateRequest):
    await update_watchlist(db_pool, watchlist_id, req.name, req.filters, req.is_active)
    return {"status": "updated"}


@app.delete("/api/watchlist/{watchlist_id}")
async def delete_watchlist_api(watchlist_id: int):
    await delete_watchlist(db_pool, watchlist_id)
    return {"status": "deleted"}


@app.get("/api/watchlist/{watchlist_id}/history")
async def get_history_api(watchlist_id: int):
    history = await get_alert_history(db_pool, watchlist_id)
    result = []
    for h in history:
        item = {**h}
        for k in ("matched_at", "notified_at"):
            if item.get(k):
                item[k] = str(item[k])
        result.append(item)
    return {"history": result}


# ─── 기존 API (호환 유지) ───
@app.post("/api/analyze")
async def analyze_video(req: AnalyzeUrlRequest):
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
        import re

        patterns = [r'(?:v=|/v/|youtu\.be/)([a-zA-Z0-9_-]{11})']
        video_id = None
        for pattern in patterns:
            match = re.search(pattern, req.youtube_url)
            if match:
                video_id = match.group(1)
                break

        if not video_id:
            return {"error": "유효한 YouTube URL이 아닙니다"}

        try:
            ytt = YouTubeTranscriptApi()
            transcript = ytt.fetch(video_id, languages=['ko', 'en'])
            transcript_text = " ".join([snippet.text for snippet in transcript])
        except Exception as e:
            return {"error": f"자막 추출 실패: {str(e)}"}

        result = await _parse_with_ai(transcript_text)
        result["video_id"] = video_id
        result["youtube_url"] = req.youtube_url
        return result

    except ImportError:
        return {"error": "youtube_transcript_api not installed"}
    except Exception as e:
        logger.error(f"Analyze error: {e}")
        return {"error": str(e)}


@app.post("/api/parse")
async def parse_transcript(req: ParseRequest):
    return await _parse_with_ai(req.transcript)


@app.post("/api/screen")
async def screen_stocks_api(req: ScreenRequest):
    try:
        result = await _screen_stocks(req.filters)
        return result
    except Exception as e:
        logger.error(f"Screen error: {e}")
        return {"error": str(e)}


async def _screen_stocks(filters: dict) -> dict:
    """KRX 데이터로 종목 스크리닝 (pykrx fallback 포함)"""
    import pandas as pd

    df = None
    today = datetime.now().strftime("%Y%m%d")
    source = "unknown"

    # 방법 1: pykrx
    try:
        from pykrx import stock
        df = stock.get_market_fundamental(today, market="ALL")
        if df is not None and not df.empty:
            source = "pykrx"
    except Exception as e:
        logger.warning(f"pykrx failed: {e}")

    # 방법 2: KRX 직접 API
    if df is None or df.empty:
        try:
            df, today = await _fetch_krx_direct(today)
            if df is not None and not df.empty:
                source = "krx_direct"
        except Exception as e:
            logger.warning(f"KRX direct failed: {e}")

    if df is None or df.empty:
        return {"error": "시장 데이터를 가져올 수 없습니다. 장 운영 시간을 확인해주세요.", "date": today}

    # 필터 적용
    results = df.copy()
    applied_filters = []
    for key, value in filters.items():
        parts = key.rsplit("_", 1)
        if len(parts) != 2:
            continue
        col, op = parts
        col_upper = col.upper()
        # 컬럼명 매칭 (대소문자 무시)
        matched_col = None
        for c in results.columns:
            if c.upper() == col_upper:
                matched_col = c
                break
        if matched_col is None:
            continue

        try:
            if op == "lte":
                results = results[(results[matched_col] <= value) & (results[matched_col] > 0)]
            elif op == "gte":
                results = results[results[matched_col] >= value]
            elif op == "eq":
                results = results[results[matched_col] == value]
            elif op == "lt":
                results = results[(results[matched_col] < value) & (results[matched_col] > 0)]
            elif op == "gt":
                results = results[results[matched_col] > value]
            applied_filters.append(f"{matched_col} {op} {value}")
        except Exception as e:
            logger.warning(f"Filter {key}={value} error: {e}")

    # 종목명 추가
    tickers = results.index.tolist()[:50]
    if source == "pykrx":
        try:
            from pykrx import stock
            names = {t: stock.get_market_ticker_name(t) for t in tickers}
        except:
            names = {}
    else:
        names = {}

    results = results.head(50)
    if "종목명" not in results.columns and names:
        results = results.copy()
        results["종목명"] = results.index.map(lambda x: names.get(x, ""))

    stocks = []
    for idx, row in results.iterrows():
        stock_data = {"ticker": idx}
        for col in results.columns:
            val = row[col]
            if hasattr(val, 'item'):
                val = val.item()
            stock_data[col] = val
        stocks.append(stock_data)

    return {
        "count": len(stocks),
        "date": today,
        "source": source,
        "filters_applied": applied_filters,
        "stocks": stocks,
    }


async def _fetch_krx_direct(date: str):
    """KRX API 직접 호출 (pykrx 우회)"""
    import pandas as pd

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101'
    }
    params = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT03501',
        'locale': 'ko_KR',
        'searchType': '1',
        'mktId': 'ALL',
        'trdDd': date,
    }

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd',
            data=params,
            headers=headers,
            timeout=15,
        )
        data = resp.json()

    if 'output' not in data or not data['output']:
        return None, date

    records = data['output']
    df = pd.DataFrame(records)

    # 컬럼 정리 - KRX 응답 컬럼명을 표준화
    col_map = {}
    for col in df.columns:
        upper = col.upper()
        if 'PER' in upper and 'BPS' not in upper:
            col_map[col] = 'PER'
        elif 'PBR' in upper:
            col_map[col] = 'PBR'
        elif 'EPS' in upper:
            col_map[col] = 'EPS'
        elif 'BPS' in upper:
            col_map[col] = 'BPS'
        elif 'DIV' in upper or '배당' in col:
            col_map[col] = 'DIV'
        elif 'DPS' in upper:
            col_map[col] = 'DPS'
        elif '종목코드' in col or 'ISU_SRT_CD' in col:
            col_map[col] = 'ticker'
        elif '종목명' in col or 'ISU_ABBRV' in col:
            col_map[col] = '종목명'

    if col_map:
        df = df.rename(columns=col_map)

    # ticker를 인덱스로
    if 'ticker' in df.columns:
        df = df.set_index('ticker')

    # 숫자 변환
    for col in ['PER', 'PBR', 'EPS', 'BPS', 'DIV', 'DPS']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')

    return df, date


@app.post("/api/notify")
async def send_telegram_notification(req: NotifyRequest):
    bot_token = req.bot_token or TELEGRAM_BOT_TOKEN
    if not bot_token:
        raise HTTPException(400, "bot_token required")

    payload = {"chat_id": req.chat_id, "text": req.message, "parse_mode": "HTML"}
    if WEBAPP_URL:
        payload["reply_markup"] = {"inline_keyboard": [[{"text": "📊 StockTube에서 확인", "web_app": {"url": WEBAPP_URL}}]]}

    async with httpx.AsyncClient() as client:
        resp = await client.post(f"https://api.telegram.org/bot{bot_token}/sendMessage", json=payload)
        return resp.json()


@app.post("/api/telegram/webhook")
async def telegram_webhook(request: Request):
    body = await request.json()
    message = body.get("message", {})
    text = message.get("text", "")
    chat_id = str(message.get("chat", {}).get("id", ""))

    if text == "/start":
        user = message.get("from", {})
        await upsert_user(db_pool, chat_id, user.get("first_name"), user.get("username"))

        welcome = ("👋 <b>StockTube Alert에 오신 걸 환영합니다!</b>\n\n"
                    "YouTube 투자 영상을 AI로 분석하여 종목 스크리닝 검색식을 자동 생성합니다.\n\n"
                    "아래 버튼을 눌러 시작하세요 👇")
        payload = {"chat_id": chat_id, "text": welcome, "parse_mode": "HTML"}
        if WEBAPP_URL:
            payload["reply_markup"] = {"inline_keyboard": [[{"text": "📺 StockTube Alert 열기", "web_app": {"url": WEBAPP_URL}}]]}

        async with httpx.AsyncClient() as client:
            await client.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", json=payload)

    return {"ok": True}


# ─── AI Helper ───
async def _parse_with_ai(transcript: str) -> dict:
    if not OPENAI_API_KEY:
        return {"error": "OPENAI_API_KEY not configured", "transcript_preview": transcript[:200]}
    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=OPENAI_API_KEY)
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": """당신은 주식 투자 전문가입니다. YouTube 투자 영상의 자막을 분석하여 다음을 추출하세요:
1. 핵심 투자 전략 요약
2. 언급된 종목 리스트
3. 종목 스크리닝 검색식 (PER, PBR, ROE 등 재무지표 기반 필터)

JSON 형식으로 응답:
{"strategy_summary": "전략 요약", "mentioned_stocks": ["종목1"], "screen_filters": {"per_lte": 10, "roe_gte": 15}, "confidence": 0.8}"""},
                {"role": "user", "content": f"다음 자막을 분석해주세요:\n\n{transcript[:4000]}"}
            ],
            response_format={"type": "json_object"},
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        return {"error": f"AI parsing failed: {str(e)}"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
