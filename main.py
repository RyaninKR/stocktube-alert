"""
StockTube Alert MVP v0.5.0
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
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
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


app = FastAPI(title="StockTube Alert MVP", version="0.5.0", lifespan=lifespan)


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
        .result { margin-top: 12px; white-space: pre-wrap; font-size: 0.85rem; max-height: 400px; overflow-y: auto; }
        .analysis-card { background: var(--bg); border: 1px solid var(--border); border-radius: 10px; padding: 14px; margin-top: 12px; }
        .analysis-card + .analysis-card { margin-top: 8px; }
        .analysis-card h3 { font-size: 0.9rem; margin-bottom: 8px; display: flex; align-items: center; gap: 6px; }
        .filter-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
        .filter-chip { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 10px; text-align: center; }
        .filter-chip .filter-label { font-size: 0.75rem; color: #888; }
        .filter-chip .filter-value { font-size: 1.1rem; font-weight: 700; color: var(--primary); margin-top: 2px; }
        .filter-chip .filter-op { font-size: 0.7rem; color: #aaa; }
        .tag { display: inline-block; background: #e8f4fd; color: #0077b6; padding: 3px 10px; border-radius: 12px; font-size: 0.78rem; font-weight: 600; margin: 2px; }
        .insight-item { padding: 6px 0; font-size: 0.85rem; display: flex; gap: 6px; }
        .insight-item + .insight-item { border-top: 1px solid var(--border); }
        .stock-chip { display: inline-block; background: #fff3cd; color: #856404; padding: 3px 10px; border-radius: 12px; font-size: 0.82rem; font-weight: 600; margin: 2px; }
        .confidence-bar { height: 6px; background: #eee; border-radius: 3px; margin-top: 6px; overflow: hidden; }
        .confidence-fill { height: 100%; border-radius: 3px; transition: width 0.5s; }
        .conf-high { background: var(--success); }
        .conf-mid { background: #ffc107; }
        .conf-low { background: var(--danger); }
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
        const filterLabels = {per:'PER',pbr:'PBR',roe:'ROE',roa:'ROA',eps:'EPS',bps:'BPS',div:'배당률(%)',dps:'주당배당금',debt_ratio:'부채비율(%)',market_cap:'시가총액',revenue_growth:'매출성장률(%)',operating_margin:'영업이익률(%)'};
        const opLabels = {lte:'이하',gte:'이상',lt:'미만',gt:'초과',eq:'동일'};
        const opSymbols = {lte:'≤',gte:'≥',lt:'<',gt:'>',eq:'='};

        function formatValue(col, val) {
            if (col === 'market_cap') {
                if (val >= 1e12) return (val/1e12).toFixed(1) + '조';
                if (val >= 1e8) return (val/1e8).toFixed(0) + '억';
                return val.toLocaleString();
            }
            if (typeof val === 'number' && val % 1 !== 0) return val.toFixed(1);
            return val.toLocaleString();
        }

        function renderAnalysis(data) {
            if (data.error) return `<div class="analysis-card"><h3>⚠️ 오류</h3><p>${data.error}</p></div>`;

            let html = '';

            // 전략 요약
            html += `<div class="analysis-card">
                <h3>📌 전략 요약</h3>
                <p style="font-size:0.9rem;line-height:1.5">${data.strategy_summary || 'N/A'}</p>`;
            if (data.strategy_tags?.length) {
                html += `<div style="margin-top:8px">${data.strategy_tags.map(t => `<span class="tag">${t}</span>`).join('')}</div>`;
            }
            html += `</div>`;

            // 언급 종목
            if (data.mentioned_stocks?.length) {
                html += `<div class="analysis-card">
                    <h3>📊 언급 종목</h3>
                    <div>${data.mentioned_stocks.map(s => `<span class="stock-chip">${s}</span>`).join('')}</div>
                </div>`;
            }

            // 검색식 (핵심!)
            if (data.screen_filters && Object.keys(data.screen_filters).length > 0) {
                const entries = Object.entries(data.screen_filters);
                html += `<div class="analysis-card">
                    <h3>🔍 스크리닝 검색식 <span style="font-size:0.75rem;color:#888">(${entries.length}개 조건)</span></h3>
                    <div class="filter-grid">`;
                for (const [k, v] of entries) {
                    const parts = k.split('_');
                    const op = parts.pop();
                    const col = parts.join('_');
                    html += `<div class="filter-chip">
                        <div class="filter-label">${filterLabels[col] || col.toUpperCase()}</div>
                        <div class="filter-value">${opSymbols[op]||op} ${formatValue(col, v)}</div>
                        <div class="filter-op">${opLabels[op]||op}</div>
                    </div>`;
                }
                html += `</div>`;
                // filter_sources (원문 인용)
                if (data.filter_sources?.length) {
                    html += `<div style="margin-top:10px;padding:8px;background:#f0f7ff;border-radius:6px;font-size:0.8rem;color:#555">`;
                    html += `<b>📝 영상 원문:</b><br>`;
                    data.filter_sources.forEach(s => { html += `<i>"${s}"</i><br>`; });
                    html += `</div>`;
                }
                html += `</div>`;
            } else {
                // screen_filters가 비어있으면
                html += `<div class="analysis-card">
                    <h3>🔍 스크리닝 검색식</h3>
                    <p style="color:#888;font-size:0.85rem">영상에서 구체적인 수치 조건이 언급되지 않았습니다. 위 전략 요약을 참고하여 직접 검색식을 구성해보세요.</p>
                </div>`;
            }

            // 핵심 인사이트
            if (data.key_insights?.length) {
                html += `<div class="analysis-card">
                    <h3>💡 핵심 인사이트</h3>`;
                data.key_insights.forEach(i => {
                    html += `<div class="insight-item"><span>•</span><span>${i}</span></div>`;
                });
                html += `</div>`;
            }

            // 신뢰도
            const conf = Math.round((data.confidence || 0) * 100);
            const confClass = conf >= 70 ? 'conf-high' : conf >= 40 ? 'conf-mid' : 'conf-low';
            html += `<div class="analysis-card">
                <h3>📈 분석 신뢰도 <span style="font-weight:700;color:var(--primary)">${conf}%</span></h3>
                <div class="confidence-bar"><div class="confidence-fill ${confClass}" style="width:${conf}%"></div></div>
            </div>`;

            return html;
        }

        async function analyzeVideo() {
            const url = document.getElementById('youtube-url').value;
            if (!url) return alert('YouTube URL을 입력하세요');
            const btn = document.getElementById('analyze-btn');
            btn.disabled = true; btn.textContent = '분석 중...';
            document.getElementById('analyze-result').innerHTML = '';
            document.getElementById('save-watchlist-section').style.display = 'none';
            try {
                const res = await fetch('/api/analyze', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({youtube_url:url})});
                const data = await res.json();
                lastAnalysis = data;
                document.getElementById('analyze-result').innerHTML = renderAnalysis(data);
                if (data.screen_filters && Object.keys(data.screen_filters).length > 0) {
                    document.getElementById('save-watchlist-section').style.display = 'block';
                    const tags = data.strategy_tags ? ` [${data.strategy_tags[0]}]` : '';
                    document.getElementById('wl-name').value = (data.strategy_summary?.substring(0, 25) || '새 검색식') + tags;
                    document.getElementById('screen-filters').value = JSON.stringify(data.screen_filters);
                }
            } catch(e) { document.getElementById('analyze-result').innerHTML = `<div class="analysis-card"><h3>⚠️ 오류</h3><p>${e.message}</p></div>`; }
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
    import re
    import subprocess
    import tempfile

    patterns = [r'(?:v=|/v/|youtu\.be/)([a-zA-Z0-9_-]{11})']
    video_id = None
    for pattern in patterns:
        match = re.search(pattern, req.youtube_url)
        if match:
            video_id = match.group(1)
            break

    if not video_id:
        return {"error": "유효한 YouTube URL이 아닙니다"}

    transcript_text = None
    extraction_method = None

    # 방법 1: yt-dlp로 자막 추출 (클라우드 IP 우회 가능)
    try:
        result = subprocess.run(
            ["yt-dlp", "--skip-download", "--write-auto-sub", "--write-sub",
             "--sub-lang", "ko,en", "--sub-format", "json3",
             "--output", "%(id)s", req.youtube_url],
            capture_output=True, text=True, timeout=30,
            cwd=tempfile.gettempdir()
        )

        # 자막 파일 찾기
        import glob
        sub_files = glob.glob(f"{tempfile.gettempdir()}/{video_id}*.json3")
        if sub_files:
            with open(sub_files[0], 'r') as f:
                sub_data = json.load(f)
            # json3 형식에서 텍스트 추출
            segments = sub_data.get("events", [])
            texts = []
            for seg in segments:
                segs = seg.get("segs", [])
                for s in segs:
                    t = s.get("utf8", "").strip()
                    if t and t != "\n":
                        texts.append(t)
            transcript_text = " ".join(texts)
            extraction_method = "yt-dlp"
            # 임시 파일 정리
            for f in sub_files:
                os.remove(f)

        if not transcript_text:
            # yt-dlp로 자막 파일이 없는 경우 - vtt 시도
            result2 = subprocess.run(
                ["yt-dlp", "--skip-download", "--write-auto-sub", "--write-sub",
                 "--sub-lang", "ko,en", "--sub-format", "vtt",
                 "--output", "%(id)s", req.youtube_url],
                capture_output=True, text=True, timeout=30,
                cwd=tempfile.gettempdir()
            )
            vtt_files = glob.glob(f"{tempfile.gettempdir()}/{video_id}*.vtt")
            if vtt_files:
                with open(vtt_files[0], 'r') as f:
                    vtt_content = f.read()
                # VTT에서 텍스트만 추출
                lines = []
                for line in vtt_content.split('\n'):
                    line = line.strip()
                    if not line or '-->' in line or line.startswith('WEBVTT') or line.startswith('Kind:') or line.startswith('Language:') or re.match(r'^\d+$', line):
                        continue
                    # HTML 태그 제거
                    clean = re.sub(r'<[^>]+>', '', line)
                    if clean:
                        lines.append(clean)
                transcript_text = " ".join(lines)
                extraction_method = "yt-dlp-vtt"
                for f in vtt_files:
                    os.remove(f)

    except FileNotFoundError:
        logger.warning("yt-dlp not installed")
    except subprocess.TimeoutExpired:
        logger.warning("yt-dlp timeout")
    except Exception as e:
        logger.warning(f"yt-dlp failed: {e}")

    # 방법 2: youtube-transcript-api fallback
    if not transcript_text:
        try:
            from youtube_transcript_api import YouTubeTranscriptApi
            ytt = YouTubeTranscriptApi()
            transcript = ytt.fetch(video_id, languages=['ko', 'en'])
            transcript_text = " ".join([snippet.text for snippet in transcript])
            extraction_method = "youtube-transcript-api"
        except Exception as e:
            return {"error": f"자막 추출 실패: {str(e)}\n\nYouTube가 서버 IP를 차단했을 수 있습니다. 자막이 있는 다른 영상으로 시도해보세요."}

    if not transcript_text or len(transcript_text.strip()) < 50:
        return {"error": "자막을 찾을 수 없거나 내용이 너무 짧습니다. 자막이 있는 영상인지 확인해주세요."}

    # 중복 텍스트 제거 (자동자막 특성)
    words = transcript_text.split()
    deduped = [words[0]] if words else []
    for w in words[1:]:
        if w != deduped[-1]:
            deduped.append(w)
    transcript_text = " ".join(deduped)

    logger.info(f"Transcript extracted via {extraction_method}: {len(transcript_text)} chars")

    result = await _parse_with_ai(transcript_text)
    result["video_id"] = video_id
    result["youtube_url"] = req.youtube_url
    result["extraction_method"] = extraction_method
    return result


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
    if ANTHROPIC_API_KEY:
        return await _parse_with_claude(transcript)
    if not OPENAI_API_KEY:
        return {"error": "API key not configured", "transcript_preview": transcript[:200]}
    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=OPENAI_API_KEY)
        response = await client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": """당신은 한국 주식 시장 전문 퀀트 애널리스트입니다. YouTube 투자 영상의 자막을 정밀하게 분석하여 **영상에서 실제로 언급된 구체적인 수치와 조건**을 추출하세요.

## 추출 규칙
1. **strategy_summary**: 영상의 핵심 투자 전략을 2-3문장으로 요약
2. **mentioned_stocks**: 영상에서 직접 언급된 종목명 (추측하지 말 것)
3. **screen_filters**: 영상에서 언급된 **구체적 재무 지표 조건**을 필터로 변환
4. **strategy_tags**: 전략 유형 태그 (예: "가치투자", "모멘텀", "배당", "성장주", "턴어라운드" 등)
5. **key_insights**: 영상의 핵심 인사이트 3-5개 (배열)
6. **confidence**: 검색식의 신뢰도 (0.0~1.0)

## 사용 가능한 필터 키
재무지표_조건 형식. 조건: lte(이하), gte(이상), lt(미만), gt(초과), eq(같음)

- per_lte, per_gte: PER (주가수익비율)
- pbr_lte, pbr_gte: PBR (주가순자산비율)  
- eps_gte: EPS (주당순이익)
- bps_gte: BPS (주당순자산)
- div_gte: 배당수익률(%)
- dps_gte: 주당배당금
- roe_gte: ROE (자기자본이익률)
- roa_gte: ROA (총자산이익률)
- debt_ratio_lte: 부채비율
- market_cap_gte, market_cap_lte: 시가총액
- revenue_growth_gte: 매출성장률
- operating_margin_gte: 영업이익률

## 중요
- 영상에서 **명시적으로 언급한 수치**를 우선 사용하세요
- 수치가 언급되지 않았지만 전략에서 유추 가능한 경우, 합리적인 범위를 설정하고 confidence를 낮추세요
- 가능한 한 **3개 이상의 필터 조건**을 생성하세요
- 단순히 PER/ROE만 넣지 말고, 영상 내용에 맞는 다양한 지표를 활용하세요

## 응답 형식 (JSON)
{
  "strategy_summary": "구체적 전략 요약",
  "mentioned_stocks": ["삼성전자", "SK하이닉스"],
  "screen_filters": {
    "per_lte": 15,
    "pbr_lte": 1.5,
    "roe_gte": 10,
    "div_gte": 2.0,
    "eps_gte": 1000
  },
  "strategy_tags": ["가치투자", "배당"],
  "key_insights": [
    "현재 시장은 가치주 중심으로 재편 중",
    "PER 15 이하 종목 중 배당수익률 2% 이상이 유망"
  ],
  "confidence": 0.75
}"""},
                {"role": "user", "content": f"다음 YouTube 투자 영상 자막을 분석하여 종목 스크리닝 검색식을 생성해주세요. 영상에서 언급된 구체적 수치와 조건을 최대한 반영하세요:\n\n{transcript[:6000]}"}
            ],
            response_format={"type": "json_object"},
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        return {"error": f"AI parsing failed: {str(e)}"}


async def _parse_with_claude(transcript: str) -> dict:
    """Claude 4.6 Sonnet으로 투자 전략 파싱"""
    try:
        async with httpx.AsyncClient() as client:
            # Get the system prompt from the OpenAI path
            system_prompt = """당신은 한국 주식 시장 전문 퀀트 애널리스트입니다. YouTube 투자 영상의 자막에서 **화자가 실제로 말한 종목 스크리닝 조건**만 정확히 추출하세요.

## 절대 규칙
- **영상에서 명시적으로 언급한 수치와 조건만** screen_filters에 넣으세요
- 영상에서 말하지 않은 조건을 **절대 추가하지 마세요**
- 화자가 "PER 10배 이하인 종목"이라고 했으면 → {"per_lte": 10}
- 화자가 구체적 수치 없이 "저평가 종목"이라고만 했으면 → screen_filters를 비워두고, strategy_summary에 설명
- **추측, 유추, 일반적 기준값 삽입 금지**

## 추출 항목
1. **strategy_summary**: 영상의 핵심 투자 전략을 2-3문장으로 요약
2. **mentioned_stocks**: 영상에서 직접 언급된 종목명만 (추측 금지)
3. **screen_filters**: 영상에서 **화자가 직접 말한 수치 조건만** 필터로 변환. 언급 없으면 빈 객체 {}
4. **filter_sources**: screen_filters의 각 항목이 영상의 어떤 발언에서 추출되었는지 원문 인용 (배열)
5. **strategy_tags**: 전략 유형 태그
6. **key_insights**: 영상의 핵심 인사이트 3-5개
7. **confidence**: 검색식이 영상 내용을 정확히 반영하는 정도 (0.0~1.0). 조건이 명시적일수록 높음

## 사용 가능한 필터 키
재무지표_조건 형식. 조건: lte(이하), gte(이상), lt(미만), gt(초과)

- per_lte, per_gte: PER (주가수익비율)
- pbr_lte, pbr_gte: PBR (주가순자산비율)
- eps_gte: EPS (주당순이익)
- bps_gte: BPS (주당순자산)
- div_gte: 배당수익률(%)
- dps_gte: 주당배당금
- roe_gte, roe_lte: ROE (자기자본이익률)
- roa_gte: ROA (총자산이익률)
- debt_ratio_lte: 부채비율(%)
- market_cap_gte, market_cap_lte: 시가총액(원)
- revenue_growth_gte: 매출성장률(%)
- operating_margin_gte: 영업이익률(%)

## 응답 예시

영상에서 "PER 10배 이하이면서 ROE 15% 이상인 종목을 찾아라"라고 했다면:
```json
{
  "strategy_summary": "저PER 고ROE 가치주 발굴 전략",
  "mentioned_stocks": [],
  "screen_filters": {"per_lte": 10, "roe_gte": 15},
  "filter_sources": ["PER 10배 이하이면서 ROE 15% 이상인 종목을 찾아라"],
  "strategy_tags": ["가치투자"],
  "key_insights": ["PER과 ROE 두 지표를 결합한 스크리닝"],
  "confidence": 0.95
}
```

영상에서 구체적 수치 없이 "주도주를 매수하라"고만 했다면:
```json
{
  "strategy_summary": "시장 주도주 중심 매수 전략",
  "mentioned_stocks": ["삼성전자", "SK하이닉스"],
  "screen_filters": {},
  "filter_sources": [],
  "strategy_tags": ["모멘텀"],
  "key_insights": ["주도주 중심 포트폴리오 구성 권장"],
  "confidence": 0.3
}
```

반드시 JSON만 출력하세요."""

            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 2048,
                    "system": system_prompt,
                    "messages": [
                        {"role": "user", "content": f"다음 YouTube 투자 영상 자막을 분석하여 종목 스크리닝 검색식을 생성해주세요. 영상에서 언급된 구체적 수치와 조건을 최대한 반영하세요:\n\n{transcript[:8000]}"}
                    ],
                },
                timeout=60,
            )

            data = resp.json()
            content = data["content"][0]["text"]

            # JSON 추출 (```json ... ``` 감싸진 경우 처리)
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0]
            elif "```" in content:
                content = content.split("```")[1].split("```")[0]

            return json.loads(content.strip())

    except Exception as e:
        logger.error(f"Claude parsing failed: {e}")
        return {"error": f"Claude parsing failed: {str(e)}"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
