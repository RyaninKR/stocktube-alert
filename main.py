"""
StockTube Alert MVP
YouTube 투자 영상 → AI 검색식 자동 생성 → 실시간 종목 스크리닝
웹앱 + 텔레그램 미니앱 동시 지원
"""

import os
import json
import hmac
import hashlib
import logging
from urllib.parse import parse_qs, unquote
from datetime import datetime
from typing import Optional

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="StockTube Alert MVP", version="0.2.0")

# ─── Config ───
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
WEBAPP_URL = os.getenv("WEBAPP_URL", "")  # e.g. https://web-production-05a0c.up.railway.app

# ─── In-memory store (프로덕션에서는 DB로 교체) ───
registered_users: dict = {}  # chat_id -> { user_info, settings }


# ─── Pydantic Models ───
class AnalyzeUrlRequest(BaseModel):
    youtube_url: str


class ParseRequest(BaseModel):
    transcript: str


class ScreenRequest(BaseModel):
    filters: dict


class NotifyRequest(BaseModel):
    bot_token: Optional[str] = None  # deprecated, 하위호환용
    chat_id: str
    message: str


class TelegramRegisterRequest(BaseModel):
    init_data: str


class NotificationSettings(BaseModel):
    chat_id: str
    notify_on_analyze: bool = True
    notify_on_screen: bool = True
    notify_on_new_video: bool = True


# ─── Telegram initData 검증 ───
def verify_telegram_init_data(init_data: str, bot_token: str) -> dict:
    """텔레그램 미니앱 initData HMAC-SHA256 검증"""
    parsed = parse_qs(init_data)

    # hash 추출
    received_hash = parsed.get("hash", [None])[0]
    if not received_hash:
        raise ValueError("hash not found in init_data")

    # hash를 제외한 나머지를 정렬하여 data_check_string 생성
    data_pairs = []
    for key, values in parsed.items():
        if key == "hash":
            continue
        data_pairs.append(f"{key}={unquote(values[0])}")
    data_pairs.sort()
    data_check_string = "\n".join(data_pairs)

    # HMAC 검증
    secret_key = hmac.new(
        b"WebAppData", bot_token.encode(), hashlib.sha256
    ).digest()
    computed_hash = hmac.new(
        secret_key, data_check_string.encode(), hashlib.sha256
    ).hexdigest()

    if computed_hash != received_hash:
        raise ValueError("Invalid init_data hash")

    # user 정보 파싱
    user_data = parsed.get("user", [None])[0]
    if user_data:
        return json.loads(unquote(user_data))
    return {}


# ─── HTML (웹앱 + 미니앱 겸용) ───
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
        :root {
            --bg-color: #ffffff;
            --text-color: #1a1a2e;
            --card-bg: #f8f9fa;
            --primary: #0088cc;
            --border: #e0e0e0;
        }
        .tg-theme {
            --bg-color: var(--tg-theme-bg-color, #ffffff);
            --text-color: var(--tg-theme-text-color, #1a1a2e);
            --card-bg: var(--tg-theme-secondary-bg-color, #f8f9fa);
            --primary: var(--tg-theme-button-color, #0088cc);
            --border: var(--tg-theme-hint-color, #e0e0e0);
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: var(--bg-color);
            color: var(--text-color);
            padding: 16px;
            max-width: 600px;
            margin: 0 auto;
        }
        h1 { font-size: 1.5rem; margin-bottom: 8px; }
        .subtitle { color: #666; margin-bottom: 24px; font-size: 0.9rem; }
        .card {
            background: var(--card-bg);
            border-radius: 12px;
            padding: 16px;
            margin-bottom: 16px;
            border: 1px solid var(--border);
        }
        .card h2 { font-size: 1.1rem; margin-bottom: 12px; }
        input[type="text"], input[type="url"] {
            width: 100%;
            padding: 12px;
            border: 1px solid var(--border);
            border-radius: 8px;
            font-size: 1rem;
            margin-bottom: 12px;
            background: var(--bg-color);
            color: var(--text-color);
        }
        button {
            width: 100%;
            padding: 12px;
            background: var(--primary);
            color: white;
            border: none;
            border-radius: 8px;
            font-size: 1rem;
            cursor: pointer;
            font-weight: 600;
        }
        button:disabled { opacity: 0.5; }
        .result { margin-top: 16px; white-space: pre-wrap; font-size: 0.9rem; }
        .toggle-row {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 8px 0;
        }
        .toggle-row + .toggle-row { border-top: 1px solid var(--border); }
        .switch {
            position: relative;
            width: 48px;
            height: 26px;
        }
        .switch input { opacity: 0; width: 0; height: 0; }
        .slider {
            position: absolute;
            cursor: pointer;
            inset: 0;
            background: #ccc;
            border-radius: 26px;
            transition: 0.3s;
        }
        .slider:before {
            content: "";
            position: absolute;
            height: 20px;
            width: 20px;
            left: 3px;
            bottom: 3px;
            background: white;
            border-radius: 50%;
            transition: 0.3s;
        }
        input:checked + .slider { background: var(--primary); }
        input:checked + .slider:before { transform: translateX(22px); }
        #notification-section { display: none; }
        .status-badge {
            display: inline-block;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 0.8rem;
            font-weight: 600;
        }
        .status-connected { background: #d4edda; color: #155724; }
        .status-web { background: #e2e3e5; color: #383d41; }
    </style>
</head>
<body>
    <h1>📺 StockTube Alert</h1>
    <p class="subtitle">
        YouTube 투자 영상 → AI 검색식 자동 생성 → 실시간 종목 스크리닝
        <br>
        <span id="mode-badge" class="status-badge status-web">웹 모드</span>
    </p>

    <!-- 영상 분석 -->
    <div class="card">
        <h2>🔍 영상 분석</h2>
        <input type="url" id="youtube-url" placeholder="YouTube URL을 입력하세요">
        <button id="analyze-btn" onclick="analyzeVideo()">분석 시작</button>
        <div id="analyze-result" class="result"></div>
    </div>

    <!-- 종목 스크리닝 -->
    <div class="card">
        <h2>📊 종목 스크리닝</h2>
        <input type="text" id="screen-filters" placeholder='필터 JSON (예: {"per_lte": 10})'>
        <button onclick="screenStocks()">스크리닝 실행</button>
        <div id="screen-result" class="result"></div>
    </div>

    <!-- 알림 설정 (텔레그램 미니앱에서만 표시) -->
    <div id="notification-section" class="card">
        <h2>🔔 알림 설정</h2>
        <div class="toggle-row">
            <span>분석 완료 알림</span>
            <label class="switch">
                <input type="checkbox" id="notify-analyze" checked onchange="saveSettings()">
                <span class="slider"></span>
            </label>
        </div>
        <div class="toggle-row">
            <span>스크리닝 결과 알림</span>
            <label class="switch">
                <input type="checkbox" id="notify-screen" checked onchange="saveSettings()">
                <span class="slider"></span>
            </label>
        </div>
        <div class="toggle-row">
            <span>신규 영상 알림</span>
            <label class="switch">
                <input type="checkbox" id="notify-video" checked onchange="saveSettings()">
                <span class="slider"></span>
            </label>
        </div>
    </div>

    <script>
        // ─── 텔레그램 미니앱 감지 & 초기화 ───
        const tg = window.Telegram?.WebApp;
        const isTelegram = !!(tg && tg.initData);
        let telegramUser = null;

        if (isTelegram) {
            tg.ready();
            tg.expand();
            document.body.classList.add('tg-theme');
            document.getElementById('mode-badge').textContent = '텔레그램 미니앱';
            document.getElementById('mode-badge').className = 'status-badge status-connected';
            document.getElementById('notification-section').style.display = 'block';

            telegramUser = tg.initDataUnsafe?.user;

            // 서버에 유저 등록
            fetch('/api/telegram/register', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ init_data: tg.initData })
            }).then(r => r.json()).then(d => {
                console.log('Registered:', d);
                // 기존 설정 불러오기
                if (d.settings) {
                    document.getElementById('notify-analyze').checked = d.settings.notify_on_analyze;
                    document.getElementById('notify-screen').checked = d.settings.notify_on_screen;
                    document.getElementById('notify-video').checked = d.settings.notify_on_new_video;
                }
            }).catch(e => console.error('Register failed:', e));
        }

        // ─── API 호출 함수들 ───
        async function analyzeVideo() {
            const url = document.getElementById('youtube-url').value;
            if (!url) return alert('YouTube URL을 입력하세요');

            const btn = document.getElementById('analyze-btn');
            btn.disabled = true;
            btn.textContent = '분석 중...';
            document.getElementById('analyze-result').textContent = '';

            try {
                const res = await fetch('/api/analyze', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ youtube_url: url })
                });
                const data = await res.json();
                document.getElementById('analyze-result').textContent = JSON.stringify(data, null, 2);
            } catch (e) {
                document.getElementById('analyze-result').textContent = '오류: ' + e.message;
            } finally {
                btn.disabled = false;
                btn.textContent = '분석 시작';
            }
        }

        async function screenStocks() {
            const filtersStr = document.getElementById('screen-filters').value;
            let filters = {};
            try {
                if (filtersStr) filters = JSON.parse(filtersStr);
            } catch { return alert('올바른 JSON 형식으로 입력하세요'); }

            try {
                const res = await fetch('/api/screen', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ filters })
                });
                const data = await res.json();
                document.getElementById('screen-result').textContent = JSON.stringify(data, null, 2);
            } catch (e) {
                document.getElementById('screen-result').textContent = '오류: ' + e.message;
            }
        }

        async function saveSettings() {
            if (!isTelegram || !telegramUser) return;
            const settings = {
                chat_id: String(telegramUser.id),
                notify_on_analyze: document.getElementById('notify-analyze').checked,
                notify_on_screen: document.getElementById('notify-screen').checked,
                notify_on_new_video: document.getElementById('notify-video').checked,
            };
            try {
                await fetch('/api/telegram/settings', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(settings)
                });
            } catch (e) {
                console.error('Settings save failed:', e);
            }
        }
    </script>
</body>
</html>"""


# ─── 텔레그램 미니앱 전용 API ───
@app.post("/api/telegram/register")
async def register_telegram_user(req: TelegramRegisterRequest):
    """미니앱에서 initData를 받아 유저 등록 & chat_id 저장"""
    if not TELEGRAM_BOT_TOKEN:
        raise HTTPException(500, "TELEGRAM_BOT_TOKEN not configured")

    try:
        user_info = verify_telegram_init_data(req.init_data, TELEGRAM_BOT_TOKEN)
    except ValueError as e:
        raise HTTPException(400, str(e))

    chat_id = str(user_info.get("id", ""))
    if not chat_id:
        raise HTTPException(400, "user id not found")

    # 기존 설정 유지 또는 기본값 생성
    if chat_id not in registered_users:
        registered_users[chat_id] = {
            "user_info": user_info,
            "settings": {
                "notify_on_analyze": True,
                "notify_on_screen": True,
                "notify_on_new_video": True,
            },
            "registered_at": datetime.utcnow().isoformat(),
        }
    else:
        registered_users[chat_id]["user_info"] = user_info

    logger.info(f"Telegram user registered: {chat_id} ({user_info.get('first_name', '')})")

    return {
        "status": "ok",
        "chat_id": chat_id,
        "settings": registered_users[chat_id]["settings"],
    }


@app.post("/api/telegram/settings")
async def update_notification_settings(settings: NotificationSettings):
    """알림 설정 업데이트"""
    chat_id = settings.chat_id

    if chat_id not in registered_users:
        registered_users[chat_id] = {
            "user_info": {},
            "settings": {},
            "registered_at": datetime.utcnow().isoformat(),
        }

    registered_users[chat_id]["settings"] = {
        "notify_on_analyze": settings.notify_on_analyze,
        "notify_on_screen": settings.notify_on_screen,
        "notify_on_new_video": settings.notify_on_new_video,
    }

    logger.info(f"Settings updated for {chat_id}: {registered_users[chat_id]['settings']}")
    return {"status": "ok", "settings": registered_users[chat_id]["settings"]}


@app.get("/api/telegram/settings/{chat_id}")
async def get_notification_settings(chat_id: str):
    """알림 설정 조회"""
    if chat_id not in registered_users:
        raise HTTPException(404, "User not found")
    return {"settings": registered_users[chat_id]["settings"]}


# ─── 기존 API (호환 유지) ───
@app.post("/api/analyze")
async def analyze_video(req: AnalyzeUrlRequest):
    """YouTube 자막 추출 + AI 전략 파싱 (다중 fallback)"""
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
        import re

        # YouTube URL에서 video_id 추출
        patterns = [
            r'(?:v=|/v/|youtu\.be/)([a-zA-Z0-9_-]{11})',
        ]
        video_id = None
        for pattern in patterns:
            match = re.search(pattern, req.youtube_url)
            if match:
                video_id = match.group(1)
                break

        if not video_id:
            return {"error": "유효한 YouTube URL이 아닙니다"}

        # 자막 추출
        try:
            transcript_list = YouTubeTranscriptApi.get_transcript(video_id, languages=['ko', 'en'])
            transcript_text = " ".join([t['text'] for t in transcript_list])
        except Exception as e:
            return {"error": f"자막 추출 실패: {str(e)}"}

        # AI 파싱
        result = await _parse_with_ai(transcript_text)
        result["video_id"] = video_id
        result["youtube_url"] = req.youtube_url

        # 텔레그램 알림 (등록된 유저 중 설정 ON인 유저에게)
        if TELEGRAM_BOT_TOKEN:
            await _notify_users("analyze", result)

        return result

    except ImportError:
        return {"error": "youtube_transcript_api not installed"}
    except Exception as e:
        logger.error(f"Analyze error: {e}")
        return {"error": str(e)}


@app.post("/api/parse")
async def parse_transcript(req: ParseRequest):
    """자막 텍스트를 받아 AI 전략 파싱"""
    return await _parse_with_ai(req.transcript)


@app.post("/api/screen")
async def screen_stocks_api(req: ScreenRequest):
    """실제 KRX 데이터로 종목 스크리닝"""
    try:
        from pykrx import stock

        today = datetime.now().strftime("%Y%m%d")
        df = stock.get_market_fundamental(today, market="ALL")

        if df.empty:
            # 주말/공휴일 → 가장 최근 영업일
            from pykrx.stock import get_nearest_business_day_in_a_week
            today = get_nearest_business_day_in_a_week(datetime.now().strftime("%Y%m%d"))
            df = stock.get_market_fundamental(today, market="ALL")

        results = df.copy()
        filters = req.filters

        # 필터 적용
        for key, value in filters.items():
            col, op = key.rsplit("_", 1)
            col = col.upper()
            if col not in results.columns:
                continue
            if op == "lte":
                results = results[results[col] <= value]
            elif op == "gte":
                results = results[results[col] >= value]
            elif op == "eq":
                results = results[results[col] == value]

        # 종목명 추가
        tickers = results.index.tolist()
        names = {t: stock.get_market_ticker_name(t) for t in tickers[:50]}
        results = results.head(50)
        results["종목명"] = results.index.map(lambda x: names.get(x, ""))

        return {
            "count": len(results),
            "date": today,
            "stocks": results.reset_index().to_dict(orient="records"),
        }

    except Exception as e:
        logger.error(f"Screen error: {e}")
        return {"error": str(e)}


@app.post("/api/notify")
async def send_telegram_notification(req: NotifyRequest):
    """텔레그램 알림 전송"""
    bot_token = req.bot_token or TELEGRAM_BOT_TOKEN
    if not bot_token:
        raise HTTPException(400, "bot_token required (set TELEGRAM_BOT_TOKEN or pass in request)")

    webapp_url = WEBAPP_URL or ""

    payload = {
        "chat_id": req.chat_id,
        "text": req.message,
        "parse_mode": "HTML",
    }

    # 미니앱 버튼 추가
    if webapp_url:
        payload["reply_markup"] = {
            "inline_keyboard": [[{
                "text": "📊 StockTube에서 확인",
                "web_app": {"url": webapp_url}
            }]]
        }

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json=payload,
        )
        return resp.json()


# ─── 내부 헬퍼 ───
async def _parse_with_ai(transcript: str) -> dict:
    """AI로 투자 전략/검색식 파싱"""
    if not OPENAI_API_KEY:
        return {"error": "OPENAI_API_KEY not configured", "transcript_preview": transcript[:200]}

    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=OPENAI_API_KEY)

        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": """당신은 주식 투자 전문가입니다. YouTube 투자 영상의 자막을 분석하여 다음을 추출하세요:
1. 핵심 투자 전략 요약
2. 언급된 종목 리스트
3. 종목 스크리닝 검색식 (PER, PBR, ROE 등 재무지표 기반 필터)

JSON 형식으로 응답:
{
  "strategy_summary": "전략 요약",
  "mentioned_stocks": ["종목1", "종목2"],
  "screen_filters": {"per_lte": 10, "roe_gte": 15},
  "confidence": 0.8
}"""
                },
                {"role": "user", "content": f"다음 자막을 분석해주세요:\n\n{transcript[:4000]}"}
            ],
            response_format={"type": "json_object"},
        )

        return json.loads(response.choices[0].message.content)
    except Exception as e:
        return {"error": f"AI parsing failed: {str(e)}", "transcript_preview": transcript[:200]}


async def _notify_users(event_type: str, data: dict):
    """등록된 유저들에게 텔레그램 알림 전송"""
    setting_key = f"notify_on_{event_type}"

    for chat_id, user_data in registered_users.items():
        settings = user_data.get("settings", {})
        if not settings.get(setting_key, True):
            continue

        if event_type == "analyze":
            message = (
                f"🔔 <b>새 분석 완료!</b>\n\n"
                f"📺 영상: {data.get('youtube_url', 'N/A')}\n"
                f"📊 전략: {data.get('strategy_summary', 'N/A')}\n"
                f"🏷 종목: {', '.join(data.get('mentioned_stocks', []))}"
            )
        else:
            message = f"🔔 StockTube Alert: {event_type}"

        try:
            req = NotifyRequest(chat_id=chat_id, message=message)
            await send_telegram_notification(req)
        except Exception as e:
            logger.error(f"Notify failed for {chat_id}: {e}")


# ─── Telegram Webhook (선택적) ───
@app.post("/api/telegram/webhook")
async def telegram_webhook(request: Request):
    """텔레그램 봇 웹훅 (봇 /start 명령 처리)"""
    body = await request.json()
    message = body.get("message", {})
    text = message.get("text", "")
    chat_id = str(message.get("chat", {}).get("id", ""))

    if text == "/start":
        # 유저 등록 + 환영 메시지
        if chat_id not in registered_users:
            registered_users[chat_id] = {
                "user_info": message.get("from", {}),
                "settings": {
                    "notify_on_analyze": True,
                    "notify_on_screen": True,
                    "notify_on_new_video": True,
                },
                "registered_at": datetime.utcnow().isoformat(),
            }

        webapp_url = WEBAPP_URL or ""
        welcome = (
            "👋 <b>StockTube Alert에 오신 걸 환영합니다!</b>\n\n"
            "YouTube 투자 영상을 AI로 분석하여 종목 스크리닝 검색식을 자동 생성합니다.\n\n"
            "아래 버튼을 눌러 시작하세요 👇"
        )

        payload = {
            "chat_id": chat_id,
            "text": welcome,
            "parse_mode": "HTML",
        }
        if webapp_url:
            payload["reply_markup"] = {
                "inline_keyboard": [[{
                    "text": "📺 StockTube Alert 열기",
                    "web_app": {"url": webapp_url}
                }]]
            }

        async with httpx.AsyncClient() as client:
            await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json=payload,
            )

    return {"ok": True}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
