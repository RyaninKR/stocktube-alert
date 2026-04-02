# 📺 StockTube Alert

YouTube 투자 영상 → AI 검색식 자동 생성 → 실시간 종목 스크리닝

**웹앱 + 텔레그램 미니앱 동시 지원**

## 기능

- 🔍 YouTube 투자 영상 자막 자동 추출 & AI 분석
- 📊 AI 기반 종목 스크리닝 검색식 자동 생성
- 📈 KRX 실시간 데이터로 종목 스크리닝
- 🔔 텔레그램 미니앱 알림 (분석 완료, 스크리닝 결과, 신규 영상)

## 기술 스택

- **Backend**: FastAPI (Python 3.11)
- **AI**: OpenAI GPT-4o-mini
- **Data**: pykrx (KRX 시장 데이터)
- **Deploy**: Railway
- **Notification**: Telegram Bot API + Mini App

## API 엔드포인트

| Method | Path | 설명 |
|--------|------|------|
| GET | `/` | 웹앱 / 미니앱 메인 페이지 |
| POST | `/api/analyze` | YouTube URL → AI 전략 파싱 |
| POST | `/api/parse` | 자막 텍스트 → AI 전략 파싱 |
| POST | `/api/screen` | KRX 데이터 종목 스크리닝 |
| POST | `/api/notify` | 텔레그램 알림 전송 |
| POST | `/api/telegram/register` | 미니앱 유저 등록 |
| POST | `/api/telegram/settings` | 알림 설정 변경 |
| GET | `/api/telegram/settings/{chat_id}` | 알림 설정 조회 |
| POST | `/api/telegram/webhook` | 텔레그램 봇 웹훅 |

## 환경변수

```env
TELEGRAM_BOT_TOKEN=   # @BotFather에서 발급
OPENAI_API_KEY=       # OpenAI API 키
WEBAPP_URL=           # 앱 공개 URL
PORT=8000             # 서버 포트
```

## 로컬 실행

```bash
pip install -r requirements.txt
cp .env.example .env
# .env 파일에 API 키 설정
uvicorn main:app --reload
```

## 텔레그램 미니앱 설정

1. `@BotFather` → `/newbot` → 봇 생성
2. `@BotFather` → `/mybots` → Bot Settings → Configure Mini App → URL 등록
3. Railway 환경변수에 `TELEGRAM_BOT_TOKEN` 추가
4. 웹훅 설정: `https://api.telegram.org/bot<TOKEN>/setWebhook?url=<WEBAPP_URL>/api/telegram/webhook`
