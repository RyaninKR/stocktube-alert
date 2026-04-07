"""
한국투자증권 Open API 연동 모듈
- OAuth 토큰 발급 & 캐시
- 하이브리드 전종목 스크리닝 (pykrx + KIS 상세 재무)
- 개별 종목 현재가 조회
- HTTP 클라이언트 싱글턴 + Rate Limit 관리
- 시장 데이터 캐싱
"""

import os
import time
import asyncio
import logging
from datetime import datetime, timezone, timedelta

import httpx
import pandas as pd

logger = logging.getLogger(__name__)

KIS_APP_KEY = os.getenv("KIS_APP_KEY", "")
KIS_APP_SECRET = os.getenv("KIS_APP_SECRET", "")
KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"
KST = timezone(timedelta(hours=9))

# ─── 토큰 캐시 ───
_token_cache = {"token": None, "expires_at": 0}

# ─── HTTP 클라이언트 싱글턴 (#3) ───
_http_client: httpx.AsyncClient | None = None


async def get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(
            timeout=15,
            limits=httpx.Limits(max_connections=20)
        )
    return _http_client

# ─── Rate Limit 세마포어 (#3) ───
_api_semaphore = asyncio.Semaphore(10)

# ─── 시장 데이터 캐시 (#5-A) ───
_market_cache: dict = {"data": None, "timestamp": 0}


async def get_access_token() -> str:
    """OAuth 액세스 토큰 발급 (캐시)"""
    now = time.time()
    if _token_cache["token"] and _token_cache["expires_at"] > now + 60:
        return _token_cache["token"]

    client = await get_http_client()
    resp = await client.post(
        f"{KIS_BASE_URL}/oauth2/tokenP",
        json={
            "grant_type": "client_credentials",
            "appkey": KIS_APP_KEY,
            "appsecret": KIS_APP_SECRET,
        },
        timeout=10,
    )
    data = resp.json()

    if "access_token" not in data:
        raise Exception(f"KIS token error: {data}")

    _token_cache["token"] = data["access_token"]
    _token_cache["expires_at"] = now + 23 * 3600
    logger.info("KIS access token refreshed")
    return _token_cache["token"]


def _common_headers(token: str, tr_id: str) -> dict:
    return {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": tr_id,
    }


async def get_stock_price(ticker: str) -> dict:
    """개별 종목 현재가 조회"""
    token = await get_access_token()
    headers = _common_headers(token, "FHKST01010100")
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": ticker,
    }

    client = await get_http_client()
    resp = await client.get(
        f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price",
        headers=headers,
        params=params,
        timeout=10,
    )
    data = resp.json()

    if data.get("rt_cd") != "0":
        logger.error(f"KIS price error for {ticker}: {data.get('msg1', '')}")
        return {}

    output = data.get("output", {})
    return {
        "ticker": ticker,
        "name": output.get("hts_kor_isnm", ""),
        "price": int(output.get("stck_prpr", 0)),
        "change_rate": float(output.get("prdy_ctrt", 0)),
        "volume": int(output.get("acml_vol", 0)),
        "market_cap": int(output.get("hts_avls", 0)) * 100000000,
        "per": float(output.get("per", 0)),
        "pbr": float(output.get("pbr", 0)),
        "eps": float(output.get("eps", 0)),
        "bps": float(output.get("bps", 0)),
        "high_52w": int(output.get("stck_dryy_hgpr", 0)),
        "low_52w": int(output.get("stck_dryy_lwpr", 0)),
    }


async def get_stock_detail(ticker: str) -> dict:
    """종목 상세 재무 정보 (스크리닝용)"""
    price_data = await get_stock_price(ticker)
    if not price_data:
        return {}

    token = await get_access_token()
    headers = _common_headers(token, "FHKST66430300")
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": ticker,
    }

    client = await get_http_client()
    try:
        resp = await client.get(
            f"{KIS_BASE_URL}/uapi/domestic-stock/v1/finance/financial-ratio",
            headers=headers,
            params=params,
            timeout=10,
        )
        fin_data = resp.json()
        if fin_data.get("rt_cd") == "0" and fin_data.get("output"):
            latest = fin_data["output"][0]
            price_data.update({
                "roe": float(latest.get("roe_val", 0)),
                "roa": float(latest.get("roa_val", 0) if latest.get("roa_val") else 0),
                "debt_ratio": float(latest.get("lblt_rate", 0)),
                "operating_margin": float(latest.get("bsop_prfi_inrt", 0) if latest.get("bsop_prfi_inrt") else 0),
                "revenue_growth": float(latest.get("sles_inrt", 0) if latest.get("sles_inrt") else 0),
                "div": float(latest.get("dvdn_yld", 0) if latest.get("dvdn_yld") else 0),
            })
    except Exception as e:
        logger.warning(f"Financial ratio error for {ticker}: {e}")

    return price_data


async def get_stock_detail_throttled(ticker: str) -> dict:
    """Rate-limited 상세 재무 조회 (#3)"""
    async with _api_semaphore:
        result = await get_stock_detail(ticker)
        await asyncio.sleep(0.1)
        return result


# ─── 시장 데이터 조회 (pykrx + KRX 직접 API) ───

async def _fetch_pykrx_data(today_str: str) -> pd.DataFrame | None:
    """pykrx로 전종목 기본 지표 조회"""
    try:
        from pykrx import stock
        df = stock.get_market_fundamental(today_str, market="ALL")
        if df is not None and not df.empty:
            logger.info(f"pykrx data loaded: {len(df)} rows")
            return df
    except Exception as e:
        logger.warning(f"pykrx failed: {e}")
    return None


async def _fetch_krx_direct(today_str: str) -> pd.DataFrame | None:
    """KRX API 직접 호출 (pykrx fallback)"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101'
    }
    params = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT03501',
        'locale': 'ko_KR',
        'searchType': '1',
        'mktId': 'ALL',
        'trdDd': today_str,
    }

    client = await get_http_client()
    try:
        resp = await client.post(
            'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd',
            data=params,
            headers=headers,
            timeout=15,
        )
        data = resp.json()
    except Exception as e:
        logger.warning(f"KRX direct API failed: {e}")
        return None

    if 'output' not in data or not data['output']:
        return None

    records = data['output']
    df = pd.DataFrame(records)

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

    if 'ticker' in df.columns:
        df = df.set_index('ticker')

    for col in ['PER', 'PBR', 'EPS', 'BPS', 'DIV', 'DPS']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')

    logger.info(f"KRX direct data loaded: {len(df)} rows")
    return df


async def _fetch_all_market_data() -> dict | None:
    """pykrx → KRX 직접 API 순서로 전종목 데이터 조회. 당일 데이터 없으면 직전 거래일까지 시도."""
    from datetime import timedelta as td

    today = datetime.now(KST).date()

    # 최대 7일 전까지 시도 (주말/공휴일 대비)
    for days_back in range(8):
        target_date = today - td(days=days_back)
        date_str = target_date.strftime("%Y%m%d")

        df = await _fetch_pykrx_data(date_str)
        source = "pykrx"

        if df is None or df.empty:
            df = await _fetch_krx_direct(date_str)
            source = "krx_direct"

        if df is not None and not df.empty:
            if days_back > 0:
                logger.info(f"Using data from {date_str} ({days_back} days ago)")
            return {"date": date_str, "data": df, "source": source}

    return None


async def get_cached_market_data(max_age_sec: int = 300) -> dict | None:
    """캐시된 시장 데이터 반환 (#5-A)"""
    now = time.time()
    if _market_cache["data"] is not None and (now - _market_cache["timestamp"]) < max_age_sec:
        logger.info("Using cached market data")
        return _market_cache["data"]

    data = await _fetch_all_market_data()
    if data is not None:
        _market_cache.update({"data": data, "timestamp": now})
    return data


# ─── 하이브리드 스크리닝 (#1) ───

def _apply_basic_filters(df: pd.DataFrame, filters: dict) -> tuple[pd.DataFrame, list[str], dict]:
    """기본 필터(PER, PBR, EPS, BPS, DIV, DPS) 적용. 상세 필터는 분리 반환."""
    basic_cols = {"per", "pbr", "eps", "bps", "div", "dps"}
    results = df.copy()
    applied = []
    detail_filters = {}

    for key, value in filters.items():
        parts = key.rsplit("_", 1)
        if len(parts) != 2:
            continue
        col, op = parts[0], parts[1]

        if col.lower() not in basic_cols:
            detail_filters[key] = value
            continue

        col_upper = col.upper()
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
            elif op == "lt":
                results = results[(results[matched_col] < value) & (results[matched_col] > 0)]
            elif op == "gt":
                results = results[results[matched_col] > value]
            elif op == "eq":
                results = results[results[matched_col] == value]
            applied.append(f"{matched_col} {op} {value}")
        except Exception as e:
            logger.warning(f"Filter {key}={value}: {e}")

    return results, applied, detail_filters


def _apply_detail_filters(stocks: list[dict], filters: dict) -> tuple[list[dict], list[str]]:
    """상세 필터(ROE, ROA, debt_ratio, etc.) 적용"""
    applied = []
    result = stocks

    for key, value in filters.items():
        parts = key.rsplit("_", 1)
        if len(parts) != 2:
            continue
        col, op = parts[0], parts[1]

        filtered = []
        for s in result:
            v = s.get(col)
            if v is None:
                continue
            try:
                v = float(v)
                if op == "lte" and v <= value and v > 0:
                    filtered.append(s)
                elif op == "gte" and v >= value:
                    filtered.append(s)
                elif op == "lt" and v < value and v > 0:
                    filtered.append(s)
                elif op == "gt" and v > value:
                    filtered.append(s)
                elif op == "eq" and v == value:
                    filtered.append(s)
            except (ValueError, TypeError):
                continue

        result = filtered
        applied.append(f"{col} {op} {value}")

    return result, applied


async def screen_stocks_hybrid(filters: dict) -> dict:
    """
    하이브리드 전종목 스크리닝 (#1)
    [1단계] pykrx/KRX로 전종목 PER, PBR, EPS, BPS, DIV 조회 → 기본 필터 적용
    [2단계] 후보 종목(최대 50개)에 KIS API로 상세 재무 조회 → 상세 필터 적용
    """
    if not KIS_APP_KEY:
        return {"error": "KIS API key not configured"}

    today_str = datetime.now(KST).strftime("%Y%m%d")

    # 1단계: 전종목 기본 지표 (캐시 사용)
    market_data = await get_cached_market_data()
    if market_data is None:
        return {"error": "시장 데이터를 가져올 수 없습니다. 장 운영 시간을 확인해주세요.", "date": today_str}

    df = market_data["data"]
    source = market_data["source"]

    # 기본 필터 적용
    candidates, applied_basic, detail_filters = _apply_basic_filters(df, filters)
    logger.info(f"[1단계] {source}: {len(df)} → {len(candidates)} after basic filters")

    # 2단계: 상세 필터가 있으면 KIS API로 상세 재무 조회
    applied_detail = []
    if detail_filters and len(candidates) > 0:
        # 후보를 최대 50개로 제한
        candidate_tickers = candidates.index.tolist()[:50]
        logger.info(f"[2단계] Fetching detail for {len(candidate_tickers)} candidates")

        # 병렬 API 호출 (#3)
        tasks = [get_stock_detail_throttled(t) for t in candidate_tickers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        enriched = []
        for r in results:
            if isinstance(r, Exception):
                logger.warning(f"Detail fetch error: {r}")
                continue
            if r:
                enriched.append(r)

        # 상세 필터 적용
        enriched, applied_detail = _apply_detail_filters(enriched, detail_filters)

        stocks = enriched[:50]
    else:
        # 상세 필터 없으면 기본 데이터로 결과 구성
        tickers = candidates.index.tolist()[:50]

        # 종목명 추가
        names = {}
        if source == "pykrx":
            try:
                from pykrx import stock as pykrx_stock
                names = {t: pykrx_stock.get_market_ticker_name(t) for t in tickers}
            except Exception:
                pass

        stocks = []
        for ticker in tickers:
            row = candidates.loc[ticker]
            stock_data = {"ticker": ticker}
            if ticker in names:
                stock_data["name"] = names[ticker]
            elif "종목명" in candidates.columns:
                stock_data["name"] = str(row.get("종목명", ""))
            for col in candidates.columns:
                if col == "종목명":
                    continue
                val = row[col]
                if hasattr(val, 'item'):
                    val = val.item()
                stock_data[col.lower()] = val
            stocks.append(stock_data)

    return {
        "count": len(stocks),
        "date": today_str,
        "source": source,
        "filters_applied": applied_basic + applied_detail,
        "stocks": stocks,
    }


# ─── 하위 호환: 기존 screen_stocks_kis는 screen_stocks_hybrid로 위임 ───
async def screen_stocks_kis(filters: dict) -> dict:
    return await screen_stocks_hybrid(filters)
