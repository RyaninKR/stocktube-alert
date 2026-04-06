"""
한국투자증권 Open API 연동 모듈
- OAuth 토큰 발급 & 캐시
- 전종목 시세/재무지표 조회
- 개별 종목 현재가 조회
"""

import os
import time
import logging
from datetime import datetime, timezone, timedelta

import httpx

logger = logging.getLogger(__name__)

KIS_APP_KEY = os.getenv("KIS_APP_KEY", "")
KIS_APP_SECRET = os.getenv("KIS_APP_SECRET", "")
KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"

# 토큰 캐시
_token_cache = {"token": None, "expires_at": 0}


async def get_access_token() -> str:
    """OAuth 액세스 토큰 발급 (캐시)"""
    now = time.time()
    if _token_cache["token"] and _token_cache["expires_at"] > now + 60:
        return _token_cache["token"]

    async with httpx.AsyncClient() as client:
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
    # 토큰 유효기간: 보통 24시간, 안전하게 23시간으로 설정
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

    async with httpx.AsyncClient() as client:
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
        "market_cap": int(output.get("hts_avls", 0)) * 100000000,  # 억 → 원
        "per": float(output.get("per", 0)),
        "pbr": float(output.get("pbr", 0)),
        "eps": float(output.get("eps", 0)),
        "bps": float(output.get("bps", 0)),
        "high_52w": int(output.get("stck_dryy_hgpr", 0)),
        "low_52w": int(output.get("stck_dryy_lwpr", 0)),
    }


async def get_market_stocks(market: str = "ALL") -> list:
    """
    전종목 시세 조회 (조건검색 용도)
    한투 API의 국내주식 전종목 시세를 활용
    market: "KOSPI", "KOSDAQ", "ALL"
    """
    token = await get_access_token()
    all_stocks = []

    markets = []
    if market in ("ALL", "KOSPI"):
        markets.append(("J", "0001"))  # KOSPI
    if market in ("ALL", "KOSDAQ"):
        markets.append(("J", "1001"))  # KOSDAQ

    for mrkt_code, mrkt_id in markets:
        try:
            stocks = await _fetch_market_ranking(token, mrkt_code)
            all_stocks.extend(stocks)
        except Exception as e:
            logger.error(f"Market {mrkt_id} fetch error: {e}")

    return all_stocks


async def _fetch_market_ranking(token: str, mrkt_div: str) -> list:
    """
    조건별 전종목 조회 - 시가총액 상위 종목 조회
    """
    headers = _common_headers(token, "FHPST01710000")
    stocks = []

    # 거래량 상위 종목 조회 (최대 30개씩)
    params = {
        "FID_COND_MRKT_DIV_CODE": mrkt_div,
        "FID_COND_SCR_DIV_CODE": "20170",
        "FID_INPUT_ISCD": "0000",
        "FID_DIV_CLS_CODE": "0",
        "FID_BLNG_CLS_CODE": "0",
        "FID_TRGT_CLS_CODE": "111111111",
        "FID_TRGT_EXLS_CLS_CODE": "000000",
        "FID_INPUT_PRICE_1": "",
        "FID_INPUT_PRICE_2": "",
        "FID_VOL_CNT": "",
        "FID_INPUT_DATE_1": "",
    }

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/volume-rank",
            headers=headers,
            params=params,
            timeout=15,
        )
        data = resp.json()

    if data.get("rt_cd") != "0":
        logger.warning(f"Volume rank error: {data.get('msg1', '')}")
        return []

    for item in data.get("output", []):
        try:
            stocks.append({
                "ticker": item.get("mksc_shrn_iscd", ""),
                "name": item.get("hts_kor_isnm", ""),
                "price": int(item.get("stck_prpr", 0)),
                "change_rate": float(item.get("prdy_ctrt", 0)),
                "volume": int(item.get("acml_vol", 0)),
                "per": float(item.get("per", 0)) if item.get("per") else 0,
                "pbr": float(item.get("pbr", 0)) if item.get("pbr") else 0,
            })
        except (ValueError, TypeError):
            continue

    return stocks


async def get_stock_detail(ticker: str) -> dict:
    """종목 상세 재무 정보 (스크리닝용)"""
    price_data = await get_stock_price(ticker)
    if not price_data:
        return {}

    # 추가 재무 정보 조회
    token = await get_access_token()
    headers = _common_headers(token, "FHKST66430300")
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": ticker,
    }

    async with httpx.AsyncClient() as client:
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


async def screen_stocks_kis(filters: dict) -> dict:
    """
    한투 API를 이용한 종목 스크리닝
    1. 전종목 시세에서 기본 필터링 (PER, PBR)
    2. 매칭 후보 종목의 상세 재무 정보 조회
    3. 나머지 필터 적용
    """
    import pandas as pd

    if not KIS_APP_KEY:
        return {"error": "KIS API key not configured"}

    today = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")

    # 1단계: 전종목 시세 조회
    all_stocks = await get_market_stocks("ALL")
    if not all_stocks:
        return {"error": "시장 데이터를 가져올 수 없습니다", "date": today}

    df = pd.DataFrame(all_stocks)
    if df.empty:
        return {"error": "종목 데이터가 비어있습니다", "date": today}

    logger.info(f"KIS: {len(df)} stocks loaded")

    # 2단계: 기본 필터 적용 (PER, PBR - 전종목 데이터에 포함)
    basic_filters = {}
    detail_filters = {}
    basic_cols = {"per", "pbr", "price", "change_rate", "volume"}

    for key, value in filters.items():
        parts = key.rsplit("_", 1)
        if len(parts) != 2:
            continue
        col = parts[0]
        if col in basic_cols:
            basic_filters[key] = value
        else:
            detail_filters[key] = value

    # 기본 필터 적용
    results = df.copy()
    applied = []
    for key, value in basic_filters.items():
        parts = key.rsplit("_", 1)
        col, op = parts[0], parts[1]
        if col not in results.columns:
            continue
        try:
            if op == "lte":
                results = results[(results[col] <= value) & (results[col] > 0)]
            elif op == "gte":
                results = results[results[col] >= value]
            elif op == "lt":
                results = results[(results[col] < value) & (results[col] > 0)]
            elif op == "gt":
                results = results[results[col] > value]
            applied.append(f"{col} {op} {value}")
        except Exception as e:
            logger.warning(f"Filter {key}={value}: {e}")

    # 3단계: 상세 필터가 있으면 후보 종목의 재무 정보 조회
    if detail_filters and len(results) > 0:
        # 후보 종목 수 제한 (API 호출 수 관리)
        candidates = results.head(30)
        enriched = []

        for _, row in candidates.iterrows():
            try:
                detail = await get_stock_detail(row["ticker"])
                if detail:
                    enriched.append(detail)
            except Exception as e:
                logger.warning(f"Detail fetch error for {row['ticker']}: {e}")

        if enriched:
            results = pd.DataFrame(enriched)

            # 상세 필터 적용
            for key, value in detail_filters.items():
                parts = key.rsplit("_", 1)
                col, op = parts[0], parts[1]
                if col not in results.columns:
                    continue
                try:
                    if op == "lte":
                        results = results[(results[col] <= value) & (results[col] > 0)]
                    elif op == "gte":
                        results = results[results[col] >= value]
                    elif op == "lt":
                        results = results[(results[col] < value) & (results[col] > 0)]
                    elif op == "gt":
                        results = results[results[col] > value]
                    applied.append(f"{col} {op} {value}")
                except Exception as e:
                    logger.warning(f"Detail filter {key}={value}: {e}")

    # 결과 정리
    stocks = []
    for _, row in results.head(50).iterrows():
        stock = {}
        for col in results.columns:
            val = row[col]
            if hasattr(val, 'item'):
                val = val.item()
            stock[col] = val
        stocks.append(stock)

    return {
        "count": len(stocks),
        "date": today,
        "source": "kis",
        "filters_applied": applied,
        "stocks": stocks,
    }
