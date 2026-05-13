import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

YAHOO_FINANCE_BASE_URL = "https://query1.finance.yahoo.com"

NASDAQ_100_SYMBOLS = [
    "AAPL", "ABNB", "ADBE", "ADI", "ADP", "ADSK", "AEP", "AMAT", "AMD",
    "AMGN", "AMZN", "ANSS", "APP", "ARM", "ASML", "AVGO", "AZN", "BIIB",
    "BKNG", "BKR", "CDNS", "CDW", "CEG", "CHTR", "CMCSA", "COIN", "COST",
    "CPRT", "CRWD", "CSGP", "CSCO", "CTAS", "CTSH", "DASH", "DDOG",
    "DLTR", "DXCM", "EA", "EXC", "FANG", "FAST", "FTNT", "GEHC", "GFS",
    "GILD", "GOOG", "GOOGL", "HON", "IDXX", "ILMN", "INTC", "INTU",
    "ISRG", "KDP", "KHC", "KLAC", "LIN", "LRCX", "LULU", "MAR", "MCHP",
    "MDB", "MDLZ", "MELI", "META", "MNST", "MRNA", "MRVL", "MSFT", "MU",
    "NFLX", "NVDA", "NXPI", "ODFL", "ON", "ORLY", "PANW", "PAYX", "PCAR",
    "PDD", "PEP", "PLTR", "PYPL", "QCOM", "REGN", "ROP", "ROST", "SBUX",
    "SMCI", "SNPS", "TEAM", "TMUS", "TSLA", "TTD", "TTWO", "TXN", "VRSK",
    "VRTX", "WBD", "WDAY", "XEL", "ZS",
]


def _safe_round(value, decimals: int = 2):
    """Round a numeric value safely, returning None for non-numeric inputs."""
    try:
        if value is None:
            return None
        return round(float(value), decimals)
    except (TypeError, ValueError):
        return None


def _timestamp_iso(epoch) -> str | None:
    """Convert an epoch timestamp to ISO 8601 UTC string."""
    if isinstance(epoch, (int, float)):
        return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()
    return None


def _unwrap_yahoo_value(value):
    """Extract the raw Yahoo Finance value from nested API payloads."""
    if isinstance(value, dict):
        if value.get("raw") is not None:
            return value["raw"]
        if value.get("fmt") is not None:
            return value["fmt"]
    return value


def _build_url(path: str, params: Dict[str, str] | None = None) -> str:
    url = f"{YAHOO_FINANCE_BASE_URL}{path}"
    if params:
        url = f"{url}?{urlencode(params)}"
    return url


def _fetch_yahoo_json(path: str, params: Dict[str, str] | None = None) -> Dict:
    request = Request(
        _build_url(path, params),
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json,text/plain,*/*",
        },
    )
    try:
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise ValueError(f"Yahoo Finance HTTP error {exc.code} for {path}") from exc
    except URLError as exc:
        raise ValueError(f"Yahoo Finance request error for {path}: {exc.reason}") from exc


def _fetch_quote(symbol: str) -> Dict:
    payload = _fetch_yahoo_json("/v7/finance/quote", {"symbols": symbol})
    response = payload.get("quoteResponse") or {}
    error = response.get("error")
    if error:
        raise ValueError(error.get("description") or str(error))

    results = response.get("result") or []
    if not results:
        raise ValueError(f"No quote data found for {symbol}")
    return results[0]


def _fetch_quote_summary(symbol: str) -> Dict:
    payload = _fetch_yahoo_json(
        f"/v10/finance/quoteSummary/{quote(symbol, safe='')}",
        {
            "modules": ",".join(
                [
                    "price",
                    "summaryProfile",
                    "defaultKeyStatistics",
                    "financialData",
                    "summaryDetail",
                ]
            )
        },
    )
    response = payload.get("quoteSummary") or {}
    error = response.get("error")
    if error:
        raise ValueError(error.get("description") or str(error))

    results = response.get("result") or []
    if not results:
        raise ValueError(f"No company data found for {symbol}")
    return results[0]


def _section(payload: Dict, name: str) -> Dict:
    value = payload.get(name)
    return value if isinstance(value, dict) else {}


def get_hourly_stock_data(symbol: str) -> Dict:
    """
    Extract hourly price snapshot for a single symbol.

    Captures time-sensitive market data meant to be collected once per hour:
    current price, day high/low, open, previous close, volume, and market cap.

    Returns:
        Dict with hourly price fields or an error payload.
    """
    try:
        symbol = symbol.upper().strip()
        if not symbol:
            return {"error": "Invalid Symbol", "message": "Symbol cannot be empty"}

        info = _fetch_quote(symbol)
        return {
            "symbol": symbol,
            "extracted_at": datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S"),
            "current_price": _safe_round(_unwrap_yahoo_value(info.get("regularMarketPrice"))),
            "day_high": _safe_round(_unwrap_yahoo_value(info.get("regularMarketDayHigh"))),
            "day_low": _safe_round(_unwrap_yahoo_value(info.get("regularMarketDayLow"))),
            "previous_close": _safe_round(_unwrap_yahoo_value(info.get("regularMarketPreviousClose"))),
            "volume": _unwrap_yahoo_value(info.get("regularMarketVolume")),
            "market_cap": _unwrap_yahoo_value(info.get("marketCap")),
            "fifty_day_average": _safe_round(_unwrap_yahoo_value(info.get("fiftyDayAverage"))),
            "two_hundred_day_average": _safe_round(_unwrap_yahoo_value(info.get("twoHundredDayAverage"))),
            "fifty_two_week_high": _safe_round(_unwrap_yahoo_value(info.get("fiftyTwoWeekHigh"))),
            "fifty_two_week_low": _safe_round(_unwrap_yahoo_value(info.get("fiftyTwoWeekLow"))),
        }
    except Exception as e:
        logger.error("Hourly data extraction failed for %s: %s", symbol, e)
        return {"error": "Lookup Error", "symbol": symbol, "message": str(e)}


def get_company_info(symbol: str) -> Dict:
    """
    Extract general (mostly static) company information for a single symbol.

    This data changes infrequently and is suited for a daily or on-demand refresh.

    Returns:
        Dict with company profile fields or an error payload.
    """
    try:
        symbol = symbol.upper().strip()
        if not symbol:
            return {"error": "Invalid Symbol", "message": "Symbol cannot be empty"}

        info = _fetch_quote_summary(symbol)
        price = _section(info, "price")
        profile = _section(info, "summaryProfile")
        statistics = _section(info, "defaultKeyStatistics")
        financial = _section(info, "financialData")
        detail = _section(info, "summaryDetail")

        return {
            # Identity & profile
            "symbol": symbol,
            "company_name": _unwrap_yahoo_value(price.get("longName")) or _unwrap_yahoo_value(price.get("shortName")) or symbol,
            "sector": _unwrap_yahoo_value(profile.get("sector")),
            "industry": _unwrap_yahoo_value(profile.get("industry")),
            "quote_type": _unwrap_yahoo_value(price.get("quoteType")),
            "long_business_summary": _unwrap_yahoo_value(profile.get("longBusinessSummary")),

            # Location & contact
            "country": _unwrap_yahoo_value(profile.get("country")),
            "city": _unwrap_yahoo_value(profile.get("city")),
            "state": _unwrap_yahoo_value(profile.get("state")),
            "address": _unwrap_yahoo_value(profile.get("address1")),
            "zip_code": _unwrap_yahoo_value(profile.get("zip")),
            "phone": _unwrap_yahoo_value(profile.get("phone")),
            "website": _unwrap_yahoo_value(profile.get("website")),
            "ir_website": _unwrap_yahoo_value(profile.get("irWebsite")),

            # Workforce
            "full_time_employees": _unwrap_yahoo_value(profile.get("fullTimeEmployees")),

            # Exchange & market
            "exchange": _unwrap_yahoo_value(price.get("exchange")) or _unwrap_yahoo_value(price.get("exchangeName")),
            "full_exchange_name": _unwrap_yahoo_value(price.get("fullExchangeName")),
            "currency": _unwrap_yahoo_value(price.get("financialCurrency")) or _unwrap_yahoo_value(price.get("currency")),
            "regular_market_time": _timestamp_iso(_unwrap_yahoo_value(price.get("regularMarketTime"))),
            "regular_market_open": _safe_round(_unwrap_yahoo_value(price.get("regularMarketOpen")) or _unwrap_yahoo_value(detail.get("open"))),
            "first_trade_date": _timestamp_iso(
                (_unwrap_yahoo_value(price.get("firstTradeDateMilliseconds")) / 1000)
                if _unwrap_yahoo_value(price.get("firstTradeDateMilliseconds"))
                else None
            ),
            "last_split_date": _timestamp_iso(_unwrap_yahoo_value(detail.get("lastSplitDate"))),
            "last_split_factor": _unwrap_yahoo_value(detail.get("lastSplitFactor")),

            # Share structure
            "shares_outstanding": _unwrap_yahoo_value(statistics.get("sharesOutstanding")),
            "float_shares": _unwrap_yahoo_value(statistics.get("floatShares")),
            "held_percent_insiders": _safe_round(_unwrap_yahoo_value(statistics.get("heldPercentInsiders")), 4),
            "held_percent_institutions": _safe_round(_unwrap_yahoo_value(statistics.get("heldPercentInstitutions")), 4),

            # Risk & governance
            "beta": _safe_round(_unwrap_yahoo_value(detail.get("beta")), 3),
            "audit_risk": _unwrap_yahoo_value(statistics.get("auditRisk")),
            "board_risk": _unwrap_yahoo_value(statistics.get("boardRisk")),
            "compensation_risk": _unwrap_yahoo_value(statistics.get("compensationRisk")),
            "share_holder_rights_risk": _unwrap_yahoo_value(statistics.get("shareHolderRightsRisk")),
            "overall_risk": _unwrap_yahoo_value(statistics.get("overallRisk")),

            # Dividends
            "dividend_rate": _safe_round(_unwrap_yahoo_value(detail.get("dividendRate"))),
            "dividend_yield": _safe_round(_unwrap_yahoo_value(detail.get("dividendYield")), 4),

            # Analyst sentiment
            "number_of_analyst_opinions": _unwrap_yahoo_value(financial.get("numberOfAnalystOpinions")),
            "recommendation_key": _unwrap_yahoo_value(price.get("recommendationKey")) or _unwrap_yahoo_value(financial.get("recommendationKey")),

            # Financials (latest snapshot)
            "total_revenue": _unwrap_yahoo_value(financial.get("totalRevenue")),
            "total_debt": _unwrap_yahoo_value(financial.get("totalDebt")),
            "total_cash": _unwrap_yahoo_value(financial.get("totalCash")),
            "free_cashflow": _unwrap_yahoo_value(financial.get("freeCashflow")),
            "book_value": _safe_round(_unwrap_yahoo_value(statistics.get("bookValue"))),
            "revenue_growth": _safe_round(_unwrap_yahoo_value(financial.get("revenueGrowth")), 4),
            "earnings_growth": _safe_round(_unwrap_yahoo_value(financial.get("earningsGrowth")), 4),
            "profit_margins": _safe_round(_unwrap_yahoo_value(financial.get("profitMargins")), 4),
            "gross_margins": _safe_round(_unwrap_yahoo_value(financial.get("grossMargins")), 4),
            "operating_margins": _safe_round(_unwrap_yahoo_value(financial.get("operatingMargins")), 4),
            "return_on_assets": _safe_round(_unwrap_yahoo_value(financial.get("returnOnAssets")), 4),
            "return_on_equity": _safe_round(_unwrap_yahoo_value(financial.get("returnOnEquity")), 4),
            "trailing_pe": _safe_round(_unwrap_yahoo_value(statistics.get("trailingPE")) or _unwrap_yahoo_value(detail.get("trailingPE"))),
            "forward_pe": _safe_round(_unwrap_yahoo_value(statistics.get("forwardPE")) or _unwrap_yahoo_value(detail.get("forwardPE"))),
            "peg_ratio": _safe_round(_unwrap_yahoo_value(statistics.get("pegRatio"))),
            "trailing_eps": _safe_round(_unwrap_yahoo_value(statistics.get("trailingEps"))),
            "forward_eps": _safe_round(_unwrap_yahoo_value(statistics.get("forwardEps"))),

            "extracted_at": datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S"),
        }
    except Exception as e:
        logger.error("Company info error for %s: %s", symbol, e)
        return {"error": "Lookup Error", "symbol": symbol, "message": str(e)}


def extract_hourly_prices(symbols: List[str] | None = None) -> List[Dict]:
    """Fetch hourly price snapshots for all given symbols (default: NASDAQ 100)."""
    symbols = symbols or NASDAQ_100_SYMBOLS
    results = []
    for sym in symbols:
        logger.info("Fetching hourly data for %s", sym)
        results.append(get_hourly_stock_data(sym))
    return results


def extract_company_info(symbols: List[str] | None = None) -> List[Dict]:
    """Fetch company info for all given symbols (default: NASDAQ 100)."""
    symbols = symbols or NASDAQ_100_SYMBOLS
    results = []
    for sym in symbols:
        logger.info("Fetching company info for %s", sym)
        results.append(get_company_info(sym))
    return results


def write_json(data: List[Dict], filename: str, path: Path) -> Path:
    """Write a list of dicts to a timestamped JSON file under DATA_DIR."""
    path.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
    path_str = path / f"{filename}_{ts}0000.json"
    with open(path_str, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    logger.info("Wrote %d records to %s", len(data), path_str)
    return path_str
