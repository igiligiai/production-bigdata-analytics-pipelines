import json

import utils.stock_prices as stock_prices


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def read(self):
        return self._payload.encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_get_hourly_stock_data_uses_yahoo_quote_endpoint(monkeypatch):
    requested_urls = []

    def fake_urlopen(request, timeout=30):
        requested_urls.append(request.full_url)
        payload = {
            "quoteResponse": {
                "result": [
                    {
                        "regularMarketPrice": {"raw": 123.45},
                        "regularMarketDayHigh": {"raw": 125.0},
                        "regularMarketDayLow": {"raw": 120.5},
                        "regularMarketPreviousClose": {"raw": 122.0},
                        "regularMarketVolume": {"raw": 1000},
                        "marketCap": {"raw": 5000000},
                        "fiftyDayAverage": {"raw": 118.75},
                        "twoHundredDayAverage": {"raw": 111.25},
                        "fiftyTwoWeekHigh": {"raw": 150.0},
                        "fiftyTwoWeekLow": {"raw": 90.0},
                    }
                ]
            }
        }
        return FakeResponse(json.dumps(payload))

    monkeypatch.setattr(stock_prices, "urlopen", fake_urlopen)

    result = stock_prices.get_hourly_stock_data("msft")

    assert requested_urls == [
        "https://query1.finance.yahoo.com/v7/finance/quote?symbols=MSFT"
    ]
    assert result["symbol"] == "MSFT"
    assert result["current_price"] == 123.45
    assert result["market_cap"] == 5000000
    assert result["fifty_two_week_low"] == 90.0


def test_get_company_info_uses_yahoo_quote_summary_endpoint(monkeypatch):
    requested_urls = []

    def fake_urlopen(request, timeout=30):
        requested_urls.append(request.full_url)
        payload = {
            "quoteSummary": {
                "result": [
                    {
                        "price": {
                            "longName": {"raw": "Microsoft Corporation"},
                            "shortName": {"raw": "Microsoft"},
                            "quoteType": {"raw": "EQUITY"},
                            "exchange": {"raw": "NMS"},
                            "fullExchangeName": {"raw": "NasdaqGS"},
                            "financialCurrency": {"raw": "USD"},
                            "regularMarketTime": {"raw": 1710000000},
                            "regularMarketOpen": {"raw": 420.0},
                            "firstTradeDateMilliseconds": {"raw": 567993600000},
                            "recommendationKey": {"raw": "buy"},
                        },
                        "summaryProfile": {
                            "sector": {"raw": "Technology"},
                            "industry": {"raw": "Software"},
                            "longBusinessSummary": {"raw": "Summary"},
                            "country": {"raw": "United States"},
                            "city": {"raw": "Redmond"},
                            "state": {"raw": "WA"},
                            "address1": {"raw": "1 Microsoft Way"},
                            "zip": {"raw": "98052"},
                            "phone": {"raw": "123"},
                            "website": {"raw": "https://microsoft.com"},
                            "irWebsite": {"raw": "https://microsoft.com/investor"},
                            "fullTimeEmployees": {"raw": 221000},
                        },
                        "defaultKeyStatistics": {
                            "sharesOutstanding": {"raw": 7430000000},
                            "floatShares": {"raw": 7420000000},
                            "heldPercentInsiders": {"raw": 0.0007},
                            "heldPercentInstitutions": {"raw": 0.74},
                            "auditRisk": {"raw": 1},
                            "boardRisk": {"raw": 2},
                            "compensationRisk": {"raw": 3},
                            "shareHolderRightsRisk": {"raw": 4},
                            "overallRisk": {"raw": 2},
                            "bookValue": {"raw": 35.5},
                            "trailingPE": {"raw": 32.1},
                            "forwardPE": {"raw": 28.4},
                            "pegRatio": {"raw": 1.8},
                            "trailingEps": {"raw": 13.2},
                            "forwardEps": {"raw": 14.8},
                        },
                        "financialData": {
                            "numberOfAnalystOpinions": {"raw": 40},
                            "totalRevenue": {"raw": 250000000000},
                            "totalDebt": {"raw": 90000000000},
                            "totalCash": {"raw": 75000000000},
                            "freeCashflow": {"raw": 60000000000},
                            "revenueGrowth": {"raw": 0.12},
                            "earningsGrowth": {"raw": 0.15},
                            "profitMargins": {"raw": 0.3},
                            "grossMargins": {"raw": 0.68},
                            "operatingMargins": {"raw": 0.42},
                            "returnOnAssets": {"raw": 0.12},
                            "returnOnEquity": {"raw": 0.4},
                        },
                        "summaryDetail": {
                            "beta": {"raw": 1.1},
                            "dividendRate": {"raw": 3.0},
                            "dividendYield": {"raw": 0.007},
                            "lastSplitDate": {"raw": 1609459200},
                            "lastSplitFactor": {"raw": "2:1"},
                        },
                    }
                ]
            }
        }
        return FakeResponse(json.dumps(payload))

    monkeypatch.setattr(stock_prices, "urlopen", fake_urlopen)

    result = stock_prices.get_company_info("msft")

    assert requested_urls == [
        "https://query1.finance.yahoo.com/v10/finance/quoteSummary/MSFT?modules=price%2CsummaryProfile%2CdefaultKeyStatistics%2CfinancialData%2CsummaryDetail"
    ]
    assert result["company_name"] == "Microsoft Corporation"
    assert result["sector"] == "Technology"
    assert result["regular_market_time"] == "2024-03-09T16:00:00+00:00"
    assert result["beta"] == 1.1
    assert result["recommendation_key"] == "buy"
    assert result["total_revenue"] == 250000000000
    assert result["dividend_yield"] == 0.007
