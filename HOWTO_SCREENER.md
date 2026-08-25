# How-To: Pulling Stock Data with the Screener

Audience: agents (and humans) that need to fetch Finviz screener data from this repo.
Everything below was verified against the live site on 2026-08-25 (commit `2b4d0a9`).

## Install

```bash
pip install -e .          # core deps (requests, lxml, aiohttp, tenacity, tqdm)
pip install pandas pyarrow  # optional: to_dataframe() / to_parquet()
```

## Quickstart — every ticker, today's price + change

```python
from finviz.screener import Screener

s = Screener(table="Overview", request_method="async")  # whole market
s.to_parquet("market_today.parquet")                    # preferred output
```

- Full market ≈ 11,600 tickers in ~45–60 s with `request_method="async"`.
- Columns (Overview): `No., Ticker, Company, Sector, Industry, Country, Market Cap, P/E, Price, Change %, Volume`.
- All values are strings; see the DuckDB recipes below for typed casts.

## Constructor parameters

| Param | Type | Meaning | Example |
|---|---|---|---|
| `tickers` | list | Restrict to specific tickers | `["AAPL", "MSFT"]` |
| `filters` | list | Finviz filter codes (see below) | `["exch_nasd", "idx_sp500"]` |
| `rows` | int | Cap on rows. **Quantized to 20/page** — `rows=50` returns 60 | `rows=100` |
| `order` | str | Sort key; `-` prefix = descending | `"-price"`, `"ticker"` |
| `signal` | str | Signal feed (top news, upgrades…) | `"n_majornews"` |
| `table` | str | View: `Overview`, `Valuation`, `Ownership`, `Performance`, `Custom`, `Financial`, `Technical` | `"Overview"` |
| `custom` | list | Custom column IDs (forces table `152`; `No.` added automatically) | `["0","21","23"]` |
| `request_method` | str | `"sequential"` (default) or `"async"` | `"async"` |

Discovery:

```python
Screener.load_filter_dict()   # dict of every valid filter code -> options
```

Inspect an unfamiliar table's exact columns before relying on them:

```python
s = Screener(table="Performance", rows=1)
print(s.headers)
```

## Performance guidance (important)

| Mode | Full market | When to use |
|---|---|---|
| `request_method="async"` | ~45–60 s | Default choice for bulk pulls. Has retry + backoff (1s/2s/4s) for rate-limited pages. |
| `request_method="sequential"` (default) | ~16 min | Only for small pulls (`rows≤100`) or when you must be maximally gentle. |

Completeness is enforced: if retries fail, `async` raises `ConnectionError`; if the final
row count is short of Finviz's reported total by >1%, the Screener raises
`RuntimeError("Incomplete data: ...")`. **A returned Screener is complete** — silent
partial data was a pre-`2b4d0a9` bug and is now guarded.

## Reading the result

```python
s = Screener(table="Overview", request_method="async")

len(s)            # actual number of rows fetched (== len(s.data))
s[0]              # first row as dict, e.g. {"Ticker": "AA", "Price": "154.23", ...}
s.data            # list of row dicts
s.headers         # column names
```

## Export

```python
s.to_parquet("out.parquet")   # Apache Parquet — ~2.4x smaller than CSV (needs pyarrow)
s.to_csv("out.csv")           # plain CSV
s.to_dataframe()              # pandas DataFrame (needs pandas)
s.to_sqlite("out.db")         # sqlite table
```

## DuckDB recipes

```sql
-- Typed price + change from parquet
SELECT Ticker,
       CAST(Price AS DOUBLE)                                   AS price,
       CAST(REPLACE("Change %", '%', '') AS DOUBLE)             AS change_pct,
       CAST(REPLACE(Volume, ',', '') AS BIGINT)                 AS volume
FROM read_parquet('market_today.parquet');

-- Top 20 gainers
SELECT Ticker, CAST(Price AS DOUBLE) AS price,
       CAST(REPLACE("Change %", '%', '') AS DOUBLE) AS chg
FROM read_parquet('market_today.parquet')
ORDER BY chg DESC LIMIT 20;
```

Note `Market Cap` is suffixed (`43.58B`, `293.13M`); strip + rescale if you need it numeric.

## Common pitfalls

1. **`rows` rounds up to a multiple of 20** (page size). Request 50 → get 60. Use exact multiples when the count matters.
2. **Missing values are the string `"-"`** (e.g. ETFs have no P/E). Cast with TRY_CAST or filter them.
3. **Ticker cell text can be split across DOM nodes** — handled internally; never parse raw HTML yourself, always go through `Screener`.
4. **Rate limits**: avoid running several full-market pulls concurrently; one async pull at a time is fast and safe. Hammering invites throttling and retries.
5. **`Change %` is day change relative to previous close** — intraday value moves with the market.

## Repo layout (for agents navigating the code)

```
finviz/screener.py                            # Screener class (params, exports, validation)
finviz/helper_functions/scraper_functions.py  # HTML table extraction (get_table, pagination)
finviz/helper_functions/request_functions.py  # HTTP: sequential + async Connector w/ retry
finviz/tests/test_screener.py                 # tests (network-marked)
example.py                                    # small runnable examples
```
