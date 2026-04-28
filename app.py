import os
import json
import time
import requests
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, render_template, jsonify
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
import redis
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
try:
    # Upstash uses rediss:// (TLS). ssl_cert_reqs=None avoids cert validation issues on Render.
    r = redis.from_url(redis_url, decode_responses=True, ssl_cert_reqs=None)
    r.ping()
    logger.info("Redis connected successfully.")
except Exception as e:
    logger.error(f"Redis connection failed: {e}")
    r = None

def _safe_float(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default

def get_binance_data():
    """Fetch 24H ticker data. Try multiple sources in order."""
    headers = {'User-Agent': 'Mozilla/5.0 (compatible; BinanceRadar/1.0)'}

    # ── 1. Binance Global (works outside US) ──
    for url in [
        'https://api.binance.com/api/v3/ticker/24hr',
        'https://api1.binance.com/api/v3/ticker/24hr',
        'https://api2.binance.com/api/v3/ticker/24hr',
        'https://api3.binance.com/api/v3/ticker/24hr',
    ]:
        try:
            resp = requests.get(url, timeout=15, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and len(data) > 0:
                    logger.info(f"[OK] Binance global: {len(data)} tickers")
                    return data
            elif resp.status_code == 451:
                logger.warning("Binance global: HTTP 451 (US IP blocked). Skipping remaining global endpoints.")
                break  # All global endpoints will also be 451, skip early
            else:
                logger.warning(f"Binance global {url}: HTTP {resp.status_code}")
        except Exception as e:
            logger.warning(f"Binance global {url}: {e}")

    # ── 2. Binance US (same format, designed for US IPs) ──
    try:
        resp = requests.get('https://api.binance.us/api/v3/ticker/24hr', timeout=15, headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list) and len(data) > 0:
                logger.info(f"[OK] Binance US: {len(data)} tickers")
                return data
        else:
            logger.warning(f"Binance US: HTTP {resp.status_code}")
    except Exception as e:
        logger.warning(f"Binance US: {e}")

    # ── 3. OKX (no geo-restrictions) ──
    try:
        resp = requests.get('https://www.okx.com/api/v5/market/tickers?instType=SPOT', timeout=15, headers=headers)
        if resp.status_code == 200:
            okx_data = resp.json().get('data', [])
            normalized = []
            for t in okx_data:
                inst_id = t.get('instId', '')
                if not inst_id.endswith('-USDT'):
                    continue
                sym = inst_id.replace('-', '')
                last = float(t.get('last') or 0)
                open_ = float(t.get('open24h') or 0)
                pct = ((last - open_) / open_ * 100) if open_ > 0 else 0
                vol = float(t.get('volCcy24h') or 0)
                normalized.append({
                    'symbol': sym,
                    'priceChangePercent': str(round(pct, 4)),
                    'lastPrice': str(last),
                    'quoteVolume': str(vol),
                })
            if normalized:
                logger.info(f"[OK] OKX fallback: {len(normalized)} tickers")
                return normalized
        else:
            logger.warning(f"OKX: HTTP {resp.status_code}")
    except Exception as e:
        logger.warning(f"OKX: {e}")

    # ── 4. CoinGecko (last resort, rate-limited) ──
    try:
        time.sleep(1)  # small delay to avoid instant 429
        cg_url = 'https://api.coingecko.com/api/v3/coins/markets'
        params = {
            'vs_currency': 'usd',
            'order': 'price_change_percentage_24h_desc',
            'per_page': 250,
            'page': 1,
            'sparkline': False,
        }
        resp = requests.get(cg_url, params=params, timeout=20, headers=headers)
        if resp.status_code == 200:
            cg_data = resp.json()
            normalized = []
            for coin in cg_data:
                pct = coin.get('price_change_percentage_24h') or 0
                sym = (coin.get('symbol') or '').upper() + 'USDT'
                normalized.append({
                    'symbol': sym,
                    'priceChangePercent': str(pct),
                    'lastPrice': str(coin.get('current_price') or 0),
                    'quoteVolume': str(coin.get('total_volume') or 0),
                })
            if normalized:
                logger.info(f"[OK] CoinGecko fallback: {len(normalized)} tickers")
                return normalized
        else:
            logger.error(f"CoinGecko: HTTP {resp.status_code}")
    except Exception as e:
        logger.error(f"CoinGecko: {e}")

    logger.error("All data sources failed.")
    return None






import re

futures_symbols_cache = set()
futures_symbols_cache_time = 0

def get_futures_symbols():
    """Fetch the list of symbols that have perpetual futures contracts."""
    global futures_symbols_cache, futures_symbols_cache_time
    # Cache for 6 hours
    if time.time() - futures_symbols_cache_time < 6 * 3600 and futures_symbols_cache:
        return futures_symbols_cache
        
    headers = {'User-Agent': 'Mozilla/5.0 (compatible; BinanceRadar/1.0)'}
    new_symbols = set()
    
    def normalize_sym(s):
        # Remove multiplier prefixes like 1000PEPEUSDT -> PEPEUSDT
        m = re.match(r'^(1000000|100000|10000|1000|100)?(.*)$', s)
        return m.group(2) if m else s

    # 1. Try Binance Futures API
    try:
        resp = requests.get('https://fapi.binance.com/fapi/v1/exchangeInfo', timeout=15, headers=headers)
        if resp.status_code == 200:
            for s in resp.json().get('symbols', []):
                if s.get('contractType') == 'PERPETUAL':
                    new_symbols.add(normalize_sym(s['symbol']))
            if new_symbols:
                futures_symbols_cache = new_symbols
                futures_symbols_cache_time = time.time()
                logger.info(f"[OK] Fetched {len(new_symbols)} futures from Binance")
                return new_symbols
    except Exception as e:
        logger.warning(f"Failed to fetch Binance Futures: {e}")
        
    # 2. Try CoinGecko Derivatives API as fallback
    try:
        time.sleep(1)
        resp = requests.get('https://api.coingecko.com/api/v3/derivatives/exchanges/binance_futures?include_tickers=unexpired', timeout=15, headers=headers)
        if resp.status_code == 200:
            for t in resp.json().get('tickers', []):
                sym = t.get('symbol', '').upper().replace('-', '')
                new_symbols.add(normalize_sym(sym))
            if new_symbols:
                futures_symbols_cache = new_symbols
                futures_symbols_cache_time = time.time()
                logger.info(f"[OK] Fetched {len(new_symbols)} futures from CoinGecko")
                return new_symbols
    except Exception as e:
        logger.warning(f"Failed to fetch CoinGecko Futures: {e}")

    return futures_symbols_cache


def fall_snap():
    logger.info("Task: fall_snap starting...")
    data = get_binance_data()
    futures_syms = get_futures_symbols()
    
    if data and isinstance(data, list) and r:
        sorted_coins = []
        for d in data:
            try:
                sym = d['symbol']
                if not sym.endswith('USDT'):  # 只處理 USDT 配對
                    continue
                if futures_syms and sym not in futures_syms: # 只處理有合約的幣
                    continue
                pct = float(d['priceChangePercent'])
                if pct > 0:
                    sorted_coins.append({
                        "sym": sym,
                        "pct": pct,
                        "vol": float(d['quoteVolume']),
                        "price": float(d['lastPrice'])
                    })
            except:
                pass
        sorted_coins.sort(key=lambda x: x['pct'], reverse=True)
        top_200 = sorted_coins[:200]
        for i, coin in enumerate(top_200):
            coin['rank'] = i + 1
            
        snap = {
            "ts": int(time.time() * 1000),
            "ranked": top_200
        }
        r.rpush("radar:fall:snaps", json.dumps(snap))
        # Keep only the last 75 snapshots (75 hours)
        r.ltrim("radar:fall:snaps", -75, -1)
        r.delete("radar:last_error") # Clear any previous error
        logger.info("Task: fall_snap completed.")
    elif r:
        err_msg = f"Failed to get valid list data from Binance. Received: {str(data)[:200]}"
        logger.error(err_msg)
        r.set("radar:last_error", err_msg)

from datetime import datetime

# Initialize scheduler
scheduler = BackgroundScheduler()

def init_startup():
    if r:
        if r.llen("radar:fall:snaps") == 0:
            fall_snap()

# ── EMA200 Breakout ──────────────────────────────────────────
def fetch_klines_1h(symbol, limit=212):
    """Fetch 1H klines from Binance Futures API (accessible from US servers)."""
    headers = {'User-Agent': 'Mozilla/5.0 (compatible; BinanceRadar/1.0)'}
    try:
        resp = requests.get(
            'https://fapi.binance.com/fapi/v1/klines',
            params={'symbol': symbol, 'interval': '1h', 'limit': limit},
            timeout=10, headers=headers
        )
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        logger.debug(f"klines {symbol}: {e}")
    return None

def ema_snap():
    """Scan futures USDT pairs for 1H EMA200 breakouts."""
    logger.info("Task: ema_snap starting...")
    if not r:
        return

    futures_syms = get_futures_symbols()
    if not futures_syms:
        logger.warning("ema_snap: no futures symbols, skipping")
        return

    usdt_syms = sorted([s for s in futures_syms if s.endswith('USDT')])
    logger.info(f"ema_snap: checking {len(usdt_syms)} symbols")

    K = 2 / 201  # EMA200 smoothing factor

    def check_ema_break(sym):
        klines = fetch_klines_1h(sym, limit=212)
        if not klines or len(klines) < 202:
            return None
        try:
            closes = [float(k[4]) for k in klines]
            # Seed EMA200 with SMA of first 200 candles
            ema = sum(closes[:200]) / 200
            # Update through second-to-last closed candle (skip last which is open)
            for price in closes[200:-2]:
                ema = price * K + ema * (1 - K)
            prev_ema   = ema
            prev_close = closes[-3]
            # Advance one candle to the most recently closed bar
            curr_close = closes[-2]
            curr_ema   = curr_close * K + prev_ema * (1 - K)
            # Breakout: prev closed below EMA, current closed above
            if prev_close < prev_ema and curr_close >= curr_ema:
                vol = float(klines[-2][7])  # quote volume of that candle
                return {
                    'sym': sym,
                    'price': curr_close,
                    'ema200': round(curr_ema, 8),
                    'pct_above': round((curr_close - curr_ema) / curr_ema * 100, 2),
                    'vol': round(vol, 0),
                }
        except Exception as e:
            logger.debug(f"ema check {sym}: {e}")
        return None

    breaks = []
    with ThreadPoolExecutor(max_workers=15) as executor:
        future_map = {executor.submit(check_ema_break, sym): sym for sym in usdt_syms}
        for future in as_completed(future_map):
            result = future.result()
            if result:
                breaks.append(result)

    breaks.sort(key=lambda x: x['pct_above'], reverse=True)
    snap = {'ts': int(time.time() * 1000), 'breaks': breaks}
    r.set('radar:ema200:latest', json.dumps(snap))
    _cache['ts'] = 0  # invalidate API cache
    logger.info(f"ema_snap: found {len(breaks)} EMA200 breaks")

# Run once on startup in background to populate empty database
scheduler.add_job(init_startup, 'date', run_date=datetime.now())

# fall_snap at minute 0, ema_snap at minute 3 every hour
scheduler.add_job(fall_snap, 'cron', minute='0')
scheduler.add_job(ema_snap,  'cron', minute='3')
scheduler.start()

# Server-side cache to avoid hammering Upstash on every poll
_cache = {"data": None, "ts": 0}
CACHE_TTL = 60  # seconds

# API Endpoints
@app.route('/')
def index():
    try:
        return render_template('index.html')
    except Exception as e:
        import traceback
        return f"<pre>{traceback.format_exc()}</pre>", 500

@app.route('/api/data')
def api_data():
    try:
        if not r:
            return jsonify({"error": "Redis not connected"}), 500
        
        # Serve from cache if fresh (reduces Upstash command usage)
        if _cache["data"] and (time.time() - _cache["ts"]) < CACHE_TTL:
            return jsonify(_cache["data"])
        
        fall_snaps = r.lrange("radar:fall:snaps", 0, -1)
        last_error  = r.get("radar:last_error")
        ema_raw     = r.get("radar:ema200:latest")
        ema_data    = json.loads(ema_raw) if ema_raw else {'ts': 0, 'breaks': []}
        
        result = {
            "fall_snaps": [json.loads(s) for s in fall_snaps],
            "ema_breaks": ema_data,
            "error": last_error
        }
        _cache["data"] = result
        _cache["ts"] = time.time()
        return jsonify(result)
    except Exception as e:
        import traceback
        return f"<pre>{traceback.format_exc()}</pre>", 500

@app.route('/api/trigger', methods=['POST'])
def trigger_snap():
    try:
        if not r:
            return jsonify({"error": "Redis not connected"}), 500
        scheduler.add_job(fall_snap, 'date', run_date=datetime.now())
        scheduler.add_job(ema_snap,  'date', run_date=datetime.now())
        return jsonify({"status": "Snapshots triggered in background"})
    except Exception as e:
        import traceback
        return f"<pre>{traceback.format_exc()}</pre>", 500

@app.route('/ping')
def ping():
    return jsonify({"status": "ok", "time": time.time()})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
