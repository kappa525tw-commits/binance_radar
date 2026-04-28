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

HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; BinanceRadar/1.0)'}

def get_bybit_ticker():
    """Fetch 24H ticker from Bybit linear perpetuals (no geo-restrictions)."""
    try:
        resp = requests.get(
            'https://api.bybit.com/v5/market/tickers',
            params={'category': 'linear'}, timeout=20, headers=HEADERS
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get('retCode') == 0:
                normalized = []
                for t in data.get('result', {}).get('list', []):
                    sym = t.get('symbol', '')
                    if not sym.endswith('USDT'):
                        continue
                    pct = float(t.get('price24hPcnt', 0)) * 100  # Bybit = decimal, convert to %
                    normalized.append({
                        'symbol': sym,
                        'priceChangePercent': str(round(pct, 4)),
                        'lastPrice': str(t.get('lastPrice', 0)),
                        'quoteVolume': str(t.get('turnover24h', 0)),
                    })
                if normalized:
                    logger.info(f"[OK] Bybit ticker fallback: {len(normalized)} symbols")
                    return normalized
    except Exception as e:
        logger.error(f"Bybit ticker: {e}")
    return None


def get_futures_ticker():
    """Fetch 24H ticker: try Binance Futures first, fall back to Bybit."""
    # 1. Binance Futures (may be blocked from US IPs)
    try:
        resp = requests.get(
            'https://fapi.binance.com/fapi/v1/ticker/24hr',
            timeout=20, headers=HEADERS
        )
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list) and data:
                logger.info(f"[OK] Binance Futures ticker: {len(data)} symbols")
                return data
        logger.warning(f"Binance fapi ticker: HTTP {resp.status_code}")
    except Exception as e:
        logger.warning(f"Binance fapi ticker: {e}")

    # 2. Bybit fallback
    return get_bybit_ticker()


def get_bybit_symbols():
    """Get list of Bybit USDT linear perpetual symbols as fallback."""
    try:
        resp = requests.get(
            'https://api.bybit.com/v5/market/tickers',
            params={'category': 'linear'}, timeout=20, headers=HEADERS
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get('retCode') == 0:
                syms = {t['symbol'] for t in data.get('result', {}).get('list', [])
                        if t.get('symbol', '').endswith('USDT')}
                if syms:
                    logger.info(f"[OK] Bybit symbols: {len(syms)}")
                    return syms
    except Exception as e:
        logger.error(f"Bybit symbols: {e}")
    return set()


def get_futures_symbols():
    """Get USDT perpetual symbols: try Binance fapi, fall back to Bybit."""
    global futures_symbols_cache, futures_symbols_cache_time
    if time.time() - futures_symbols_cache_time < 6 * 3600 and futures_symbols_cache:
        return futures_symbols_cache

    new_symbols = set()
    # 1. Binance Futures exchangeInfo
    try:
        resp = requests.get('https://fapi.binance.com/fapi/v1/exchangeInfo',
                            timeout=20, headers=HEADERS)
        if resp.status_code == 200:
            for s in resp.json().get('symbols', []):
                if s.get('contractType') == 'PERPETUAL' and s['symbol'].endswith('USDT'):
                    new_symbols.add(s['symbol'])
            if new_symbols:
                futures_symbols_cache = new_symbols
                futures_symbols_cache_time = time.time()
                logger.info(f"[OK] Binance futures symbols: {len(new_symbols)}")
                return new_symbols
        logger.warning(f"Binance fapi exchangeInfo: HTTP {resp.status_code}")
    except Exception as e:
        logger.warning(f"Binance fapi exchangeInfo: {e}")

    # 2. Bybit fallback
    new_symbols = get_bybit_symbols()
    if new_symbols:
        futures_symbols_cache = new_symbols
        futures_symbols_cache_time = time.time()
    return futures_symbols_cache





def fall_snap():
    logger.info("Task: fall_snap starting...")
    data = get_futures_ticker()

    if data and isinstance(data, list) and r:
        sorted_coins = []
        for d in data:
            try:
                sym = d['symbol']
                if not sym.endswith('USDT'):  # USDT 配對限定
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
        r.ltrim("radar:fall:snaps", -75, -1)
        r.delete("radar:last_error")
        logger.info(f"Task: fall_snap completed. {len(top_200)} coins ranked.")
    elif r:
        err_msg = f"Failed to get valid list data. Received: {str(data)[:200]}"
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
def fetch_klines_bybit(symbol, limit=212):
    """Fetch 1H klines from Bybit as fallback. Returns Binance-compatible format."""
    try:
        resp = requests.get(
            'https://api.bybit.com/v5/market/kline',
            params={'category': 'linear', 'symbol': symbol, 'interval': '60', 'limit': limit},
            timeout=10, headers=HEADERS
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get('retCode') == 0:
                klines = data.get('result', {}).get('list', [])
                # Bybit = newest first → reverse to oldest first
                klines.reverse()
                # Normalize to Binance-compatible: index4=close, index7=quoteVolume
                # Bybit format: [time, open, high, low, close, volume, turnover]
                return [[k[0], k[1], k[2], k[3], k[4], k[5], None, k[6]] for k in klines]
    except Exception as e:
        logger.debug(f"Bybit klines {symbol}: {e}")
    return None


def fetch_klines_1h(symbol, limit=212):
    """Fetch 1H klines: try Binance Futures, fall back to Bybit."""
    try:
        resp = requests.get(
            'https://fapi.binance.com/fapi/v1/klines',
            params={'symbol': symbol, 'interval': '1h', 'limit': limit},
            timeout=10, headers=HEADERS
        )
        if resp.status_code == 200:
            return resp.json()
        logger.debug(f"Binance klines {symbol}: HTTP {resp.status_code}, trying Bybit")
    except Exception as e:
        logger.debug(f"Binance klines {symbol}: {e}, trying Bybit")
    return fetch_klines_bybit(symbol, limit)


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

@app.route('/api/debug')
def api_debug():
    """Test fapi connectivity directly - open this URL in browser to diagnose."""
    headers = {'User-Agent': 'Mozilla/5.0 (compatible; BinanceRadar/1.0)'}
    results = []
    test_urls = [
        'https://fapi.binance.com/fapi/v1/ping',
        'https://fapi.binance.com/fapi/v1/ticker/24hr',
    ]
    for url in test_urls:
        try:
            resp = requests.get(url, timeout=20, headers=headers)
            content_len = len(resp.content)
            results.append({
                "url": url,
                "status": resp.status_code,
                "size_bytes": content_len,
                "ok": resp.status_code == 200
            })
        except Exception as e:
            results.append({"url": url, "error": str(e), "ok": False})
    
    redis_ok = bool(r and r.ping()) if r else False
    snap_count = r.llen("radar:fall:snaps") if r else 0
    
    return jsonify({
        "fapi_tests": results,
        "redis_ok": redis_ok,
        "fall_snap_count": snap_count,
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
