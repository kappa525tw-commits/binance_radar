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

def get_futures_ticker():
    """Fetch 24H ticker data from Binance Futures API (all perpetuals)."""
    headers = {'User-Agent': 'Mozilla/5.0 (compatible; BinanceRadar/1.0)'}
    endpoints = [
        'https://fapi.binance.com/fapi/v1/ticker/24hr',
        'https://fapi.binance.com/fapi/v2/ticker/24hr',  # v2 fallback
    ]
    for attempt in range(3):  # up to 3 retries
        for url in endpoints:
            try:
                resp = requests.get(url, timeout=30, headers=headers)
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, list) and len(data) > 0:
                        logger.info(f"[OK] Futures ticker ({url}): {len(data)} symbols")
                        return data
                logger.warning(f"Futures ticker {url}: HTTP {resp.status_code}")
            except Exception as e:
                logger.warning(f"Futures ticker {url} attempt {attempt+1}: {e}")
        if attempt < 2:
            time.sleep(3)  # wait 3s before retry
    logger.error("get_futures_ticker: all attempts failed")
    return None



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
