import os
import json
import time
import requests
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
    """Fetch 24H ticker data. Try Binance first, fall back to CoinGecko."""
    headers = {'User-Agent': 'Mozilla/5.0 (compatible; BinanceRadar/1.0)'}
    binance_endpoints = [
        'https://api.binance.com/api/v3/ticker/24hr',
        'https://api1.binance.com/api/v3/ticker/24hr',
        'https://api2.binance.com/api/v3/ticker/24hr',
        'https://api3.binance.com/api/v3/ticker/24hr',
    ]
    for url in binance_endpoints:
        try:
            resp = requests.get(url, timeout=20, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and len(data) > 0:
                    logger.info(f"Binance data fetched OK from {url}, {len(data)} tickers")
                    return data
            else:
                logger.warning(f"Binance endpoint {url} returned HTTP {resp.status_code}")
        except Exception as e:
            logger.warning(f"Binance endpoint {url} failed: {e}")
            continue

    # --- Fallback: CoinGecko (no region restrictions) ---
    logger.warning("All Binance endpoints failed. Trying CoinGecko fallback...")
    try:
        # Fetch top 250 by 24H change
        cg_url = 'https://api.coingecko.com/api/v3/coins/markets'
        params = {
            'vs_currency': 'usd',
            'order': 'price_change_percentage_24h_desc',
            'per_page': 250,
            'page': 1,
            'sparkline': False,
            'price_change_percentage': '24h'
        }
        resp = requests.get(cg_url, params=params, timeout=20, headers=headers)
        if resp.status_code == 200:
            cg_data = resp.json()
            # Normalize to Binance-like format
            normalized = []
            for coin in cg_data:
                pct = coin.get('price_change_percentage_24h') or 0
                price = coin.get('current_price') or 0
                vol = coin.get('total_volume') or 0
                sym = (coin.get('symbol') or '').upper() + 'USDT'
                normalized.append({
                    'symbol': sym,
                    'priceChangePercent': str(pct),
                    'lastPrice': str(price),
                    'quoteVolume': str(vol),
                })
            logger.info(f"CoinGecko fallback OK, {len(normalized)} tickers")
            return normalized
        else:
            logger.error(f"CoinGecko returned HTTP {resp.status_code}")
    except Exception as e:
        logger.error(f"CoinGecko fallback also failed: {e}")

    return None




def fall_snap():
    logger.info("Task: fall_snap starting...")
    data = get_binance_data()
    if data and isinstance(data, list) and r:
        sorted_coins = []
        for d in data:
            try:
                pct = float(d['priceChangePercent'])
                if pct > 0:
                    sorted_coins.append({
                        "sym": d['symbol'],
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

# Run once on startup in background to populate empty database
scheduler.add_job(init_startup, 'date', run_date=datetime.now())

# Use cron for precise scheduling even if server restarts
scheduler.add_job(fall_snap, 'cron', minute='0')
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
        last_error = r.get("radar:last_error")
        
        result = {
            "fall_snaps": [json.loads(s) for s in fall_snaps],
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
        return jsonify({"status": "Snapshot triggered in background"})
    except Exception as e:
        import traceback
        return f"<pre>{traceback.format_exc()}</pre>", 500

@app.route('/ping')
def ping():
    return jsonify({"status": "ok", "time": time.time()})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
