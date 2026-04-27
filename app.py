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
    endpoints = [
        'https://api.binance.com/api/v3/ticker/24hr',
        'https://api1.binance.com/api/v3/ticker/24hr',
        'https://api2.binance.com/api/v3/ticker/24hr',
        'https://api3.binance.com/api/v3/ticker/24hr',
    ]
    for url in endpoints:
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                return resp.json()
        except:
            continue
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
        logger.info("Task: fall_snap completed.")

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
            
        fall_snaps = r.lrange("radar:fall:snaps", 0, -1)
        
        return jsonify({
            "fall_snaps": [json.loads(s) for s in fall_snaps]
        })
    except Exception as e:
        import traceback
        return f"<pre>{traceback.format_exc()}</pre>", 500

@app.route('/ping')
def ping():
    return jsonify({"status": "ok", "time": time.time()})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
