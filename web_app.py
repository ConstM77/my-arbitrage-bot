import asyncio
import ccxt
import time
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pybit.unified_trading import WebSocket as BybitWS

app = FastAPI()

BASE_CURRENCY = 'USDT'
ALTS = ['BTC', 'ETH', 'SOL', 'XRP', 'ADA', 'DOT', 'LINK', 'LTC']

TRADE_VOLUME_USDT = 10.0   # Объем одной сделки для симуляции
RESCAN_INTERVAL = 60       # Интервал переоценки рынка (15 секунд)
TOP_TRIANGLES_COUNT = 5    # Мониторим ТОП-5 маршрутов одновременно

VIRTUAL_PROFIT_USDT = 0.0  
TOTAL_TRADES_COUNT = 0     
LOG_FILE_PATH = "arbitrage_history.txt"

live_prices = {}
active_pairs = set()
active_triangles = []
ws_client = None

web_stream_data = {
    "best_roi": -999.0,
    "best_route": "Сбор данных...",
    "profit": 0.0,
    "trades": 0,
    "active_routes_list": []  
}

HTML_PAGE = """
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Арбитраж Бот Панель</title>
    <style>
        body { font-family: 'Segoe UI', Arial, sans-serif; background: #0f0f12; color: #e4e4e7; text-align: center; padding: 20px; margin: 0; }
        .container { max-width: 800px; margin: 0 auto; }
        header { margin-bottom: 30px; }
        h1 { color: #ffffff; margin-bottom: 5px; font-size: 28px; }
        .status-badge { display: inline-block; background: #1e1b4b; color: #818cf8; padding: 5px 12px; border-radius: 20px; font-size: 12px; font-weight: bold; }
        .dashboard { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 25px; }
        .card { background: #18181b; border: 1px solid #27272a; border-radius: 12px; padding: 20px; box-shadow: 0 4px 15px rgba(0,0,0,0.6); text-align: left; }
        .card-center { text-align: center; }
        h3 { margin-top: 0; color: #a1a1aa; font-size: 14px; text-transform: uppercase; letter-spacing: 0.5px; }
        .big-value { font-size: 32px; font-weight: bold; color: #ffffff; margin: 10px 0; }
        .profit-up { color: #10b981; }
        .route-title { font-size: 20px; color: #f59e0b; font-weight: 600; margin: 10px 0; }
        .list-card { background: #18181b; border: 1px solid #27272a; border-radius: 12px; padding: 20px; text-align: left; }
        .route-item { display: flex; justify-content: space-between; padding: 10px 0; border-bottom: 1px solid #27272a; font-size: 14px; }
        .route-item:last-child { border-bottom: none; }
        .route-name { color: #e4e4e7; font-family: monospace; font-size: 15px; }
        .route-roi { font-weight: bold; color: #a1a1aa; }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>Панель Крипто-Арбитража</h1>
            <div class="status-badge">● СТРИМИНГ WEBSOCKET (BYBIT) ЗАПУЩЕН</div>
        </header>
        <div class="dashboard">
            <div class="card card-center">
                <h3>Лучший Живой Спред</h3>
                <div id="route" class="route-title">Загрузка маршрутов...</div>
                <div class="big-value"><span id="roi">0.000</span>%</div>
            </div>
            <div class="card">
                <h3>Paper Trading Мониторинг</h3>
                <div>Виртуальный профит:</div>
                <div class="big-value profit-up">+<span id="profit">0.0000</span> USDT</div>
                <div style="margin-top: 10px;">Исполнено симуляций: <strong id="trades" style="color: #ffffff;">0</strong></div>
            </div>
        </div>
        <div class="list-card">
            <h3>Текущая матрица ТОП-5 (Грубый спред REST)</h3>
            <div id="routes-list">
                <div style="color: #a1a1aa; text-align: center; padding: 10px;">Ожидание ребалансировки рынка...</div>
            </div>
        </div>
    </div>
    <script>
        const ws = new WebSocket(`ws://${window.location.host}/ws`);
        ws.onmessage = function(event) {
            const data = JSON.parse(event.data);
            document.getElementById("route").innerText = data.best_route;
            document.getElementById("roi").innerText = data.best_roi.toFixed(3);
            document.getElementById("profit").innerText = data.profit.toFixed(4);
            document.getElementById("trades").innerText = data.trades;
            const listContainer = document.getElementById("routes-list");
            if (data.active_routes_list && data.active_routes_list.length > 0) {
                listContainer.innerHTML = "";
                data.active_routes_list.forEach(item => {
                    const row = document.createElement("div");
                    row.className = "route-item";
                    row.innerHTML = `
                        <span class="route-name">${item.route}</span>
                        <span class="route-roi" style="color: ${item.roi > 0 ? '#10b981' : '#ef4444'}">${item.roi.toFixed(3)}%</span>
                    `;
                    listContainer.appendChild(row);
                });
            }
        };
    </script>
</body>
</html>
"""

def generate_valid_triangles(base, assets, active_markets):
    triangles = []
    for asset1 in assets:
        for asset2 in assets:
            if asset1 == asset2: continue
            p1, p3 = f"{asset1}{base}", f"{asset2}{base}"
            p2_v1, p2_v2 = f"{asset2}{asset1}", f"{asset1}{asset2}"
            p2 = None
            is_p2_reversed = False
            if p2_v1 in active_markets:
                p2 = p2_v1
                is_p2_reversed = False
            elif p2_v2 in active_markets:
                p2 = p2_v2
                is_p2_reversed = True
            if p1 in active_markets and p3 in active_markets and p2 is not None:
                triangles.append({
                    'route': f"{base}->{asset1}->{asset2}->{base}",
                    'leg1': asset1, 'leg2': asset2,
                    'pair1': p1, 'pair2': p2, 'pair3': p3,
                    'p2_reversed': is_p2_reversed
                })
    return triangles
def handle_orderbook_message(message):
    """Callback-функция для обработки WebSocket-потока стакана Bybit."""
    data = message.get('data')
    topic = message.get('topic')
    if not data or not topic: return
    pair = topic.split('.')[-1]
    if pair in live_prices:
        try:
            asks = data.get('a', [])
            bids = data.get('b', [])
            if asks and isinstance(asks, list) and len(asks) > 0:
                first_ask = asks[0]
                if isinstance(first_ask, list) and len(first_ask) >= 2:
                    live_prices[pair]['ask_price'] = float(first_ask[0])
                    live_prices[pair]['ask_vol']   = float(first_ask[1])
            if bids and isinstance(bids, list) and len(bids) > 0:
                first_bid = bids[0]
                if isinstance(first_bid, list) and len(first_bid) >= 2:
                    live_prices[pair]['bid_price'] = float(first_bid[0])
                    live_prices[pair]['bid_vol']   = float(first_bid[1])
        except Exception:
            pass

async def update_subscriptions(new_pairs):
    global active_pairs, ws_client, live_prices
    pairs_to_unsubscribe = active_pairs - new_pairs
    pairs_to_subscribe = new_pairs - active_pairs
    for pair in pairs_to_unsubscribe:
        clean_pair = pair.replace('/', '')
        if clean_pair in live_prices: del live_prices[clean_pair]
    if pairs_to_subscribe:
        try:
            bybit_rest = ccxt.bybit({'enableRateLimit': True})
            tickers = bybit_rest.fetch_tickers(list(pairs_to_subscribe))
            for pair in pairs_to_subscribe:
                clean_pair = pair.replace('/', '')
                ticker_data = tickers.get(pair, {})
                live_prices[clean_pair] = {
                    'ask_price': float(ticker_data.get('ask', 0.0)) if ticker_data.get('ask') else 0.0,
                    'ask_vol': float(ticker_data.get('askVolume', 1.0)) if ticker_data.get('askVolume') else 1.0,
                    'bid_price': float(ticker_data.get('bid', 0.0)) if ticker_data.get('bid') else 0.0,
                    'bid_vol': float(ticker_data.get('bidVolume', 1.0)) if ticker_data.get('bidVolume') else 1.0
                }
        except Exception:
            for pair in pairs_to_subscribe:
                clean_pair = pair.replace('/', '')
                live_prices[clean_pair] = {'ask_price': 0.0, 'ask_vol': 0.0, 'bid_price': 0.0, 'bid_vol': 0.0}
    if pairs_to_subscribe and ws_client:
        for pair in pairs_to_subscribe:
            clean_pair = pair.replace('/', '')
            try: ws_client.orderbook_stream(depth=50, symbol=clean_pair, callback=handle_orderbook_message)
            except Exception: pass
            await asyncio.sleep(0.02)
    active_pairs = new_pairs

async def market_rebalancer_loop():
    global active_triangles, web_stream_data
    bybit_rest = ccxt.bybit({'enableRateLimit': True})
    print("⏳ Первичный анализ рынка и запуск rebalancer...")
    while True:
        try:
            markets = bybit_rest.load_markets()
            active_markets = [m['id'] for m in markets.values() if m['spot']]
            all_possible = generate_valid_triangles(BASE_CURRENCY, ALTS, active_markets)
            tickers = bybit_rest.fetch_tickers([m['symbol'] for m in markets.values() if m['spot']])
            ticker_prices = {t.replace('/', ''): {'ask': v['ask'], 'bid': v['bid']} for t, v in tickers.items() if v['ask'] and v['bid']}
            scored_triangles = []
            fee = 0.001
            for t in all_possible:
                p1, p2, p3 = t['pair1'], t['pair2'], t['pair3']
                if p1 not in ticker_prices or p2 not in ticker_prices or p3 not in ticker_prices: continue
                amt1 = (TRADE_VOLUME_USDT / ticker_prices[p1]['ask']) * (1 - fee)
                if not t['p2_reversed']: amt2 = (amt1 / ticker_prices[p2]['ask']) * (1 - fee)
                else: amt2 = (amt1 * ticker_prices[p2]['bid']) * (1 - fee)
                final = (amt2 * ticker_prices[p3]['bid']) * (1 - fee)
                roi = ((final - TRADE_VOLUME_USDT) / TRADE_VOLUME_USDT) * 100
                scored_triangles.append((roi, t))
            scored_triangles.sort(key=lambda x: x[0], reverse=True)
            top_selected = scored_triangles[:TOP_TRIANGLES_COUNT]
            new_pairs, new_triangles, routes_list_for_web = set(), [], []
            print(f"\n🔄 [РЕБАЛАНСИРОВКА] Обновление матрицы ТОП-{TOP_TRIANGLES_COUNT} лучших маршрутов...")
            for rank, (roi, t) in enumerate(top_selected, 1):
                new_triangles.append(t)
                routes_list_for_web.append({"route": t['route'], "roi": roi})
                asset1, asset2 = t['leg1'], t['leg2']
                new_pairs.add(f"{asset1}/{BASE_CURRENCY}")
                if not t['p2_reversed']: new_pairs.add(f"{asset2}/{asset1}")
                else: new_pairs.add(f"{asset1}/{asset2}")
                new_pairs.add(f"{asset2}/{BASE_CURRENCY}")
            web_stream_data["active_routes_list"] = routes_list_for_web
            await update_subscriptions(new_pairs)
            active_triangles = new_triangles
        except Exception as e: print(f"⚠️ Ошибка при ребалансировке: {e}")
        await asyncio.sleep(RESCAN_INTERVAL)
def log_virtual_trade(route, volume, profit, roi):
    try:
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        log_line = f"[{timestamp}] СВЯЗКА: {route} | Объем: {volume} USDT | Профит: +{profit:.4f} USDT | ROI: {roi:.3f}%\n"
        with open(LOG_FILE_PATH, "a", encoding="utf-8") as file: file.write(log_line)
    except Exception: pass

async def arbitrage_math_loop():
    global VIRTUAL_PROFIT_USDT, TOTAL_TRADES_COUNT, web_stream_data
    fee, cooldowns = 0.001, {}
    while True:
        if not active_triangles:
            await asyncio.sleep(0.1)
            continue
        best_roi, best_route = -999.0, "Сбор данных..."
        for t in active_triangles:
            p1, p2, p3 = t['pair1'], t['pair2'], t['pair3']
            try:
                pair1_data, pair2_data, pair3_data = live_prices.get(p1), live_prices.get(p2), live_prices.get(p3)
                if not pair1_data or not pair2_data or not pair3_data: continue
                if pair1_data['ask_price'] == 0 or pair2_data['ask_price'] == 0 or pair3_data['bid_price'] == 0: continue
                ask1_p, ask1_v = pair1_data['ask_price'], pair1_data['ask_vol']
                amt1_needed = TRADE_VOLUME_USDT / ask1_p
                if ask1_v < amt1_needed: continue
                amt1 = amt1_needed * (1 - fee)
                ask2_p, ask2_v, bid2_p, bid2_v = pair2_data['ask_price'], pair2_data['ask_vol'], pair2_data['bid_price'], pair2_data['bid_vol']
                if not t['p2_reversed']:
                    amt2_needed = amt1 / ask2_p
                    if ask2_v < amt2_needed: continue
                    amt2 = amt2_needed * (1 - fee)
                else:
                    if bid2_v < amt1: continue
                    amt2 = (amt1 * bid2_p) * (1 - fee)
                bid3_p, bid3_v = pair3_data['bid_price'], pair3_data['bid_vol']
                if bid3_v < amt2: continue
                final_balance = (amt2 * bid3_p) * (1 - fee)
                profit_loss = final_balance - TRADE_VOLUME_USDT
                roi = (profit_loss / TRADE_VOLUME_USDT) * 100
                if roi > best_roi: best_roi, best_route = roi, t['route']
                if profit_loss > 0:
                    current_timestamp = time.time()
                    if t['route'] not in cooldowns or (current_timestamp - cooldowns[t['route']]) > 3.0:
                        cooldowns[t['route']] = current_timestamp
                        VIRTUAL_PROFIT_USDT += profit_loss
                        TOTAL_TRADES_COUNT += 1
                        log_virtual_trade(t['route'], TRADE_VOLUME_USDT, profit_loss, roi)
                        print(f"\n📈 [PAPER TRADE #{TOTAL_TRADES_COUNT}] [{t['route']}] Профит: +{profit_loss:.4f} USDT")
            except Exception: continue
        web_stream_data["best_roi"], web_stream_data["best_route"] = best_roi, best_route
        web_stream_data["profit"], web_stream_data["trades"] = VIRTUAL_PROFIT_USDT, TOTAL_TRADES_COUNT
        await asyncio.sleep(0.02)

@app.get("/")
async def get_dashboard(): return HTMLResponse(HTML_PAGE)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            await websocket.send_json(web_stream_data)
            await asyncio.sleep(0.5)
    except WebSocketDisconnect: pass

async def ws_supervisor():
    global ws_client, active_pairs
    while True:
        try:
            print("📡 Инициализация WebSocket-подключения к Bybit...")
            ws_client = BybitWS(testnet=False, channel_type="spot")
            if active_pairs:
                for pair in active_pairs:
                    clean_pair = pair.replace('/', '')
                    ws_client.orderbook_stream(depth=50, symbol=clean_pair, callback=handle_orderbook_message)
            while True: await asyncio.sleep(5)
        except Exception as e:
            print(f"⚠️ WebSocket потерял соединение: {e}. Реконнект через 3 секунды...")
            if ws_client:
                try: ws_client.exit()
                except Exception: pass
            await asyncio.sleep(3)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(ws_supervisor())
    asyncio.create_task(market_rebalancer_loop())
    asyncio.create_task(arbitrage_math_loop())

if __name__ == "__main__":
    import uvicorn
    import os
    # Облачные сервисы сами назначают порт через переменную окружения PORT
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("web_app:app", host="0.0.0.0", port=port, reload=False)



