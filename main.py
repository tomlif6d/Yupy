# ####################################################################################
# السكربت النهائي: محاكي تداول عالي الأداء وموثوق (نسخة بدون Pandas)
# ------------------------------------------------------------------------------------
# الميزات:
# 1. بنية Event-Driven: اكتشاف الفرص فورًا عند تحديث الأسعار (أقصى سرعة).
# 2. إعادة اتصال تلقائي: يضمن استمرارية العمل عبر إعادة الاتصال بالـ WebSocket.
# 3. هيكلية متعددة الخيوط: خيوط منفصلة لاستقبال البيانات، المراقبة، والكتابة.
# 4. مراقبة سريعة للصفقات: خيط مخصص لمراقبة أهداف الربح/الخسارة 20 مرة في الثانية.
# 5. تحليل AggTrade: يضيف بيانات ضغط البيع/الشراء عند فتح كل صفقة.
# 6. جاهز للتشغيل الآلي: يتوقف بعد مدة محددة ومناسب لـ GitHub Actions.
# 7. تحليل فني مدمج بالكامل: حسابات RSI, MACD, EMA مبرمجة بالكامل داخل السكربت بدون أي مكتبات خارجية.
# ####################################################################################

# --- التثبيت المطلوب للمكتبات ---
# pip install requests websocket-client

import websocket
import json
import time
import threading
import csv
from datetime import datetime, timezone
from collections import deque
import queue
import os
import requests

# --- الإعدادات الرئيسية ---
SYMBOLS = ["BTC", "ETH", "ATOM", "DYDX", "SOL", "AVAX", "BNB", "APE", "OP", "LTC", "ARB", "DOGE", "INJ", "SUI", "CRV", "LDO", "LINK", "STX", "CFX", "GMX", "SNX", "XRP", "BCH", "APT", "AAVE", "COMP", "MKR", "WLD", "FXS", "YGG", "TRX", "UNI", "SEI", "RUNE", "ZRO", "DOT", "BANANA", "TRB", "ARK", "BIGTIME", "KAS", "BLUR", "TIA", "BSV", "ADA", "TON", "MINA", "POLYX", "GAS", "PENDLE", "FET", "NEAR", "MEME", "ORDI", "NEO", "ZEN", "FIL", "PYTH", "SUSHI", "IMX", "GMT", "SUPER", "USTC", "JUP", "RSR", "GALA", "JTO", "ACE", "MAV", "WIF", "CAKE", "PEOPLE", "ENS", "ETC", "XAI", "MANTA", "UMA", "ONDO", "ALT", "ZETA", "DYM", "MAVIA", "W", "STRK", "TAO", "AR", "BOME", "ETHFI", "ENA", "TNSR", "SAGA", "MERL", "HBAR", "POPCAT", "OMNI", "EIGEN", "REZ", "NOT", "TURBO", "BRETT", "IO", "ZK", "MEW", "RENDER", "POL", "CELO", "HMSTR", "SCR", "NEIROETH", "GOAT", "MOODENG", "GRASS", "PNUT", "XLM", "CHILLGUY", "SAND", "IOTA", "ALGO", "HYPE", "ME", "MOVE", "VIRTUAL", "PENGU", "USUAL", "FARTCOIN", "AI16Z", "AIXBT", "ZEREBRO", "BIO", "GRIFFAIN", "SPX", "S", "MORPHO", "TRUMP", "MELANIA", "ANIME", "VINE", "VVV", "BERA", "TST", "LAYER", "IP", "OM", "KAITO", "NIL", "PAXG", "PROMPT", "BABY", "WCT", "HYPER", "ZORA", "INIT", "DOOD", "NXPC", "SOPH", "RESOLV", "SYRUP", "PUMP", "PROVE"] 
SYMBOLS_BINANCE = [f"{s.lower()}usdt" for s in SYMBOLS]
SYMBOLS_HYPERLIQUID = SYMBOLS

SPREAD_THRESHOLD = 0.6
TP_PERCENT_OF_GAP = 0.30
SL_PERCENT_OF_GAP = -0.90
AGG_TRADE_DEPTH = 100
CANDLE_COUNT = 20
CANDLE_INTERVAL = '1m'

DEFAULT_DURATION = 300
try:
    COLLECTION_DURATION_MINUTES = int(os.environ.get('SIM_DURATION', DEFAULT_DURATION))
except (ValueError, TypeError):
    COLLECTION_DURATION_MINUTES = DEFAULT_DURATION

DATA_FILE = "trading_simulation_advanced_pure_python.csv"
DATA_HEADERS = [
    "symbol", "signal_type", "outcome", "entry_time", "exit_time", "duration_seconds",
    "initial_gap_usd", "pnl_usd", "pnl_as_percent_of_gap", "entry_spread_percent",
    "binance_entry_price", "binance_exit_price", "entry_binance_buy_volume", 
    "entry_binance_sell_volume", "entry_buy_sell_ratio", "entry_buy_percent", "entry_sell_percent",
    "binance_rsi_14", "binance_macd_12_26_9", "binance_macdh_12_26_9", "binance_macds_12_26_9", "binance_ema_20",
    "hl_rsi_14", "hl_macd_12_26_9", "hl_macdh_12_26_9", "hl_macds_12_26_9", "hl_ema_20"
]

# --- المتغيرات المشتركة ---
latest_data = { "binance": {s: {} for s in SYMBOLS_BINANCE}, "hyperliquid": {s: {} for s in SYMBOLS_HYPERLIQUID} }
agg_trades_binance = {s: deque(maxlen=AGG_TRADE_DEPTH) for s in SYMBOLS_BINANCE}
tracked_trades = {}
lock = threading.Lock()
log_queue = queue.Queue()
stop_event = threading.Event()

# --- دوال حساب المؤشرات الفنية (باستخدام Python فقط) ---
def calculate_ema_list(prices, period):
    if len(prices) < period: return []
    multiplier = 2 / (period + 1)
    ema = []
    # Initial SMA
    initial_sma = sum(prices[:period]) / period
    ema.append(initial_sma)
    # Subsequent EMAs
    for price in prices[period:]:
        new_ema = (price * multiplier) + (ema[-1] * (1 - multiplier))
        ema.append(new_ema)
    return ema

def calculate_rsi_list(prices, period=14):
    if len(prices) <= period: return []
    gains, losses = [], []
    for i in range(1, len(prices)):
        delta = prices[i] - prices[i - 1]
        if delta > 0:
            gains.append(delta)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(delta))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    
    rsi_values = []
    for i in range(period, len(gains)):
        if avg_loss == 0:
            rsi = 100
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
        rsi_values.append(rsi)
        
        # Wilder's Smoothing for next iteration
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period
        
    return rsi_values

def calculate_macd_list(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow: return [], [], []
    ema_fast = calculate_ema_list(prices, fast)
    ema_slow = calculate_ema_list(prices, slow)
    
    # Align lists
    ema_fast_aligned = ema_fast[slow - fast:]
    
    macd_line = [(f - s) for f, s in zip(ema_fast_aligned, ema_slow)]
    if len(macd_line) < signal: return [], [], []
    
    signal_line = calculate_ema_list(macd_line, signal)
    
    # Align again
    macd_line_aligned = macd_line[len(macd_line) - len(signal_line):]

    histogram = [(m - s) for m, s in zip(macd_line_aligned, signal_line)]
    
    return macd_line_aligned, signal_line, histogram

def get_technical_indicators(platform, symbol):
    try:
        if platform == 'binance':
            url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol.upper()}&interval={CANDLE_INTERVAL}&limit={CANDLE_COUNT + 50}" # More data for stability
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            data = response.json()
            close_prices = [float(kline[4]) for kline in data]
        
        elif platform == 'hyperliquid':
            url = "https://api.hyperliquid.xyz/info"
            payload = {"type": "candles", "coin": symbol.upper(), "interval": "1m", "startTime": int((time.time() - (CANDLE_COUNT + 50) * 60) * 1000), "endTime": int(time.time() * 1000)}
            response = requests.post(url, json=payload, timeout=5)
            response.raise_for_status()
            data = response.json()
            close_prices = [float(candle['c']) for candle in data]
        else: return {}

        if not close_prices or len(close_prices) < 50: return {}
        
        # --- Manual Indicator Calculation ---
        rsi = calculate_rsi_list(close_prices, period=14)
        macd, macds, macdh = calculate_macd_list(close_prices, fast=12, slow=26, signal=9)
        ema = calculate_ema_list(close_prices, period=20)
        
        if not all([rsi, macd, macds, macdh, ema]): return {}

        return {
            "rsi_14": rsi[-1], "macd_12_26_9": macd[-1], "macdh_12_26_9": macdh[-1],
            "macds_12_26_9": macds[-1], "ema_20": ema[-1]
        }
    except Exception as e:
        print(f"Error fetching TIs for {platform} {symbol}: {e}")
        return {}

# --- الدالة المركزية لاكتشاف الفرص ---
def check_for_opportunity(symbol_upper):
    with lock:
        if symbol_upper in tracked_trades: return
        symbol_binance = f"{symbol_upper.lower()}usdt"
        binance_data = latest_data["binance"].get(symbol_binance, {})
        hl_data = latest_data["hyperliquid"].get(symbol_upper, {})

        if not all(k in binance_data for k in ['bid_price', 'ask_price']) or not all(k in hl_data for k in ['bid_price', 'ask_price']): return
        try:
            b_bid, b_ask = float(binance_data['bid_price']), float(binance_data['ask_price'])
            hl_bid, hl_ask = float(hl_data['bid_price']), float(hl_data['ask_price'])
        except (ValueError, TypeError): return

        b_mid, hl_mid = (b_bid + b_ask) / 2, (hl_bid + hl_ask) / 2
        if b_mid == 0: return
        current_spread = ((hl_mid - b_mid) / b_mid) * 100

        signal, entry_price = None, 0
        if current_spread > SPREAD_THRESHOLD:
            signal, entry_price = 'LONG_ON_BINANCE', b_ask
        elif current_spread < -SPREAD_THRESHOLD:
            signal, entry_price = 'SHORT_ON_BINANCE', b_bid

        if signal:
            trades_to_analyze = list(agg_trades_binance[symbol_binance])
            buy_volume, sell_volume = 0.0, 0.0
            for t in trades_to_analyze:
                if t['m']: sell_volume += t['q']
                else: buy_volume += t['q']
            total_volume = buy_volume + sell_volume
            ratio = buy_volume / sell_volume if sell_volume > 0 else float('inf') if buy_volume > 0 else 1.0
            buy_percent = (buy_volume / total_volume * 100) if total_volume > 0 else 0
            sell_percent = (sell_volume / total_volume * 100) if total_volume > 0 else 0

            binance_indicators = get_technical_indicators('binance', symbol_binance)
            hl_indicators = get_technical_indicators('hyperliquid', symbol_upper)
            initial_gap_usd = abs(hl_mid - b_mid)
            
            trade_data = {
                'signal': signal, 'entry_time': datetime.now(timezone.utc), 'entry_spread': current_spread,
                'initial_gap_usd': initial_gap_usd, 'entry_price': entry_price, 
                'buy_volume': buy_volume, 'sell_volume': sell_volume, 'ratio': ratio,
                'buy_percent': buy_percent, 'sell_percent': sell_percent,
                'binance_indicators': binance_indicators, 'hl_indicators': hl_indicators
            }
            tracked_trades[symbol_upper] = trade_data
            print(f"🔥 TRADE OPENED: {symbol_upper} ({signal}) @ ${entry_price:.4f}. Spread: {current_spread:.3f}%")

# --- دوال WebSocket (لا تغيير هنا) ---
def on_message_binance(ws, message):
    payload = json.loads(message)
    if 'stream' not in payload or 'data' not in payload: return
    data = payload['data']
    event_type, symbol_lower = data.get('e'), data.get('s', '').lower()
    if event_type == 'bookTicker' and symbol_lower in latest_data["binance"]:
        with lock: latest_data["binance"][symbol_lower] = {'bid_price': data['b'], 'ask_price': data['a']}
        check_for_opportunity(symbol_lower[:-4].upper())
    elif event_type == 'aggTrade' and symbol_lower in agg_trades_binance:
        with lock: agg_trades_binance[symbol_lower].append({'q': float(data['q']), 'm': data['m']})

def on_message_hyperliquid(ws, message):
    data = json.loads(message)
    if data.get("channel") != "l2Book" or "data" not in data: return
    coin_data = data["data"]
    symbol_upper, levels = coin_data.get("coin"), coin_data.get("levels", [])
    if symbol_upper in latest_data["hyperliquid"] and len(levels) >= 2 and levels[0] and levels[1]:
        with lock: latest_data["hyperliquid"][symbol_upper] = {'bid_price': levels[0][0]['px'], 'ask_price': levels[1][0]['px']}
        check_for_opportunity(symbol_upper)

def on_error(ws, error): print(f"### WebSocket Error: {error} ###")
def on_close(ws, close_status_code, close_msg): print(f"### WebSocket Closed: {close_msg} ###")

def on_open_binance(ws):
    print(">>> Binance WebSocket Opened <<<")
    streams = [f"{s}@bookTicker" for s in SYMBOLS_BINANCE] + [f"{s}@aggTrade" for s in SYMBOLS_BINANCE]
    ws.send(json.dumps({"method": "SUBSCRIBE", "params": streams, "id": 1}))

def on_open_hyperliquid(ws):
    print(">>> Hyperliquid WebSocket Opened <<<")
    for symbol in SYMBOLS_HYPERLIQUID:
        ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "l2Book", "coin": symbol}}))

def run_websocket_resilient(name, ws_url, on_message_func, on_open_func):
    while not stop_event.is_set():
        try:
            print(f"[{name}] Attempting to connect...")
            ws = websocket.WebSocketApp(ws_url, on_message=on_message_func, on_open=on_open_func, on_error=on_error, on_close=on_close)
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print(f"[{name}] WebSocket run_forever crashed with exception: {e}")
        if not stop_event.is_set():
            print(f"[{name}] Connection lost. Reconnecting in 5 seconds...")
            time.sleep(5)

# --- الخيوط العاملة (لا تغيير هنا) ---
def pnl_monitor():
    print("--- PnL Monitor Thread Started (Fast-Paced) ---")
    while not stop_event.is_set():
        time.sleep(0.05)
        with lock:
            if not tracked_trades: continue
            for symbol_upper in list(tracked_trades.keys()):
                trade = tracked_trades.get(symbol_upper)
                if not trade: continue
                symbol_binance = f"{symbol_upper.lower()}usdt"
                binance_data = latest_data["binance"].get(symbol_binance, {})
                if not binance_data.get('bid_price'): continue
                
                b_bid, b_ask = float(binance_data['bid_price']), float(binance_data['ask_price'])
                pnl_usd, exit_price = 0, 0
                if trade['signal'] == 'LONG_ON_BINANCE': exit_price, pnl_usd = b_bid, b_bid - trade['entry_price']
                elif trade['signal'] == 'SHORT_ON_BINANCE': exit_price, pnl_usd = b_ask, trade['entry_price'] - b_ask
                
                pnl_percent_of_gap = pnl_usd / trade['initial_gap_usd'] if trade['initial_gap_usd'] != 0 else 0
                outcome = None
                if pnl_percent_of_gap >= TP_PERCENT_OF_GAP: outcome = "TAKE_PROFIT"
                elif pnl_percent_of_gap <= SL_PERCENT_OF_GAP: outcome = "STOP_LOSS"
                
                if outcome:
                    exit_time = datetime.now(timezone.utc)
                    duration = (exit_time - trade['entry_time']).total_seconds()
                    b_ind, hl_ind = trade.get('binance_indicators', {}), trade.get('hl_indicators', {})

                    log_entry = [
                        symbol_upper, trade['signal'], outcome, trade['entry_time'].isoformat(), exit_time.isoformat(), f"{duration:.2f}",
                        f"{trade['initial_gap_usd']:.4f}", f"{pnl_usd:.4f}", f"{pnl_percent_of_gap:.4f}", f"{trade['entry_spread']:.4f}", 
                        f"{trade['entry_price']:.4f}", f"{exit_price:.4f}", f"{trade['buy_volume']:.4f}", f"{trade['sell_volume']:.4f}", 
                        f"{trade['ratio']:.2f}", f"{trade.get('buy_percent', 0):.2f}", f"{trade.get('sell_percent', 0):.2f}",
                        f"{b_ind.get('rsi_14', ''):.4f}" if b_ind.get('rsi_14') is not None else "",
                        f"{b_ind.get('macd_12_26_9', ''):.4f}" if b_ind.get('macd_12_26_9') is not None else "",
                        f"{b_ind.get('macdh_12_26_9', ''):.4f}" if b_ind.get('macdh_12_26_9') is not None else "",
                        f"{b_ind.get('macds_12_26_9', ''):.4f}" if b_ind.get('macds_12_26_9') is not None else "",
                        f"{b_ind.get('ema_20', ''):.4f}" if b_ind.get('ema_20') is not None else "",
                        f"{hl_ind.get('rsi_14', ''):.4f}" if hl_ind.get('rsi_14') is not None else "",
                        f"{hl_ind.get('macd_12_26_9', ''):.4f}" if hl_ind.get('macd_12_26_9') is not None else "",
                        f"{hl_ind.get('macdh_12_26_9', ''):.4f}" if hl_ind.get('macdh_12_26_9') is not None else "",
                        f"{hl_ind.get('macds_12_26_9', ''):.4f}" if hl_ind.get('macds_12_26_9') is not None else "",
                        f"{hl_ind.get('ema_20', ''):.4f}" if hl_ind.get('ema_20') is not None else "",
                    ]
                    log_queue.put(log_entry)
                    print(f"** {outcome} ** for {symbol_upper}. PnL: ${pnl_usd:.4f}. Queued for logging.")
                    del tracked_trades[symbol_upper]

def log_writer():
    print("--- Log Writer Thread Started ---")
    file_exists = os.path.isfile(DATA_FILE)
    with open(DATA_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists: writer.writerow(DATA_HEADERS)
        while not stop_event.is_set() or not log_queue.empty():
            try:
                log_entry = log_queue.get(timeout=1)
                writer.writerow(log_entry)
            except queue.Empty: continue
    print(f"--- Log writer finished. Data saved to {DATA_FILE} ---")

# --- الدالة الرئيسية (لا تغيير هنا) ---
if __name__ == "__main__":
    print("--- Starting Simulator (Pure Python TI Version) ---")
    threads = [
        threading.Thread(target=run_websocket_resilient, args=("Binance", "wss://fstream.binance.com/stream", on_message_binance, on_open_binance), daemon=True),
        threading.Thread(target=run_websocket_resilient, args=("Hyperliquid", "wss://api.hyperliquid.xyz/ws", on_message_hyperliquid, on_open_hyperliquid), daemon=True),
        threading.Thread(target=pnl_monitor, daemon=True)
    ]
    log_writer_thread = threading.Thread(target=log_writer)
    threads.append(log_writer_thread)

    for t in threads:
        t.start()
        
    print(f"\nSimulator will run for {COLLECTION_DURATION_MINUTES} minutes...")
    start_time = time.time()
    while time.time() - start_time < COLLECTION_DURATION_MINUTES * 60:
        time.sleep(30)
        with lock: active_trades = len(tracked_trades)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Uptime: {int((time.time() - start_time)/60)} mins. Monitoring {active_trades} active trades.")

    print("\n--- Stopping Simulation ---")
    stop_event.set()
    log_writer_thread.join()
    print("--- All threads stopped. Simulation complete. ---")
