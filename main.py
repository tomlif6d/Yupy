# ####################################################################################
# السكربت النهائي: محاكي تداول عالي الأداء وموثوق
# ------------------------------------------------------------------------------------
# الميزات:
# 1. بنية Event-Driven: اكتشاف الفرص فورًا عند تحديث الأسعار (أقصى سرعة).
# 2. إعادة اتصال تلقائي: يضمن استمرارية العمل عبر إعادة الاتصال بالـ WebSocket.
# 3. هيكلية متعددة الخيوط: خيوط منفصلة لاستقبال البيانات، المراقبة، والكتابة.
# 4. مراقبة سريعة للصفقات: خيط مخصص لمراقبة أهداف الربح/الخسارة 20 مرة في الثانية.
# 5. تحليل AggTrade: يضيف بيانات ضغط البيع/الشراء عند فتح كل صفقة.
# 6. جاهز للتشغيل الآلي: يتوقف بعد مدة محددة ومناسب لـ GitHub Actions.
# ####################################################################################

import websocket
import json
import time
import threading
import csv
from datetime import datetime, timezone
from collections import deque
import queue
import os

# --- الإعدادات الرئيسية ---
# قائمة عملات أصغر مناسبة للتشغيل السريع في GitHub Actions (يمكن توسيعها)
SYMBOLS = ["BTC", "ETH", "ATOM", "DYDX", "SOL", "AVAX", "BNB", "APE", "OP", "LTC", "ARB", "DOGE", "INJ", "SUI", "CRV", "LDO", "LINK", "STX", "CFX", "GMX", "SNX", "XRP", "BCH", "APT", "AAVE", "COMP", "MKR", "WLD", "FXS", "YGG", "TRX", "UNI", "SEI", "RUNE", "ZRO", "DOT", "BANANA", "TRB", "ARK", "BIGTIME", "KAS", "BLUR", "TIA", "BSV", "ADA", "TON", "MINA", "POLYX", "GAS", "PENDLE", "FET", "NEAR", "MEME", "ORDI", "NEO", "ZEN", "FIL", "PYTH", "SUSHI", "IMX", "GMT", "SUPER", "USTC", "JUP", "RSR", "GALA", "JTO", "ACE", "MAV", "WIF", "CAKE", "PEOPLE", "ENS", "ETC", "XAI", "MANTA", "UMA", "ONDO", "ALT", "ZETA", "DYM", "MAVIA", "W", "STRK", "TAO", "AR", "BOME", "ETHFI", "ENA", "TNSR", "SAGA", "MERL", "HBAR", "POPCAT", "OMNI", "EIGEN", "REZ", "NOT", "TURBO", "BRETT", "IO", "ZK", "MEW", "RENDER", "POL", "CELO", "HMSTR", "SCR", "NEIROETH", "GOAT", "MOODENG", "GRASS", "PNUT", "XLM", "CHILLGUY", "SAND", "IOTA", "ALGO", "HYPE", "ME", "MOVE", "VIRTUAL", "PENGU", "USUAL", "FARTCOIN", "AI16Z", "AIXBT", "ZEREBRO", "BIO", "GRIFFAIN", "SPX", "S", "MORPHO", "TRUMP", "MELANIA", "ANIME", "VINE", "VVV", "BERA", "TST", "LAYER", "IP", "OM", "KAITO", "NIL", "PAXG", "PROMPT", "BABY", "WCT", "HYPER", "ZORA", "INIT", "DOOD", "NXPC", "SOPH", "RESOLV", "SYRUP", "PUMP", "PROVE"] 
SYMBOLS_BINANCE = [f"{s.lower()}usdt" for s in SYMBOLS]
SYMBOLS_HYPERLIQUID = SYMBOLS

SPREAD_THRESHOLD = 0.4      # عتبة الفجوة المئوية لفتح الصفقة
TP_PERCENT_OF_GAP = 0.80     # هدف الربح: 80% من الفجوة الأولية
SL_PERCENT_OF_GAP = -0.50    # وقف الخسارة: 50% خسارة من الفجوة الأولية
AGG_TRADE_DEPTH = 100      # عمق تحليل صفقات Binance

# مدة تشغيل السكربت بالدقائق (يأخذ القيمة من متغير بيئة إذا وجد، وإلا يستخدم القيمة الافتراضية)
DEFAULT_DURATION = 150
try:
    COLLECTION_DURATION_MINUTES = int(os.environ.get('SIM_DURATION', DEFAULT_DURATION))
except (ValueError, TypeError):
    COLLECTION_DURATION_MINUTES = DEFAULT_DURATION

DATA_FILE = "trading_simulation_binance_only.csv"
DATA_HEADERS = [
    "symbol", "signal_type", "outcome", "entry_time", "exit_time", "duration_seconds",
    "initial_gap_usd", "pnl_usd", "pnl_as_percent_of_gap", "entry_spread_percent",
    "binance_entry_price", "binance_exit_price", "entry_binance_buy_volume", 
    "entry_binance_sell_volume", "entry_buy_sell_ratio"
]

# --- المتغيرات المشتركة بين الخيوط ---
latest_data = {
    "binance": {symbol: {} for symbol in SYMBOLS_BINANCE},
    "hyperliquid": {symbol: {} for symbol in SYMBOLS_HYPERLIQUID}
}
agg_trades_binance = {symbol: deque(maxlen=AGG_TRADE_DEPTH) for symbol in SYMBOLS_BINANCE}
tracked_trades = {}
lock = threading.Lock()
log_queue = queue.Queue()
stop_event = threading.Event()

# --- الدالة المركزية لاكتشاف الفرص ---
def check_for_opportunity(symbol_upper):
    with lock:
        if symbol_upper in tracked_trades: return
        symbol_binance = f"{symbol_upper.lower()}usdt"
        binance_data = latest_data["binance"].get(symbol_binance, {})
        hl_data = latest_data["hyperliquid"].get(symbol_upper, {})

        if not all(k in binance_data for k in ['bid_price', 'ask_price']) or \
           not all(k in hl_data for k in ['bid_price', 'ask_price']):
            return

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
            ratio = buy_volume / sell_volume if sell_volume > 0 else (float('inf') if buy_volume > 0 else 1.0)
            
            initial_gap_usd = abs(hl_mid - b_mid)
            
            tracked_trades[symbol_upper] = {
                'signal': signal, 'entry_time': datetime.now(timezone.utc), 'entry_spread': current_spread,
                'initial_gap_usd': initial_gap_usd, 'entry_price': entry_price, 'buy_volume': buy_volume, 
                'sell_volume': sell_volume, 'ratio': ratio
            }
            print(f"🔥 TRADE OPENED: {symbol_upper} ({signal}) @ ${entry_price:.4f}. Spread: {current_spread:.3f}%")

# --- دوال WebSocket ---
def on_message_binance(ws, message):
    payload = json.loads(message)
    if 'stream' not in payload or 'data' not in payload: return
    data = payload['data']
    event_type = data.get('e')
    symbol_lower = data.get('s', '').lower()
    if event_type == 'bookTicker' and symbol_lower in latest_data["binance"]:
        with lock: latest_data["binance"][symbol_lower] = {'bid_price': data['b'], 'ask_price': data['a']}
        check_for_opportunity(symbol_lower[:-4].upper())
    elif event_type == 'aggTrade' and symbol_lower in agg_trades_binance:
        with lock: agg_trades_binance[symbol_lower].append({'q': float(data['q']), 'm': data['m']})

def on_message_hyperliquid(ws, message):
    data = json.loads(message)
    if data.get("channel") != "l2Book" or "data" not in data: return
    coin_data = data["data"]
    symbol_upper = coin_data.get("coin")
    levels = coin_data.get("levels", [])
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

# --- دالة تشغيل WebSocket الموثوقة ---
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

# --- الخيوط العاملة ---
def pnl_monitor():
    print("--- PnL Monitor Thread Started (Fast-Paced) ---")
    while not stop_event.is_set():
        time.sleep(0.05) # المراقبة 20 مرة في الثانية
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
                    log_entry = [
                        symbol_upper, trade['signal'], outcome, trade['entry_time'].isoformat(), exit_time.isoformat(), f"{duration:.2f}",
                        f"{trade['initial_gap_usd']:.4f}", f"{pnl_usd:.4f}", f"{pnl_percent_of_gap:.4f}",
                        f"{trade['entry_spread']:.4f}", f"{trade['entry_price']:.4f}", f"{exit_price:.4f}",
                        f"{trade['buy_volume']:.4f}", f"{trade['sell_volume']:.4f}", f"{trade['ratio']:.2f}"
                    ]
                    log_queue.put(log_entry)
                    print(f"** {outcome} ** for {symbol_upper}. PnL: ${pnl_usd:.4f}. Queued for logging.")
                    del tracked_trades[symbol_upper]

def log_writer():
    print("--- Log Writer Thread Started ---")
    with open(DATA_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(DATA_HEADERS)
        while not stop_event.is_set() or not log_queue.empty():
            try:
                log_entry = log_queue.get(timeout=1)
                writer.writerow(log_entry)
            except queue.Empty:
                continue
    print(f"--- Log writer finished. Data saved to {DATA_FILE} ---")

# --- الدالة الرئيسية لتشغيل كل شيء ---
if __name__ == "__main__":
    print("--- Starting Resilient, Event-Driven Trading Simulator ---")
    
    threads = [
        threading.Thread(target=run_websocket_resilient, args=("Binance", "wss://fstream.binance.com/stream", on_message_binance, on_open_binance), daemon=True),
        threading.Thread(target=run_websocket_resilient, args=("Hyperliquid", "wss://api.hyperliquid.xyz/ws", on_message_hyperliquid, on_open_hyperliquid), daemon=True),
        threading.Thread(target=pnl_monitor, daemon=True)
    ]
    # خيط الكتابة ليس daemon لضمان إكماله للكتابة قبل خروج البرنامج
    log_writer_thread = threading.Thread(target=log_writer)
    threads.append(log_writer_thread)

    for t in threads:
        t.start()
        
    print(f"\nSimulator will run for {COLLECTION_DURATION_MINUTES} minutes...")
    
    # حلقة انتظار ومراقبة عدد الصفقات المفتوحة
    start_time = time.time()
    while time.time() - start_time < COLLECTION_DURATION_MINUTES * 60:
        time.sleep(30)
        active_trades = len(tracked_trades)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Uptime: {int((time.time() - start_time)/60)} mins. Monitoring {active_trades} active trades.")

    print("\n--- Stopping Simulation ---")
    stop_event.set()
    
    # انتظر خيط الكتابة لينتهي من عمله تمامًا
    log_writer_thread.join()
    
    print("--- All threads stopped. Simulation complete. ---")
