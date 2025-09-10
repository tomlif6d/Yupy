# ####################################################################################
# الجزء الخامس: محاكاة تداول احترافية مع هيكلية متعددة الخيوط (Multi-threaded)
# - خيط للبحث عن فرص، وخيط للمراقبة السريعة للصفقات المفتوحة، وخيط للكتابة.
# - مصمم للعمل داخل بيئة مؤقتة مثل GitHub Actions.
# ####################################################################################

import websocket
import json
import time
import pandas as pd
import threading
import csv
from datetime import datetime, timezone
from collections import deque
import queue # <-- لاستخدامه في تمرير الصفقات المغلقة إلى خيط الكتابة

# --- الإعدادات الرئيسية ---
# يمكنك تقليل عدد العملات لتسريع الاختبار في GitHub Actions
SYMBOLS = ["BTC", "ETH", "ATOM", "DYDX", "SOL", "AVAX", "BNB", "APE", "OP", "LTC", "ARB", "DOGE", "INJ", "SUI", "CRV", "LDO", "LINK", "STX", "CFX", "GMX", "SNX", "XRP", "BCH", "APT", "AAVE", "COMP", "MKR", "WLD", "FXS", "YGG", "TRX", "UNI", "SEI", "RUNE", "ZRO", "DOT", "BANANA", "TRB", "ARK", "BIGTIME", "KAS", "BLUR", "TIA", "BSV", "ADA", "TON", "MINA", "POLYX", "GAS", "PENDLE", "FET", "NEAR", "MEME", "ORDI", "NEO", "ZEN", "FIL", "PYTH", "SUSHI", "IMX", "GMT", "SUPER", "USTC", "JUP", "RSR", "GALA", "JTO", "ACE", "MAV", "WIF", "CAKE", "PEOPLE", "ENS", "ETC", "XAI", "MANTA", "UMA", "ONDO", "ALT", "ZETA", "DYM", "MAVIA", "W", "STRK", "TAO", "AR", "BOME", "ETHFI", "ENA", "TNSR", "SAGA", "MERL", "HBAR", "POPCAT", "OMNI", "EIGEN", "REZ", "NOT", "TURBO", "BRETT", "IO", "ZK", "MEW", "RENDER", "POL", "CELO", "HMSTR", "SCR", "NEIROETH", "GOAT", "MOODENG", "GRASS", "PNUT", "XLM", "CHILLGUY", "SAND", "IOTA", "ALGO", "HYPE", "ME", "MOVE", "VIRTUAL", "PENGU", "USUAL", "FARTCOIN", "AI16Z", "AIXBT", "ZEREBRO", "BIO", "GRIFFAIN", "SPX", "S", "MORPHO", "TRUMP", "MELANIA", "ANIME", "VINE", "VVV", "BERA", "TST", "LAYER", "IP", "OM", "KAITO", "NIL", "PAXG", "PROMPT", "BABY", "WCT", "HYPER", "ZORA", "INIT", "DOOD", "NXPC", "SOPH", "RESOLV", "SYRUP", "PUMP", "PROVE"] 
SYMBOLS_BINANCE = [f"{s.lower()}usdt" for s in SYMBOLS]
SYMBOLS_HYPERLIQUID = SYMBOLS

SPREAD_THRESHOLD = 0.5
TP_PERCENT_OF_GAP = 0.80
SL_PERCENT_OF_GAP = -0.50
AGG_TRADE_DEPTH = 100
COLLECTION_DURATION_MINUTES = 10 # <-- مدة تشغيل السكربت قبل أن يتوقف تلقائيًا

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
log_queue = queue.Queue() # <-- طابور لتمرير البيانات لخيط الكتابة
stop_event = threading.Event() # <-- للتحكم في إيقاف جميع الخيوط

# --- دوال WebSocket (لا تغيير هنا) ---
def on_message_binance(ws, message):
    payload = json.loads(message)
    if 'stream' in payload and 'data' in payload:
        data = payload['data']
        event_type = data.get('e')
        symbol = data['s'].lower()
        if event_type == 'bookTicker' and symbol in latest_data["binance"]:
            with lock:
                latest_data["binance"][symbol] = {'bid_price': data['b'], 'ask_price': data['a']}
        elif event_type == 'aggTrade' and symbol in agg_trades_binance:
            with lock:
                agg_trades_binance[symbol].append({'q': float(data['q']), 'm': data['m']})

def on_message_hyperliquid(ws, message):
    data = json.loads(message)
    if data.get("channel") == "l2Book" and "data" in data:
        symbol = data["data"]["coin"]
        levels = data["data"]["levels"]
        if symbol in latest_data["hyperliquid"] and len(levels) >= 2 and levels[0] and levels[1]:
            with lock:
                latest_data["hyperliquid"][symbol] = {'bid_price': levels[0][0]['px'], 'ask_price': levels[1][0]['px']}

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

def run_websocket(ws_url, on_message_func, on_open_func):
    ws = websocket.WebSocketApp(ws_url, on_message=on_message_func, on_open=on_open_func, on_error=on_error, on_close=on_close)
    ws.run_forever()

# --- الخيط رقم 1: مراقبة الصفقات المفتوحة (سريع جدًا) ---
def pnl_monitor():
    print("--- PnL Monitor Thread Started ---")
    while not stop_event.is_set():
        time.sleep(0.1) # <-- حلقة سريعة جدًا للمراقبة الدقيقة
        
        with lock:
            if not tracked_trades:
                continue

            # نستخدم نسخة من المفاتيح لتجنب مشاكل حذف عنصر أثناء الدوران
            for symbol_upper in list(tracked_trades.keys()):
                trade = tracked_trades[symbol_upper]
                symbol_binance = f"{symbol_upper.lower()}usdt"
                
                binance_data = latest_data["binance"].get(symbol_binance, {})
                if not binance_data.get('bid_price'):
                    continue

                b_bid, b_ask = float(binance_data['bid_price']), float(binance_data['ask_price'])
                pnl_usd, exit_price = 0, 0

                if trade['signal'] == 'LONG_ON_BINANCE':
                    exit_price = b_bid
                    pnl_usd = exit_price - trade['entry_price']
                elif trade['signal'] == 'SHORT_ON_BINANCE':
                    exit_price = b_ask
                    pnl_usd = trade['entry_price'] - exit_price

                pnl_percent_of_gap = pnl_usd / trade['initial_gap_usd'] if trade['initial_gap_usd'] != 0 else 0
                outcome = None
                if pnl_percent_of_gap >= TP_PERCENT_OF_GAP: outcome = "TAKE_PROFIT"
                elif pnl_percent_of_gap <= SL_PERCENT_OF_GAP: outcome = "STOP_LOSS"

                if outcome:
                    exit_time = datetime.now(timezone.utc)
                    duration = (exit_time - trade['entry_time']).total_seconds()
                    
                    log_entry = [
                        symbol_upper, trade['signal'], outcome,
                        trade['entry_time'].isoformat(), exit_time.isoformat(), f"{duration:.2f}",
                        f"{trade['initial_gap_usd']:.4f}", f"{pnl_usd:.4f}", f"{pnl_percent_of_gap:.4f}",
                        f"{trade['entry_spread']:.4f}", f"{trade['entry_price']:.4f}", f"{exit_price:.4f}",
                        f"{trade['buy_volume']:.4f}", f"{trade['sell_volume']:.4f}", f"{trade['ratio']:.2f}"
                    ]
                    log_queue.put(log_entry) # <-- إرسال الصفقة المغلقة إلى طابور الكتابة
                    print(f"** {outcome} ** for {symbol_upper}. PnL: ${pnl_usd:.4f}. Queued for logging.")
                    del tracked_trades[symbol_upper]

# --- الخيط رقم 2: البحث عن فرص تداول جديدة (بوتيرة أبطأ) ---
def trade_opener():
    print("--- Trade Opener Thread Started ---")
    while not stop_event.is_set():
        time.sleep(1) # <-- البحث عن فرص جديدة كل ثانية

        with lock:
            for symbol_upper in SYMBOLS:
                if symbol_upper in tracked_trades:
                    continue # تجاهل العملة إذا كان لديها صفقة مفتوحة بالفعل

                symbol_binance = f"{symbol_upper.lower()}usdt"
                binance_data = latest_data["binance"].get(symbol_binance, {})
                hl_data = latest_data["hyperliquid"].get(symbol_upper, {})

                if not all(k in binance_data for k in ['bid_price', 'ask_price']) or not all(k in hl_data for k in ['bid_price', 'ask_price']):
                    continue
                try:
                    b_bid, b_ask = float(binance_data['bid_price']), float(binance_data['ask_price'])
                    hl_mid = (float(hl_data['bid_price']) + float(hl_data['ask_price'])) / 2
                except (ValueError, TypeError): continue

                b_mid = (b_bid + b_ask) / 2
                if b_mid == 0: continue
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
                    print(f"🔥 TRADE OPENED: {symbol_upper} ({signal}) @ ${entry_price:.4f}. Spread: {current_spread:.3f}%")
                    
                    tracked_trades[symbol_upper] = {
                        'signal': signal, 'entry_time': datetime.now(timezone.utc),
                        'entry_spread': current_spread, 'initial_gap_usd': initial_gap_usd,
                        'entry_price': entry_price, 'buy_volume': buy_volume, 
                        'sell_volume': sell_volume, 'ratio': ratio
                    }
                    
# --- الخيط رقم 3: كتابة النتائج في الملف ---
def log_writer():
    print("--- Log Writer Thread Started ---")
    with open(DATA_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(DATA_HEADERS)
        
        # استمر في العمل طالما السكربت يعمل أو لا يزال هناك شيء في الطابور
        while not stop_event.is_set() or not log_queue.empty():
            try:
                log_entry = log_queue.get(timeout=1) # انتظر ثانية واحدة كحد أقصى
                writer.writerow(log_entry)
            except queue.Empty:
                continue # استمر إذا كان الطابور فارغًا مؤقتًا
    print(f"--- Log writer finished. Data saved to {DATA_FILE} ---")

# --- الدالة الرئيسية لتشغيل كل شيء ---
if __name__ == "__main__":
    print("--- Starting Multi-Threaded Trading Simulator ---")
    
    threads = []
    
    # بدء خيوط WebSocket
    binance_ws_url = "wss://fstream.binance.com/stream"
    hyperliquid_ws_url = "wss://api.hyperliquid.xyz/ws"
    threads.append(threading.Thread(target=run_websocket, args=(binance_ws_url, on_message_binance, on_open_binance), daemon=True))
    threads.append(threading.Thread(target=run_websocket, args=(hyperliquid_ws_url, on_message_hyperliquid, on_open_hyperliquid), daemon=True))

    # بدء خيوط المحاكاة
    threads.append(threading.Thread(target=pnl_monitor, daemon=True))
    threads.append(threading.Thread(target=trade_opener, daemon=True))
    
    # خيط الكتابة ليس daemon لضمان إكماله للكتابة قبل الخروج
    log_writer_thread = threading.Thread(target=log_writer)
    threads.append(log_writer_thread)

    for t in threads:
        t.start()
        
    print(f"\nSimulator will run for {COLLECTION_DURATION_MINUTES} minutes...")
    time.sleep(COLLECTION_DURATION_MINUTES * 60)

    print("\n--- Stopping Simulation ---")
    stop_event.set()
    
    # انتظر خيط الكتابة لينتهي
    log_writer_thread.join()
    
    print("--- All threads stopped. ---")
