# ####################################################################################
# الجزء السادس: بنية عالية الأداء تعتمد على الأحداث (Event-Driven)
# - إلغاء خيط البحث البطيء (trade_opener).
# - يتم تشغيل منطق اكتشاف الفرص فورًا عند وصول أي تحديث سعر من WebSocket.
# - تحقيق أقصى سرعة ممكنة لاكتشاف الفرص اللحظية.
# ####################################################################################

import websocket
import json
import time
import threading
import csv
from datetime import datetime, timezone
from collections import deque
import queue

# --- الإعدادات الرئيسية ---
# قائمة عملات أصغر للتشغيل السريع في GitHub Actions
SYMBOLS = ["BTC", "ETH", "ATOM", "DYDX", "SOL", "AVAX", "BNB", "APE", "OP", "LTC", "ARB", "DOGE", "INJ", "SUI", "CRV", "LDO", "LINK", "STX", "CFX", "GMX", "SNX", "XRP", "BCH", "APT", "AAVE", "COMP", "MKR", "WLD", "FXS", "YGG", "TRX", "UNI", "SEI", "RUNE", "ZRO", "DOT", "BANANA", "TRB", "ARK", "BIGTIME", "KAS", "BLUR", "TIA", "BSV", "ADA", "TON", "MINA", "POLYX", "GAS", "PENDLE", "FET", "NEAR", "MEME", "ORDI", "NEO", "ZEN", "FIL", "PYTH", "SUSHI", "IMX", "GMT", "SUPER", "USTC", "JUP", "RSR", "GALA", "JTO", "ACE", "MAV", "WIF", "CAKE", "PEOPLE", "ENS", "ETC", "XAI", "MANTA", "UMA", "ONDO", "ALT", "ZETA", "DYM", "MAVIA", "W", "STRK", "TAO", "AR", "BOME", "ETHFI", "ENA", "TNSR", "SAGA", "MERL", "HBAR", "POPCAT", "OMNI", "EIGEN", "REZ", "NOT", "TURBO", "BRETT", "IO", "ZK", "MEW", "RENDER", "POL", "CELO", "HMSTR", "SCR", "NEIROETH", "GOAT", "MOODENG", "GRASS", "PNUT", "XLM", "CHILLGUY", "SAND", "IOTA", "ALGO", "HYPE", "ME", "MOVE", "VIRTUAL", "PENGU", "USUAL", "FARTCOIN", "AI16Z", "AIXBT", "ZEREBRO", "BIO", "GRIFFAIN", "SPX", "S", "MORPHO", "TRUMP", "MELANIA", "ANIME", "VINE", "VVV", "BERA", "TST", "LAYER", "IP", "OM", "KAITO", "NIL", "PAXG", "PROMPT", "BABY", "WCT", "HYPER", "ZORA", "INIT", "DOOD", "NXPC", "SOPH", "RESOLV", "SYRUP", "PUMP", "PROVE"] 
SYMBOLS_BINANCE = [f"{s.lower()}usdt" for s in SYMBOLS]
SYMBOLS_HYPERLIQUID = SYMBOLS

SPREAD_THRESHOLD = 0.5
TP_PERCENT_OF_GAP = 0.80
SL_PERCENT_OF_GAP = -0.50
AGG_TRADE_DEPTH = 100
COLLECTION_DURATION_MINUTES = 50 # مدة تشغيل أقصر مناسبة لـ GitHub Actions

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

# --- الدالة المركزية الجديدة لاكتشاف الفرص ---
def check_for_opportunity(symbol_upper):
    """
    يتم استدعاء هذه الدالة فورًا عند وصول أي تحديث للسعر.
    تحسب الفجوة وتفتح صفقة إذا تحققت الشروط.
    """
    with lock:
        # 1. تحقق سريع: هل هناك صفقة مفتوحة بالفعل لهذه العملة؟
        if symbol_upper in tracked_trades:
            return

        # 2. استخراج البيانات اللازمة
        symbol_binance = f"{symbol_upper.lower()}usdt"
        binance_data = latest_data["binance"].get(symbol_binance, {})
        hl_data = latest_data["hyperliquid"].get(symbol_upper, {})

        # 3. التأكد من توفر أسعار من كلتا المنصتين
        if not all(k in binance_data for k in ['bid_price', 'ask_price']) or \
           not all(k in hl_data for k in ['bid_price', 'ask_price']):
            return

        try:
            b_bid, b_ask = float(binance_data['bid_price']), float(binance_data['ask_price'])
            hl_bid, hl_ask = float(hl_data['bid_price']), float(hl_data['ask_price'])
        except (ValueError, TypeError):
            return

        # 4. حساب الفجوة
        b_mid = (b_bid + b_ask) / 2
        hl_mid = (hl_bid + hl_ask) / 2
        if b_mid == 0: return
        current_spread = ((hl_mid - b_mid) / b_mid) * 100

        # 5. تحديد الإشارة وفتح الصفقة
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
                'signal': signal, 'entry_time': datetime.now(timezone.utc),
                'entry_spread': current_spread, 'initial_gap_usd': initial_gap_usd,
                'entry_price': entry_price, 'buy_volume': buy_volume, 
                'sell_volume': sell_volume, 'ratio': ratio
            }
            # يتم الطباعة خارج القفل لتحسين الأداء قليلاً
            print(f"🔥 TRADE OPENED: {symbol_upper} ({signal}) @ ${entry_price:.4f}. Spread: {current_spread:.3f}%")


# --- دوال WebSocket (مُعدّلة لتشغيل الفحص الفوري) ---
def on_message_binance(ws, message):
    payload = json.loads(message)
    if 'stream' not in payload or 'data' not in payload: return
    
    data = payload['data']
    event_type = data.get('e')
    symbol_lower = data.get('s', '').lower()
    
    if event_type == 'bookTicker' and symbol_lower in latest_data["binance"]:
        with lock:
            latest_data["binance"][symbol_lower] = {'bid_price': data['b'], 'ask_price': data['a']}
        # --- >> التشغيل الفوري << ---
        symbol_upper = symbol_lower[:-4].upper()
        check_for_opportunity(symbol_upper)
        
    elif event_type == 'aggTrade' and symbol_lower in agg_trades_binance:
        with lock:
            agg_trades_binance[symbol_lower].append({'q': float(data['q']), 'm': data['m']})

def on_message_hyperliquid(ws, message):
    data = json.loads(message)
    if data.get("channel") != "l2Book" or "data" not in data: return

    coin_data = data["data"]
    symbol_upper = coin_data.get("coin")
    levels = coin_data.get("levels", [])
    
    if symbol_upper in latest_data["hyperliquid"] and len(levels) >= 2 and levels[0] and levels[1]:
        with lock:
            latest_data["hyperliquid"][symbol_upper] = {'bid_price': levels[0][0]['px'], 'ask_price': levels[1][0]['px']}
        # --- >> التشغيل الفوري << ---
        check_for_opportunity(symbol_upper)

# --- دوال WebSocket المساعدة (لا تغيير) ---
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


# --- خيط مراقبة الصفقات المفتوحة (لا تغيير جوهري، ربما أسرع قليلاً) ---
def pnl_monitor():
    print("--- PnL Monitor Thread Started (Fast-Paced) ---")
    while not stop_event.is_set():
        time.sleep(0.05) # <-- زيادة سرعة المراقبة إلى 20 مرة في الثانية
        
        # الكود الداخلي لم يتغير...
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

                if trade['signal'] == 'LONG_ON_BINANCE':
                    exit_price, pnl_usd = b_bid, b_bid - trade['entry_price']
                elif trade['signal'] == 'SHORT_ON_BINANCE':
                    exit_price, pnl_usd = b_ask, trade['entry_price'] - b_ask

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
                    log_queue.put(log_entry)
                    print(f"** {outcome} ** for {symbol_upper}. PnL: ${pnl_usd:.4f}. Queued for logging.")
                    del tracked_trades[symbol_upper]

# --- خيط كتابة النتائج (لا تغيير) ---
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
    print("--- Starting Event-Driven High-Performance Trading Simulator ---")
    
    # لم نعد بحاجة لخيط trade_opener
    threads = [
        threading.Thread(target=run_websocket, args=("wss://fstream.binance.com/stream", on_message_binance, on_open_binance), daemon=True),
        threading.Thread(target=run_websocket, args=("wss://api.hyperliquid.xyz/ws", on_message_hyperliquid, on_open_hyperliquid), daemon=True),
        threading.Thread(target=pnl_monitor, daemon=True)
    ]
    log_writer_thread = threading.Thread(target=log_writer)
    threads.append(log_writer_thread)

    for t in threads:
        t.start()
        
    print(f"\nSimulator will run for {COLLECTION_DURATION_MINUTES} minutes...")
    time.sleep(COLLECTION_DURATION_MINUTES * 60)

    print("\n--- Stopping Simulation ---")
    stop_event.set()
    log_writer_thread.join()
    
    print("--- All threads stopped. ---")
