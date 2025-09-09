# ####################################################################################
# الجزء الثالث: محاكاة التداول مع أهداف ربح وخسارة (TP/SL)
# يفتح صفقة عند اكتشاف فرصة ويغلقها عند تحقيق 80% ربح أو 50% خسارة من الفجوة الأولية
# ####################################################################################

# تثبيت المكتبات
!pip install websocket-client pandas

import websocket
import json
import time
import pandas as pd
import threading
import csv
from datetime import datetime, timezone

# --- الإعدادات الرئيسية ---
SYMBOLS = [
    "BTC", "ETH", "ATOM", "DYDX", "SOL", "AVAX", "BNB", "APE", "OP", "LTC", "ARB", "DOGE", "INJ", "SUI", "CRV", "LDO", "LINK", "STX", "CFX", "GMX", "SNX", "XRP", "BCH", "APT", "AAVE", "COMP", "MKR", "WLD", "FXS", "YGG", "TRX", "UNI", "SEI", "RUNE", "ZRO", "DOT", "BANANA", "TRB", "ARK", "BIGTIME", "KAS", "BLUR", "TIA", "BSV", "ADA", "TON", "MINA", "POLYX", "GAS", "PENDLE", "FET", "NEAR", "MEME", "ORDI", "NEO", "ZEN", "FIL", "PYTH", "SUSHI", "IMX", "GMT", "SUPER", "USTC", "JUP", "RSR", "GALA", "JTO", "ACE", "MAV", "WIF", "CAKE", "PEOPLE", "ENS", "ETC", "XAI", "MANTA", "UMA", "ONDO", "ALT", "ZETA", "DYM", "MAVIA", "W", "STRK", "TAO", "AR", "BOME", "ETHFI", "ENA", "TNSR", "SAGA", "MERL", "HBAR", "POPCAT", "OMNI", "EIGEN", "REZ", "NOT", "TURBO", "BRETT", "IO", "ZK", "MEW", "RENDER", "POL", "CELO", "HMSTR", "SCR", "NEIROETH", "GOAT", "MOODENG", "GRASS", "PNUT", "XLM", "CHILLGUY", "SAND", "IOTA", "ALGO", "HYPE", "ME", "MOVE", "VIRTUAL", "PENGU", "USUAL", "FARTCOIN", "AI16Z", "AIXBT", "ZEREBRO", "BIO", "GRIFFAIN", "SPX", "S", "MORPHO", "TRUMP", "MELANIA", "ANIME", "VINE", "VVV", "BERA", "TST", "LAYER", "IP", "OM", "KAITO", "NIL", "PAXG", "PROMPT", "BABY", "WCT", "HYPER", "ZORA", "INIT", "DOOD", "NXPC", "SOPH", "RESOLV", "SYRUP", "PUMP", "PROVE"
]
SYMBOLS_BINANCE = [f"{s.lower()}usdt" for s in SYMBOLS]
SYMBOLS_HYPERLIQUID = SYMBOLS

SPREAD_THRESHOLD = 0.5      # عتبة النسبة المئوية لفتح الصفقة
TP_PERCENT_OF_GAP = 0.80     # هدف الربح: 80% من الفجوة الأولية
SL_PERCENT_OF_GAP = -0.50    # وقف الخسارة: 50% خسارة من الفجوة الأولية

DATA_FILE = "trading_simulation_log_tp_sl.csv"
# --- تعديل هنا: إضافة أعمدة أسعار الخروج ---
DATA_HEADERS = [
    "symbol", "signal_type", "outcome",
    "entry_time", "exit_time", "duration_seconds",
    "initial_gap_usd", "pnl_usd", "pnl_as_percent_of_gap",
    "entry_spread_percent",
    "entry_binance_ask", "entry_binance_bid", "entry_hl_ask", "entry_hl_bid",
    "exit_binance_ask", "exit_binance_bid", "exit_hl_ask", "exit_hl_bid"
]

# --- المتغيرات الخاصة بتتبع الحالة ---
latest_data = {
    "binance": {symbol: {} for symbol in SYMBOLS_BINANCE},
    "hyperliquid": {symbol: {} for symbol in SYMBOLS_HYPERLIQUID}
}
tracked_trades = {} # لتخزين الصفقات المفتوحة
lock = threading.Lock()

# --- دوال WebSocket (مع تعديل بسيط لجمع كل بيانات bookTicker) ---
def on_message_binance(ws, message):
    payload = json.loads(message)
    if 'stream' in payload and 'data' in payload and payload['data'].get('e') == 'bookTicker':
        data = payload['data']
        symbol = data['s'].lower()
        if symbol in latest_data["binance"]:
            with lock:
                latest_data["binance"][symbol] = {
                    'bid_price': data['b'], 'bid_qty': data['B'],
                    'ask_price': data['a'], 'ask_qty': data['A']
                }

def on_message_hyperliquid(ws, message):
    data = json.loads(message)
    if data.get("channel") == "l2Book" and "data" in data:
        symbol = data["data"]["coin"]
        levels = data["data"]["levels"]
        if symbol in latest_data["hyperliquid"] and len(levels) >= 2 and levels[0] and levels[1]:
            with lock:
                latest_data["hyperliquid"][symbol] = {
                    'bid_price': levels[0][0]['px'], 'bid_qty': levels[0][0]['sz'],
                    'ask_price': levels[1][0]['px'], 'ask_qty': levels[1][0]['sz']
                }

# --- دوال WebSocket المساعدة (بدون تغيير) ---
def on_error(ws, error): print(f"### WebSocket Error: {error} ###")
def on_close(ws, close_status_code, close_msg): print(f"### WebSocket Closed: {close_msg} ###")
def on_open_binance(ws):
    print(">>> Binance WebSocket Opened <<<")
    ws.send(json.dumps({"method": "SUBSCRIBE", "params": [f"{s}@bookTicker" for s in SYMBOLS_BINANCE], "id": 1}))
def on_open_hyperliquid(ws):
    print(">>> Hyperliquid WebSocket Opened <<<")
    for symbol in SYMBOLS_HYPERLIQUID:
        ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "l2Book", "coin": symbol}}))
def run_websocket(url, on_message, on_open):
    ws = websocket.WebSocketApp(url, on_message=on_message, on_open=on_open, on_error=on_error, on_close=on_close)
    ws.run_forever()

# --- وظيفة محاكاة التداول (مُعدَّلة بالكامل) ---
def trade_simulator():
    with open(DATA_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(DATA_HEADERS)

    while not stop_logging_event.is_set():
        time.sleep(1)
        closed_trades_to_log = []

        with lock:
            for symbol_upper in SYMBOLS:
                symbol_binance = f"{symbol_upper.lower()}usdt"
                binance_data = latest_data["binance"].get(symbol_binance, {})
                hl_data = latest_data["hyperliquid"].get(symbol_upper, {})

                required_keys = ['bid_price', 'ask_price']
                if not all(k in binance_data for k in required_keys) or not all(k in hl_data for k in required_keys):
                    continue

                try:
                    # تحويل جميع الأسعار إلى أرقام عشرية
                    b_bid, b_ask = float(binance_data['bid_price']), float(binance_data['ask_price'])
                    hl_bid, hl_ask = float(hl_data['bid_price']), float(hl_data['ask_price'])
                except (ValueError, TypeError):
                    continue

                b_mid = (b_bid + b_ask) / 2
                hl_mid = (hl_bid + hl_ask) / 2

                if b_mid == 0: continue
                current_spread = ((hl_mid - b_mid) / b_mid) * 100

                # --- منطق المراقبة والإغلاق ---
                if symbol_upper in tracked_trades:
                    trade = tracked_trades[symbol_upper]
                    pnl_usd = 0

                    if trade['signal'] == 'LONG_BINANCE':
                        pnl_usd = (b_bid - trade['entry_b_ask']) + (trade['entry_hl_bid'] - hl_ask)
                    elif trade['signal'] == 'SHORT_BINANCE':
                        pnl_usd = (trade['entry_b_bid'] - b_ask) + (hl_bid - trade['entry_hl_ask'])

                    pnl_percent_of_gap = pnl_usd / trade['initial_gap_usd'] if trade['initial_gap_usd'] != 0 else 0

                    outcome = None
                    if pnl_percent_of_gap >= TP_PERCENT_OF_GAP:
                        outcome = "TAKE_PROFIT"
                    elif pnl_percent_of_gap <= SL_PERCENT_OF_GAP:
                        outcome = "STOP_LOSS"

                    if outcome:
                        exit_time = datetime.now(timezone.utc)
                        duration = (exit_time - trade['entry_time']).total_seconds()
                        print(f"** {outcome} ** for {symbol_upper} ({trade['signal']}). PnL: ${pnl_usd:.4f} ({pnl_percent_of_gap:.1%}). Duration: {duration:.1f}s")

                        # --- تعديل هنا: إضافة أسعار الخروج إلى السجل ---
                        closed_trades_to_log.append([
                            symbol_upper, trade['signal'], outcome,
                            trade['entry_time'].isoformat(), exit_time.isoformat(), f"{duration:.2f}",
                            f"{trade['initial_gap_usd']:.4f}", f"{pnl_usd:.4f}", f"{pnl_percent_of_gap:.4f}",
                            f"{trade['entry_spread']:.4f}",
                            trade['entry_b_ask'], trade['entry_b_bid'], trade['entry_hl_ask'], trade['entry_hl_bid'],
                            b_ask, b_bid, hl_ask, hl_bid  # <-- الأسعار المضافة هنا
                        ])
                        del tracked_trades[symbol_upper]

                # --- منطق البحث والفتح ---
                else:
                    signal = None
                    if current_spread > SPREAD_THRESHOLD:
                        signal = 'LONG_BINANCE'
                    elif current_spread < -SPREAD_THRESHOLD:
                        signal = 'SHORT_BINANCE'

                    if signal:
                        initial_gap_usd = abs(hl_mid - b_mid)
                        print(f"🔥 TRADE OPENED: {symbol_upper} ({signal}). Spread: {current_spread:.3f}%, Gap: ${initial_gap_usd:.4f}. Monitoring for TP/SL...")
                        tracked_trades[symbol_upper] = {
                            'signal': signal,
                            'entry_time': datetime.now(timezone.utc),
                            'entry_spread': current_spread,
                            'initial_gap_usd': initial_gap_usd,
                            'entry_b_ask': b_ask, 'entry_b_bid': b_bid,
                            'entry_hl_ask': hl_ask, 'entry_hl_bid': hl_bid
                        }

        if closed_trades_to_log:
            with open(DATA_FILE, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(closed_trades_to_log)
            print(f"--- Logged {len(closed_trades_to_log)} closed trades to {DATA_FILE} ---")

# --- بدء عملية جمع البيانات ---
print("--- Starting Trading Simulator with TP/SL Logic ---")
binance_ws_url = "wss://fstream.binance.com/stream"
hyperliquid_ws_url = "wss://api.hyperliquid.xyz/ws"
stop_logging_event = threading.Event()

threading.Thread(target=run_websocket, args=(binance_ws_url, on_message_binance, on_open_binance), daemon=True).start()
threading.Thread(target=run_websocket, args=(hyperliquid_ws_url, on_message_hyperliquid, on_open_hyperliquid), daemon=True).start()
threading.Thread(target=trade_simulator, daemon=True).start()

COLLECTION_DURATION_MINUTES = 10
print(f"\nSimulator will run for {COLLECTION_DURATION_MINUTES} minutes...")
try:
    for i in range(COLLECTION_DURATION_MINUTES * 2):
        time.sleep(30)
        active_count = len(tracked_trades)
        if active_count > 0:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Currently monitoring {active_count} active trades...")

except KeyboardInterrupt:
    print("\nUser interrupted simulation.")

print("\n--- Stopping Simulation ---"); stop_logging_event.set()
print(f"Simulation log saved to {DATA_FILE}")
