# main.py

import websocket
import json
import time
import pandas as pd
import threading
import csv
from datetime import datetime, timezone, timedelta
from decimal import Decimal, getcontext
import collections
import os

# --- الإعدادات الرئيسية ---

# قم بتقليل عدد الرموز للتجربة الأولية في GitHub Actions لتجنب استهلاك الموارد
SYMBOLS = [
    "BTC", "ETH", "ATOM", "DYDX", "SOL", "AVAX", "BNB", "APE", "OP", "LTC", "ARB", "DOGE", "INJ", "SUI", "CRV", "LDO", "LINK", "STX", "CFX", "GMX", "SNX", "XRP", "BCH", "APT", "AAVE", "COMP", "MKR", "WLD", "FXS", "YGG", "TRX", "UNI", "SEI", "RUNE", "ZRO", "DOT", "BANANA", "TRB", "ARK", "BIGTIME", "KAS", "BLUR", "TIA", "BSV", "ADA", "TON", "MINA", "POLYX", "GAS", "PENDLE", "FET", "NEAR", "MEME", "ORDI", "NEO", "ZEN", "FIL", "PYTH", "SUSHI", "IMX", "GMT", "SUPER", "USTC", "JUP", "RSR", "GALA", "JTO", "ACE", "MAV", "WIF", "CAKE", "PEOPLE", "ENS", "ETC", "XAI", "MANTA", "UMA", "ONDO", "ALT", "ZETA", "DYM", "MAVIA", "W", "STRK", "TAO", "AR", "BOME", "ETHFI", "ENA", "TNSR", "SAGA", "MERL", "HBAR", "POPCAT", "OMNI", "EIGEN", "REZ", "NOT", "TURBO", "BRETT", "IO", "ZK", "MEW", "RENDER", "POL", "CELO", "HMSTR", "SCR", "NEIROETH", "GOAT", "MOODENG", "GRASS", "PNUT", "XLM", "CHILLGUY", "SAND", "IOTA", "ALGO", "HYPE", "ME", "MOVE", "VIRTUAL", "PENGU", "USUAL", "FARTCOIN", "AI16Z", "AIXBT", "ZEREBRO", "BIO", "GRIFFAIN", "SPX", "S", "MORPHO", "TRUMP", "MELANIA", "ANIME", "VINE", "VVV", "BERA", "TST", "LAYER", "IP", "OM", "KAITO", "NIL", "PAXG", "PROMPT", "BABY", "WCT", "HYPER", "ZORA", "INIT", "DOOD", "NXPC", "SOPH", "RESOLV", "SYRUP", "PUMP", "PROVE"
]
SYMBOLS_BINANCE = [f"{s.lower()}usdt" for s in SYMBOLS]
SYMBOLS_HYPERLIQUID = SYMBOLS

# إعدادات المحاكاة
SPREAD_THRESHOLD = 0.50      # عتبة النسبة المئوية لفتح الصفقة (e.g., 0.25%)
TP_PERCENT_OF_GAP = 0.80     # هدف الربح: 80% من الفجوة الأولية
SL_PERCENT_OF_GAP = -0.50    # وقف الخسارة: 50% خسارة من الفجوة الأولية

# إعدادات تحليل ضغط الشراء/البيع (Imbalance)
AGG_TRADE_WINDOW_SECONDS = 30 # تحليل الصفقات في آخر 30 ثانية
IMBALANCE_THRESHOLD = 0.10   # يجب أن يتجاوز ضغط الشراء/البيع 10% لفتح الصفقة

# إعدادات الملف والمدة
# اقرأ مدة التشغيل من متغيرات البيئة (لـ GitHub Actions) أو استخدم قيمة افتراضية
COLLECTION_DURATION_MINUTES = int(os.environ.get('DURATION_MIN', 10))
DATA_FILE = "trading_simulation_log.csv"

# --- تعديل: إضافة عمود Imbalance ---
DATA_HEADERS = [
    "symbol", "signal_type", "outcome",
    "entry_time", "exit_time", "duration_seconds",
    "initial_gap_usd", "pnl_usd", "pnl_as_percent_of_gap",
    "entry_spread_percent", "entry_imbalance_ratio",
    "entry_binance_ask", "entry_binance_bid", "entry_hl_ask", "entry_hl_bid",
    "exit_binance_ask", "exit_binance_bid", "exit_hl_ask", "exit_hl_bid"
]

# --- المتغيرات الخاصة بتتبع الحالة ---
getcontext().prec = 18 # تحديد دقة عالية للعمليات الحسابية

latest_data = {
    "binance": {symbol: {} for symbol in SYMBOLS_BINANCE},
    "hyperliquid": {symbol: {} for symbol in SYMBOLS_HYPERLIQUID}
}
# استخدام deque لتخزين بيانات aggTrade بكفاءة
agg_trade_history = {symbol: collections.deque() for symbol in SYMBOLS_BINANCE}
tracked_trades = {}
lock = threading.Lock()
stop_event = threading.Event()

# --- دوال WebSocket (مع تعديل لـ aggTrade) ---

def on_message_binance(ws, message):
    payload = json.loads(message)
    stream = payload.get('stream')
    if not stream:
        return

    # معالجة بيانات دفتر الطلبات
    if '@bookTicker' in stream:
        data = payload['data']
        symbol = data['s'].lower()
        if symbol in latest_data["binance"]:
            with lock:
                latest_data["binance"][symbol] = {
                    'bid_price': data['b'], 'ask_price': data['a']
                }
    # معالجة بيانات الصفقات المجمعة
    elif '@aggTrade' in stream:
        data = payload['data']
        symbol = data['s'].lower()
        if symbol in agg_trade_history:
            trade_data = {
                't': data['T'],  # وقت الصفقة
                'q': Decimal(data['q']),  # الكمية
                'm': data['m']   # هل المشتري هو صانع السوق؟
            }
            with lock:
                agg_trade_history[symbol].append(trade_data)

def on_message_hyperliquid(ws, message):
    data = json.loads(message)
    if data.get("channel") == "l2Book" and "data" in data:
        symbol = data["data"]["coin"]
        levels = data["data"]["levels"]
        if symbol in latest_data["hyperliquid"] and len(levels) >= 2 and levels[0] and levels[1]:
            with lock:
                latest_data["hyperliquid"][symbol] = {
                    'bid_price': levels[0][0]['px'], 'ask_price': levels[1][0]['px']
                }

# --- دوال WebSocket المساعدة (مع تعديل on_open_binance) ---

def on_error(ws, error): print(f"### WebSocket Error: {error} ###")
def on_close(ws, close_status_code, close_msg): print(f"### WebSocket Closed: {close_msg} ###")

def on_open_binance(ws):
    print(">>> Binance WebSocket Opened <<<")
    streams = []
    for s in SYMBOLS_BINANCE:
        streams.append(f"{s}@bookTicker")
        streams.append(f"{s}@aggTrade")
    ws.send(json.dumps({"method": "SUBSCRIBE", "params": streams, "id": 1}))

def on_open_hyperliquid(ws):
    print(">>> Hyperliquid WebSocket Opened <<<")
    for symbol in SYMBOLS_HYPERLIQUID:
        ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "l2Book", "coin": symbol}}))

def run_websocket(url, on_message, on_open):
    ws = websocket.WebSocketApp(url, on_message=on_message, on_open=on_open, on_error=on_error, on_close=on_close)
    ws.run_forever()

# --- وظيفة حساب كفة البيع والشراء (Imbalance) ---

def calculate_imbalance(symbol_binance):
    now_ms = int(time.time() * 1000)
    window_start_ms = now_ms - (AGG_TRADE_WINDOW_SECONDS * 1000)
    
    buy_volume = Decimal('0')
    sell_volume = Decimal('0')
    
    with lock:
        trades = list(agg_trade_history[symbol_binance])
        # إزالة البيانات القديمة
        while agg_trade_history[symbol_binance] and agg_trade_history[symbol_binance][0]['t'] < window_start_ms:
            agg_trade_history[symbol_binance].popleft()

    # حساب الحجم من البيانات الحديثة
    for trade in reversed(trades):
        if trade['t'] < window_start_ms:
            break # توقف لأن البيانات مرتبة زمنيًا
        
        if trade['m']: # is_buyer_maker is True -> Market Sell
            sell_volume += trade['q']
        else: # is_buyer_maker is False -> Market Buy
            buy_volume += trade['q']
            
    total_volume = buy_volume + sell_volume
    if total_volume == 0:
        return 0.0
        
    imbalance = (buy_volume - sell_volume) / total_volume
    return float(imbalance)

# --- وظيفة محاكاة التداول (مُعدَّلة بالكامل) ---

def trade_simulator():
    with open(DATA_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(DATA_HEADERS)

    while not stop_event.is_set():
        time.sleep(1) # تحقق كل ثانية
        closed_trades_to_log = []

        with lock:
            for symbol_upper in SYMBOLS:
                symbol_binance = f"{symbol_upper.lower()}usdt"
                binance_data = latest_data["binance"].get(symbol_binance, {})
                hl_data = latest_data["hyperliquid"].get(symbol_upper, {})

                if not all(k in binance_data for k in ['bid_price', 'ask_price']) or \
                   not all(k in hl_data for k in ['bid_price', 'ask_price']):
                    continue

                try:
                    b_bid, b_ask = float(binance_data['bid_price']), float(binance_data['ask_price'])
                    hl_bid, hl_ask = float(hl_data['bid_price']), float(hl_data['ask_price'])
                except (ValueError, TypeError):
                    continue

                b_mid = (b_bid + b_ask) / 2
                if b_mid == 0: continue
                current_spread = ((hl_ask - b_bid) / b_mid) * 100 # Spread based on actionable prices

                # --- منطق المراقبة والإغلاق (بدون تغيير) ---
                if symbol_upper in tracked_trades:
                    trade = tracked_trades[symbol_upper]
                    pnl_usd = 0
                    if trade['signal'] == 'LONG_BINANCE': # Bought HL, Sold Binance
                        pnl_usd = (b_bid - trade['entry_b_ask']) + (trade['entry_hl_bid'] - hl_ask)
                    elif trade['signal'] == 'SHORT_BINANCE': # Sold HL, Bought Binance
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
                        print(f"** {outcome} ** for {symbol_upper} ({trade['signal']}). PnL: ${pnl_usd:.4f} ({pnl_percent_of_gap:.1%})")

                        closed_trades_to_log.append([
                            symbol_upper, trade['signal'], outcome,
                            trade['entry_time'].isoformat(), exit_time.isoformat(), f"{duration:.2f}",
                            f"{trade['initial_gap_usd']:.4f}", f"{pnl_usd:.4f}", f"{pnl_percent_of_gap:.4f}",
                            f"{trade['entry_spread']:.4f}", f"{trade['entry_imbalance']:.4f}",
                            trade['entry_b_ask'], trade['entry_b_bid'], trade['entry_hl_ask'], trade['entry_hl_bid'],
                            b_ask, b_bid, hl_ask, hl_bid
                        ])
                        del tracked_trades[symbol_upper]

                # --- منطق البحث والفتح (مُعدَّل ليتضمن Imbalance) ---
                else:
                    imbalance_ratio = calculate_imbalance(symbol_binance)
                    signal = None

                    # فرصة بيع Binance / شراء Hyperliquid
                    if current_spread > SPREAD_THRESHOLD and imbalance_ratio < -IMBALANCE_THRESHOLD:
                        signal = 'SHORT_BINANCE' # Sell HL (at bid), Buy Binance (at ask)
                        
                    # فرصة شراء Binance / بيع Hyperliquid
                    elif current_spread < -SPREAD_THRESHOLD and imbalance_ratio > IMBALANCE_THRESHOLD:
                        signal = 'LONG_BINANCE' # Buy HL (at ask), Sell Binance (at bid)

                    if signal:
                        initial_gap_usd = abs(hl_ask - b_bid) if signal == 'SHORT_BINANCE' else abs(b_ask - hl_bid)
                        print(f"🔥 TRADE OPENED: {symbol_upper} ({signal}). Spread: {current_spread:.3f}%, Imbalance: {imbalance_ratio:.2%}, Gap: ${initial_gap_usd:.4f}")
                        tracked_trades[symbol_upper] = {
                            'signal': signal,
                            'entry_time': datetime.now(timezone.utc),
                            'entry_spread': current_spread,
                            'initial_gap_usd': initial_gap_usd,
                            'entry_imbalance': imbalance_ratio,
                            'entry_b_ask': b_ask, 'entry_b_bid': b_bid,
                            'entry_hl_ask': hl_ask, 'entry_hl_bid': hl_bid
                        }

        if closed_trades_to_log:
            with open(DATA_FILE, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(closed_trades_to_log)
            print(f"--- Logged {len(closed_trades_to_log)} closed trades to {DATA_FILE} ---")

# --- بدء التنفيذ ---
if __name__ == "__main__":
    print("--- Starting Trading Simulator with Imbalance & TP/SL Logic ---")
    binance_ws_url = "wss://fstream.binance.com/stream"
    hyperliquid_ws_url = "wss://api.hyperliquid.xyz/ws"

    threading.Thread(target=run_websocket, args=(binance_ws_url, on_message_binance, on_open_binance), daemon=True).start()
    threading.Thread(target=run_websocket, args=(hyperliquid_ws_url, on_message_hyperliquid, on_open_hyperliquid), daemon=True).start()
    
    # انتظر قليلاً لجمع بعض البيانات الأولية
    print("Waiting 15 seconds for initial data...")
    time.sleep(15)

    threading.Thread(target=trade_simulator, daemon=True).start()

    print(f"\nSimulator will run for {COLLECTION_DURATION_MINUTES} minutes...")
    try:
        end_time = time.time() + COLLECTION_DURATION_MINUTES * 60
        while time.time() < end_time:
            time.sleep(30)
            active_count = len(tracked_trades)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Remaining: {round((end_time - time.time())/60)} mins. Monitoring {active_count} trades.")

    except KeyboardInterrupt:
        print("\nUser interrupted simulation.")
    finally:
        print("\n--- Stopping Simulation ---")
        stop_event.set()
        time.sleep(2) # امنح فرصة لتسجيل آخر الصفقات
        print(f"Simulation log saved to {DATA_FILE}")
