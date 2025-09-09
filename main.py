# ####################################################################################
# الجزء التاسع: الحل النهائي مع تقسيم الاشتراكات (Batching)
# - يعالج مشكلة رفض Binance للاشتراكات الكبيرة عن طريق تقسيمها إلى دفعات.
# - قادر على التعامل مع قائمة الرموز الكاملة (159 رمزًا) بكفاءة.
# - تم تحسينه لضمان الاستقرار مع عدد كبير من الاشتراكات.
# ####################################################################################

import websocket
import json
import time
import threading
import csv
from datetime import datetime, timezone
import math

# --- الإعدادات الرئيسية ---

# --- القائمة الكاملة للرموز (159 رمزًا) ---
SYMBOLS = [
    "BTC", "ETH", "ATOM", "DYDX", "SOL", "AVAX", "BNB", "APE", "OP", "LTC", "ARB", "DOGE", "INJ", "SUI", "CRV", "LDO", "LINK", "STX", "CFX", "GMX", "SNX", "XRP", "BCH", "APT", "AAVE", "COMP", "MKR", "WLD", "FXS", "YGG", "TRX", "UNI", "SEI", "RUNE", "ZRO", "DOT", "BANANA", "TRB", "ARK", "BIGTIME", "KAS", "BLUR", "TIA", "BSV", "ADA", "TON", "MINA", "POLYX", "GAS", "PENDLE", "FET", "NEAR", "MEME", "ORDI", "NEO", "ZEN", "FIL", "PYTH", "SUSHI", "IMX", "GMT", "SUPER", "USTC", "JUP", "RSR", "GALA", "JTO", "ACE", "MAV", "WIF", "CAKE", "PEOPLE", "ENS", "ETC", "XAI", "MANTA", "UMA", "ONDO", "ALT", "ZETA", "DYM", "MAVIA", "W", "STRK", "TAO", "AR", "BOME", "ETHFI", "ENA", "TNSR", "SAGA", "MERL", "HBAR", "POPCAT", "OMNI", "EIGEN", "REZ", "NOT", "TURBO", "BRETT", "IO", "ZK", "MEW", "RENDER", "POL", "CELO", "HMSTR", "SCR", "NEIROETH", "GOAT", "MOODENG", "GRASS", "PNUT", "XLM", "CHILLGUY", "SAND", "IOTA", "ALGO", "HYPE", "ME", "MOVE", "VIRTUAL", "PENGU", "USUAL", "FARTCOIN", "AI16Z", "AIXBT", "ZEREBRO", "BIO", "GRIFFAIN", "SPX", "S", "MORPHO", "TRUMP", "MELANIA", "ANIME", "VINE", "VVV", "BERA", "TST", "LAYER", "IP", "OM", "KAITO", "NIL", "PAXG", "PROMPT", "BABY", "WCT", "HYPER", "ZORA", "INIT", "DOOD", "NXPC", "SOPH", "RESOLV", "SYRUP", "PUMP", "PROVE"
]
SYMBOLS_BINANCE = [f"{s.lower()}usdt" for s in SYMBOLS]
SYMBOLS_HYPERLIQUID = SYMBOLS

# --- إعدادات المحاكاة ---
SPREAD_THRESHOLD = 0.25 # تم رفعه قليلاً لتقليل "الضوضاء" مع العدد الكبير من العملات
TP_PERCENT_OF_GAP = 0.80
SL_PERCENT_OF_GAP = -0.50
AGGTRADE_DEPTH = 100

DATA_FILE = "trading_simulation_log_large_list.csv"
DATA_HEADERS = [
    "symbol", "signal_type", "outcome", "entry_time", "exit_time", "duration_seconds",
    "initial_gap_usd", "pnl_usd", "pnl_as_percent_of_gap", "entry_spread_percent", 
    "entry_keffa_imbalance_100t", "entry_binance_ask", "entry_binance_bid", 
    "entry_hl_ask", "entry_hl_bid", "exit_binance_ask", "exit_binance_bid", 
    "exit_hl_ask", "exit_hl_bid"
]

# --- المتغيرات الخاصة بتتبع الحالة ---
latest_data = {
    "binance_book": {symbol: {} for symbol in SYMBOLS_BINANCE},
    "binance_agg_trade": {symbol: [] for symbol in SYMBOLS_BINANCE},
    "hyperliquid": {symbol: {} for symbol in SYMBOLS_HYPERLIQUID}
}
tracked_trades = {}
lock = threading.Lock()

# --- دوال WebSocket ---
def on_message_binance(ws, message):
    payload = json.loads(message)
    if 'stream' in payload and 'data' in payload:
        data = payload['data']
        symbol = data.get('s', '').lower()
        if not symbol: return
        with lock:
            if data.get('e') == 'bookTicker' and symbol in latest_data["binance_book"]:
                latest_data["binance_book"][symbol] = {'bid_price': data['b'], 'ask_price': data['a']}
            elif data.get('e') == 'aggTrade' and symbol in latest_data["binance_agg_trade"]:
                is_market_buy = not data['m']
                volume = float(data['p']) * float(data['q'])
                trades = latest_data["binance_agg_trade"][symbol]
                trades.append((is_market_buy, volume))
                if len(trades) > AGGTRADE_DEPTH + 20:
                    latest_data["binance_agg_trade"][symbol] = trades[-(AGGTRADE_DEPTH + 20):]

def on_message_hyperliquid(ws, message):
    data = json.loads(message)
    if data.get("channel") == "l2Book" and "data" in data:
        symbol = data["data"]["coin"]
        levels = data["data"]["levels"]
        if symbol in latest_data["hyperliquid"] and len(levels) >= 2 and levels[0] and levels[1]:
            with lock:
                latest_data["hyperliquid"][symbol] = {'bid_price': levels[0][0]['px'], 'ask_price': levels[1][0]['px']}

def on_error(ws, error): print(f"### WEBSOCKET ERROR: {error} ###")
def on_close(ws, close_status_code, close_msg): print(f"### WEBSOCKET CLOSED: {close_msg} ###")

# --- دالة الاشتراك المعدلة لـ Binance (مع تقسيم الدفعات) ---
def on_open_binance(ws):
    print("✅ >>> Binance WebSocket Connection Successful <<<")
    
    all_streams = [f"{s}@bookTicker" for s in SYMBOLS_BINANCE] + [f"{s}@aggTrade" for s in SYMBOLS_BINANCE]
    BATCH_SIZE = 50  # إرسال اشتراك لكل 50 ستريم
    
    num_batches = math.ceil(len(all_streams) / BATCH_SIZE)
    print(f"--> Will send {len(all_streams)} streams to Binance in {num_batches} batches...")

    for i in range(num_batches):
        start_index = i * BATCH_SIZE
        end_index = start_index + BATCH_SIZE
        batch = all_streams[start_index:end_index]
        
        subscription_payload = json.dumps({"method": "SUBSCRIBE", "params": batch, "id": i + 1})
        print(f"    Sending batch {i+1}/{num_batches} ({len(batch)} streams)...")
        ws.send(subscription_payload)
        time.sleep(1) # إضافة تأخير بسيط بين الطلبات لتجنب الضغط على الخادم

def on_open_hyperliquid(ws):
    print("✅ >>> Hyperliquid WebSocket Connection Successful <<<")
    print(f"--> Sending Hyperliquid subscriptions for {len(SYMBOLS)} symbols...")
    for symbol in SYMBOLS_HYPERLIQUID:
        ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "l2Book", "coin": symbol}}))
        # لا حاجة للتأخير هنا لأن كل اشتراك هو رسالة منفصلة بطبيعته

def run_websocket(url, on_message, on_open):
    ws = websocket.WebSocketApp(url, on_message=on_message, on_open=on_open, on_error=on_error, on_close=on_close)
    ws.run_forever(ping_interval=60, ping_timeout=10) # إضافة ping للحفاظ على الاتصال

# --- دالة حساب "الكفة" (لا تغيير) ---
def calculate_keffa(symbol_binance):
    with lock:
        recent_trades = latest_data["binance_agg_trade"][symbol_binance][-AGGTRADE_DEPTH:]
        if not recent_trades: return 0.0
        buy_volume = sum(volume for is_buy, volume in recent_trades if is_buy)
        sell_volume = sum(volume for is_buy, volume in recent_trades if not is_buy)
        total_volume = buy_volume + sell_volume
        if total_volume == 0: return 0.0
        return (buy_volume - sell_volume) / total_volume

# --- وظيفة محاكاة التداول ---
def trade_simulator():
    with open(DATA_FILE, 'w', newline='', encoding='utf-8') as f: writer.writerow(DATA_HEADERS)
    last_diag_print = time.time()
    
    while not stop_logging_event.is_set():
        time.sleep(1)
        closed_trades_to_log = []
        processed_symbols_count = 0
        
        with lock:
            for symbol_upper in SYMBOLS:
                symbol_binance = f"{symbol_upper.lower()}usdt"
                binance_data = latest_data["binance_book"].get(symbol_binance, {})
                hl_data = latest_data["hyperliquid"].get(symbol_upper, {})

                if not all(k in binance_data for k in ['bid_price', 'ask_price']) or not all(k in hl_data for k in ['bid_price', 'ask_price']):
                    continue
                processed_symbols_count += 1
                
                try:
                    b_bid, b_ask = float(binance_data['bid_price']), float(binance_data['ask_price'])
                    hl_bid, hl_ask = float(hl_data['bid_price']), float(hl_data['ask_price'])
                except (ValueError, TypeError): continue

                if b_ask == 0: continue
                b_mid = (b_bid + b_ask) / 2
                hl_mid = (hl_bid + hl_ask) / 2
                current_spread = ((hl_mid - b_mid) / b_mid) * 100 if b_mid != 0 else 0

                if symbol_upper in tracked_trades:
                    # منطق إغلاق الصفقات (مختصر)
                    trade = tracked_trades[symbol_upper]
                    pnl_usd = (b_bid - trade['entry_b_ask']) + (trade['entry_hl_bid'] - hl_ask) if trade['signal'] == 'LONG_BINANCE' else (trade['entry_b_bid'] - b_ask) + (hl_bid - trade['entry_hl_ask'])
                    pnl_percent_of_gap = pnl_usd / trade['initial_gap_usd'] if trade['initial_gap_usd'] != 0 else 0
                    outcome = "TAKE_PROFIT" if pnl_percent_of_gap >= TP_PERCENT_OF_GAP else "STOP_LOSS" if pnl_percent_of_gap <= SL_PERCENT_OF_GAP else None
                    if outcome:
                        exit_time = datetime.now(timezone.utc)
                        duration = (exit_time - trade['entry_time']).total_seconds()
                        print(f"✅ ** {outcome} ** {symbol_upper} PnL: ${pnl_usd:.4f}.")
                        # (إضافة إلى قائمة التسجيل... الكود الكامل كما في السابق)
                        closed_trades_to_log.append([
                             symbol_upper, trade['signal'], outcome, trade['entry_time'].isoformat(), exit_time.isoformat(), f"{duration:.2f}",
                             f"{trade['initial_gap_usd']:.4f}", f"{pnl_usd:.4f}", f"{pnl_percent_of_gap:.4f}",
                             f"{trade['entry_spread']:.4f}", f"{trade['entry_keffa_imbalance']:.4f}",
                             trade['entry_b_ask'], trade['entry_b_bid'], trade['entry_hl_ask'], trade['entry_hl_bid'],
                             b_ask, b_bid, hl_ask, hl_bid
                        ])
                        del tracked_trades[symbol_upper]
                else:
                    # منطق فتح الصفقات
                    if abs(current_spread) > SPREAD_THRESHOLD:
                        signal = 'LONG_BINANCE' if current_spread > 0 else 'SHORT_BINANCE'
                        print(f"🔥 TRADE OPENED: {symbol_upper} ({signal}). Spread: {current_spread:.3f}%.")
                        # (فتح الصفقة... الكود الكامل كما في السابق)
                        tracked_trades[symbol_upper] = {
                            'signal': signal, 'entry_time': datetime.now(timezone.utc), 'entry_spread': current_spread,
                            'initial_gap_usd': abs(hl_mid - b_mid), 'entry_keffa_imbalance': calculate_keffa(symbol_binance),
                            'entry_b_ask': b_ask, 'entry_b_bid': b_bid, 'entry_hl_ask': hl_ask, 'entry_hl_bid': hl_bid
                        }

        if closed_trades_to_log:
            with open(DATA_FILE, 'a', newline='', encoding='utf-8') as f:
                csv.writer(f).writerows(closed_trades_to_log)
            print(f"--- 💾 Logged {len(closed_trades_to_log)} closed trades ---")
        
        if time.time() - last_diag_print > 15: # زيادة الفاصل الزمني للتشخيص
            ts = datetime.now().strftime('%H:%M:%S')
            btc_binance_data = latest_data["binance_book"].get("btcusdt", "No Data")
            btc_hl_data = latest_data["hyperliquid"].get("BTC", "No Data")
            print(f"[{ts}] 🩺 Heartbeat: Processing {processed_symbols_count}/{len(SYMBOLS)} symbols. Active: {len(tracked_trades)}. | BTC Binance: {btc_binance_data}")
            last_diag_print = time.time()

# --- بدء التشغيل ---
if __name__ == "__main__":
    print(f"--- Starting Final Simulator with Subscription Batching ---")
    print(f"Monitoring {len(SYMBOLS)} symbols.")
    stop_logging_event = threading.Event()
    
    threading.Thread(target=run_websocket, args=("wss://fstream.binance.com/stream", on_message_binance, on_open_binance), daemon=True).start()
    threading.Thread(target=run_websocket, args=("wss://api.hyperliquid.xyz/ws", on_message_hyperliquid, on_open_hyperliquid), daemon=True).start()
    
    print("Waiting 10 seconds for all subscriptions to be sent...")
    time.sleep(10)
    
    threading.Thread(target=trade_simulator, daemon=True).start()
    
    COLLECTION_DURATION_MINUTES = 10
    print(f"\nSimulator will run for {COLLECTION_DURATION_MINUTES} minutes...")
    time.sleep(COLLECTION_DURATION_MINUTES * 60)
    
    print("\n--- Stopping Simulation ---")
    stop_logging_event.set()
    time.sleep(1)
    print(f"Simulation log saved to {DATA_FILE}")
