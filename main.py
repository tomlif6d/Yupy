# ####################################################################################
# الجزء السابع: نسخة مصححة ومحسّنة مع نظام تشخيصي فعال
# - تم تنقيح قائمة الرموز للتركيز على العملات النشطة لضمان تدفق البيانات.
# - تم تعديل عتبة الفجوة السعرية لتكون أكثر واقعية.
# - تمت إضافة طباعة دورية للفجوات السعرية الحالية لتشخيص عمل السكربت.
# ####################################################################################

import websocket
import json
import time
import threading
import csv
from datetime import datetime, timezone

# --- الإعدادات الرئيسية ---

# --- قائمة الرموز (تم تقليصها للتركيز على العملات النشطة والموجودة على المنصتين) ---
# القائمة الطويلة الأصلية قد تحتوي على عملات غير موجودة أو غير نشطة، مما يمنع ظهور أي صفقات.
SYMBOLS = [
    "BTC", "ETH", "ATOM", "DYDX", "SOL", "AVAX", "BNB", "APE", "OP", "LTC", "ARB", "DOGE", "INJ", "SUI", "CRV", "LDO", "LINK", "STX", "CFX", "GMX", "SNX", "XRP", "BCH", "APT", "AAVE", "COMP", "MKR", "WLD", "FXS", "YGG", "TRX", "UNI", "SEI", "RUNE", "ZRO", "DOT", "BANANA", "TRB", "ARK", "BIGTIME", "KAS", "BLUR", "TIA", "BSV", "ADA", "TON", "MINA", "POLYX", "GAS", "PENDLE", "FET", "NEAR", "MEME", "ORDI", "NEO", "ZEN", "FIL", "PYTH", "SUSHI", "IMX", "GMT", "SUPER", "USTC", "JUP", "RSR", "GALA", "JTO", "ACE", "MAV", "WIF", "CAKE", "PEOPLE", "ENS", "ETC", "XAI", "MANTA", "UMA", "ONDO", "ALT", "ZETA", "DYM", "MAVIA", "W", "STRK", "TAO", "AR", "BOME", "ETHFI", "ENA", "TNSR", "SAGA", "MERL", "HBAR", "POPCAT", "OMNI", "EIGEN", "REZ", "NOT", "TURBO", "BRETT", "IO", "ZK", "MEW", "RENDER", "POL", "CELO", "HMSTR", "SCR", "NEIROETH", "GOAT", "MOODENG", "GRASS", "PNUT", "XLM", "CHILLGUY", "SAND", "IOTA", "ALGO", "HYPE", "ME", "MOVE", "VIRTUAL", "PENGU", "USUAL", "FARTCOIN", "AI16Z", "AIXBT", "ZEREBRO", "BIO", "GRIFFAIN", "SPX", "S", "MORPHO", "TRUMP", "MELANIA", "ANIME", "VINE", "VVV", "BERA", "TST", "LAYER", "IP", "OM", "KAITO", "NIL", "PAXG", "PROMPT", "BABY", "WCT", "HYPER", "ZORA", "INIT", "DOOD", "NXPC", "SOPH", "RESOLV", "SYRUP", "PUMP", "PROVE"
]

SYMBOLS_BINANCE = [f"{s.lower()}usdt" for s in SYMBOLS]
SYMBOLS_HYPERLIQUID = SYMBOLS

# --- إعدادات المحاكاة (تم تعديلها لتكون أكثر واقعية) ---
SPREAD_THRESHOLD = 0.20      # تم تقليل العتبة إلى 0.20% لزيادة فرصة اكتشاف الصفقات
TP_PERCENT_OF_GAP = 0.80     # هدف الربح: 80% من الفجوة الأولية
SL_PERCENT_OF_GAP = -0.50    # وقف الخسارة: 50% خسارة من الفجوة الأولية
AGGTRADE_DEPTH = 100         # عمق بيانات التداول لحساب الكفة (آخر 100 صفقة)

DATA_FILE = "trading_simulation_log_corrected.csv"
DATA_HEADERS = [
    "symbol", "signal_type", "outcome",
    "entry_time", "exit_time", "duration_seconds",
    "initial_gap_usd", "pnl_usd", "pnl_as_percent_of_gap",
    "entry_spread_percent", "entry_keffa_imbalance_100t",
    "entry_binance_ask", "entry_binance_bid", "entry_hl_ask", "entry_hl_bid",
    "exit_binance_ask", "exit_binance_bid", "exit_hl_ask", "exit_hl_bid"
]

# --- المتغيرات الخاصة بتتبع الحالة ---
latest_data = {
    "binance_book": {symbol: {} for symbol in SYMBOLS_BINANCE},
    "binance_agg_trade": {symbol: [] for symbol in SYMBOLS_BINANCE},
    "hyperliquid": {symbol: {} for symbol in SYMBOLS_HYPERLIQUID}
}
tracked_trades = {}
lock = threading.Lock()

# --- دوال WebSocket (لا تغيير هنا) ---
def on_message_binance(ws, message):
    payload = json.loads(message)
    if 'stream' in payload and 'data' in payload:
        data = payload['data']
        symbol = data['s'].lower()
        with lock:
            if data.get('e') == 'bookTicker' and symbol in latest_data["binance_book"]:
                latest_data["binance_book"][symbol] = {'bid_price': data['b'], 'ask_price': data['a']}
            elif data.get('e') == 'aggTrade' and symbol in latest_data["binance_agg_trade"]:
                is_market_buy = not data['m']
                volume = float(data['p']) * float(data['q'])
                trades = latest_data["binance_agg_trade"][symbol]
                trades.append((is_market_buy, volume))
                # الحفاظ على حجم القائمة لمنع استهلاك الذاكرة
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
def run_websocket(url, on_message, on_open):
    ws = websocket.WebSocketApp(url, on_message=on_message, on_open=on_open, on_error=on_error, on_close=on_close)
    ws.run_forever()

# --- دالة حساب "الكفة" ---
def calculate_keffa(symbol_binance):
    with lock:
        recent_trades = latest_data["binance_agg_trade"][symbol_binance][-AGGTRADE_DEPTH:]
        if not recent_trades: return 0.0
        buy_volume = sum(volume for is_buy, volume in recent_trades if is_buy)
        sell_volume = sum(volume for is_buy, volume in recent_trades if not is_buy)
        total_volume = buy_volume + sell_volume
        if total_volume == 0: return 0.0
        imbalance = (buy_volume - sell_volume) / total_volume
        return imbalance

# --- وظيفة محاكاة التداول مع نظام التشخيص ---
def trade_simulator():
    with open(DATA_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(DATA_HEADERS)

    last_diag_print = time.time()
    
    while not stop_logging_event.is_set():
        time.sleep(1) # دورة المعالجة كل ثانية
        closed_trades_to_log = []
        processed_symbols_count = 0
        
        # --- إضافة متغيرات للتشخيص ---
        diag_spreads = {}

        with lock:
            for symbol_upper in SYMBOLS:
                symbol_binance = f"{symbol_upper.lower()}usdt"
                binance_data = latest_data["binance_book"].get(symbol_binance, {})
                hl_data = latest_data["hyperliquid"].get(symbol_upper, {})

                if not all(k in binance_data for k in ['bid_price', 'ask_price']) or not all(k in hl_data for k in ['bid_price', 'ask_price']):
                    continue # تخطي الرمز إذا كانت بياناته غير مكتملة من إحدى المنصتين
                
                processed_symbols_count += 1
                
                try:
                    b_bid, b_ask = float(binance_data['bid_price']), float(binance_data['ask_price'])
                    hl_bid, hl_ask = float(hl_data['bid_price']), float(hl_data['ask_price'])
                except (ValueError, TypeError): continue

                if b_ask == 0 or b_bid == 0: continue
                
                b_mid = (b_bid + b_ask) / 2
                hl_mid = (hl_bid + hl_ask) / 2
                
                if b_mid == 0: continue
                current_spread = ((hl_mid - b_mid) / b_mid) * 100

                # تخزين الفجوات للطباعة التشخيصية
                if symbol_upper in ["BTC", "ETH", "SOL"]:
                    diag_spreads[symbol_upper] = current_spread
                
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
                    if pnl_percent_of_gap >= TP_PERCENT_OF_GAP: outcome = "TAKE_PROFIT"
                    elif pnl_percent_of_gap <= SL_PERCENT_OF_GAP: outcome = "STOP_LOSS"

                    if outcome:
                        exit_time = datetime.now(timezone.utc)
                        duration = (exit_time - trade['entry_time']).total_seconds()
                        print(f"✅ ** {outcome} ** for {symbol_upper} ({trade['signal']}). PnL: ${pnl_usd:.4f}.")
                        closed_trades_to_log.append([
                            symbol_upper, trade['signal'], outcome, trade['entry_time'].isoformat(), exit_time.isoformat(), f"{duration:.2f}",
                            f"{trade['initial_gap_usd']:.4f}", f"{pnl_usd:.4f}", f"{pnl_percent_of_gap:.4f}",
                            f"{trade['entry_spread']:.4f}", f"{trade['entry_keffa_imbalance']:.4f}",
                            trade['entry_b_ask'], trade['entry_b_bid'], trade['entry_hl_ask'], trade['entry_hl_bid'],
                            b_ask, b_bid, hl_ask, hl_bid
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
                        keffa_imbalance_at_open = calculate_keffa(symbol_binance)
                        initial_gap_usd = abs(hl_mid - b_mid)
                        print(f"🔥 TRADE OPENED: {symbol_upper} ({signal}). Spread: {current_spread:.3f}%. (Keffa 100-trade: {keffa_imbalance_at_open:.2f})")
                        tracked_trades[symbol_upper] = {
                            'signal': signal, 'entry_time': datetime.now(timezone.utc),
                            'entry_spread': current_spread, 'initial_gap_usd': initial_gap_usd,
                            'entry_keffa_imbalance': keffa_imbalance_at_open,
                            'entry_b_ask': b_ask, 'entry_b_bid': b_bid,
                            'entry_hl_ask': hl_ask, 'entry_hl_bid': hl_bid
                        }

        # --- كتابة الصفقات المغلقة في الملف ---
        if closed_trades_to_log:
            with open(DATA_FILE, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(closed_trades_to_log)
            print(f"--- 💾 Logged {len(closed_trades_to_log)} closed trades to {DATA_FILE} ---")
        
        # --- طباعة تقرير التشخيص كل 10 ثوانٍ ---
        if time.time() - last_diag_print > 10:
            ts = datetime.now().strftime('%H:%M:%S')
            spread_info = ", ".join([f"{coin}: {spread:.4f}%" for coin, spread in diag_spreads.items()])
            print(f"[{ts}] 🩺 Heartbeat: Processing {processed_symbols_count}/{len(SYMBOLS)} symbols. Active trades: {len(tracked_trades)}. | Spreads -> {spread_info}")
            last_diag_print = time.time()

# --- بدء عملية جمع البيانات ---
if __name__ == "__main__":
    print(f"--- Starting Corrected Trading Simulator ---")
    print(f"Monitoring {len(SYMBOLS)} symbols. Trade Trigger Spread: {SPREAD_THRESHOLD}%.")
    binance_ws_url = "wss://fstream.binance.com/stream"
    hyperliquid_ws_url = "wss://api.hyperliquid.xyz/ws"
    stop_logging_event = threading.Event()
    
    threading.Thread(target=run_websocket, args=(binance_ws_url, on_message_binance, on_open_binance), daemon=True).start()
    threading.Thread(target=run_websocket, args=(hyperliquid_ws_url, on_message_hyperliquid, on_open_hyperliquid), daemon=True).start()
    
    # انتظر قليلاً حتى يتم إنشاء الاتصالات وتدفق البيانات الأولية
    print("Waiting 5 seconds for WebSocket connections to establish...")
    time.sleep(5)
    
    trade_simulator_thread = threading.Thread(target=trade_simulator)
    trade_simulator_thread.start()
    
    COLLECTION_DURATION_MINUTES = 10
    print(f"\nSimulator will run for {COLLECTION_DURATION_MINUTES} minutes...")
    
    trade_simulator_thread.join(timeout=COLLECTION_DURATION_MINUTES * 60)
    
    print("\n--- Stopping Simulation ---")
    stop_logging_event.set()
    
    # انتظر ثانية إضافية للتأكد من إغلاق أي صفقات متبقية وكتابتها
    time.sleep(1)
    
    print(f"Simulation log saved to {DATA_FILE}")
