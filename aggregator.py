# ============================================
# BTC 5-MIN BLOCK AGGREGATOR - SINGLE INSTANCE FIX
# COMPLETE FIX FOR RENDER DUPLICATE ISSUE
# ============================================

import requests
import time
import csv
import os
import sys
import json
import threading
import fcntl
from datetime import datetime, timedelta
from collections import deque
import websocket

# Force stdout flush
sys.stdout.reconfigure(line_buffering=True)

# ============================================
# TELEGRAM SETUP
# ============================================
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

def send_telegram_message(message):
    """Send message to Telegram - Plain text"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            'chat_id': TELEGRAM_CHAT_ID,
            'text': message,
        }
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            print("✅ Telegram message sent")
            return True
        else:
            print(f"❌ Telegram error: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Telegram error: {e}")
        return False

def send_connection_message():
    """Send connection confirmation"""
    message = f"""
🚀 BTC AGGREGATOR CONNECTED!

✅ Status: Connected to Binance WebSocket
💰 Symbol: BTCUSDT
⏰ Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
📊 Mode: Real-time trades
⚡ Alert Rules: NET BTC > 50 or < -50

✅ Bot is running (Single Instance)!
    """
    send_telegram_message(message)

# ============================================
# HEALTH CHECK
# ============================================
from flask import Flask

app = Flask(__name__)

@app.route('/')
def health():
    return "OK - BTC Aggregator Running", 200

@app.route('/ping')
def ping():
    return "PONG", 200

def run_health_server():
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

# ============================================
# MAIN AGGREGATOR
# ============================================

class BTCAggregator:
    def __init__(self, symbol='BTCUSDT'):
        self.symbol = symbol
        self.symbol_lower = symbol.lower()
        
        # WebSocket
        self.ws_url = f"wss://stream.binance.com:9443/ws/{self.symbol_lower}@trade"
        self.ws = None
        self.ws_connected = False
        self.ws_thread = None
        
        # Trade tracking
        self.last_trade_id = None
        self.is_running = True
        self.last_alert_time = None
        self.alert_cooldown = 300
        self.block_count = 0
        self.total_trades_received = 0
        
        # Buffer
        self.trade_buffer = deque(maxlen=100000)
        self.buffer_lock = threading.Lock()
        self.block_lock = threading.Lock()
        
        # ⭐ Block completed flag
        self.block_completed = False
        self.reset_block()
        
        # CSV
        self.csv_file = f"5min_blocks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        self.init_csv()
        
        print(f"🚀 Initialized aggregator for {symbol}")

    def reset_block(self):
        """Reset block data"""
        self.block = {
            'start_time': None,
            'end_time': None,
            'buy_count': 0,
            'sell_count': 0,
            'buy_volume': 0.0,
            'sell_volume': 0.0,
            'total_volume': 0.0,
            'price_sum': 0.0,
            'price_count': 0,
            'min_price': float('inf'),
            'max_price': 0.0,
            'trade_count': 0
        }
        self.block_completed = False
        self.block_saved = False

    def init_csv(self):
        try:
            with open(self.csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'block_start', 'block_end',
                    'buy_count', 'sell_count', 'total_trades',
                    'buy_volume_btc', 'sell_volume_btc', 'total_volume_btc',
                    'avg_price', 'min_price', 'max_price',
                    'buy_percent', 'sell_percent',
                    'buy_sell_ratio', 'net_trades', 'net_volume'
                ])
            print(f"📁 CSV created: {self.csv_file}")
        except Exception as e:
            print(f"❌ CSV error: {e}")

    # ============================================
    # WEBSOCKET METHODS
    # ============================================
    
    def on_ws_message(self, ws, message):
        try:
            data = json.loads(message)
            
            if 'e' in data and data['e'] == 'trade':
                trade = {
                    'id': data['t'],
                    'price': float(data['p']),
                    'qty': float(data['q']),
                    'time': data['T'],
                    'isBuyerMaker': data['m']
                }
                
                with self.buffer_lock:
                    self.trade_buffer.append(trade)
                    self.total_trades_received += 1
                
                if self.last_trade_id is None or trade['id'] > self.last_trade_id:
                    self.last_trade_id = trade['id']
                
                with self.block_lock:
                    self.process_trade(trade)
                
        except Exception as e:
            print(f"⚠️ WS error: {e}")

    def on_ws_error(self, ws, error):
        print(f"❌ WS Error: {error}")
        self.ws_connected = False

    def on_ws_close(self, ws, close_status_code, close_msg):
        print(f"🔌 WS Closed")
        self.ws_connected = False
        time.sleep(5)
        if self.is_running:
            self.connect_websocket()

    def on_ws_open(self, ws):
        print("✅ WebSocket connected!")
        self.ws_connected = True

    def connect_websocket(self):
        try:
            print(f"🔌 Connecting to WebSocket...")
            
            self.ws = websocket.WebSocketApp(
                self.ws_url,
                on_open=self.on_ws_open,
                on_message=self.on_ws_message,
                on_error=self.on_ws_error,
                on_close=self.on_ws_close
            )
            
            self.ws_thread = threading.Thread(target=self.ws.run_forever)
            self.ws_thread.daemon = True
            self.ws_thread.start()
            
            time.sleep(3)
            
        except Exception as e:
            print(f"❌ WebSocket failed: {e}")
            self.ws_connected = False

    # ============================================
    # TRADE PROCESSING
    # ============================================
    
    def process_trade(self, trade):
        """Process trade - FIXED"""
        trade_time = datetime.fromtimestamp(trade['time'] / 1000)
        price = trade['price']
        qty = trade['qty']
        is_buy = not trade['isBuyerMaker']
        
        # If block is already completed, skip
        if self.block_completed:
            return
        
        # Set block start time
        if self.block['start_time'] is None:
            minute = (trade_time.minute // 5) * 5
            block_start = trade_time.replace(minute=minute, second=0, microsecond=0)
            self.block['start_time'] = block_start
            self.block['end_time'] = block_start + timedelta(minutes=5)
            print(f"⏰ Block: {block_start.strftime('%H:%M:%S')} → {self.block['end_time'].strftime('%H:%M:%S')}")
        
        # ⭐ CRITICAL: Check if trade is for next block
        if trade_time >= self.block['end_time']:
            # Block is complete
            self.save_and_print_block()
            # Add this trade to next block
            self.reset_block()
            # Re-process this trade
            self.process_trade(trade)
            return
        
        # Update counts
        if is_buy:
            self.block['buy_count'] += 1
            self.block['buy_volume'] += qty
        else:
            self.block['sell_count'] += 1
            self.block['sell_volume'] += qty
        
        self.block['price_sum'] += price
        self.block['price_count'] += 1
        self.block['min_price'] = min(self.block['min_price'], price)
        self.block['max_price'] = max(self.block['max_price'], price)
        self.block['total_volume'] += qty
        self.block['trade_count'] += 1

    def save_and_print_block(self):
        """Save block - SINGLE EXECUTION"""
        # ⭐ Prevent duplicate execution
        if self.block_completed:
            return
        
        if self.block['trade_count'] == 0:
            self.block_completed = True
            return
        
        # ⭐ Mark as completed immediately
        self.block_completed = True
        
        # Calculate metrics
        avg_price = self.block['price_sum'] / self.block['price_count'] if self.block['price_count'] > 0 else 0
        total_trades = self.block['buy_count'] + self.block['sell_count']
        buy_percent = (self.block['buy_count'] / total_trades * 100) if total_trades > 0 else 0
        sell_percent = 100 - buy_percent
        net_trades = self.block['buy_count'] - self.block['sell_count']
        net_volume = self.block['buy_volume'] - self.block['sell_volume']
        buy_sell_ratio = self.block['buy_count'] / self.block['sell_count'] if self.block['sell_count'] > 0 else 999
        self.block_count += 1
        
        # Print
        print("\n" + "="*80)
        print(f"📊 BLOCK #{self.block_count} COMPLETED")
        print("="*80)
        print(f"⏰ {self.block['start_time'].strftime('%H:%M:%S')} → {self.block['end_time'].strftime('%H:%M:%S')}")
        print(f"🟢 BUYS:  {self.block['buy_count']:>8} trades | {self.block['buy_volume']:>12.6f} BTC")
        print(f"🔴 SELLS: {self.block['sell_count']:>8} trades | {self.block['sell_volume']:>12.6f} BTC")
        print(f"📊 NET:   {net_trades:>+8} trades | {net_volume:>+12.6f} BTC")
        print(f"💰 AVG PRICE: ${avg_price:>12,.2f}")
        print(f"📈 BUY %: {buy_percent:.1f}% | SELL %: {sell_percent:.1f}%")
        
        # Signal
        if buy_percent > 60:
            signal = "🔥 STRONG BUY"
        elif buy_percent > 55:
            signal = "🟢 BUY"
        elif sell_percent > 60:
            signal = "❄️ STRONG SELL"
        elif sell_percent > 55:
            signal = "🔴 SELL"
        else:
            signal = "⚪ NEUTRAL"
        
        print(f"🎯 SIGNAL: {signal}")
        print(f"📊 Total trades: {self.total_trades_received}")
        print("="*80)
        sys.stdout.flush()
        
        # Alert
        if net_volume > 50:
            alert = f"""
🔴 EXTREME BUY ALERT!

5-Min Block: {self.block['start_time'].strftime('%H:%M:%S')} → {self.block['end_time'].strftime('%H:%M:%S')}
BUYS: {self.block['buy_count']} trades | {self.block['buy_volume']:.4f} BTC
SELLS: {self.block['sell_count']} trades | {self.block['sell_volume']:.4f} BTC
NET VOLUME: +{net_volume:.4f} BTC 🚀
SIGNAL: STRONG BUY!
            """
            send_telegram_message(alert)
        elif net_volume < -50:
            alert = f"""
🔴 EXTREME SELL ALERT!

5-Min Block: {self.block['start_time'].strftime('%H:%M:%S')} → {self.block['end_time'].strftime('%H:%M:%S')}
BUYS: {self.block['buy_count']} trades | {self.block['buy_volume']:.4f} BTC
SELLS: {self.block['sell_count']} trades | {self.block['sell_volume']:.4f} BTC
NET VOLUME: {net_volume:.4f} BTC 📉
SIGNAL: STRONG SELL!
            """
            send_telegram_message(alert)
        
        # Save CSV
        try:
            with open(self.csv_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    self.block['start_time'].strftime('%Y-%m-%d %H:%M:%S'),
                    self.block['end_time'].strftime('%Y-%m-%d %H:%M:%S'),
                    self.block['buy_count'],
                    self.block['sell_count'],
                    total_trades,
                    round(self.block['buy_volume'], 6),
                    round(self.block['sell_volume'], 6),
                    round(self.block['total_volume'], 6),
                    round(avg_price, 2),
                    round(self.block['min_price'], 2),
                    round(self.block['max_price'], 2),
                    round(buy_percent, 1),
                    round(sell_percent, 1),
                    round(buy_sell_ratio, 2),
                    net_trades,
                    round(net_volume, 6)
                ])
            print(f"💾 Saved: {self.csv_file}")
        except Exception as e:
            print(f"❌ CSV error: {e}")
        
        # Reset after save
        self.reset_block()

    # ============================================
    # RUN
    # ============================================
    
    def run(self):
        print("="*80)
        print(f"🚀 BTC AGGREGATOR - SINGLE INSTANCE")
        print(f"💰 Symbol: {self.symbol}")
        print(f"📁 CSV: {self.csv_file}")
        print("="*80 + "\n")
        sys.stdout.flush()
        
        self.connect_websocket()
        
        try:
            while self.is_running:
                time.sleep(1)
                if int(time.time()) % 30 == 0:
                    print(f"📊 Status: {self.total_trades_received} trades, WS: {'Connected' if self.ws_connected else 'Disconnected'}")
                    sys.stdout.flush()
        except KeyboardInterrupt:
            print("\n🛑 Stopping...")
            self.is_running = False
            if self.ws:
                self.ws.close()
        
        if self.block['trade_count'] > 0 and not self.block_completed:
            self.save_and_print_block()
        
        print(f"\n✅ Done! Total trades: {self.total_trades_received}")

# ============================================
# MAIN - WITH SINGLE INSTANCE LOCK
# ============================================

if __name__ == "__main__":
    # ⭐ SINGLE INSTANCE LOCK - Prevents duplicates
    try:
        lock_file = open("/tmp/aggregator.lock", "w")
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        print("🔒 Single instance locked")
    except:
        print("⚠️ Another instance running. Exiting.")
        sys.exit(0)
    
    print("="*80)
    print("🚀 STARTING BTC AGGREGATOR")
    print(f"⏰ Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    sys.stdout.flush()
    
    # Health check
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()
    print("🏥 Health check running")
    sys.stdout.flush()
    
    # Start aggregator
    aggregator = BTCAggregator('BTCUSDT')
    
    # Send Telegram message (once)
    time.sleep(2)
    send_connection_message()
    
    aggregator.run()