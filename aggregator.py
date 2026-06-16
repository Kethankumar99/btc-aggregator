# ============================================
# 5-MINUTE BLOCK AGGREGATOR WITH TELEGRAM ALERTS
# FOR RENDER DEPLOYMENT
# ============================================

import requests
import time
import csv
import os
from datetime import datetime, timedelta

# ============================================
# TELEGRAM SETUP
# ============================================
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

def send_telegram_alert(message):
    """Send alert to Telegram"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Telegram credentials not set")
        return
    
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            'chat_id': TELEGRAM_CHAT_ID,
            'text': message,
            'parse_mode': 'HTML'
        }
        response = requests.post(url, json=payload, timeout=5)
        if response.status_code == 200:
            print("✅ Alert sent to Telegram")
    except Exception as e:
        print(f"❌ Telegram error: {e}")

# ============================================
# HEALTH CHECK FOR RENDER
# ============================================
from flask import Flask
import threading

app = Flask(__name__)

@app.route('/')
def health():
    return "OK", 200

@app.route('/ping')
def ping():
    return "PONG", 200

def run_health_server():
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)

# ============================================
# MAIN AGGREGATOR
# ============================================

class SilentFiveMinuteAggregator:
    def __init__(self, symbol='BTCUSDT'):
        self.symbol = symbol
        self.mirror = "https://data-api.binance.vision"
        self.last_trade_id = None
        self.is_running = True
        self.last_alert_time = None
        self.alert_cooldown = 300
        self.reset_block()
        self.csv_file = f"5min_blocks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        self.init_csv()

    def reset_block(self):
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
            'max_price': 0,
            'trade_count': 0
        }

    def init_csv(self):
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

    def get_new_trades(self):
        try:
            url = f"{self.mirror}/api/v3/trades"
            params = {'symbol': self.symbol, 'limit': 500}
            response = requests.get(url, params=params, timeout=5)
            if response.status_code == 200:
                trades = response.json()
                trades.sort(key=lambda x: x['id'])
                new_trades = []
                for trade in trades:
                    if self.last_trade_id is None or trade['id'] > self.last_trade_id:
                        new_trades.append(trade)
                if new_trades:
                    self.last_trade_id = new_trades[-1]['id']
                return new_trades
        except Exception as e:
            print(f"⚠️ Error: {e}")
        return []

    def add_trade(self, trade):
        trade_time = datetime.fromtimestamp(trade['time'] / 1000)
        price = float(trade['price'])
        qty = float(trade['qty'])
        is_buy = not trade['isBuyerMaker']

        if self.block['start_time'] is None:
            minute = (trade_time.minute // 5) * 5
            block_start = trade_time.replace(minute=minute, second=0, microsecond=0)
            self.block['start_time'] = block_start
            self.block['end_time'] = block_start + timedelta(minutes=5)

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

    def is_block_complete(self):
        if self.block['start_time'] is None:
            return False
        return datetime.now() >= self.block['end_time']

    def check_and_send_alert(self, net_volume, avg_price, buy_percent, sell_percent):
        if self.last_alert_time:
            if (datetime.now() - self.last_alert_time).seconds < self.alert_cooldown:
                return
        
        if net_volume > 50:
            self.last_alert_time = datetime.now()
            message = f"""
🔴 <b>EXTREME BUY ALERT!</b>

📊 <b>5-Minute Block Analysis</b>
⏰ {self.block['start_time'].strftime('%H:%M:%S')} → {self.block['end_time'].strftime('%H:%M:%S')}

🟢 <b>BUYS:</b> {self.block['buy_count']} trades | {self.block['buy_volume']:.4f} BTC
🔴 <b>SELLS:</b> {self.block['sell_count']} trades | {self.block['sell_volume']:.4f} BTC
📊 <b>NET VOLUME:</b> <b>+{net_volume:.4f} BTC</b> 🚀

💰 <b>AVG PRICE:</b> ${avg_price:,.2f}
📈 <b>BUY %:</b> {buy_percent:.1f}% | <b>SELL %:</b> {sell_percent:.1f}%

⚡ <b>SIGNAL:</b> <b>🔥 STRONG BUY - NET BTC > 50!</b>
            """
            send_telegram_alert(message)
            print("🔔 Alert sent: Net BTC > 50")
            
        elif net_volume < -50:
            self.last_alert_time = datetime.now()
            message = f"""
🔴 <b>EXTREME SELL ALERT!</b>

📊 <b>5-Minute Block Analysis</b>
⏰ {self.block['start_time'].strftime('%H:%M:%S')} → {self.block['end_time'].strftime('%H:%M:%S')}

🟢 <b>BUYS:</b> {self.block['buy_count']} trades | {self.block['buy_volume']:.4f} BTC
🔴 <b>SELLS:</b> {self.block['sell_count']} trades | {self.block['sell_volume']:.4f} BTC
📊 <b>NET VOLUME:</b> <b>{net_volume:.4f} BTC</b> 📉

💰 <b>AVG PRICE:</b> ${avg_price:,.2f}
📈 <b>BUY %:</b> {buy_percent:.1f}% | <b>SELL %:</b> {sell_percent:.1f}%

⚡ <b>SIGNAL:</b> <b>❄️ STRONG SELL - NET BTC < -50!</b>
            """
            send_telegram_alert(message)
            print("🔔 Alert sent: Net BTC < -50")

    def save_and_print_block(self):
        if self.block['trade_count'] == 0:
            return

        avg_price = self.block['price_sum'] / self.block['price_count']
        total_trades = self.block['buy_count'] + self.block['sell_count']
        buy_percent = (self.block['buy_count'] / total_trades * 100)
        sell_percent = (self.block['sell_count'] / total_trades * 100)
        buy_sell_ratio = self.block['buy_count'] / self.block['sell_count'] if self.block['sell_count'] > 0 else 999
        net_trades = self.block['buy_count'] - self.block['sell_count']
        net_volume = self.block['buy_volume'] - self.block['sell_volume']

        print("\n" + "="*80)
        print(f"📊 5-MINUTE BLOCK COMPLETED")
        print("="*80)
        print(f"⏰ {self.block['start_time'].strftime('%H:%M:%S')} → {self.block['end_time'].strftime('%H:%M:%S')}")
        print(f"🟢 BUYS:  {self.block['buy_count']:>6} trades | {self.block['buy_volume']:>10.6f} BTC")
        print(f"🔴 SELLS: {self.block['sell_count']:>6} trades | {self.block['sell_volume']:>10.6f} BTC")
        print(f"📊 NET:   {net_trades:>+6} trades | {net_volume:>+10.6f} BTC")
        print(f"💰 AVG PRICE: ${avg_price:>10,.2f} (${self.block['min_price']:,.2f} → ${self.block['max_price']:,.2f})")
        print(f"📊 RATIO: {buy_sell_ratio:.2f}x")

        if buy_percent > 60:
            signal = "🔥 STRONG BUY SIGNAL"
        elif buy_percent > 55:
            signal = "🟢 BUY SIGNAL"
        elif sell_percent > 60:
            signal = "❄️ STRONG SELL SIGNAL"
        elif sell_percent > 55:
            signal = "🔴 SELL SIGNAL"
        else:
            signal = "⚪ NEUTRAL"

        print(f"🎯 SIGNAL: {signal}")
        print("="*80)

        self.check_and_send_alert(net_volume, avg_price, buy_percent, sell_percent)

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

        print(f"💾 Saved: {self.csv_file}\n")
        self.reset_block()

    def run(self):
        print("="*80)
        print(f"🚀 5-MINUTE BLOCK AGGREGATOR")
        print(f"💰 Symbol: {self.symbol}")
        print(f"📁 CSV: {self.csv_file}")
        print(f"📱 Telegram alerts: {'Enabled' if TELEGRAM_TOKEN else 'Disabled'}")
        print(f"⚡ Alert threshold: NET BTC > 50 or < -50")
        print("="*80)
        print("🟢 Running...\n")

        while self.is_running:
            try:
                new_trades = self.get_new_trades()
                if new_trades:
                    for trade in new_trades:
                        self.add_trade(trade)
                    if self.is_block_complete() and self.block['trade_count'] > 0:
                        self.save_and_print_block()
                time.sleep(1)
            except KeyboardInterrupt:
                print("\n\n🛑 Stopping...")
                self.is_running = False
                break
            except Exception as e:
                print(f"❌ Error: {e}")
                time.sleep(1)

        if self.block['trade_count'] > 0:
            print("\n💾 Saving final block...")
            self.save_and_print_block()
        print(f"\n✅ Done! Data saved to: {self.csv_file}")

# ============================================
# RUN
# ============================================

if __name__ == "__main__":
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()
    print("🏥 Health check server running")
    
    aggregator = SilentFiveMinuteAggregator('BTCUSDT')
    aggregator.run()