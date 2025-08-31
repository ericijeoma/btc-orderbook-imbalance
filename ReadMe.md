# Crypto Multi-Exchange Collector & Signal Bot

A lightweight Python service that connects to multiple crypto exchanges to detect orderbook imbalance in Bitcoin
## Features
- Simultaneous WebSocket connections (Binance, Bybit, Deribit, BitMEX, Bitget and Hyperliquid)
- Real-time capture of price, size, and taker side
- Auto-reconnect and simple health monitoring
- Sends trade signals to a Telegram chat via bot API
- Easy deployment on Render

## Quick Start (Local)
```bash
git clone https://github.com/ericijeoma/btc-orderbook-imbalance.git
cd your-repo
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python main.py
