#!/usr/bin/env python3
"""
期货信号系统 - 天勤数据源
可部署到GitHub Actions
"""

import json
import sys
import sys
import os
import time
import urllib.request
import urllib.parse
import random
from datetime import datetime
from typing import Dict, List, Optional

# ============== 配置区 ==============
# 天勤账号密码 (推荐使用账号密码登录，更稳定)
TQ_ACCOUNT = "15572009997"  # 你的天勤账号
TQ_PASSWORD = "zp123789"  # 你的天勤密码

# 交易品种
# 天勤账号密码 (推荐使用账号密码登录，更稳定)
TQ_ACCOUNT = "15572009997"  # 你的天勤账号
TQ_PASSWORD = "zp123789"  # 你的天勤密码

# 交易品种
# 飞书 Webhook 地址 (获取方式：群机器人 -> 自定义机器人 -> Webhook)
FEISHU_WEBHOOK_URL = "YOUR_FEISHU_WEBHOOK_URL"

# 天勤 Token (可选，不填也能用游客权限)
TQ_TOKEN = ""

# 交易品种 (天勤期货合约代码)
SYMBOLS = [
    {"code": "TA2209.CZCE", "name": "PTA", "exchange": "CZCE"},
    {"code": "OI2209.CZCE", "name": "菜籽油", "exchange": "CZCE"},
    {"code": "V2209.CZCE", "name": "PVC", "exchange": "CZCE"},
    {"code": "P2209.DCE", "name": "棕榈油", "exchange": "DCE"},
]

# 状态文件
STATE_FILE = "signal_state.json"

# ============== 飞书推送 ==============

def send_feishu_message(message: str) -> bool:
    """推送到飞书"""
    if not FEISHU_WEBHOOK_URL or FEISHU_WEBHOOK_URL == "YOUR_FEISHU_WEBHOOK_URL":
        print("请先配置 FEISHU_WEBHOOK_URL")
        return False
    
    payload = {"msg_type": "text", "content": message}
    
    try:
        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(
            FEISHU_WEBHOOK_URL,
            data=data,
            headers={'Content-Type': 'application/json'}
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            result = json.loads(response.read().decode())
            if result.get('code') == 0:
                print("✅ 飞书推送成功")
                return True
            else:
                print(f"❌ 推送失败: {result.get('msg')}")
    except Exception as e:
        print(f"❌ 推送失败: {e}")
    return False

# ============== 状态管理 ==============

def load_state() -> Dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {}

def save_state(state: Dict):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)

def get_signal_key(symbol_code: str) -> str:
    return symbol_code.split('.')[0]

# ============== 天勤数据 ==============

def get_kline_from_tqsdk(symbol: str, duration: int = 300, count: int = 100) -> Optional[Dict]:
    """天勤数据 - 5分钟K线"""
    try:
        from tqsdk import TqApi, TqAuth
        
        # 使用账号密码登录，如果失败则用游客
        try:
            if TQ_ACCOUNT and TQ_PASSWORD:
                print(f"尝试使用账号登录天勤: {TQ_ACCOUNT}")
                api = TqApi(auth=TqAuth(TQ_ACCOUNT, TQ_PASSWORD))
            else:
                api = TqApi()
        except Exception as auth_error:
            print(f"账号登录失败，使用游客: {auth_error}")
            api = TqApi()
        
        try:
            klines = api.get_kline_serial(symbol, duration, count)
            prices = klines['close'].tolist()
            volumes = klines['volume'].tolist()
            api.close()
            
            return {
                "symbol": symbol,
                "prices": prices,
                "volumes": volumes,
                "source": "tqsdk"
            }
        except:
            api.close()
    except ImportError:
        print("天勤SDK未安装")
    except Exception as e:
        print(f"天勤错误: {e}")
    return None

# ============== 天勤数据 ==============

def get_kline_data(symbol: str, count: int = 100) -> Dict:
    """获取K线数据"""
    symbol_name = symbol.split('.')[0]
    
    # 优先天勤
    data = get_kline_from_tqsdk(symbol, 300, count)  # 5分钟 = 300秒
    if data:
        print(f"✅ 天勤数据: {symbol}")
        return data
    
    
    # 模拟
    print(f"⚠️ 模拟数据: {symbol}")
    prices = [5000 + random.uniform(-100, 100) for _ in range(count)]
    volumes = [random.randint(10000, 50000) for _ in range(count)]
    return {"symbol": symbol, "prices": prices, "volumes": volumes, "source": "mock"}

# ============== 技术指标 ==============

def calculate_ma(data: List[float], period: int) -> List[float]:
    result = []
    for i in range(len(data)):
        if i < period - 1:
            result.append(None)
        else:
            result.append(round(sum(data[i-period+1:i+1]) / period, 2))
    return result

def calculate_bollinger(data: List[float], period: int = 20) -> Dict:
    ma = calculate_ma(data, period)
    upper, lower = [], []
    for i in range(len(data)):
        if i < period - 1:
            upper.append(None)
            lower.append(None)
        else:
            slice_data = data[i-period+1:i+1]
            avg = sum(slice_data) / period
            std = (sum((x - avg) ** 2 for x in slice_data) / period) ** 0.5
            upper.append(round(avg + 2 * std, 2))
            lower.append(round(avg - 2 * std, 2))
    return {"middle": ma, "upper": upper, "lower": lower}

def calculate_rsi(data: List[float], period: int = 14) -> List[float]:
    result = [None]
    gains, losses = [], []
    for i in range(1, len(data)):
        change = data[i] - data[i-1]
        gains.append(change if change > 0 else 0)
        losses.append(abs(change) if change < 0 else 0)
    
    for i in range(1, len(data)):
        avg_gain = sum(gains[:i]) / i if i > 0 else 0
        avg_loss = sum(losses[:i]) / i if i > 0 else 0
        if avg_loss == 0:
            result.append(100)
        else:
            rsi = 100 - (100 / (1 + avg_gain / avg_loss))
            result.append(round(rsi, 2))
    return result

# ============== 信号检测 ==============

def detect_signals(prices: List[float], volumes: List[float]) -> Optional[Dict]:
    if len(prices) < 25:
        return None
    
    ma5, ma20 = calculate_ma(prices, 5), calculate_ma(prices, 20)
    bb = calculate_bollinger(prices)
    rsi = calculate_rsi(prices)
    
    price, ma5_v, ma20_v, bb_m, rsi_v = prices[-1], ma5[-1], ma20[-1], bb["middle"][-1], rsi[-1]
    
    if None in [ma5_v, ma20_v, bb_m, rsi_v]:
        return None
    
    # 多头
    if price > ma20_v and ma5_v > ma20_v and bb_m > bb["middle"][-2] and 40 < rsi_v < 70:
        return {"type": "LONG", "reason": "均线多头+布林中轨向上+RSI", "price": price}
    # 空头
    if price < ma20_v and ma5_v < ma20_v and bb_m < bb["middle"][-2] and 30 < rsi_v < 60:
        return {"type": "SHORT", "reason": "均线空头+布林中轨向下+RSI", "price": price}
    # 突破
    if bb["upper"][-1] and price > bb["upper"][-1] and volumes[-1] > volumes[-2] * 1.5:
        return {"type": "LONG", "reason": "突破布林上轨+放量", "price": price}
    if bb["lower"][-1] and price < bb["lower"][-1] and volumes[-1] > volumes[-2] * 1.5:
        return {"type": "SHORT", "reason": "跌破布林下轨+放量", "price": price}
    
    return None

# ============== 主程序 ==============

def main():
    print("="*50)
    print("期货信号系统")
    print("="*50)
    
    prev_state = load_state()
    current_state = {}
    
    for symbol in SYMBOLS:
        code = symbol["code"]
        name = symbol["name"]
        key = get_signal_key(code)
        
        print(f"\n分析: {name}")
        
        data = get_kline_data(code)
        signal = detect_signals(data["prices"], data["volumes"])
        
        current = signal["type"] if signal else "NONE"
        previous = prev_state.get(key, "NONE")
        
        if signal and current != previous:
            emoji = "🔴" if current == "LONG" else "🟢"
            direction = "做多" if current == "LONG" else "做空"
            
            msg = f"""【期货信号】{emoji} {name} {direction}

原因: {signal['reason']}
价格: {signal['price']}
时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}"""
            
            print(f"📢 新信号: {current}")
            send_feishu_message(msg)
        elif signal:
            print(f"📊 保持: {current}")
        else:
            print("无信号")
        
        current_state[key] = current
    
    save_state(current_state)
    print("\n完成!")

if __name__ == "__main__":
    main()
