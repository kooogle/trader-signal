#!/usr/bin/env python3
"""
期货信号系统 - 使用飞书应用发送消息
"""

import json
import os
import urllib.request
import urllib.parse
import random
from datetime import datetime
from typing import Dict, List, Optional

# ============== 配置区 ==============
# 飞书应用凭证 (你已经在OpenClaw配置过的)
APP_ID = "cli_a920fdf0183a9bd1"
APP_SECRET = "yjoGDTFVobSRRrEzLx81JhGl7NWvBrrG"

# 接收消息的用户ID
RECEIVER_ID = "ou_39144282ce2b237a8f95c7c9a30037bf"

# 交易品种
SYMBOLS = [
    {"code": "TA2605.CZCE", "name": "PTA", "exchange": "CZCE"},
    {"code": "OI2605.CZCE", "name": "菜籽油", "exchange": "CZCE"},
    {"code": "V2605.CZCE", "name": "PVC", "exchange": "CZCE"},
    {"code": "P2605.DCE", "name": "棕榈油", "exchange": "DCE"},
]

STATE_FILE = "signal_state.json"

# ============== 飞书应用API ==============

_feishu_access_token = None

def get_feishu_access_token() -> Optional[str]:
    global _feishu_access_token
    if _feishu_access_token:
        return _feishu_access_token
    
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": APP_ID, "app_secret": APP_SECRET}
    
    try:
        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=10) as response:
            result = json.loads(response.read().decode())
            if result.get('code') == 0:
                _feishu_access_token = result['tenant_access_token']
                return _feishu_access_token
    except Exception as e:
        print(f"获取token失败: {e}")
    return None

def send_feishu_message(open_id: str, message: str) -> bool:
    token = get_feishu_access_token()
    if not token:
        return False
    
    url = "https://open.feishu.cn/open-apis/im/v1/messages"
    params = {"receive_id_type": "open_id"}
    payload = {"receive_id": open_id, "msg_type": "text", "content": json.dumps({"text": message})}
    
    try:
        query_string = urllib.parse.urlencode(params)
        full_url = f"{url}?{query_string}"
        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(full_url, data=data, headers={'Content-Type': 'application/json', 'Authorization': f'Bearer {token}'})
        with urllib.request.urlopen(req, timeout=10) as response:
            result = json.loads(response.read().decode())
            if result.get('code') == 0:
                print("✅ 飞书消息发送成功")
                return True
            else:
                print(f"❌ 发送失败: {result.get('msg')}")
    except Exception as e:
        print(f"❌ 发送失败: {e}")
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

def get_kline_from_tqsdk(symbol: str, duration: int = 60, count: int = 100) -> Optional[Dict]:
    """天勤数据"""
    try:
        from tqsdk import TqApi, TqAuth
        api = TqApi(auth=TqAuth("", ""))
        try:
            klines = api.get_kline_serial(symbol, duration, count)
            prices = klines['close'].tolist()
            volumes = klines['volume'].tolist()
            api.close()
            return {"symbol": symbol, "prices": prices, "volumes": volumes, "source": "tqsdk"}
        except:
            api.close()
    except ImportError:
        print("天勤SDK未安装")
    except Exception as e:
        print(f"天勤错误: {e}")
    return None

# ============== 新浪数据 ==============

def get_kline_from_sina(symbol_name: str, count: int = 50) -> Optional[Dict]:
    symbol_map = {"TA2209": "TA0", "OI2209": "OI0", "V2209": "V0", "P2209": "P0"}
    sina_code = symbol_map.get(symbol_name, symbol_name[:-4] + "0")
    url = "https://stock2.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesDailyKLine"
    
    try:
        full_url = f"{url}?symbol={sina_code}"
        req = urllib.request.Request(full_url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=15) as response:
            result = json.loads(response.read().decode('utf-8'))
            if result:
                data = result[-count:] if len(result) > count else result
                prices = [float(d[2]) for d in data]
                volumes = [float(d[5]) for d in data]
                return {"symbol": symbol_name, "prices": prices, "volumes": volumes, "source": "sina"}
    except Exception as e:
        print(f"新浪错误: {e}")
    return None

def get_kline_data(symbol: str, count: int = 100) -> Dict:
    """只使用天勤数据源"""
    # 只使用天勤
    data = get_kline_from_tqsdk(symbol, 60, count)
    if data:
        print(f"✅ 天勤数据: {symbol}")
        return data
    
    # 天勤失败则报错退出
    print(f"❌ 天勤数据获取失败: {symbol}")
    exit(1)

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

# ============== 信号检测 (优化版) ==============

def calculate_atr(prices: List[float], period: int = 14) -> List[float]:
    """计算ATR（平均真实波幅）"""
    if len(prices) < period + 1:
        return [None] * len(prices)
    
    tr = []
    for i in range(1, len(prices)):
        high = prices[i]
        low = prices[i-1]
        prev_close = prices[i-1]
        
        h_l = high - low
        h_c = abs(high - prev_close)
        l_c = abs(low - prev_close)
        
        tr.append(max(h_l, h_c, l_c))
    
    atr = []
    for i in range(len(tr)):
        if i < period - 1:
            atr.append(None)
        else:
            avg = sum(tr[i-period+1:i+1]) / period
            atr.append(round(avg, 2))
    
    return atr

def detect_signals(prices: List[float], volumes: List[float]) -> Optional[Dict]:
    """优化的信号检测"""
    if len(prices) < 30:
        return None
    
    # 计算所有指标
    ma5 = calculate_ma(prices, 5)
    ma20 = calculate_ma(prices, 20)
    ma60 = calculate_ma(prices, 60)  # 新增60日均线判断趋势
    bb = calculate_bollinger(prices)
    rsi = calculate_rsi(prices)
    atr = calculate_atr(prices)  # 新增ATR
    
    price = prices[-1]
    ma5_v, ma20_v, ma60_v = ma5[-1], ma20[-1], ma60[-1] if len(ma60) > 0 and ma60[-1] else None
    bb_m = bb["middle"][-1]
    rsi_v = rsi[-1]
    atr_v = atr[-1] if atr and atr[-1] else 0
    
    if None in [ma5_v, ma20_v, bb_m, rsi_v]:
        return None
    
    # ====== 做多信号 ======
    # 条件1: 均线多头 + 布林中轨向上 + RSI在合理区间
    long_cond1 = (price > ma20_v and ma5_v > ma20_v and 
                   bb_m > bb["middle"][-2] and 
                   35 < rsi_v < 75)
    
    # 条件2: 突破布林上轨 + 明显放量 + ATR过滤假突破
    long_cond2 = (bb["upper"][-1] and price > bb["upper"][-1] and 
                  volumes[-1] > volumes[-2] * 1.3 and
                  (price - bb["upper"][-1]) > atr_v * 0.3)  # 突破幅度>0.3倍ATR
    
    # 条件3: 底背离 + RSI超卖
    price_recent_low = min(prices[-10:-1])
    valid_rsi = [r for r in rsi[-10:-1] if r]
    if valid_rsi:
        rsi_low = min(valid_rsi)
        long_cond3 = (price < price_recent_low * 1.01 and 
                      rsi_v < 35 and 
                      rsi_v < rsi_low * 0.95)
    else:
        long_cond3 = False
    
    # 综合判断做多：满足条件1或(条件2+ATR过滤)
    if long_cond1 or long_cond2 or long_cond3:
        reason = ""
        if long_cond1:
            reason = "均线多头+布林中轨向上+RSI"
        elif long_cond2:
            reason = f"突破布林上轨+放量(ATR:{atr_v})"
        elif long_cond3:
            reason = "底背离+RSI超卖"
        
        return {"type": "LONG", "reason": reason, "price": price}
    
    # ====== 做空信号 ======
    # 条件1: 均线空头 + 布林中轨向下 + RSI在合理区间
    short_cond1 = (price < ma20_v and ma5_v < ma20_v and 
                   bb_m < bb["middle"][-2] and 
                   25 < rsi_v < 65)
    
    # 条件2: 跌破布林下轨 + 明显放量 + ATR过滤假突破
    short_cond2 = (bb["lower"][-1] and price < bb["lower"][-1] and 
                   volumes[-1] > volumes[-2] * 1.3 and
                   (bb["lower"][-1] - price) > atr_v * 0.3)
    
    # 条件3: 顶背离 + RSI超买
    price_recent_high = max(prices[-10:-1])
    if valid_rsi:
        rsi_high = max(valid_rsi)
        short_cond3 = (price > price_recent_high * 0.99 and 
                       rsi_v > 65 and 
                       rsi_v > rsi_high * 1.05)
    else:
        short_cond3 = False
    
    # 综合判断做空
    if short_cond1 or short_cond2 or short_cond3:
        reason = ""
        if short_cond1:
            reason = "均线空头+布林中轨向下+RSI"
        elif short_cond2:
            reason = f"跌破布林下轨+放量(ATR:{atr_v})"
        elif short_cond3:
            reason = "顶背离+RSI超买"
        
        return {"type": "SHORT", "reason": reason, "price": price}
    
    return None

# ============== 主程序 ==============

def send_signal(symbol: Dict, signal: Dict):
    emoji = "🔴" if signal["type"] == "LONG" else "🟢"
    direction = "做多" if signal["type"] == "LONG" else "做空"
    
    message = f"""【期货信号】

{emoji} {symbol['name']} {direction}

原因: {signal['reason']}
价格: {signal['price']}
时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}"""
    
    send_feishu_message(RECEIVER_ID, message)

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
            print(f"📢 新信号: {current}")
            send_signal(symbol, signal)
        elif signal:
            print(f"📊 保持: {current}")
        else:
            print("无信号")
        
        current_state[key] = current
    
    save_state(current_state)
    print("\n完成!")

if __name__ == "__main__":
    main()
